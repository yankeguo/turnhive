// Package controller holds the HTTP handlers and routing for turnhive.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
	"github.com/yankeguo/turnhive/registry"
	"github.com/yankeguo/turnhive/storage"
)

// headerForwarded marks a request that has already been forwarded by
// another node, preventing forwarding loops.
const headerForwarded = "X-Turnhive-Forwarded"

// lookupTimeout bounds etcd ownership lookups on the request path.
const lookupTimeout = 5 * time.Second

// maxJSONBody bounds the request body of the JSON-decoding endpoints.
const maxJSONBody = 4 << 20

// allocateTimeout bounds sandbox allocation; ironhive may block up to 30s
// server-side waiting for a standby pod.
const allocateTimeout = 40 * time.Second

// newSessionID generates a session id: sess-<lowercase ULID>, the same
// scheme as ironhive's sandbox names.
func newSessionID() string {
	return "sess-" + strings.ToLower(ulid.Make().String())
}

// newTurnID generates a turn id, following the session id scheme.
func newTurnID() string {
	return "turn-" + strings.ToLower(ulid.Make().String())
}

// turnTimeout bounds a single agent turn.
const turnTimeout = time.Hour

// skillURLTTL is the validity of the presigned URLs a sandbox uses to
// download skill tarballs.
const skillURLTTL = 15 * time.Minute

// skillsRoot is the sandbox directory, relative to the sandbox's working
// directory, where skill tarballs are installed; it is read-only for the
// agent. turnhive deliberately does not assume the sandbox's working
// directory: all tool paths are relative to it.
const skillsRoot = ".agents/skills"

// Controller holds the dependencies shared by all HTTP handlers.
type Controller struct {
	nodeID   string
	registry *registry.Registry
	ironhive *ironhive.Client
	store    *storage.Store
	// sandboxLease is the lease duration requested when allocating a
	// session sandbox.
	sandboxLease time.Duration
	// idleTimeout bounds session inactivity before the idle reaper
	// releases the session's sandbox (the session lives on).
	idleTimeout time.Duration

	// sessions holds the sessions owned by this node, keyed by session ID.
	sessions sync.Map
	// proxies caches one reverse proxy per owner node address.
	proxies sync.Map

	sweeperStop chan struct{}
	sweeperDone sync.WaitGroup
}

// New creates a Controller for the given node and starts the idle
// reaper.
func New(nodeID string, reg *registry.Registry, ih *ironhive.Client, store *storage.Store, sandboxLease, idleTimeout time.Duration) *Controller {
	c := &Controller{
		nodeID: nodeID, registry: reg, ironhive: ih, store: store,
		sandboxLease: sandboxLease, idleTimeout: idleTimeout,
		sweeperStop: make(chan struct{}),
	}
	c.sweeperDone.Add(1)
	go c.runSweeper()
	return c
}

// runSweeper periodically releases the sandboxes of sessions that have
// been idle past idleTimeout, until Close.
func (c *Controller) runSweeper() {
	defer c.sweeperDone.Done()
	interval := c.idleTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.sweeperStop:
			return
		case <-ticker.C:
			c.reapIdleSandboxes()
		}
	}
}

// reapIdleSandboxes releases the sandbox of every session that has been
// inactive for at least idleTimeout. The session record, its event hub
// and its persisted files survive; the sandbox is rebuilt on the next
// message (see ensureSandbox).
func (c *Controller) reapIdleSandboxes() {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		sb, stop := sess.takeSandboxIfIdle(c.idleTimeout)
		if sb == nil {
			return true
		}
		if stop != nil {
			stop()
		}
		log.Printf("session %s idle past %s, releasing sandbox %s", sess.ID, c.idleTimeout, sb.Name)
		releaseSandbox(sb)
		return true
	})
}

// RegisterRoutes registers all routes on the given mux using the Go 1.22+
// method-and-pattern routing syntax.
func (c *Controller) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", c.handleHealthz)
	mux.HandleFunc("POST /v1/sessions", c.handleCreateSession)
	mux.HandleFunc("POST /v1/sessions/{id}/messages", c.handleCreateMessage)
	mux.HandleFunc("GET /v1/sessions/{id}/events", c.handleSessionEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/tool_results", c.handleCreateToolResult)
	mux.HandleFunc("DELETE /v1/sessions/{id}", c.handleDeleteSession)
}

func (c *Controller) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleCreateSession creates a session owned by this node and publishes
// its ownership record so any node in the cluster can route to it.
func (c *Controller) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req CreateSessionRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	id := newSessionID()
	sess := &Session{ID: id, Spec: req, hub: newEventHub()}
	sess.touch()

	// Build the sandbox before registering anything, so a failure leaves
	// no state behind.
	allocCtx, allocCancel := context.WithTimeout(r.Context(), allocateTimeout)
	defer allocCancel()
	if err := c.ensureSandbox(allocCtx, sess); err != nil {
		log.Printf("prepare sandbox for session %s: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to prepare sandbox"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.RegisterSession(ctx, id); err != nil {
		log.Printf("register session %s: %v", id, err)
		if sb, stop := sess.takeSandbox(); sb != nil {
			if stop != nil {
				stop()
			}
			releaseSandbox(sb)
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to register session"})
		return
	}
	c.sessions.Store(id, sess)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// skillRefsOf converts the session spec's skills to agent skill refs.
func skillRefsOf(skills []SkillSpec) []agent.SkillRef {
	refs := make([]agent.SkillRef, 0, len(skills))
	for _, s := range skills {
		refs = append(refs, agent.SkillRef{Name: s.Name, Description: s.Description, ObjectKey: s.ObjectKey})
	}
	return refs
}

// buildLoop creates the agent Loop for a session from its spec and the
// given sandbox. Called at creation and every time the sandbox is
// rebuilt after an idle reap.
func (c *Controller) buildLoop(sess *Session, sandbox *ironhive.Sandbox) *agent.Loop {
	req := sess.Spec
	skillRefs := skillRefsOf(req.Skills)
	externalTools := make([]agent.ExternalToolSpec, 0, len(req.Tools))
	for _, t := range req.Tools {
		externalTools = append(externalTools, agent.ExternalToolSpec{Name: t.Name, Description: t.Description, Parameters: t.Parameters})
	}
	mcpServers := make([]agent.MCPServerSpec, 0, len(req.MCPServers))
	for _, m := range req.MCPServers {
		mcpServers = append(mcpServers, agent.MCPServerSpec{Name: m.Name, URL: m.URL, Headers: m.Headers, Transport: m.Transport})
	}
	return agent.NewLoop(agent.LoopConfig{
		ModelURL:      req.Model.URL,
		ModelHeaders:  req.Model.Headers,
		ModelName:     req.Model.Name,
		SystemPrompt:  agent.BuildSystemPrompt(req.Prompt.System, skillRefs, skillsRoot),
		Sandbox:       sandbox,
		SupportImage:  slices.Contains(req.Model.Features, ModelFeatureSupportImage),
		PersistStore:  c.store,
		SessionID:     sess.ID,
		OnPersisted:   sess.recordPersisted,
		ExternalTools: externalTools,
		Waiter:        sess,
		History:       agent.S3History(c.store, sess.ID),
		MaxContext:    req.Model.MaxContext,
		MCPServers:    mcpServers,
		OnMCPStatus: func(st agent.MCPServerStatus) {
			if st.Err != nil {
				log.Printf("session %s mcp %s: %v", sess.ID, st.Name, st.Err)
			} else {
				log.Printf("session %s mcp %s: %d tools mounted", sess.ID, st.Name, st.ToolCount)
			}
		},
	})
}

// ensureSandbox makes sure the session holds a live sandbox. When the
// sandbox was reaped for idleness (sessions outlive sandboxes), it is
// rebuilt: allocate, reinstall skills, restore persisted files, rebuild
// the agent Loop (its history reloads from S3) and restart lease
// renewal.
func (c *Controller) ensureSandbox(ctx context.Context, sess *Session) error {
	if sess.hasSandbox() {
		return nil
	}
	sandbox, err := c.ironhive.Allocate(ctx, sess.Spec.Ironhive.Pool, c.sandboxLease)
	if err != nil {
		return fmt.Errorf("allocate sandbox from pool %q: %w", sess.Spec.Ironhive.Pool, err)
	}
	if err = agent.InstallSkills(ctx, sandbox, c.store, skillRefsOf(sess.Spec.Skills), skillsRoot, skillURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("install skills: %w", err)
	}
	if err = agent.RestorePersisted(ctx, sandbox, c.store, sess.Persisted(), skillURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("restore persisted files: %w", err)
	}
	l := c.buildLoop(sess, sandbox)
	if err = l.LoadHistory(ctx); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("load history: %w", err)
	}
	sess.setLoop(l)
	// Keep the sandbox lease alive while the session holds it; without
	// renewal ironhive destroys the sandbox when the lease expires.
	renewCtx, stopRenew := context.WithCancel(context.Background())
	go c.renewSandbox(renewCtx, sandbox)
	if !sess.setSandbox(sandbox, stopRenew) {
		// The session was deleted (or the node is shutting down) while
		// the sandbox was being rebuilt; stop the renewal and release
		// the sandbox instead of leaking both.
		stopRenew()
		releaseSandbox(sandbox)
		return errors.New("session closed during sandbox rebuild")
	}
	return nil
}

// renewSandbox renews the sandbox lease at half-lease intervals until ctx
// is cancelled (session deleted or node shutdown). Failures are only
// logged: the turn that next touches a dead sandbox reports the error.
func (c *Controller) renewSandbox(ctx context.Context, sb *ironhive.Sandbox) {
	interval := c.sandboxLease / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, lookupTimeout)
			if err := sb.Renew(renewCtx, c.sandboxLease); err != nil {
				log.Printf("renew sandbox %s: %v", sb.Name, err)
			}
			cancel()
		}
	}
}

// CreateMessageRequest is the JSON body of POST /v1/sessions/{id}/messages.
type CreateMessageRequest struct {
	// Content is the user input for this turn.
	Content string `json:"content"`
}

// handleCreateMessage accepts one user input and runs its turn
// asynchronously: the response is the new turn id, and all turn events
// flow over the session event stream (GET .../events). A session runs
// one turn at a time; a concurrent message is rejected with 409
// session_busy.
func (c *Controller) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req CreateMessageRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if req.Content == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "content is required"})
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	// The turn runs detached from this request: a client disconnect does
	// not abort it (DELETE session or node shutdown does).
	turnID := newTurnID()
	ctx, cancel := context.WithTimeout(context.Background(), turnTimeout)
	if !sess.startTurn(turnID, cancel) {
		cancel()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "session_busy"})
		return
	}
	sess.touch()
	sess.hub.publish(turnID, "turn_started", map[string]string{"turn_id": turnID})
	go func() {
		defer sess.finishTurn()
		defer cancel()
		defer sess.touch()
		rep := &hubReporter{hub: sess.hub, turnID: turnID}
		// The sandbox may have been reaped for idleness; rebuild it from
		// the session spec and persisted files first.
		if err := c.ensureSandbox(ctx, sess); err != nil {
			log.Printf("session %s turn %s: %v", id, turnID, err)
			rep.Error("failed to prepare sandbox")
			return
		}
		if err := sess.getLoop().RunTurn(ctx, req.Content, rep); err != nil {
			if errors.Is(err, agent.ErrBusy) {
				// Unreachable given startTurn; publish so subscribers are
				// not stuck watching an open turn.
				rep.Error("session busy")
			}
			log.Printf("session %s turn %s: %v", id, turnID, err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"turn_id": turnID})
}

// handleSessionEvents streams the session's events as SSE. On connect
// the client receives a sync control event (current turn + latest seq),
// then the buffered backlog after last_seq (query parameter or
// Last-Event-ID header), then live events. Slow subscribers are dropped
// and expected to reconnect with their last seq.
func (c *Controller) handleSessionEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	lastSeq, _ := strconv.ParseInt(r.URL.Query().Get("last_seq"), 10, 64)
	if lastSeq == 0 {
		lastSeq, _ = strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64)
	}

	ch, backlog, currentTurn, latest := sess.hub.subscribe(lastSeq)
	defer sess.hub.unsubscribe(ch)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	// The sync frame carries the full merged history (completed turns as
	// {user, assistant} pairs) so a fresh client synchronizes in one
	// frame; the backlog then only matters for in-flight progress.
	history := sess.getLoop().Messages()
	messages := make([]syncMessage, 0, len(history))
	for _, m := range history {
		messages = append(messages, syncMessage{Role: m.Role, Content: m.Content})
	}
	writeSSESync(w, currentTurn, latest, messages, sess.Persisted())
	for _, ev := range backlog {
		writeSSEFrame(w, ev)
	}
	flusher.Flush()

	keepalive := time.NewTicker(sseKeepalive)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				// Dropped for lagging behind; the client reconnects.
				return
			}
			writeSSEFrame(w, ev)
			flusher.Flush()
		case <-keepalive.C:
			_, _ = io.WriteString(w, sseKeepaliveComment)
			flusher.Flush()
		}
	}
}

// handleCreateToolResult accepts an externally reported tool result.
// The request is routed to the session's owner node first (leaving the
// body untouched for forwarding), then validated and queued on the
// session for the agent runtime to consume.
func (c *Controller) handleCreateToolResult(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req ToolResultRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	if err := v.(*Session).AddToolResult(req); err != nil {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

// handleDeleteSession releases the session on its owner node.
func (c *Controller) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	v, _ := c.sessions.Load(id)
	c.sessions.Delete(id)
	if sess, ok := v.(*Session); ok {
		sess.cancelTurn()
		if sb, stop := sess.closeSession(); sb != nil {
			if stop != nil {
				stop()
			}
			releaseSandbox(sb)
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.UnregisterSession(ctx, id); err != nil {
		log.Printf("unregister session %s: %v", id, err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// Close stops the idle reaper, then tears down every session owned by
// this node: running turns are cancelled, lease renewal loops stopped
// and sandboxes released. It is called during graceful shutdown, after
// the HTTP server has stopped, so no handler is still touching the
// sessions map.
func (c *Controller) Close() {
	close(c.sweeperStop)
	c.sweeperDone.Wait()
	c.sessions.Range(func(key, v any) bool {
		c.sessions.Delete(key)
		if sess, ok := v.(*Session); ok {
			sess.cancelTurn()
			if sb, stop := sess.closeSession(); sb != nil {
				if stop != nil {
					stop()
				}
				releaseSandbox(sb)
			}
		}
		return true
	})
}

// releaseSandbox destroys a session's sandbox on a detached context, so
// cleanup survives client disconnects. Failures are only logged: the
// sandbox lease guarantees eventual reclamation by ironhive.
func releaseSandbox(sb *ironhive.Sandbox) {
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	if err := sb.Release(ctx); err != nil {
		log.Printf("release sandbox %s: %v", sb.Name, err)
	}
}

// routeSession ensures the request is handled on the node that owns the
// session. It reports whether the session is local and the caller should
// proceed; otherwise the response has already been written (forwarded to
// the owner node, or an error).
func (c *Controller) routeSession(w http.ResponseWriter, r *http.Request, id string) bool {
	if _, ok := c.sessions.Load(id); ok {
		return true
	}
	// Already forwarded once: the owner does not have this session,
	// so it does not exist. Never forward a second time.
	if r.Header.Get(headerForwarded) != "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	addr, ok, err := c.registry.SessionOwner(ctx, id)
	if err != nil {
		log.Printf("lookup session %s owner: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to locate session"})
		return false
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return false
	}
	p, err := c.proxy(addr)
	if err != nil {
		// The address came from etcd (written by another, possibly
		// misbehaving or outdated node): report a bad gateway instead of
		// taking this process down.
		log.Printf("session %s owner address: %v", id, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "invalid session owner address"})
		return false
	}
	p.ServeHTTP(w, r)
	return false
}

// proxy returns the cached reverse proxy for the given owner node
// address. The address comes from etcd, so it is validated here.
func (c *Controller) proxy(addr string) (*httputil.ReverseProxy, error) {
	if p, ok := c.proxies.Load(addr); ok {
		return p.(*httputil.ReverseProxy), nil
	}
	target, err := url.Parse(addr)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("invalid node address %q", addr)
	}
	p := httputil.NewSingleHostReverseProxy(target)
	p.FlushInterval = -1 // stream responses (SSE) without buffering
	p.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		req.Header.Set(headerForwarded, "1")
	}
	actual, _ := c.proxies.LoadOrStore(addr, p)
	return actual.(*httputil.ReverseProxy), nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
