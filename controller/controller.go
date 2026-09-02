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

// errTurnCancelled is the context cause of a turn interrupted through the
// cancel endpoint; it distinguishes a user-initiated interruption from a
// failure (timeout, stream error) when the terminal event is published.
var errTurnCancelled = errors.New("turn cancelled")

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
	// coldTimeout bounds session inactivity before the sweeper evicts the
	// whole session from memory and etcd to cold storage; zero disables
	// eviction. A later request re-adopts the session from S3.
	coldTimeout time.Duration

	// sessions holds the sessions owned by this node, keyed by session ID.
	sessions sync.Map
	// adopting tracks in-flight session adoptions (id -> chan struct{},
	// closed on completion): concurrent requests for the same cold
	// session — local or forwarded mid-adoption — wait for the outcome
	// instead of duplicating the work or being 404'd.
	adopting sync.Map
	// proxies caches one reverse proxy per owner node address.
	proxies sync.Map

	sweeperStop chan struct{}
	sweeperDone sync.WaitGroup
}

// New creates a Controller for the given node and starts the idle
// reaper.
func New(nodeID string, reg *registry.Registry, ih *ironhive.Client, store *storage.Store, sandboxLease, idleTimeout, coldTimeout time.Duration) *Controller {
	c := &Controller{
		nodeID: nodeID, registry: reg, ironhive: ih, store: store,
		sandboxLease: sandboxLease, idleTimeout: idleTimeout, coldTimeout: coldTimeout,
		sweeperStop: make(chan struct{}),
	}
	c.sweeperDone.Add(1)
	go c.runSweeper()
	return c
}

// runSweeper periodically releases the sandboxes of sessions that have
// been idle past idleTimeout, and evicts sessions idle past coldTimeout
// to cold storage (when configured), until Close.
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
			if c.coldTimeout > 0 {
				c.evictColdSessions()
			}
		}
	}
}

// evictColdSessions retires every session that has been inactive for at
// least coldTimeout: it leaves memory and etcd entirely and lives on
// only in S3 (spec, history, persisted files), where any node adopts it
// on the next request. Eviction reuses the teardown actions of DELETE
// (cancel turn — none is running by definition — stop renewal, release
// sandbox) plus closing the event hub so live SSE subscribers reconnect
// and resynchronize.
func (c *Controller) evictColdSessions() {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		sb, stop, evicted := sess.takeIfCold(c.coldTimeout)
		if !evicted {
			return true
		}
		c.sessions.Delete(sess.ID)
		if stop != nil {
			stop()
		}
		if sb != nil {
			log.Printf("session %s idle past %s, releasing sandbox %s", sess.ID, c.coldTimeout, sb.Name)
			releaseSandbox(sb)
		}
		sess.hub.closeAll()
		ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
		if err := c.registry.UnregisterSession(ctx, sess.ID); err != nil {
			log.Printf("unregister cold session %s: %v", sess.ID, err)
		}
		cancel()
		log.Printf("session %s idle past %s, evicted to cold storage", sess.ID, c.coldTimeout)
		return true
	})
}

// ReregisterSessions re-registers the ownership of every session held in
// memory. It is wired to registry.OnReconnected: an etcd keepalive loss
// takes down the node record and every session record with it, and this
// restores the latter once the node record is back.
func (c *Controller) ReregisterSessions(ctx context.Context) {
	c.sessions.Range(func(_, v any) bool {
		sess, ok := v.(*Session)
		if !ok {
			return true
		}
		if err := c.registry.RegisterSession(ctx, sess.ID); err != nil {
			log.Printf("re-register session %s: %v", sess.ID, err)
		}
		return true
	})
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
	mux.HandleFunc("POST /v1/sessions/{id}/cancel", c.handleCancelTurn)
	mux.HandleFunc("GET /v1/sessions/{id}/events", c.handleSessionEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/tool_results", c.handleCreateToolResult)
	mux.HandleFunc("POST /v1/sessions/{id}/files", c.handleCreateFiles)
	mux.HandleFunc("GET /v1/sessions/{id}", c.handleGetSession)
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

	// Persist the creation spec so any node can adopt the session when
	// its owner dies (or after a cold eviction). A created session must
	// be adoptable, so a failed write aborts the creation.
	if err := writeSessionSpec(allocCtx, c.store, id, req); err != nil {
		log.Printf("persist spec for session %s: %v", id, err)
		releaseSessionSandbox(sess)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to persist session spec"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.RegisterSession(ctx, id); err != nil {
		log.Printf("register session %s: %v", id, err)
		releaseSessionSandbox(sess)
		// Best-effort: leave no spec behind for a session that was never
		// created, or a later request would adopt a ghost.
		if derr := c.store.DeleteObject(ctx, specObjectKey(id)); derr != nil {
			log.Printf("delete spec of unregistered session %s: %v", id, derr)
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to register session"})
		return
	}
	c.sessions.Store(id, sess)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// releaseSessionSandbox rolls back a failed session creation: detaches
// the sandbox, stops its lease renewal and releases it.
func releaseSessionSandbox(sess *Session) {
	if sb, stop := sess.takeSandbox(); sb != nil {
		if stop != nil {
			stop()
		}
		releaseSandbox(sb)
	}
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
		// A backgrounded command that exits on its own is queued and
		// drained into a synthesized user turn; a busy session holds the
		// queue until the running turn completes.
		OnBackgroundExit: func(info agent.BgProcessExit) {
			log.Printf("session %s background process %d exited (code %d)", sess.ID, info.Pid, info.ExitCode)
			sess.recordBackgroundExit(info)
			c.drainBackgroundExits(sess)
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
	if err = agent.InjectUploads(ctx, sandbox, c.store, sess.Uploads(), skillURLTTL); err != nil {
		releaseSandbox(sandbox)
		return fmt.Errorf("inject user files: %w", err)
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
	// Content is the user input for this turn. Referencing attached
	// files (POST .../files) is the caller's business: it composes
	// whatever marker text it likes — the files are always at
	// .agents/uploads/<name> inside the sandbox.
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

	turnID, started := c.runTurn(sess, req.Content)
	if !started {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "session_busy"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"turn_id": turnID})
}

// runTurn starts a detached turn for content (a client message or a
// cluster-synthesized one, e.g. the background-process exit
// notification) and reports the new turn id; a session runs one turn at
// a time, so it returns "" when a turn is already running. The turn
// runs detached from any HTTP request: a client disconnect does not
// abort it (DELETE session or node shutdown does), and all of its
// events flow over the session event stream.
func (c *Controller) runTurn(sess *Session, content string) (string, bool) {
	turnID := newTurnID()
	ctx, cancel := context.WithTimeout(context.Background(), turnTimeout)
	if !sess.startTurn(turnID, cancel) {
		cancel()
		return "", false
	}
	sess.touch()
	sess.hub.publish(turnID, "turn_started", map[string]string{"turn_id": turnID})
	go func() {
		// A finished turn is the drain trigger for queued background
		// exits: finishTurn clears the busy mark first, so the drain can
		// start their notification turn immediately.
		defer func() {
			sess.finishTurn()
			c.drainBackgroundExits(sess)
		}()
		defer cancel()
		defer sess.touch()
		rep := &hubReporter{hub: sess.hub, turnID: turnID, cause: sess.turnCause}
		// The sandbox may have been reaped for idleness; rebuild it from
		// the session spec and persisted files first.
		if err := c.ensureSandbox(ctx, sess); err != nil {
			log.Printf("session %s turn %s: %v", sess.ID, turnID, err)
			rep.Error("failed to prepare sandbox")
			return
		}
		if err := sess.getLoop().RunTurn(ctx, content, rep); err != nil {
			if errors.Is(err, agent.ErrBusy) {
				// Unreachable given startTurn; publish so subscribers are
				// not stuck watching an open turn.
				rep.Error("session busy")
			}
			log.Printf("session %s turn %s: %v", sess.ID, turnID, err)
		}
	}()
	return turnID, true
}

// drainBackgroundExits reports queued background-process exits as one
// synthesized user message in a new turn (aligned with the agentdesk
// runner's bg-notifier): the agent reacts to the exits immediately
// instead of learning about them from the user's next message. Exits
// accumulated while a turn is running coalesce into a single message;
// when the session is busy the queue goes back and the running turn's
// completion drains it again.
func (c *Controller) drainBackgroundExits(sess *Session) {
	exits := sess.takeBackgroundExits()
	if len(exits) == 0 {
		return
	}
	if _, started := c.runTurn(sess, buildBgExitMessage(exits)); !started {
		sess.requeueBackgroundExits(exits)
	}
}

// buildBgExitMessage renders the synthesized user message reporting
// background-process exits. The note steers the agent: no interactive
// user is present, so it must not ask questions unless strictly
// necessary. Format follows the agentdesk runner: bare open/close tags,
// metadata as `key: value` lines.
func buildBgExitMessage(exits []agent.BgProcessExit) string {
	var b strings.Builder
	b.WriteString("<background_processes_exited>\n")
	b.WriteString("note: Automated notification: these background processes have exited. React autonomously " +
		"(inspect the output files, continue any follow-up work); do NOT ask the user questions " +
		"unless strictly necessary.\n")
	for _, e := range exits {
		exitCode := strconv.Itoa(e.ExitCode)
		if e.ExitCode < 0 {
			exitCode = "unknown"
		}
		b.WriteString("\n<process>\n")
		fmt.Fprintf(&b, "pid: %d\ncommand: %s\nexit_code: %s\nstdout: %s\nstderr: %s\n",
			e.Pid, e.Command, exitCode, e.StdoutFile, e.StderrFile)
		b.WriteString("</process>\n")
	}
	b.WriteString("</background_processes_exited>")
	return b.String()
}

// handleCancelTurn interrupts the session's running turn: the turn's
// context is cancelled so the agent loop aborts (the partial reply is
// persisted and the turn closes with a turn_finished event carrying the
// cancelled status), and the handler returns only once finishTurn has
// cleared the busy mark — so the client can resend its message
// immediately to continue (resend is the recovery; the cancelled turn
// is not replayed or resumable).
func (c *Controller) handleCancelTurn(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// No adoption: a cold session has no running turn, so cancelling one
	// must not recover it from storage first.
	if !c.routeSessionMode(w, r, id, false) {
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	turnID, done := sess.CancelTurn()
	if turnID == "" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "no_turn_running"})
		return
	}
	if done != nil {
		select {
		case <-done:
		case <-r.Context().Done():
			// Client gave up waiting: the cancel already took effect,
			// but finishTurn has not cleared the busy mark yet — a 202
			// here would invite an immediate resend that collides with
			// session_busy. Report the gateway timeout instead; the
			// client tracks completion via the SSE turn_finished event.
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{
				"error": "cancel pending; the turn is finishing",
			})
			return
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"turn_id": turnID, "status": "cancelled"})
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
	writeSSESync(w, currentTurn, latest, messages, sess.Persisted(), sess.Uploads())
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
	// The spec is the adoption handle: deleting the session must make it
	// un-adoptable. History and persisted files stay, as before.
	if err := c.store.DeleteObject(ctx, specObjectKey(id)); err != nil {
		log.Printf("delete spec of session %s: %v", id, err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// CloseSubscribers disconnects every SSE subscriber of every session
// owned by this node, without touching session state. It is called
// during graceful shutdown *before* http.Server.Shutdown: the events
// handlers are infinite loops that only return when their subscription
// channel closes or the request context ends, so Shutdown would
// otherwise block on them until its timeout and report failure.
func (c *Controller) CloseSubscribers() {
	c.sessions.Range(func(_, v any) bool {
		if sess, ok := v.(*Session); ok {
			sess.hub.closeAll()
		}
		return true
	})
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
// session, adopting the session from storage when it is cold. It reports
// whether the session is local and the caller should proceed; otherwise
// the response has already been written.
func (c *Controller) routeSession(w http.ResponseWriter, r *http.Request, id string) bool {
	return c.routeSessionMode(w, r, id, true)
}

// routeSessionMode is routeSession with control over adoption. The cancel
// endpoint passes allowAdopt=false: a cold session has no running turn by
// definition, so adopting it (S3 reads, an etcd claim) just to answer
// no_turn_running would be wasted work.
func (c *Controller) routeSessionMode(w http.ResponseWriter, r *http.Request, id string, allowAdopt bool) bool {
	if _, ok := c.sessions.Load(id); ok {
		return true
	}
	// An adoption is in flight on this node (a concurrent request is
	// recovering this cold session): wait for it instead of duplicating
	// the work — or 404ing a forwarded request whose owner is still
	// mid-adoption.
	if ch, ok := c.adopting.Load(id); ok {
		select {
		case <-ch.(chan struct{}):
		case <-r.Context().Done():
			// The in-flight adoption outlived the client (or its
			// patience): say so instead of hanging the connection open
			// with no status at all.
			writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "session adoption in progress"})
			return false
		}
		if _, ok = c.sessions.Load(id); ok {
			return true
		}
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
		if !allowAdopt {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
			return false
		}
		// No owner record: the session may be cold — its owner node died
		// (the lease took the record down) or it was evicted past
		// cold_timeout. Adopt it from storage when a spec exists.
		adoptCtx, adoptCancel := context.WithTimeout(r.Context(), adoptTimeout)
		defer adoptCancel()
		adopted, aerr := c.adoptSession(adoptCtx, id)
		switch {
		case errors.Is(aerr, errClaimLost):
			// A concurrent adoption claimed the session: serve it locally
			// when this node won in the meantime, otherwise re-resolve
			// and forward to the winner. The first lookup context may be
			// expired by the adoption attempt; use a fresh one.
			if _, ok = c.sessions.Load(id); ok {
				return true
			}
			retryCtx, retryCancel := context.WithTimeout(r.Context(), lookupTimeout)
			defer retryCancel()
			addr, ok, err = c.registry.SessionOwner(retryCtx, id)
			if err != nil || !ok {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
				return false
			}
		case aerr != nil:
			log.Printf("adopt session %s: %v", id, aerr)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to recover session"})
			return false
		case adopted:
			return true
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
			return false
		}
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
