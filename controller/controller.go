// Package controller holds the HTTP handlers and routing for turnhive.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
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

// allocateTimeout bounds sandbox allocation; ironhive may block up to 30s
// server-side waiting for a standby pod.
const allocateTimeout = 40 * time.Second

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

	// sessions holds the sessions owned by this node, keyed by session ID.
	sessions sync.Map
	// proxies caches one reverse proxy per owner node address.
	proxies sync.Map
}

// New creates a Controller for the given node.
func New(nodeID string, reg *registry.Registry, ih *ironhive.Client, store *storage.Store, sandboxLease time.Duration) *Controller {
	return &Controller{nodeID: nodeID, registry: reg, ironhive: ih, store: store, sandboxLease: sandboxLease}
}

// RegisterRoutes registers all routes on the given mux using the Go 1.22+
// method-and-pattern routing syntax.
func (c *Controller) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", c.handleHealthz)
	mux.HandleFunc("POST /v1/sessions", c.handleCreateSession)
	mux.HandleFunc("POST /v1/sessions/{id}/messages", c.handleCreateMessage)
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

	// Allocate the sandbox before registering anything, so a failed
	// allocation leaves no state behind.
	allocCtx, allocCancel := context.WithTimeout(r.Context(), allocateTimeout)
	defer allocCancel()
	sandbox, err := c.ironhive.Allocate(allocCtx, req.Ironhive.Pool, c.sandboxLease)
	if err != nil {
		log.Printf("allocate sandbox from pool %q: %v", req.Ironhive.Pool, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to allocate sandbox"})
		return
	}

	// Install the skill tarballs into the sandbox via presigned URLs;
	// the sandbox downloads them itself.
	skillRefs := make([]agent.SkillRef, 0, len(req.Skills))
	for _, s := range req.Skills {
		skillRefs = append(skillRefs, agent.SkillRef{Name: s.Name, Description: s.Description, ObjectKey: s.ObjectKey})
	}
	if err = agent.InstallSkills(allocCtx, sandbox, c.store, skillRefs, skillsRoot, skillURLTTL); err != nil {
		log.Printf("install skills: %v", err)
		releaseSandbox(sandbox)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to install skills"})
		return
	}

	id := "sess-" + strings.ToLower(ulid.Make().String())

	sess := &Session{ID: id, Spec: req, Sandbox: sandbox}
	externalTools := make([]agent.ExternalToolSpec, 0, len(req.Tools))
	for _, t := range req.Tools {
		externalTools = append(externalTools, agent.ExternalToolSpec{Name: t.Name, Description: t.Description, Parameters: t.Parameters})
	}
	sess.Loop = agent.NewLoop(agent.LoopConfig{
		ModelURL:      req.Model.URL,
		ModelHeaders:  req.Model.Headers,
		ModelName:     req.Model.Name,
		SystemPrompt:  agent.BuildSystemPrompt(req.Prompt.System, skillRefs, skillsRoot),
		Sandbox:       sandbox,
		SupportImage:  slices.Contains(req.Model.Features, ModelFeatureSupportImage),
		ExternalTools: externalTools,
		Waiter:        sess,
		History:       agent.S3History(c.store, id),
	})

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.RegisterSession(ctx, id); err != nil {
		log.Printf("register session %s: %v", id, err)
		releaseSandbox(sandbox)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to register session"})
		return
	}
	// Keep the sandbox lease alive for as long as the session lives;
	// without renewal ironhive destroys the sandbox when the lease
	// granted at allocation time expires.
	renewCtx, stopRenew := context.WithCancel(context.Background())
	sess.stopRenew = stopRenew
	go c.renewSandbox(renewCtx, sandbox)
	c.sessions.Store(id, sess)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
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

// handleCreateMessage runs one agent turn and streams it back as SSE.
// Events: delta, reasoning_delta, tool_call, done, error.
func (c *Controller) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req CreateMessageRequest
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

	rep := newSSEReporter(w)
	ctx, cancel := context.WithTimeout(r.Context(), turnTimeout)
	defer cancel()
	err := sess.Loop.RunTurn(ctx, req.Content, rep)
	rep.Close()
	if errors.Is(err, agent.ErrBusy) {
		// No SSE bytes were written yet, so a plain JSON error is still
		// possible.
		writeJSON(w, http.StatusConflict, map[string]string{"error": "session_busy"})
		return
	}
	if err != nil {
		log.Printf("session %s turn: %v", id, err)
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
	v.(*Session).AddToolResult(req)
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
		if sess.stopRenew != nil {
			sess.stopRenew()
		}
		if sess.Sandbox != nil {
			releaseSandbox(sess.Sandbox)
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.UnregisterSession(ctx, id); err != nil {
		log.Printf("unregister session %s: %v", id, err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// Close tears down every session owned by this node: it stops the lease
// renewal loops and releases the sandboxes. It is called during graceful
// shutdown, after the HTTP server has stopped, so no handler is still
// touching the sessions map.
func (c *Controller) Close() {
	c.sessions.Range(func(key, v any) bool {
		c.sessions.Delete(key)
		if sess, ok := v.(*Session); ok {
			if sess.stopRenew != nil {
				sess.stopRenew()
			}
			if sess.Sandbox != nil {
				releaseSandbox(sess.Sandbox)
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
	c.proxy(addr).ServeHTTP(w, r)
	return false
}

// proxy returns the cached reverse proxy for the given owner node address.
func (c *Controller) proxy(addr string) *httputil.ReverseProxy {
	if p, ok := c.proxies.Load(addr); ok {
		return p.(*httputil.ReverseProxy)
	}
	target, err := url.Parse(addr)
	if err != nil {
		// node.advertise is validated at startup, so this cannot happen.
		log.Fatalf("invalid node address %q: %v", addr, err)
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
	return actual.(*httputil.ReverseProxy)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
