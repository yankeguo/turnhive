// Package controller holds the HTTP handlers and routing for turnhive.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/registry"
)

// headerForwarded marks a request that has already been forwarded by
// another node, preventing forwarding loops.
const headerForwarded = "X-Turnhive-Forwarded"

// lookupTimeout bounds etcd ownership lookups on the request path.
const lookupTimeout = 5 * time.Second

// allocateTimeout bounds sandbox allocation; ironhive may block up to 30s
// server-side waiting for a standby pod.
const allocateTimeout = 40 * time.Second

// Controller holds the dependencies shared by all HTTP handlers.
type Controller struct {
	nodeID   string
	registry *registry.Registry
	ironhive *ironhive.Client
	// sandboxLease is the lease duration requested when allocating a
	// session sandbox.
	sandboxLease time.Duration

	// sessions holds the sessions owned by this node, keyed by session ID.
	sessions sync.Map
	// proxies caches one reverse proxy per owner node address.
	proxies sync.Map
}

// New creates a Controller for the given node.
func New(nodeID string, reg *registry.Registry, ih *ironhive.Client, sandboxLease time.Duration) *Controller {
	return &Controller{nodeID: nodeID, registry: reg, ironhive: ih, sandboxLease: sandboxLease}
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
	sandbox, err := c.ironhive.Allocate(allocCtx, req.Ironhive.Pool, c.sandboxLease)
	allocCancel()
	if err != nil {
		log.Printf("allocate sandbox from pool %q: %v", req.Ironhive.Pool, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to allocate sandbox"})
		return
	}

	var b [16]byte
	_, _ = rand.Read(b[:])
	id := hex.EncodeToString(b[:])

	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.RegisterSession(ctx, id); err != nil {
		log.Printf("register session %s: %v", id, err)
		releaseSandbox(sandbox)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to register session"})
		return
	}
	c.sessions.Store(id, &Session{ID: id, Spec: req, Sandbox: sandbox})
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// handleCreateMessage is a placeholder for the streaming (SSE) interaction
// endpoint; it only implements session routing for now.
func (c *Controller) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	if !c.routeSession(w, r, r.PathValue("id")) {
		return
	}
	writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "not implemented"})
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
	v, _ := c.sessions.Load(id)
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
	if sess, ok := v.(*Session); ok && sess.Sandbox != nil {
		releaseSandbox(sess.Sandbox)
	}
	ctx, cancel := context.WithTimeout(r.Context(), lookupTimeout)
	defer cancel()
	if err := c.registry.UnregisterSession(ctx, id); err != nil {
		log.Printf("unregister session %s: %v", id, err)
	}
	w.WriteHeader(http.StatusNoContent)
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
