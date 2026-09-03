// Package controller holds the HTTP handlers and routing for turnhive.
package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
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

// lookupTimeout bounds etcd ownership lookups on the request path.
const lookupTimeout = 5 * time.Second

// maxJSONBody bounds the request body of the JSON-decoding endpoints.
const maxJSONBody = 4 << 20

// newSessionID generates a session id: sess-<lowercase ULID>, the same
// scheme as ironhive's sandbox names.
func newSessionID() string {
	return "sess-" + strings.ToLower(ulid.Make().String())
}

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

	// fileStore presigns downloads of user-provided files into
	// sandboxes; in production it is store, tests substitute a fake.
	fileStore agent.FileStore

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
	c.fileStore = store
	c.sweeperDone.Add(1)
	go c.runSweeper()
	return c
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
	writeSSESync(w, currentTurn, latest, messages, sess.Persisted(), sess.Files())
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
