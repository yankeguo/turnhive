package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yankeguo/turnhive/agent"
	"github.com/yankeguo/turnhive/storage"
)

// uploadsNameCountCap bounds how many distinct files one session
// carries, so the manifest and the sync frame stay small.
const uploadsNameCountCap = 64

// filesInjectTimeout bounds the injection of newly attached files into a
// live sandbox.
const filesInjectTimeout = 30 * time.Second

// filesManifestObjectKey is the S3 object key of a session's files
// manifest: the durable copy of its user-provided files, loaded on
// adoption. It lives next to the history (and survives DELETE, like the
// persisted files), not inside the spec — files are attached over the
// session's life, not fixed at creation.
func filesManifestObjectKey(sessionID string) string {
	return "sessions/" + sessionID + "/files.json"
}

// writeFilesManifest persists the session's files manifest.
func writeFilesManifest(ctx context.Context, s adoptionStore, sessionID string, uploads []agent.UploadRecord) error {
	data, err := json.Marshal(uploads)
	if err != nil {
		return fmt.Errorf("marshal files manifest: %w", err)
	}
	if err = s.PutObject(ctx, filesManifestObjectKey(sessionID), data); err != nil {
		return fmt.Errorf("write files manifest: %w", err)
	}
	return nil
}

// loadFilesManifest reads a session's files manifest; a missing manifest
// is an empty set, not an error (sessions created before files existed,
// or that never received one).
func loadFilesManifest(ctx context.Context, s adoptionStore, sessionID string) (map[string]agent.UploadRecord, error) {
	data, err := s.GetObject(ctx, filesManifestObjectKey(sessionID))
	if errors.Is(err, storage.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var uploads []agent.UploadRecord
	if err = json.Unmarshal(data, &uploads); err != nil {
		return nil, fmt.Errorf("parse files manifest: %w", err)
	}
	out := make(map[string]agent.UploadRecord, len(uploads))
	for _, u := range uploads {
		out[u.Name] = u
	}
	return out, nil
}

// AddFilesRequest is the JSON body of POST /v1/sessions/{id}/files.
type AddFilesRequest struct {
	// Files are the user-provided files to attach; each is an object key
	// in the shared S3 bucket (the caller puts the object itself).
	Files []FileRef `json:"files"`
}

// handleCreateFiles attaches user-provided files to the session: the
// records join the session state (and the persisted manifest, so an
// adopted session restores them) and are injected into the sandbox —
// immediately when a live sandbox exists, otherwise on the next sandbox
// build (ensureSandbox injects the full set).
func (c *Controller) handleCreateFiles(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	var req AddFilesRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if len(req.Files) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "files is required"})
		return
	}
	if err := validateFileRefs(req.Files); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		// Lost a race with DELETE after routeSession.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)

	records := make([]agent.UploadRecord, 0, len(req.Files))
	for _, f := range req.Files {
		records = append(records, agent.UploadRecord{
			Name:      f.Name,
			ObjectKey: f.ObjectKey,
			Size:      f.Size,
			At:        time.Now().UTC(),
		})
	}

	if err := c.attachFiles(r.Context(), sess, records); err != nil {
		switch {
		case errors.Is(err, errSessionClosed):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		case errors.Is(err, errTooManyFiles):
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
		default:
			log.Printf("session %s: inject files: %v", id, err)
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to inject files into the sandbox"})
		}
		return
	}

	// Persist the manifest so adoption restores the files. Best-effort
	// like history saves: the in-memory state is authoritative, and a
	// failed write only degrades crash recovery (logged).
	if err := writeFilesManifest(r.Context(), c.store, id, sess.Uploads()); err != nil {
		log.Printf("session %s: write files manifest: %v", id, err)
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

var (
	// errSessionClosed reports an attach racing the session's teardown
	// (DELETE, shutdown, cold eviction) — the record would never reach a
	// sandbox on this node.
	errSessionClosed = errors.New("session closed")
	// errTooManyFiles reports a session exceeding the per-session file
	// count cap.
	errTooManyFiles = errors.New("too many files")
)

// attachFiles records the uploads and, when the session holds a live
// sandbox, injects them into it first, so a message sent after the
// attach always finds the files on disk. filesMu is held across the
// sandbox check and the injection: a concurrent detach (idle reap, cold
// eviction, teardown) either happened before the attach (no sandbox —
// the record waits for the next build) or after it (the injection
// landed; the record is also persisted for the next build), but never
// in between, which would accept a file into no sandbox at all.
func (c *Controller) attachFiles(ctx context.Context, sess *Session, records []agent.UploadRecord) error {
	sess.filesMu.Lock()
	defer sess.filesMu.Unlock()
	if sess.isClosed() {
		return errSessionClosed
	}
	if sb, ok := sess.liveSandbox(); ok {
		injectCtx, cancel := context.WithTimeout(ctx, filesInjectTimeout)
		err := agent.InjectUploads(injectCtx, sb, c.uploadStore, records, agent.UploadURLTTL)
		cancel()
		if err != nil {
			return err
		}
	}
	// Reject attachments that would grow the session past the cap
	// *before* recording, so a rejected request leaves no partial state.
	// Distinct names count; re-attaching an existing name replaces it.
	known := make(map[string]bool)
	for _, u := range sess.Uploads() {
		known[u.Name] = true
	}
	added := 0
	for _, u := range records {
		if !known[u.Name] {
			added++
		}
	}
	if len(known)+added > uploadsNameCountCap {
		return fmt.Errorf("%w (limit %d)", errTooManyFiles, uploadsNameCountCap)
	}
	for _, u := range records {
		sess.recordUpload(u)
	}
	return nil
}

// SessionDetail is the response body of GET /v1/sessions/{id}: the
// session's state for inspection, statistics and audit purposes.
type SessionDetail struct {
	// ID is the session id.
	ID string `json:"id"`
	// TurnID is the currently running turn ("" when idle).
	TurnID string `json:"turn_id"`
	// Files are the user-provided files attached to the session.
	Files []agent.UploadRecord `json:"files"`
	// Persisted are the files the session persisted via the persist
	// tool.
	Persisted []agent.PersistedObject `json:"persisted"`
}

// handleGetSession reports the session's details (files, persisted
// objects, running turn) for inspection and audit tooling. It adopts
// cold sessions like any other session-scoped route.
func (c *Controller) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !c.routeSession(w, r, id) {
		return
	}
	v, ok := c.sessions.Load(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	sess := v.(*Session)
	writeJSON(w, http.StatusOK, SessionDetail{
		ID:        id,
		TurnID:    sess.TurnID(),
		Files:     sess.Uploads(),
		Persisted: sess.Persisted(),
	})
}
