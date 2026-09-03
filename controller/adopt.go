package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/yankeguo/turnhive/agent"
	"github.com/yankeguo/turnhive/storage"
)

// adoptTimeout bounds one session adoption (spec + history reads and the
// ownership claim) on the request path.
const adoptTimeout = 15 * time.Second

// errClaimLost reports that another node claimed the session while an
// adoption was in flight; the caller re-resolves the owner and forwards.
var errClaimLost = errors.New("session claimed by another node")

// specObjectKey is the S3 object key of a session's persisted creation
// spec — the handle that makes an orphaned session adoptable.
func specObjectKey(sessionID string) string {
	return "sessions/" + sessionID + "/spec.json"
}

// persistedPrefix is the S3 prefix of a session's persisted files.
func persistedPrefix(sessionID string) string {
	return "sessions/" + sessionID + "/persisted/"
}

// adoptionStore is the subset of storage operations the adoption path
// needs; *storage.Store satisfies it, tests use an in-memory fake.
type adoptionStore interface {
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, body []byte) error
	ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error)
}

// writeSessionSpec persists the session's creation spec so any node can
// adopt the session after its owner died (or after a cold eviction).
// Note the spec carries credentials (model and MCP headers): the bucket
// already holds the full conversation history, so they share one trust
// domain.
func writeSessionSpec(ctx context.Context, s adoptionStore, sessionID string, spec CreateSessionRequest) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal session spec: %w", err)
	}
	if err = s.PutObject(ctx, specObjectKey(sessionID), data); err != nil {
		return fmt.Errorf("write session spec: %w", err)
	}
	return nil
}

// loadSessionSpec reads a session's persisted creation spec; ok is false
// when no spec was ever written (the session never existed or was
// deleted). A persisted spec is re-validated: the object could have been
// edited outside turnhive.
func loadSessionSpec(ctx context.Context, s adoptionStore, sessionID string) (spec CreateSessionRequest, ok bool, err error) {
	data, err := s.GetObject(ctx, specObjectKey(sessionID))
	if errors.Is(err, storage.ErrNotExist) {
		return CreateSessionRequest{}, false, nil
	}
	if err != nil {
		return CreateSessionRequest{}, false, err
	}
	if err = json.Unmarshal(data, &spec); err != nil {
		return CreateSessionRequest{}, false, fmt.Errorf("parse session spec: %w", err)
	}
	if err = spec.Validate(); err != nil {
		return CreateSessionRequest{}, false, fmt.Errorf("stored session spec is invalid: %w", err)
	}
	return spec, true, nil
}

// listPersistedObjects rebuilds a session's persisted-files manifest by
// listing its persisted prefix: the object keys encode the in-sandbox
// paths, so the manifest itself never needs its own object.
func listPersistedObjects(ctx context.Context, s adoptionStore, sessionID string) (map[string]agent.PersistedObject, error) {
	prefix := persistedPrefix(sessionID)
	objs, err := s.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	persisted := make(map[string]agent.PersistedObject, len(objs))
	for _, o := range objs {
		p := strings.TrimPrefix(o.Key, prefix)
		if p == "" || p == o.Key {
			continue
		}
		persisted[p] = agent.PersistedObject{Path: p, ObjectKey: o.Key, Size: o.Size, At: o.LastModified}
	}
	return persisted, nil
}

// loadAdoptableSession loads everything an adoption needs from storage —
// the persisted creation spec and the persisted-files manifest — and
// builds the in-memory Session. found is false when no spec exists (the
// session never existed or was deleted).
func loadAdoptableSession(ctx context.Context, s adoptionStore, id string) (sess *Session, found bool, err error) {
	spec, ok, err := loadSessionSpec(ctx, s, id)
	if err != nil || !ok {
		return nil, false, err
	}
	persisted, err := listPersistedObjects(ctx, s, id)
	if err != nil {
		return nil, false, fmt.Errorf("list persisted files: %w", err)
	}
	files, err := loadFilesManifest(ctx, s, id)
	if err != nil {
		return nil, false, fmt.Errorf("load files manifest: %w", err)
	}
	sess = &Session{ID: id, Spec: spec, hub: newEventHub(), persisted: persisted, files: files}
	sess.touch()
	return sess, true, nil
}

// adoptSession recovers a session whose ownership record is gone (owner
// node crashed, or the session was evicted after cold_timeout) from its
// persisted spec: the spec and the persisted-files manifest are loaded,
// ownership is claimed in etcd (put-if-absent, so a concurrent adoption
// elsewhere loses and forwards here), and a history-only Loop is built —
// the first message's ensureSandbox rebuilds the full Loop with a fresh
// sandbox.
//
// Concurrent adoptions on this node collapse onto the first caller via
// the adopting map; a waiter simply reports adopted=true once the
// session appears (or false when the winning adoption failed).
// errClaimLost reports a claim won by another node; the caller
// re-resolves the owner and forwards.
func (c *Controller) adoptSession(ctx context.Context, id string) (adopted bool, err error) {
	ch := make(chan struct{})
	if actual, loaded := c.adopting.LoadOrStore(id, ch); loaded {
		select {
		case <-actual.(chan struct{}):
			_, adopted = c.sessions.Load(id)
			return adopted, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	defer func() { c.adopting.Delete(id); close(ch) }()

	sess, found, err := loadAdoptableSession(ctx, c.store, id)
	if err != nil || !found {
		return false, err
	}

	won, err := c.registry.ClaimSession(ctx, id)
	if err != nil {
		return false, fmt.Errorf("claim session: %w", err)
	}
	if !won {
		return false, errClaimLost
	}

	l := c.buildLoop(sess, nil)
	if err = l.LoadHistory(ctx); err != nil {
		_ = c.registry.UnregisterSession(ctx, id)
		return false, fmt.Errorf("load history: %w", err)
	}
	// A node crash mid-turn leaves a dangling user message in the
	// history (write-ahead); seal it with the interruption marker so the
	// model and the client see the turn was never completed.
	if err = l.SealInterruptedTurn(ctx); err != nil {
		log.Printf("session %s: seal interrupted turn: %v", id, err)
	}
	sess.setLoop(l)
	c.sessions.Store(id, sess)
	log.Printf("session %s adopted from storage", id)
	return true, nil
}
