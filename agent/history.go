package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yankeguo/turnhive/llm"
	"github.com/yankeguo/turnhive/storage"
)

// HistoryStore persists the LLM-facing message history of a session.
// Only {user, assistant} pairs are ever stored; tool exchanges of a turn
// are transient.
type HistoryStore interface {
	// Load returns the stored history, or an empty history when none was
	// ever saved.
	Load(ctx context.Context) ([]llm.Message, error)
	// Save replaces the stored history.
	Save(ctx context.Context, msgs []llm.Message) error
}

// s3HistoryStore keeps history as JSONL at sessions/{id}/history.jsonl
// (under the store's configured prefix, i.e.
// {prefix}/sessions/{id}/history.jsonl).
type s3HistoryStore struct {
	store *storage.Store
	key   string
}

// S3History keeps the history of sessionID in store as JSONL.
func S3History(store *storage.Store, sessionID string) HistoryStore {
	return &s3HistoryStore{store: store, key: "sessions/" + sessionID + "/history.jsonl"}
}

// Load reads and parses the JSONL history; a missing object means an
// empty history.
func (s *s3HistoryStore) Load(ctx context.Context) ([]llm.Message, error) {
	body, err := s.store.GetObject(ctx, s.key)
	if err != nil {
		if errors.Is(err, storage.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	msgs, err := parseHistoryJSONL(body)
	if err != nil {
		return nil, fmt.Errorf("read history %q: %w", s.key, err)
	}
	return msgs, nil
}

// parseHistoryJSONL parses the JSONL history. A single malformed line is
// skipped rather than failing the whole load — losing one line beats
// bricking the session (a failed Load never marks the history ready, so
// every later turn would fail too).
func parseHistoryJSONL(body []byte) ([]llm.Message, error) {
	var msgs []llm.Message
	sc := bufio.NewScanner(bytes.NewReader(body))
	// Allow very large single lines (e.g. a persisted long reply).
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var msg llm.Message
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		msgs = append(msgs, msg)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return msgs, nil
}

// Save marshals all messages as JSONL and overwrites the object.
func (s *s3HistoryStore) Save(ctx context.Context, msgs []llm.Message) error {
	var buf bytes.Buffer
	for _, msg := range msgs {
		line, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal history message: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return s.store.PutObject(ctx, s.key, buf.Bytes())
}
