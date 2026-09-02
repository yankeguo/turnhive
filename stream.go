package turnhive

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EventType identifies the kind of a Stream Event.
type EventType string

// Stream event types, matching the turnhive SSE event names.
const (
	// EventSync is the control event delivered on connect: Event.TurnID
	// is the currently running turn ("" when idle), Event.Seq the latest
	// sequence number and Event.Messages the full merged history.
	// Event.Seq is also the reset baseline for reconnect bookkeeping: a
	// session recovered from storage (node crash, cold eviction) restarts
	// its numbering, so on a sync event discard any previously tracked
	// seq and resume from Event.Seq.
	EventSync EventType = "sync"
	// EventTurnStarted marks the beginning of a turn (Event.TurnID).
	EventTurnStarted EventType = "turn_started"
	// EventDelta carries one assistant content delta in Event.Text.
	EventDelta EventType = "delta"
	// EventReasoningDelta carries one reasoning content delta in
	// Event.Text.
	EventReasoningDelta EventType = "reasoning_delta"
	// EventToolCall reports a tool call starting or finishing; see
	// Event.Status.
	EventToolCall EventType = "tool_call"
	// EventDone ends a successful turn; Event.Text is the full reply.
	EventDone EventType = "done"
	// EventError ends a failed turn; Event.Message describes the failure.
	EventError EventType = "error"
	// EventTurnCancelled ends a turn interrupted through the cancel
	// endpoint (Event.TurnID) — a user-initiated interruption, not a
	// failure.
	EventTurnCancelled EventType = "turn_cancelled"
)

// Tool call statuses of an EventToolCall event.
const (
	ToolCallRunning = "running"
	ToolCallDone    = "done"
	ToolCallError   = "error"
)

// Event is one event of the session event stream. The meaningful fields
// depend on Type; see the EventType constants. TurnID identifies the
// turn the event belongs to, Seq is its per-session sequence number
// (pass it back as Events' lastSeq after a reconnect).
type Event struct {
	Type    EventType
	Seq     int64
	TurnID  string
	Text    string // delta, reasoning_delta, done
	ID      string // tool_call
	Name    string // tool_call
	Title   string // tool_call, optional
	Status  string // tool_call: ToolCallRunning / ToolCallDone / ToolCallError
	Message string // error
	// Messages is the full merged history carried by the sync event
	// (completed turns as {user, assistant} pairs); empty for all other
	// event types.
	Messages []SyncMessage
	// Persisted lists the files the session has stored to object storage
	// via the persist tool; only set on the sync event.
	Persisted []PersistedObject
}

// SyncMessage is one message of the merged history in a sync event.
type SyncMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// PersistedObject records one file a session persisted to object
// storage. The object key is relative to the cluster's configured S3
// prefix (same convention as SkillSpec.ObjectKey).
type PersistedObject struct {
	Path      string    `json:"path"`
	ObjectKey string    `json:"object_key"`
	Size      int64     `json:"size"`
	At        time.Time `json:"at"`
}

// eventPayload is the JSON data carried by one SSE event; every event
// kind decodes from the same flat shape.
type eventPayload struct {
	TurnID    string            `json:"turn_id"`
	Seq       int64             `json:"seq"`
	Text      string            `json:"text"`
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Title     string            `json:"title"`
	Status    string            `json:"status"`
	Message   string            `json:"message"`
	Messages  []SyncMessage     `json:"messages"`
	Persisted []PersistedObject `json:"persisted"`
}

// Stream is the session-level event stream returned by Client.Events. It
// carries the events of every turn of the session; the stream ends only
// when the connection breaks or Close is called — a finished turn does
// not close it. Events must be consumed until the channel closes; then
// Err reports whether the stream ended cleanly. Close abandons the
// stream early.
type Stream struct {
	body   io.ReadCloser
	events chan Event
	done   chan struct{}

	closeOnce sync.Once

	mu     sync.Mutex
	err    error
	closed bool
}

func newStream(body io.ReadCloser) *Stream {
	s := &Stream{body: body, events: make(chan Event, 16), done: make(chan struct{})}
	go s.run()
	return s
}

// Events returns the channel of session events. It is closed when the
// connection breaks or Close is called.
func (s *Stream) Events() <-chan Event {
	return s.events
}

// Err reports why the stream ended, once the Events channel has closed.
// It is nil for a clean end (including after Close).
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close aborts the stream and releases the underlying connection. Events
// not yet consumed may be lost.
func (s *Stream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
	return s.body.Close()
}

// fail records a terminal error unless the stream was deliberately
// closed, then closes the event channel. Called exactly once, by run.
func (s *Stream) finish(err error) {
	s.mu.Lock()
	if s.closed {
		err = nil
	}
	s.err = err
	s.mu.Unlock()
	_ = s.body.Close()
	close(s.events)
}

// run parses the SSE response body and delivers events until the stream
// ends. SSE comments (keepalive lines) and blank lines are skipped.
func (s *Stream) run() {
	reader := bufio.NewReader(s.body)
	var name string
	var data strings.Builder
	var frameSeq int64
	dispatch := func() bool {
		defer func() { name, data, frameSeq = "", strings.Builder{}, 0 }()
		if data.Len() == 0 {
			return true
		}
		var p eventPayload
		if err := json.Unmarshal([]byte(data.String()), &p); err != nil {
			s.finish(fmt.Errorf("malformed event data %q: %w", truncate(data.String(), 256), err))
			return false
		}
		seq := frameSeq
		if seq == 0 {
			// Control events without a frame id (sync) carry their seq in
			// the payload.
			seq = p.Seq
		}
		select {
		case s.events <- Event{
			Type:      EventType(name),
			Seq:       seq,
			TurnID:    p.TurnID,
			Text:      p.Text,
			ID:        p.ID,
			Name:      p.Name,
			Title:     p.Title,
			Status:    p.Status,
			Message:   p.Message,
			Messages:  p.Messages,
			Persisted: p.Persisted,
		}:
			return true
		case <-s.done:
			// Close was called while the consumer stopped draining; exit
			// through the normal finish path instead of leaking run.
			s.finish(nil)
			return false
		}
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// EOF may arrive with a final partial line, and the last
				// event may lack its trailing blank line.
				if len(strings.TrimSpace(line)) > 0 {
					parseLine(line, &name, &data, &frameSeq)
				}
				if !dispatch() {
					return
				}
				s.finish(nil)
			} else {
				s.finish(fmt.Errorf("read event stream: %w", err))
			}
			return
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			// Blank line: dispatch the buffered event.
			if !dispatch() {
				return
			}
			continue
		}
		parseLine(line, &name, &data, &frameSeq)
	}
}

// parseLine folds one SSE line into the pending event name, data and
// frame id. Comment lines (": ...") and unknown fields are ignored.
func parseLine(line string, name *string, data *strings.Builder, frameSeq *int64) {
	if strings.HasPrefix(line, ":") {
		return
	}
	if v, ok := strings.CutPrefix(line, "event:"); ok {
		*name = strings.TrimPrefix(v, " ")
		return
	}
	if v, ok := strings.CutPrefix(line, "id:"); ok {
		// Leave frameSeq untouched on a malformed id so dispatch falls
		// back to the seq carried in the payload.
		if n, err := strconv.ParseInt(strings.TrimPrefix(v, " "), 10, 64); err == nil {
			*frameSeq = n
		}
		return
	}
	if v, ok := strings.CutPrefix(line, "data:"); ok {
		v = strings.TrimPrefix(v, " ")
		if data.Len() > 0 {
			data.WriteByte('\n')
		}
		data.WriteString(v)
	}
}

// truncate shortens s to at most n bytes for inclusion in error messages.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
