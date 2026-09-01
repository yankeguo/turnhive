package turnhive

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// EventType identifies the kind of a Stream Event.
type EventType string

// Stream event types, matching the turnhive SSE event names.
const (
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
)

// Tool call statuses of an EventToolCall event.
const (
	ToolCallRunning = "running"
	ToolCallDone    = "done"
	ToolCallError   = "error"
)

// Event is one event of a turn stream. The meaningful fields depend on
// Type; see the EventType constants.
type Event struct {
	Type    EventType
	Text    string // delta, reasoning_delta, done
	ID      string // tool_call
	Name    string // tool_call
	Title   string // tool_call, optional
	Status  string // tool_call: ToolCallRunning / ToolCallDone / ToolCallError
	Message string // error
}

// eventPayload is the JSON data carried by one SSE event; every event
// kind decodes from the same flat shape.
type eventPayload struct {
	Text    string `json:"text"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Title   string `json:"title"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// Stream is the event stream of one turn, returned by SendMessage.
// Events must be consumed until the channel closes; then Err reports
// whether the stream ended cleanly. Close abandons the stream early.
type Stream struct {
	body   io.ReadCloser
	events chan Event

	mu     sync.Mutex
	err    error
	closed bool
}

func newStream(body io.ReadCloser) *Stream {
	s := &Stream{body: body, events: make(chan Event, 16)}
	go s.run()
	return s
}

// Events returns the channel of turn events. It is closed when the turn
// ends, the stream breaks, or Close is called.
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
	dispatch := func() bool {
		defer func() { name, data = "", strings.Builder{} }()
		if data.Len() == 0 {
			return true
		}
		var p eventPayload
		if err := json.Unmarshal([]byte(data.String()), &p); err != nil {
			s.finish(fmt.Errorf("malformed event data %q: %w", truncate(data.String(), 256), err))
			return false
		}
		s.events <- Event{
			Type:    EventType(name),
			Text:    p.Text,
			ID:      p.ID,
			Name:    p.Name,
			Title:   p.Title,
			Status:  p.Status,
			Message: p.Message,
		}
		return true
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// EOF may arrive with a final partial line, and the last
				// event may lack its trailing blank line.
				if len(strings.TrimSpace(line)) > 0 {
					parseLine(line, &name, &data)
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
		parseLine(line, &name, &data)
	}
}

// parseLine folds one SSE line into the pending event name and data.
// Comment lines (": ...") and unknown fields are ignored.
func parseLine(line string, name *string, data *strings.Builder) {
	if strings.HasPrefix(line, ":") {
		return
	}
	if v, ok := strings.CutPrefix(line, "event:"); ok {
		*name = strings.TrimPrefix(v, " ")
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
