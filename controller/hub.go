package controller

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/yankeguo/turnhive/agent"
)

// hubBufferCap is the maximum number of events kept for replay
// (mirroring the agentdesk runner's replay buffer).
const hubBufferCap = 2000

// hubSubscriberCap bounds a subscriber's pending events. A slower
// subscriber is dropped — it reconnects with last_seq and replays from
// the buffer, so no event is lost for well-behaved clients.
const hubSubscriberCap = 64

// hubEvent is one sequenced session event.
type hubEvent struct {
	seq  int64
	name string
	data json.RawMessage
}

// eventHub sequences, buffers and fans out the events of one session.
// The buffer survives turn boundaries so a client reconnecting mid-turn
// can replay; the hub tracks the running turn from the event stream
// itself (turn_started opens it, turn_finished closes it).
type eventHub struct {
	mu          sync.Mutex
	seq         int64
	buf         []hubEvent
	currentTurn string
	subs        map[chan hubEvent]struct{}
}

func newEventHub() *eventHub {
	return &eventHub{subs: make(map[chan hubEvent]struct{})}
}

// publish assigns the next sequence number to one event, appends it to
// the replay buffer and broadcasts it to all subscribers. payload must
// be JSON-marshalable.
func (h *eventHub) publish(turnID, name string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	ev := hubEvent{name: name, data: data}
	h.mu.Lock()
	h.seq++
	ev.seq = h.seq
	h.buf = append(h.buf, ev)
	if len(h.buf) > hubBufferCap {
		h.buf = h.buf[len(h.buf)-hubBufferCap:]
	}
	switch name {
	case "turn_started":
		h.currentTurn = turnID
	case "turn_finished":
		h.currentTurn = ""
	}
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
			// Slow subscriber: drop it; it replays on reconnect.
			delete(h.subs, ch)
			close(ch)
		}
	}
	h.mu.Unlock()
}

// subscribe registers a listener, returning its channel, the buffered
// events after lastSeq, the currently running turn ("" when idle) and
// the latest sequence number.
func (h *eventHub) subscribe(lastSeq int64) (ch chan hubEvent, backlog []hubEvent, currentTurn string, latest int64) {
	ch = make(chan hubEvent, hubSubscriberCap)
	h.mu.Lock()
	for _, ev := range h.buf {
		if ev.seq > lastSeq {
			backlog = append(backlog, ev)
		}
	}
	h.subs[ch] = struct{}{}
	currentTurn, latest = h.currentTurn, h.seq
	h.mu.Unlock()
	return ch, backlog, currentTurn, latest
}

// unsubscribe removes a listener previously returned by subscribe.
func (h *eventHub) unsubscribe(ch chan hubEvent) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// closeAll disconnects every subscriber without touching the buffer. Used
// by cold eviction: the SSE handlers return, and the clients' reconnects
// re-adopt the session from storage and resynchronize from its sync
// frame.
func (h *eventHub) closeAll() {
	h.mu.Lock()
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// Turn terminal statuses carried by the turn_finished event: one event
// closes every turn, and its status says how.
const (
	turnStatusDone      = "done"
	turnStatusError     = "error"
	turnStatusCancelled = "cancelled"
)

// hubReporter implements agent.Reporter, publishing one session event
// per call. Every payload carries the turn id so subscribers can
// correlate events to turns.
type hubReporter struct {
	hub    *eventHub
	turnID string
	// cause reports why the turn's context ended (context.Cause), or nil
	// while it is running; it lets Error distinguish a turn interrupted
	// through the cancel endpoint from a failed one.
	cause func() error
}

func (r *hubReporter) Delta(text string) {
	r.hub.publish(r.turnID, "delta", map[string]string{"turn_id": r.turnID, "text": text})
}

func (r *hubReporter) ReasoningDelta(text string) {
	r.hub.publish(r.turnID, "reasoning_delta", map[string]string{"turn_id": r.turnID, "text": text})
}

func (r *hubReporter) ToolCall(ev agent.ToolCallEvent) {
	r.hub.publish(r.turnID, "tool_call", struct {
		TurnID string `json:"turn_id"`
		agent.ToolCallEvent
	}{TurnID: r.turnID, ToolCallEvent: ev})
}

func (r *hubReporter) Done(text string) {
	r.hub.publish(r.turnID, "turn_finished", map[string]string{
		"turn_id": r.turnID, "status": turnStatusDone, "text": text,
	})
}

func (r *hubReporter) Error(msg string) {
	// A turn interrupted through the cancel endpoint ends with the
	// cancelled status, so subscribers can tell it apart from a failure
	// (timeout, stream error, ...).
	if r.cause != nil && errors.Is(r.cause(), errTurnCancelled) {
		r.hub.publish(r.turnID, "turn_finished", map[string]string{
			"turn_id": r.turnID, "status": turnStatusCancelled,
		})
		return
	}
	r.hub.publish(r.turnID, "turn_finished", map[string]string{
		"turn_id": r.turnID, "status": turnStatusError, "message": msg,
	})
}
