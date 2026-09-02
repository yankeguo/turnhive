package controller

import (
	"encoding/json"
	"testing"

	"github.com/yankeguo/turnhive/agent"
)

func TestHubPublishSubscribe(t *testing.T) {
	h := newEventHub()

	rep := &hubReporter{hub: h, turnID: "turn-1"}
	h.publish("turn-1", "turn_started", map[string]string{"turn_id": "turn-1"})
	rep.Delta("Hel")
	rep.Delta("lo")
	rep.ToolCall(agent.ToolCallEvent{ID: "c1", Name: "shell", Status: "running"})
	rep.Done("Hello")

	ch, backlog, currentTurn, latest := h.subscribe(0)
	if latest != 5 {
		t.Fatalf("latest = %d, want 5", latest)
	}
	if currentTurn != "" {
		t.Fatalf("turn must be closed after done, got %q", currentTurn)
	}
	if len(backlog) != 5 {
		t.Fatalf("backlog = %d events, want 5", len(backlog))
	}
	names := []string{"turn_started", "delta", "delta", "tool_call", "turn_finished"}
	for i, ev := range backlog {
		if ev.name != names[i] || ev.seq != int64(i+1) {
			t.Errorf("backlog[%d] = %s seq %d, want %s seq %d", i, ev.name, ev.seq, names[i], i+1)
		}
	}
	// Every payload carries the turn id.
	var p struct {
		TurnID string `json:"turn_id"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(backlog[1].data, &p); err != nil || p.TurnID != "turn-1" || p.Text != "Hel" {
		t.Errorf("unexpected delta payload %s", backlog[1].data)
	}

	// last_seq filters the backlog.
	_, backlog, _, _ = h.subscribe(3)
	if len(backlog) != 2 || backlog[0].seq != 4 {
		t.Fatalf("subscribe(3) backlog = %+v", backlog)
	}

	// Live events reach the subscriber.
	h.publish("turn-2", "turn_started", map[string]string{"turn_id": "turn-2"})
	select {
	case ev := <-ch:
		if ev.seq != 6 || ev.name != "turn_started" {
			t.Fatalf("live event = %+v", ev)
		}
	default:
		t.Fatal("subscriber did not receive live event")
	}
	if _, _, cur, _ := h.subscribe(6); cur != "turn-2" {
		t.Fatalf("current turn = %q, want turn-2", cur)
	}

	// Unsubscribe stops delivery and closes the channel.
	h.unsubscribe(ch)
	if _, ok := <-ch; ok {
		t.Fatal("channel must be closed after unsubscribe")
	}
}

func TestHubBufferEviction(t *testing.T) {
	h := newEventHub()
	for range hubBufferCap + 100 {
		h.publish("turn-1", "delta", map[string]string{"text": "x"})
	}
	_, backlog, _, latest := h.subscribe(0)
	if latest != hubBufferCap+100 {
		t.Fatalf("latest = %d", latest)
	}
	if len(backlog) != hubBufferCap {
		t.Fatalf("backlog = %d, want capped %d", len(backlog), hubBufferCap)
	}
	if backlog[0].seq != 101 {
		t.Fatalf("oldest retained seq = %d, want 101", backlog[0].seq)
	}
}

func TestHubDropsSlowSubscriber(t *testing.T) {
	h := newEventHub()
	ch, _, _, _ := h.subscribe(0)
	// Overfill the subscriber's channel without draining it.
	for range hubSubscriberCap + 1 {
		h.publish("turn-1", "delta", map[string]string{"text": "x"})
	}
	// The slow subscriber was dropped and its channel closed.
	for {
		if _, ok := <-ch; !ok {
			break
		}
	}
	// A fresh subscriber still receives new events.
	ch2, _, _, _ := h.subscribe(0)
	h.publish("turn-1", "delta", map[string]string{"text": "y"})
	select {
	case <-ch2:
	default:
		t.Fatal("fresh subscriber did not receive event")
	}
}

func TestHubCloseAll(t *testing.T) {
	h := newEventHub()
	ch1, _, _, _ := h.subscribe(0)
	ch2, _, _, _ := h.subscribe(0)

	h.closeAll()

	// Every subscriber channel is closed; the buffer is untouched and a
	// fresh subscriber still works (the hub of an evicted session is
	// discarded as a whole, but closeAll itself must not brick it).
	for _, ch := range []chan hubEvent{ch1, ch2} {
		if _, ok := <-ch; ok {
			t.Fatal("subscriber channel must be closed by closeAll")
		}
	}
	ch3, _, _, _ := h.subscribe(0)
	h.publish("turn-1", "delta", map[string]string{"text": "x"})
	select {
	case <-ch3:
	default:
		t.Fatal("fresh subscriber did not receive event after closeAll")
	}
}

func TestHubReporterTurnFinishedStatus(t *testing.T) {
	h := newEventHub()
	rep := &hubReporter{hub: h, turnID: "turn-1", cause: func() error { return errTurnCancelled }}
	h.publish("turn-1", "turn_started", map[string]string{"turn_id": "turn-1"})
	rep.Error("context canceled")

	// The terminal event is turn_finished with the cancelled status (not
	// error), and it closes the hub's current-turn tracking.
	_, backlog, currentTurn, _ := h.subscribe(0)
	if currentTurn != "" {
		t.Fatalf("currentTurn = %q, want empty after turn_finished", currentTurn)
	}
	last := backlog[len(backlog)-1]
	if last.name != "turn_finished" {
		t.Fatalf("last event = %q, want turn_finished", last.name)
	}
	var p struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(last.data, &p); err != nil || p.Status != turnStatusCancelled {
		t.Fatalf("cancelled turn payload = %s, want status %q", last.data, turnStatusCancelled)
	}

	// A failure (nil cause) ends with the error status and the message.
	rep2 := &hubReporter{hub: h, turnID: "turn-2"}
	h.publish("turn-2", "turn_started", map[string]string{"turn_id": "turn-2"})
	rep2.Error("stream blew up")
	_, backlog2, _, _ := h.subscribe(0)
	last2 := backlog2[len(backlog2)-1]
	if last2.name != "turn_finished" {
		t.Fatalf("last event = %q, want turn_finished", last2.name)
	}
	p = struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}{}
	if err := json.Unmarshal(last2.data, &p); err != nil || p.Status != turnStatusError || p.Message != "stream blew up" {
		t.Fatalf("failed turn payload = %s, want status %q with message", last2.data, turnStatusError)
	}

	// A successful turn ends with the done status and the full reply.
	rep3 := &hubReporter{hub: h, turnID: "turn-3"}
	h.publish("turn-3", "turn_started", map[string]string{"turn_id": "turn-3"})
	rep3.Done("all good")
	_, backlog3, _, _ := h.subscribe(0)
	last3 := backlog3[len(backlog3)-1]
	var p3 struct {
		Status string `json:"status"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal(last3.data, &p3); err != nil || p3.Status != turnStatusDone || p3.Text != "all good" {
		t.Fatalf("done turn payload = %s, want status %q with text", last3.data, turnStatusDone)
	}
}
