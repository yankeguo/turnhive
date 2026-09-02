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
	names := []string{"turn_started", "delta", "delta", "tool_call", "done"}
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
