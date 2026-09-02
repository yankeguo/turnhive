package turnhive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// newTestServer routes the turnhive API with the given handlers.
func newTestServer(t *testing.T, handler http.Handler) (*Client, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	return NewClient(srv.URL), srv.Close
}

func TestCreateSession(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sessions" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var req CreateSessionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Model.Name != "m" || req.Ironhive.Pool != "default" || req.Tools[0].Name != "deploy" {
			t.Errorf("unexpected request: %+v", req)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"sess-abc"}`))
	}))
	defer done()

	sess, err := cli.CreateSession(context.Background(), CreateSessionRequest{
		Model:    ModelSpec{URL: "http://llm/v1/chat/completions", Protocol: ProtocolOpenAICompletions, Name: "m"},
		Prompt:   PromptSpec{System: "sys"},
		Ironhive: IronhiveSpec{Pool: "default"},
		Tools: []ToolSpec{{
			Name:       "deploy",
			Parameters: map[string]any{"type": "object"},
		}},
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if sess.ID != "sess-abc" {
		t.Fatalf("unexpected session id %q", sess.ID)
	}
}

func TestCreateSessionError(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"model.url is required"}`))
	}))
	defer done()

	_, err := cli.CreateSession(context.Background(), CreateSessionRequest{})
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *Error, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 400 || apiErr.Message != "model.url is required" {
		t.Fatalf("unexpected error: %+v", apiErr)
	}
}

func TestDeleteSessionNotFound(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"session not found"}`))
	}))
	defer done()

	if err := cli.DeleteSession(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestSendMessage(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sessions/s1/messages" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var req struct {
			Content string `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Content != "hello" {
			t.Errorf("unexpected message request: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"turn_id":"turn-abc"}`))
	}))
	defer done()

	turnID, err := cli.SendMessage(context.Background(), "s1", "hello")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if turnID != "turn-abc" {
		t.Fatalf("unexpected turn id %q", turnID)
	}
}

func TestSendMessageBusy(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"session_busy"}`))
	}))
	defer done()

	_, err := cli.SendMessage(context.Background(), "s1", "hi")
	if !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("expected ErrSessionBusy, got %v", err)
	}
}

// serveEvents writes one canned event-stream response: a sync frame, a
// turn's events with sequence ids and turn ids, and keepalive comments.
func serveEvents(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "id: 4\nevent: sync\ndata: {\"turn_id\":\"turn-abc\",\"seq\":4,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"},{\"role\":\"assistant\",\"content\":\"hello\"}],\"persisted\":[{\"path\":\"report.txt\",\"object_key\":\"sessions/s1/persisted/report.txt\",\"size\":10,\"at\":\"2026-09-02T01:00:00Z\"}]}\n\n")
	fmt.Fprint(w, ": keepalive\n\n")
	fmt.Fprint(w, "id: 1\nevent: turn_started\ndata: {\"turn_id\":\"turn-abc\"}\n\n")
	fmt.Fprint(w, "id: 2\nevent: delta\ndata: {\"turn_id\":\"turn-abc\",\"text\":\"Hel\"}\n\n")
	fmt.Fprint(w, "id: 3\nevent: tool_call\ndata: {\"turn_id\":\"turn-abc\",\"id\":\"c1\",\"name\":\"shell\",\"status\":\"running\"}\n\n")
	fmt.Fprint(w, "id: 4\nevent: turn_finished\ndata: {\"turn_id\":\"turn-abc\",\"status\":\"done\",\"text\":\"Hello\"}\n")
}

func TestEventsStream(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/sessions/s1/events" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		serveEvents(w)
	}))
	defer done()

	stream, err := cli.Events(context.Background(), "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}

	var events []Event
	for ev := range stream.Events() {
		events = append(events, ev)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}

	want := []Event{
		{Type: EventSync, Seq: 4, TurnID: "turn-abc", Messages: []SyncMessage{
			{Role: "user", Content: "hi"},
			{Role: "assistant", Content: "hello"},
		}, Persisted: []PersistedObject{
			{Path: "report.txt", ObjectKey: "sessions/s1/persisted/report.txt", Size: 10, At: time.Date(2026, 9, 2, 1, 0, 0, 0, time.UTC)},
		}},
		{Type: EventTurnStarted, Seq: 1, TurnID: "turn-abc"},
		{Type: EventDelta, Seq: 2, TurnID: "turn-abc", Text: "Hel"},
		{Type: EventToolCall, Seq: 3, TurnID: "turn-abc", ID: "c1", Name: "shell", Status: ToolCallRunning},
		{Type: EventTurnFinished, Seq: 4, TurnID: "turn-abc", Status: TurnStatusDone, Text: "Hello"},
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(events), len(want), events)
	}
	for i := range want {
		if !reflect.DeepEqual(events[i], want[i]) {
			t.Errorf("event %d: got %+v, want %+v", i, events[i], want[i])
		}
	}
}

func TestEventsResumePassesLastSeq(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("last_seq"); got != "42" {
			t.Errorf("expected last_seq=42, got %q", got)
		}
		serveEvents(w)
	}))
	defer done()

	stream, err := cli.Events(context.Background(), "s1", 42)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for range stream.Events() {
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}
}

func TestEventsServerErrorEvent(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 7\nevent: turn_finished\ndata: {\"turn_id\":\"turn-abc\",\"status\":\"error\",\"message\":\"boom\"}\n\n")
	}))
	defer done()

	stream, err := cli.Events(context.Background(), "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	ev, ok := <-stream.Events()
	if !ok {
		t.Fatal("stream closed without events")
	}
	if ev.Type != EventTurnFinished || ev.Status != TurnStatusError || ev.Message != "boom" || ev.Seq != 7 || ev.TurnID != "turn-abc" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	for range stream.Events() {
	}
	if stream.Err() != nil {
		t.Fatalf("stream error: %v", stream.Err())
	}
}

func TestStreamClose(t *testing.T) {
	release := make(chan struct{})
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release // never released; Close must unblock the reader
	}))
	defer done()
	defer close(release)

	stream, err := cli.Events(context.Background(), "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for range stream.Events() {
	}
	if stream.Err() != nil {
		t.Fatalf("stream error after Close: %v", stream.Err())
	}
}

func TestEventsContextCancel(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // block until the client goes away
	}))
	defer done()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.Events(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-stream.Events():
			if !ok {
				if stream.Err() == nil {
					t.Fatal("expected stream error after cancel")
				}
				return
			}
		case <-deadline:
			t.Fatal("stream did not end after cancel")
		}
	}
}

func TestReportToolResult(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/sessions/s1/tool_results" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]json.RawMessage
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if string(req["call_id"]) != `"c1"` || string(req["result"]) != `{"temp":31}` {
			t.Errorf("unexpected body: %s", body)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}))
	defer done()

	err := cli.ReportToolResult(context.Background(), "s1", "c1", map[string]int{"temp": 31})
	if err != nil {
		t.Fatalf("report tool result: %v", err)
	}
}

func TestReportToolError(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]string
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req["call_id"] != "c2" || req["error"] != "deploy failed" {
			t.Errorf("unexpected body: %s", body)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer done()

	err := cli.ReportToolError(context.Background(), "s1", "c2", errors.New("deploy failed"))
	if err != nil {
		t.Fatalf("report tool error: %v", err)
	}
}

func TestEventsBadFrameIDFallsBackToPayloadSeq(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: notanumber\nevent: sync\ndata: {\"turn_id\":\"\",\"seq\":9}\n\n")
	}))
	defer done()

	stream, err := cli.Events(context.Background(), "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	ev, ok := <-stream.Events()
	if !ok {
		t.Fatal("stream closed without events")
	}
	if ev.Seq != 9 {
		t.Fatalf("expected payload seq 9, got %d", ev.Seq)
	}
	for range stream.Events() {
	}
	if stream.Err() != nil {
		t.Fatalf("stream error: %v", stream.Err())
	}
}

func TestStreamCloseUnblocksProducer(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f, _ := w.(http.Flusher)
		// Push events until the connection dies; the client below stops
		// draining, so the run goroutine blocks on send once the buffer
		// fills. Close must unblock it.
		for i := 1; ; i++ {
			if _, err := fmt.Fprintf(w, "id: %d\nevent: delta\ndata: {\"turn_id\":\"t\",\"text\":\"x\"}\n\n", i); err != nil {
				return
			}
			if f != nil {
				f.Flush()
			}
		}
	}))
	defer done()

	stream, err := cli.Events(context.Background(), "s1", 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	// Read a few events, then abandon the stream so the buffer fills.
	for i := 0; i < 4; i++ {
		if _, ok := <-stream.Events(); !ok {
			t.Fatal("stream closed early")
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-stream.Events():
			if !ok {
				if stream.Err() != nil {
					t.Fatalf("stream error after Close: %v", stream.Err())
				}
				return
			}
		case <-deadline:
			t.Fatal("events channel did not close after Close")
		}
	}
}
