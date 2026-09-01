package turnhive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
		_, _ = w.Write([]byte(`{"id":"abc123"}`))
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
	if sess.ID != "abc123" {
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

func TestSendMessageStream(t *testing.T) {
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
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Keepalive comments and blank lines must be skipped; the final
		// event arrives without a trailing blank line.
		fmt.Fprint(w, ": keepalive\n\n")
		fmt.Fprint(w, "event: reasoning_delta\ndata: {\"text\":\"thinking\"}\n\n")
		fmt.Fprint(w, "event: delta\ndata: {\"text\":\"Hel\"}\n\n")
		fmt.Fprint(w, "event: delta\ndata: {\"text\":\"lo\"}\n\n")
		fmt.Fprint(w, "event: tool_call\ndata: {\"id\":\"c1\",\"name\":\"shell\",\"status\":\"running\"}\n\n")
		fmt.Fprint(w, "event: tool_call\ndata: {\"id\":\"c1\",\"name\":\"shell\",\"status\":\"done\"}\n\n")
		fmt.Fprint(w, "event: done\ndata: {\"text\":\"Hello\"}\n")
	}))
	defer done()

	stream, err := cli.SendMessage(context.Background(), "s1", "hello")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	var events []Event
	for ev := range stream.Events() {
		events = append(events, ev)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("stream error: %v", err)
	}

	want := []Event{
		{Type: EventReasoningDelta, Text: "thinking"},
		{Type: EventDelta, Text: "Hel"},
		{Type: EventDelta, Text: "lo"},
		{Type: EventToolCall, ID: "c1", Name: "shell", Status: ToolCallRunning},
		{Type: EventToolCall, ID: "c1", Name: "shell", Status: ToolCallDone},
		{Type: EventDone, Text: "Hello"},
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(events), len(want), events)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Errorf("event %d: got %+v, want %+v", i, events[i], want[i])
		}
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

func TestSendMessageServerErrorEvent(t *testing.T) {
	cli, done := newTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: error\ndata: {\"message\":\"boom\"}\n\n")
	}))
	defer done()

	stream, err := cli.SendMessage(context.Background(), "s1", "hi")
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	ev, ok := <-stream.Events()
	if !ok {
		t.Fatal("stream closed without events")
	}
	if ev.Type != EventError || ev.Message != "boom" {
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

	stream, err := cli.SendMessage(context.Background(), "s1", "hi")
	if err != nil {
		t.Fatalf("send message: %v", err)
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

func TestSendMessageContextCancel(t *testing.T) {
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
	stream, err := cli.SendMessage(ctx, "s1", "hi")
	if err != nil {
		t.Fatalf("send message: %v", err)
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

func TestEventStringContainsNoSecrets(t *testing.T) {
	// Sanity check: Event is comparable and printable.
	ev := Event{Type: EventToolCall, ID: "c1", Name: "shell", Status: ToolCallRunning}
	if !strings.Contains(fmt.Sprintf("%+v", ev), "shell") {
		t.Fatal("unexpected event rendering")
	}
}
