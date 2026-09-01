package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// sseServer returns an httptest server answering with the given SSE body.
func sseServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestStreamContentDeltas(t *testing.T) {
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"content\":\", world\"}}]}\n\n"+
		"data: [DONE]\n\n")

	var events []Event
	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, func(ev Event) {
		events = append(events, ev)
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Role != "assistant" {
		t.Errorf("Role = %q, want %q", msg.Role, "assistant")
	}
	if msg.Content != "Hello, world" {
		t.Errorf("Content = %q, want %q", msg.Content, "Hello, world")
	}
	if len(events) != 2 {
		t.Fatalf("events = %v, want 2 events", events)
	}
	for i, want := range []string{"Hello", ", world"} {
		if events[i].Type != EventDelta || events[i].Text != want {
			t.Errorf("events[%d] = %+v, want {EventDelta %q}", i, events[i], want)
		}
	}
}

func TestStreamReasoningDeltas(t *testing.T) {
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think \"}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"reasoning\":\"hard\"}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"}}]}\n\n"+
		"data: [DONE]\n\n")

	var events []Event
	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, func(ev Event) {
		events = append(events, ev)
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Content != "answer" {
		t.Errorf("Content = %q, want %q", msg.Content, "answer")
	}
	if len(events) != 3 {
		t.Fatalf("events = %v, want 3 events", events)
	}
	if events[0].Type != EventReasoning || events[0].Text != "think " {
		t.Errorf("events[0] = %+v, want reasoning %q", events[0], "think ")
	}
	if events[1].Type != EventReasoning || events[1].Text != "hard" {
		t.Errorf("events[1] = %+v, want reasoning %q", events[1], "hard")
	}
	if events[2].Type != EventDelta || events[2].Text != "answer" {
		t.Errorf("events[2] = %+v, want delta %q", events[2], "answer")
	}
}

func TestStreamFragmentedToolCalls(t *testing.T) {
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"city\\\":\"}}]}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_2\",\"function\":{\"name\":\"get_time\",\"arguments\":\"{\\\"tz\\\":\\\"UTC\\\"}\"}}]}}]}\n\n"+
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\" \\\"Paris\\\"}\"}}]}}]}\n\n"+
		"data: {\"choices\":[{\"finish_reason\":\"tool_calls\",\"delta\":{}}]}\n\n"+
		"data: [DONE]\n\n")

	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(msg.ToolCalls) != 2 {
		t.Fatalf("ToolCalls = %+v, want 2 calls", msg.ToolCalls)
	}
	first, second := msg.ToolCalls[0], msg.ToolCalls[1]
	if first.ID != "call_1" || first.Name != "get_weather" {
		t.Errorf("ToolCalls[0] = %+v, want id call_1 name get_weather", first)
	}
	if string(first.Arguments) != `{"city": "Paris"}` {
		t.Errorf("ToolCalls[0].Arguments = %s, want %s", first.Arguments, `{"city": "Paris"}`)
	}
	if second.ID != "call_2" || second.Name != "get_time" {
		t.Errorf("ToolCalls[1] = %+v, want id call_2 name get_time", second)
	}
	if string(second.Arguments) != `{"tz":"UTC"}` {
		t.Errorf("ToolCalls[1].Arguments = %s, want %s", second.Arguments, `{"tz":"UTC"}`)
	}
}

func TestStreamUsageFromFinalChunk(t *testing.T) {
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
		"data: {\"choices\":[{\"finish_reason\":\"stop\",\"delta\":{}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12}}\n\n"+
		"data: [DONE]\n\n")

	msg, usage, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Content != "hi" {
		t.Errorf("Content = %q, want %q", msg.Content, "hi")
	}
	want := Usage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}
	if usage != want {
		t.Errorf("Usage = %+v, want %+v", usage, want)
	}
}

func TestStreamNon2xxError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"message":"rate limit exceeded"}}`)
	}))
	t.Cleanup(srv.Close)

	_, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err == nil {
		t.Fatal("Stream: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error %q does not contain status 429", err)
	}
	if !strings.Contains(err.Error(), "rate limit exceeded") {
		t.Errorf("error %q does not contain response body", err)
	}
}

func TestStreamNonStreamingFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"plain answer","tool_calls":[{"id":"call_9","type":"function","function":{"name":"noop","arguments":"{}"}}]},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`)
	}))
	t.Cleanup(srv.Close)

	msg, usage, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Role != "assistant" || msg.Content != "plain answer" {
		t.Errorf("Message = %+v, want assistant %q", msg, "plain answer")
	}
	if len(msg.ToolCalls) != 1 || msg.ToolCalls[0].ID != "call_9" || msg.ToolCalls[0].Name != "noop" {
		t.Errorf("ToolCalls = %+v, want one noop call_9", msg.ToolCalls)
	}
	want := Usage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7}
	if usage != want {
		t.Errorf("Usage = %+v, want %+v", usage, want)
	}
}

func TestStreamRequestShape(t *testing.T) {
	var (
		capturedBody    []byte
		capturedHeaders http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)

	_, _, err := Stream(context.Background(), Request{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer test-key", "X-Extra": "yes"},
		Model:   "qwen3-8b",
		Messages: []Message{
			{Role: "system", Content: "you are helpful"},
			{Role: "assistant", Content: "", ToolCalls: []ToolCall{
				{ID: "call_1", Name: "get_weather", Arguments: json.RawMessage(`{"city":"Paris"}`)},
			}},
			{Role: "tool", Content: "sunny", ToolCallID: "call_1"},
		},
		Tools: []ToolDef{
			{Name: "get_weather", Description: "Get weather", Parameters: map[string]any{"type": "object"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	if got := capturedHeaders.Get("Authorization"); got != "Bearer test-key" {
		t.Errorf("Authorization header = %q, want %q", got, "Bearer test-key")
	}
	if got := capturedHeaders.Get("X-Extra"); got != "yes" {
		t.Errorf("X-Extra header = %q, want %q", got, "yes")
	}
	if got := capturedHeaders.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type header = %q, want %q", got, "application/json")
	}

	var body struct {
		Model    string          `json:"model"`
		Messages []messageWire   `json:"messages"`
		Tools    []toolDefWire   `json:"tools"`
		Stream   bool            `json:"stream"`
		Options  json.RawMessage `json:"stream_options"`
	}
	if err := json.Unmarshal(capturedBody, &body); err != nil {
		t.Fatalf("unmarshal captured body: %v\nbody: %s", err, capturedBody)
	}
	if body.Model != "qwen3-8b" {
		t.Errorf("model = %q, want %q", body.Model, "qwen3-8b")
	}
	if !body.Stream {
		t.Error("stream = false, want true")
	}
	var options struct {
		IncludeUsage bool `json:"include_usage"`
	}
	if err := json.Unmarshal(body.Options, &options); err != nil || !options.IncludeUsage {
		t.Errorf("stream_options = %s, want {\"include_usage\":true}", body.Options)
	}
	if len(body.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(body.Messages))
	}
	assistant := body.Messages[1]
	if len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant tool_calls = %+v, want 1 call", assistant.ToolCalls)
	}
	tc := assistant.ToolCalls[0]
	if tc.Type != "function" || tc.ID != "call_1" || tc.Function.Name != "get_weather" {
		t.Errorf("tool_call = %+v, want function call_1 get_weather", tc)
	}
	if tc.Function.Arguments != `{"city":"Paris"}` {
		t.Errorf("tool_call arguments = %q, want JSON string %q", tc.Function.Arguments, `{"city":"Paris"}`)
	}
	if body.Messages[2].ToolCallID != "call_1" {
		t.Errorf("tool message tool_call_id = %q, want %q", body.Messages[2].ToolCallID, "call_1")
	}
	if len(body.Tools) != 1 || body.Tools[0].Type != "function" || body.Tools[0].Function.Name != "get_weather" {
		t.Errorf("tools = %+v, want one function get_weather", body.Tools)
	}
}

func TestStreamContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done() // never send [DONE]; hang until the client gives up
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := Stream(ctx, Request{URL: srv.URL, Model: "m"}, nil)
	if err == nil {
		t.Fatal("Stream: expected error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}
