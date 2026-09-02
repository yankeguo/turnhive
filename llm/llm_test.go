package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want errors.Is context.Canceled", err)
	}
}

func TestStreamEOFWithoutDone(t *testing.T) {
	// The connection drops mid-stream: no [DONE], no finish_reason. The
	// partial message must not be accepted as a complete reply.
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n")

	_, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err == nil {
		t.Fatal("Stream: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "stream ended without [DONE]") {
		t.Errorf("error = %q, want it to mention a missing [DONE]", err)
	}
}

func TestStreamEOFAfterFinishReason(t *testing.T) {
	// Endpoints that omit [DONE] but send finish_reason are tolerated.
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"+
		"data: {\"choices\":[{\"finish_reason\":\"stop\",\"delta\":{}}]}\n\n")

	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Content != "hi" {
		t.Errorf("Content = %q, want %q", msg.Content, "hi")
	}
}

func TestStreamErrorChunk(t *testing.T) {
	// Some endpoints report failures as `data: {"error": {...}}` inside a
	// 200 SSE stream.
	srv := sseServer(t, ""+
		"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"+
		"data: {\"error\":{\"message\":\"model overloaded\",\"type\":\"server_error\"}}\n\n")

	_, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err == nil {
		t.Fatal("Stream: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "model overloaded") {
		t.Errorf("error = %q, want it to contain the upstream error message", err)
	}
}

func TestMessageImagesWire(t *testing.T) {
	// A user message with images marshals as a content-part array.
	msg := Message{Role: "user", Content: "what is this?", Images: []string{"data:image/png;base64,AAAA"}}
	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(raw)
	if !strings.Contains(s, `"type":"text"`) || !strings.Contains(s, `"type":"image_url"`) || !strings.Contains(s, `"url":"data:image/png;base64,AAAA"`) {
		t.Fatalf("unexpected wire form: %s", s)
	}

	// Round-trip: text parts concatenate, image parts collect.
	var back Message
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Content != "what is this?" || len(back.Images) != 1 || back.Images[0] != "data:image/png;base64,AAAA" {
		t.Fatalf("round-trip mismatch: %+v", back)
	}

	// Text-only messages keep the plain-string content form.
	raw, _ = json.Marshal(Message{Role: "user", Content: "hi"})
	if !strings.Contains(string(raw), `"content":"hi"`) {
		t.Fatalf("text message must use string content: %s", raw)
	}
}

// rateLimitServer answers the first `limited` requests with 429 (and the
// given Retry-After header), then streams a one-delta completion. It
// records the number of requests.
func rateLimitServer(t *testing.T, limited int, retryAfter string) (*httptest.Server, *int) {
	t.Helper()
	count := new(int)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*count++
		if *count <= limited {
			if retryAfter != "" {
				w.Header().Set("Retry-After", retryAfter)
			}
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"message":"slow down"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv, count
}

func TestStreamRateLimitRetry(t *testing.T) {
	srv, count := rateLimitServer(t, 2, "0")

	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if msg.Content != "ok" {
		t.Errorf("Content = %q, want %q", msg.Content, "ok")
	}
	if *count != 3 {
		t.Errorf("requests = %d, want 3 (2 rate limited + 1 success)", *count)
	}
}

func TestStreamRateLimitExhausted(t *testing.T) {
	srv, count := rateLimitServer(t, maxRateLimitRetries+10, "0")

	_, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err == nil || !strings.Contains(err.Error(), "status 429") {
		t.Fatalf("expected a 429 error, got %v", err)
	}
	if *count != maxRateLimitRetries+1 {
		t.Errorf("requests = %d, want %d (1 + %d retries)", *count, maxRateLimitRetries+1, maxRateLimitRetries)
	}
}

func TestStreamRateLimitDefaultBackoff(t *testing.T) {
	// No Retry-After header: exponential backoff from rateLimitBaseWait.
	old := rateLimitBaseWait
	rateLimitBaseWait = time.Millisecond
	t.Cleanup(func() { rateLimitBaseWait = old })

	srv, count := rateLimitServer(t, 1, "")
	msg, _, err := Stream(context.Background(), Request{URL: srv.URL, Model: "m"}, nil)
	if err != nil || msg.Content != "ok" {
		t.Fatalf("Stream: msg=%+v err=%v", msg, err)
	}
	if *count != 2 {
		t.Errorf("requests = %d, want 2", *count)
	}
}

func TestStreamRateLimitWaitCancelled(t *testing.T) {
	// A long Retry-After wait is abandoned when the context is cancelled.
	srv, _ := rateLimitServer(t, 100, "30")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, _, err := Stream(ctx, Request{URL: srv.URL, Model: "m"}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
