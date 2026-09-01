package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/yankeguo/turnhive/llm"
)

// fakeReporter records every Reporter call.
type fakeReporter struct {
	mu         sync.Mutex
	deltas     []string
	reasoning  []string
	toolCalls  []ToolCallEvent
	doneText   string
	doneCalled bool
	errText    string
	errCalled  bool
}

func (r *fakeReporter) Delta(text string) {
	r.mu.Lock()
	r.deltas = append(r.deltas, text)
	r.mu.Unlock()
}
func (r *fakeReporter) ReasoningDelta(text string) {
	r.mu.Lock()
	r.reasoning = append(r.reasoning, text)
	r.mu.Unlock()
}
func (r *fakeReporter) ToolCall(ev ToolCallEvent) {
	r.mu.Lock()
	r.toolCalls = append(r.toolCalls, ev)
	r.mu.Unlock()
}
func (r *fakeReporter) Done(text string) {
	r.mu.Lock()
	r.doneText, r.doneCalled = text, true
	r.mu.Unlock()
}
func (r *fakeReporter) Error(msg string) {
	r.mu.Lock()
	r.errText, r.errCalled = msg, true
	r.mu.Unlock()
}

// fakeStream serves queued stream behaviors and records requests.
type fakeStream struct {
	mu       sync.Mutex
	queue    []streamFunc
	requests []llm.Request
}

type streamFunc func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error)

func (f *fakeStream) push(fn streamFunc) {
	f.mu.Lock()
	f.queue = append(f.queue, fn)
	f.mu.Unlock()
}

// textReply queues a plain assistant text reply.
func (f *fakeStream) textReply(text string) {
	f.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		if onEvent != nil {
			onEvent(llm.Event{Type: llm.EventDelta, Text: text})
		}
		return llm.Message{Role: "assistant", Content: text}, llm.Usage{}, nil
	})
}

// toolCallReply queues an assistant reply carrying tool calls.
func (f *fakeStream) toolCallReply(calls ...llm.ToolCall) {
	f.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		return llm.Message{Role: "assistant", ToolCalls: calls}, llm.Usage{}, nil
	})
}

func (f *fakeStream) stream(ctx context.Context, req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	if len(f.queue) == 0 {
		f.mu.Unlock()
		return llm.Message{}, llm.Usage{}, errors.New("fake stream: no queued behavior")
	}
	fn := f.queue[0]
	f.queue = f.queue[1:]
	f.mu.Unlock()
	return fn(req, onEvent)
}

func (f *fakeStream) lastRequest() llm.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

// fakeHistory is an in-memory HistoryStore.
type fakeHistory struct {
	mu    sync.Mutex
	msgs  []llm.Message
	saved [][]llm.Message
}

func (h *fakeHistory) Load(context.Context) ([]llm.Message, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]llm.Message(nil), h.msgs...), nil
}

func (h *fakeHistory) Save(_ context.Context, msgs []llm.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.saved = append(h.saved, append([]llm.Message(nil), msgs...))
	return nil
}

func (h *fakeHistory) lastSaved() []llm.Message {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.saved) == 0 {
		return nil
	}
	return h.saved[len(h.saved)-1]
}

// newTestLoop builds a Loop with the fake stream installed.
func newTestLoop(cfg LoopConfig, fs *fakeStream) *Loop {
	l := NewLoop(cfg)
	l.stream = fs.stream
	return l
}

func TestLoopSingleTextReply(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("hello there")
	hist := &fakeHistory{msgs: []llm.Message{
		{Role: "user", Content: "earlier question"},
		{Role: "assistant", Content: "earlier answer"},
	}}
	l := newTestLoop(LoopConfig{
		ModelName:    "test-model",
		SystemPrompt: "you are a test",
		History:      hist,
	}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "hi", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !r.doneCalled || r.doneText != "hello there" {
		t.Fatalf("expected Done(hello there), got %+v", r)
	}
	if r.errCalled {
		t.Fatalf("unexpected Error call: %s", r.errText)
	}

	// The request is [system] + loaded history + user.
	req := fs.lastRequest()
	roles := []string{}
	for _, m := range req.Messages {
		roles = append(roles, m.Role)
	}
	if strings.Join(roles, ",") != "system,user,assistant,user" {
		t.Fatalf("unexpected request roles %v", roles)
	}
	if req.Messages[0].Content != "you are a test" || req.Messages[3].Content != "hi" {
		t.Fatalf("unexpected request messages %+v", req.Messages)
	}
	if len(req.Tools) != 0 {
		t.Fatalf("expected no tools, got %d", len(req.Tools))
	}

	// History saved with the appended {user, assistant} pair only.
	saved := hist.lastSaved()
	if len(saved) != 4 || saved[2].Role != "user" || saved[3].Role != "assistant" || saved[3].Content != "hello there" {
		t.Fatalf("unexpected saved history %+v", saved)
	}
	if msgs := l.Messages(); len(msgs) != 4 {
		t.Fatalf("Messages() = %d messages", len(msgs))
	}
}

func TestLoopSandboxToolCall(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "shell", Arguments: json.RawMessage(`{"command": "echo from-tool"}`)})
	fs.textReply("all done")
	hist := &fakeHistory{}
	l := newTestLoop(LoopConfig{
		ModelName: "test-model",
		Sandbox:   sb,
		History:   hist,
	}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "run something", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !r.doneCalled || r.doneText != "all done" {
		t.Fatalf("expected Done(all done), got %+v", r)
	}

	// The tool was reported running then done.
	if len(r.toolCalls) != 2 || r.toolCalls[0].Status != ToolCallRunning || r.toolCalls[1].Status != ToolCallDone {
		t.Fatalf("unexpected tool call events %+v", r.toolCalls)
	}
	if r.toolCalls[0].ID != "c1" || r.toolCalls[0].Name != "shell" {
		t.Fatalf("unexpected tool call event %+v", r.toolCalls[0])
	}

	// The second request carries the assistant tool call and the tool
	// result with the command output.
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil || toolMsg.ToolCallID != "c1" || toolMsg.Content != "from-tool\n(exit code: 0)" {
		t.Fatalf("expected tool result in next request, got %+v", req.Messages)
	}
	foundAssistantCall := false
	for _, m := range req.Messages {
		if m.Role == "assistant" && len(m.ToolCalls) == 1 && m.ToolCalls[0].Name == "shell" {
			foundAssistantCall = true
		}
	}
	if !foundAssistantCall {
		t.Fatalf("expected assistant tool-call message in next request, got %+v", req.Messages)
	}

	// History holds only the {user, assistant} pair: tool exchanges are
	// transient.
	saved := hist.lastSaved()
	if len(saved) != 2 || saved[0].Role != "user" || saved[1].Role != "assistant" || saved[1].Content != "all done" {
		t.Fatalf("unexpected saved history %+v", saved)
	}
	if len(saved[1].ToolCalls) != 0 {
		t.Fatalf("assistant history message must not carry tool calls: %+v", saved[1])
	}
}

// fakeWaiter answers external tool results from a map.
type fakeWaiter struct {
	results map[string]json.RawMessage
	errs    map[string]string
}

func (w *fakeWaiter) WaitToolResult(ctx context.Context, callID string) (json.RawMessage, string, error) {
	if e, ok := w.errs[callID]; ok {
		return nil, e, nil
	}
	if r, ok := w.results[callID]; ok {
		return r, "", nil
	}
	return nil, "", errors.New("no result for " + callID)
}

func TestLoopExternalTool(t *testing.T) {
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c9", Name: "get_weather", Arguments: json.RawMessage(`{"city":"Paris"}`)})
	fs.textReply("sunny")
	waiter := &fakeWaiter{results: map[string]json.RawMessage{"c9": json.RawMessage(`{"temp": 20}`)}}
	l := newTestLoop(LoopConfig{
		ModelName: "test-model",
		ExternalTools: []ExternalToolSpec{{
			Name:        "get_weather",
			Description: "Get the weather",
			Parameters:  jsonSchema(map[string]any{"city": stringProp("City")}, "city"),
		}},
		Waiter: waiter,
	}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "weather?", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !r.doneCalled || r.doneText != "sunny" {
		t.Fatalf("expected Done(sunny), got %+v", r)
	}

	// The raw JSON result went back as the tool message.
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil || toolMsg.Content != `{"temp": 20}` {
		t.Fatalf("expected external result in next request, got %+v", req.Messages)
	}

	// The external tool definition was advertised.
	if len(fs.requests[0].Tools) != 1 || fs.requests[0].Tools[0].Name != "get_weather" {
		t.Fatalf("expected external tool def, got %+v", fs.requests[0].Tools)
	}
}

func TestLoopExternalToolError(t *testing.T) {
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "boom", Arguments: json.RawMessage(`{}`)})
	fs.textReply("recovered")
	waiter := &fakeWaiter{errs: map[string]string{"c1": "it broke"}}
	l := newTestLoop(LoopConfig{
		ModelName:     "test-model",
		ExternalTools: []ExternalToolSpec{{Name: "boom", Parameters: map[string]any{}}},
		Waiter:        waiter,
	}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "go", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if len(r.toolCalls) != 2 || r.toolCalls[1].Status != ToolCallError {
		t.Fatalf("expected tool error status, got %+v", r.toolCalls)
	}
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil || !strings.Contains(toolMsg.Content, "it broke") {
		t.Fatalf("expected error text fed back, got %+v", req.Messages)
	}
}

func TestLoopUnknownTool(t *testing.T) {
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "nope", Arguments: json.RawMessage(`{}`)})
	fs.textReply("ok")
	l := newTestLoop(LoopConfig{ModelName: "test-model"}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "go", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil || !strings.Contains(toolMsg.Content, "unknown tool: nope") {
		t.Fatalf("expected unknown-tool text fed back, got %+v", req.Messages)
	}
}

func TestLoopBusy(t *testing.T) {
	fs := &fakeStream{}
	entered := make(chan struct{})
	release := make(chan struct{})
	fs.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		close(entered)
		<-release
		return llm.Message{Role: "assistant", Content: "late"}, llm.Usage{}, nil
	})
	l := newTestLoop(LoopConfig{ModelName: "test-model"}, fs)

	first := make(chan error, 1)
	go func() { first <- l.RunTurn(context.Background(), "one", &fakeReporter{}) }()
	<-entered

	if err := l.RunTurn(context.Background(), "two", &fakeReporter{}); !errors.Is(err, ErrBusy) {
		t.Fatalf("expected ErrBusy, got %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first turn: %v", err)
	}
}

func TestLoopMaxSteps(t *testing.T) {
	fs := &fakeStream{}
	l := newTestLoop(LoopConfig{ModelName: "test-model"}, fs)
	// Always answer with a tool call; the unknown tool keeps each step
	// cheap (error text fed back).
	l.stream = func(ctx context.Context, req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		fs.mu.Lock()
		fs.requests = append(fs.requests, req)
		fs.mu.Unlock()
		return llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{
			{ID: "c", Name: "nope", Arguments: json.RawMessage(`{}`)},
		}}, llm.Usage{}, nil
	}

	r := &fakeReporter{}
	err := l.RunTurn(context.Background(), "go", r)
	if err == nil || !strings.Contains(err.Error(), "max steps exceeded") {
		t.Fatalf("expected max steps error, got %v", err)
	}
	if !r.errCalled || r.errText != "max steps exceeded" {
		t.Fatalf("expected Error(max steps exceeded), got %+v", r)
	}
	if len(fs.requests) != maxTurnSteps {
		t.Fatalf("expected %d steps, got %d", maxTurnSteps, len(fs.requests))
	}
}

func TestLoopStreamErrorPersistsPartial(t *testing.T) {
	fs := &fakeStream{}
	fs.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		if onEvent != nil {
			onEvent(llm.Event{Type: llm.EventDelta, Text: "partial"})
		}
		return llm.Message{}, llm.Usage{}, context.DeadlineExceeded
	})
	hist := &fakeHistory{}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, fs)

	r := &fakeReporter{}
	err := l.RunTurn(context.Background(), "go", r)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if !r.errCalled {
		t.Fatalf("expected Error call")
	}
	saved := hist.lastSaved()
	if len(saved) != 2 || saved[0].Role != "user" || saved[1].Role != "assistant" || saved[1].Content != "partial" {
		t.Fatalf("expected partial text persisted, got %+v", saved)
	}
}

func TestLoopShellToolSpill(t *testing.T) {
	sb, f := newFakeIronhive(t)
	fs := &fakeStream{}
	// Produce output beyond the strict limit; the full output must be
	// spilled to a sandbox file and only a preview plus the file path go
	// back to the model.
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "shell", Arguments: json.RawMessage(`{"command": "seq 1 5000"}`)})
	fs.textReply("done")
	l := newTestLoop(LoopConfig{ModelName: "test-model", Sandbox: sb}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "go", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil {
		t.Fatalf("no tool message in %+v", req.Messages)
	}
	const spillPath = spillDir + "/shell-0001.txt"
	if !strings.Contains(toolMsg.Content, "truncated...") || !strings.Contains(toolMsg.Content, "The full output was saved to: "+spillPath) {
		t.Fatalf("expected spilled tool output, got (tail) %q", toolMsg.Content[len(toolMsg.Content)-300:])
	}
	if len(toolMsg.Content) > StrictMaxBytes+4096 {
		t.Fatalf("tool output not bounded: %d bytes", len(toolMsg.Content))
	}
	// The spilled file really holds the full output in the sandbox.
	full, err := os.ReadFile(f.local(spillPath))
	if err != nil {
		t.Fatalf("read spill file: %v", err)
	}
	if !strings.Contains(string(full), "5000") {
		t.Fatalf("spill file incomplete: %d bytes", len(full))
	}
}
