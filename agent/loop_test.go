package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	f.textReplyUsage(text, llm.Usage{})
}

// textReplyUsage queues a plain assistant text reply with usage.
func (f *fakeStream) textReplyUsage(text string, usage llm.Usage) {
	f.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		if onEvent != nil {
			onEvent(llm.Event{Type: llm.EventDelta, Text: text})
		}
		return llm.Message{Role: "assistant", Content: text}, usage, nil
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
	hist := &fakeHistory{}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, fs)
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
	// Like any failed turn, the {user, assistant-partial} pair is
	// persisted.
	saved := hist.lastSaved()
	if len(saved) != 2 || saved[0].Role != "user" || saved[1].Role != "assistant" {
		t.Fatalf("expected {user, assistant} pair persisted, got %+v", saved)
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

func TestLoopLoadMedia(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	st := newSandboxTools(sb)
	if err := st.sb.PutFile(context.Background(), "dot.png", bytes.NewReader(tinyPNG), nil); err != nil {
		t.Fatalf("put image: %v", err)
	}
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "load_media", Arguments: json.RawMessage(`{"file_path": "dot.png"}`)})
	fs.textReply("a single pixel")
	l := newTestLoop(LoopConfig{ModelName: "test-model", Sandbox: sb, SupportImage: true}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "look at dot.png", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !r.doneCalled || r.doneText != "a single pixel" {
		t.Fatalf("expected Done, got %+v", r)
	}

	// The second request carries the tool message followed by a user
	// message with the image.
	req := fs.lastRequest()
	toolIdx, imageIdx := -1, -1
	for i, m := range req.Messages {
		if m.Role == "tool" && m.ToolCallID == "c1" {
			toolIdx = i
		}
		if m.Role == "user" && len(m.Images) == 1 {
			imageIdx = i
		}
	}
	if toolIdx < 0 || imageIdx != toolIdx+1 {
		t.Fatalf("expected image user message right after the tool message, got %+v", req.Messages)
	}
	if !strings.Contains(req.Messages[toolIdx].Content, "Image loaded: dot.png") {
		t.Fatalf("unexpected tool result %q", req.Messages[toolIdx].Content)
	}
	if !strings.HasPrefix(req.Messages[imageIdx].Images[0], "data:image/png;base64,") {
		t.Fatalf("unexpected image %q", req.Messages[imageIdx].Images[0][:64])
	}

	// The image exchange is transient: history holds only the final
	// {user, assistant} pair without images.
	if msgs := l.Messages(); len(msgs) != 2 || len(msgs[1].Images) != 0 {
		t.Fatalf("history must not carry images: %+v", msgs)
	}
}

func TestLoopLoadMediaGated(t *testing.T) {
	sb, _ := newFakeIronhive(t)
	fs := &fakeStream{}
	fs.textReply("ok")
	l := newTestLoop(LoopConfig{ModelName: "test-model", Sandbox: sb}, fs)
	if err := l.RunTurn(context.Background(), "hi", &fakeReporter{}); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	for _, td := range fs.lastRequest().Tools {
		if td.Name == "load_media" {
			t.Fatalf("load_media must not be advertised without support_image")
		}
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

func TestLoopPreTurnTrim(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("ok")
	// Long history: 4 distinct pairs, each ~200 chars.
	var msgs []llm.Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs,
			llm.Message{Role: "user", Content: fmt.Sprintf("user-%d ", i) + strings.Repeat("u", 200)},
			llm.Message{Role: "assistant", Content: fmt.Sprintf("assistant-%d ", i) + strings.Repeat("a", 200)})
	}
	hist := &fakeHistory{msgs: msgs}
	// Absurd budget (reserve dwarfs the window): only the last old turn
	// may survive — the user's most recent message is never dropped.
	l := newTestLoop(LoopConfig{ModelName: "m", History: hist, MaxContext: 150}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "hi", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	// The request must contain only the last old turn, not the older ones.
	req := fs.lastRequest()
	var requestUsers []string
	for _, m := range req.Messages {
		if m.Role == "user" {
			requestUsers = append(requestUsers, m.Content)
		}
	}
	if len(requestUsers) != 2 || !strings.HasPrefix(requestUsers[0], "user-3 ") {
		t.Fatalf("expected only user-3 plus the new message, got %v", requestUsers)
	}
	// Saved history is the last old pair plus the new one.
	saved := hist.lastSaved()
	if len(saved) != 4 || !strings.HasPrefix(saved[0].Content, "user-3 ") {
		t.Fatalf("unexpected saved history: %d messages, first %q", len(saved), saved[0].Content[:20])
	}
}

func TestLoopPostTurnCompaction(t *testing.T) {
	fs := &fakeStream{}
	// MaxContext=20000: pre-turn budget (12000) fits the small history,
	// usage 17050 crosses the 0.8 overflow threshold (16000).
	fs.textReplyUsage("final answer with enough chars to matter", llm.Usage{PromptTokens: 16850, CompletionTokens: 200})
	var msgs []llm.Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs,
			llm.Message{Role: "user", Content: "question with a fair amount of text in it"},
			llm.Message{Role: "assistant", Content: "answer with a fair amount of text in it"})
	}
	hist := &fakeHistory{msgs: msgs}
	l := newTestLoop(LoopConfig{ModelName: "m", History: hist, MaxContext: 20000}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "wrap up", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	saved := hist.lastSaved()
	// Compacted: summary + last 2 turns (one old pair + the new pair).
	if len(saved) != 5 {
		t.Fatalf("expected summary + 4 messages, got %d", len(saved))
	}
	if saved[0].Role != "user" || !strings.HasPrefix(saved[0].Content, "<context-summary>") {
		t.Fatalf("history must start with the context summary: %+v", saved[0])
	}
	if saved[len(saved)-1].Content != "final answer with enough chars to matter" {
		t.Fatalf("latest reply must be preserved verbatim: %+v", saved[len(saved)-1])
	}
}

// failingHistory fails every Save.
type failingHistory struct {
	msgs []llm.Message
	err  error
}

func (h *failingHistory) Load(context.Context) ([]llm.Message, error) {
	return append([]llm.Message(nil), h.msgs...), nil
}

func (h *failingHistory) Save(context.Context, []llm.Message) error { return h.err }

// ctxCheckHistory records whether the context passed to Save was already
// cancelled.
type ctxCheckHistory struct {
	saved        [][]llm.Message
	ctxCancelled bool
}

func (h *ctxCheckHistory) Load(context.Context) ([]llm.Message, error) { return nil, nil }

func (h *ctxCheckHistory) Save(ctx context.Context, msgs []llm.Message) error {
	h.ctxCancelled = ctx.Err() != nil
	h.saved = append(h.saved, msgs)
	return nil
}

func TestLoopFailTurnSavesWithCancelledContext(t *testing.T) {
	fs := &fakeStream{}
	fs.push(func(req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error) {
		if onEvent != nil {
			onEvent(llm.Event{Type: llm.EventDelta, Text: "partial"})
		}
		return llm.Message{}, llm.Usage{}, context.Canceled
	})
	hist := &ctxCheckHistory{}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, fs)

	// The turn context is already cancelled (session deleted, controller
	// closing, turn timeout): the partial reply must still be persisted.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &fakeReporter{}
	err := l.RunTurn(ctx, "go", r)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// Two saves happen: the write-ahead of the user message (allowed to
	// ride the turn context — a cancelled context just makes it fail,
	// and the final save below still covers the pair) and failTurn's
	// partial-reply save, which must use a detached context.
	if hist.ctxCancelled {
		t.Fatalf("failTurn's save used the cancelled turn context")
	}
	if len(hist.saved) != 2 {
		t.Fatalf("expected write-ahead + final save, got %d saves", len(hist.saved))
	}
	saved := hist.saved[len(hist.saved)-1]
	if len(saved) != 2 || saved[0].Role != "user" || saved[1].Role != "assistant" || saved[1].Content != "partial" {
		t.Fatalf("expected partial pair persisted, got %+v", saved)
	}
}

func TestLoopSaveFailureStillReportsDone(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("hi")
	hist := &failingHistory{err: errors.New("s3 down")}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, fs)

	r := &fakeReporter{}
	err := l.RunTurn(context.Background(), "go", r)
	// The save error is returned for the caller to log, but the turn
	// succeeded: Done was reported and no Error was sent.
	if err == nil || !strings.Contains(err.Error(), "s3 down") {
		t.Fatalf("expected the save error returned, got %v", err)
	}
	if !r.doneCalled || r.doneText != "hi" {
		t.Fatalf("expected Done(hi) before the save, got %+v", r)
	}
	if r.errCalled {
		t.Fatalf("save failure must not be reported as a turn error: %s", r.errText)
	}
	// The in-memory history is authoritative.
	if msgs := l.Messages(); len(msgs) != 2 {
		t.Fatalf("expected in-memory history updated, got %+v", msgs)
	}
}

func TestLoopPreTurnTrimSaveFailureContinues(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("ok")
	// History needing a pre-turn trim, with a failing store.
	hist := &failingHistory{msgs: pairs(4, "x"), err: errors.New("s3 down")}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist, MaxContext: 150}, fs)

	r := &fakeReporter{}
	err := l.RunTurn(context.Background(), "hi", r)
	// The failed trim save did not abort the turn; only the final save
	// error surfaces as the return value.
	if !r.doneCalled || r.doneText != "ok" {
		t.Fatalf("expected the turn to complete despite the failed trim save, got %+v", r)
	}
	if r.errCalled {
		t.Fatalf("save failures must not be reported as turn errors: %s", r.errText)
	}
	if err == nil || !strings.Contains(err.Error(), "s3 down") {
		t.Fatalf("expected the final save error returned, got %v", err)
	}
}

func TestLoopToolErrorTruncated(t *testing.T) {
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "boom", Arguments: json.RawMessage(`{}`)})
	fs.textReply("recovered")
	waiter := &fakeWaiter{errs: map[string]string{"c1": strings.Repeat("x", 64*1024)}}
	l := newTestLoop(LoopConfig{
		ModelName:     "test-model",
		ExternalTools: []ExternalToolSpec{{Name: "boom", Parameters: map[string]any{}}},
		Waiter:        waiter,
	}, fs)

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
	if len(toolMsg.Content) > StrictMaxBytes+4096 {
		t.Fatalf("tool error text not bounded: %d bytes", len(toolMsg.Content))
	}
	if !strings.Contains(toolMsg.Content, "truncated...") {
		t.Fatalf("expected truncation notice, got (tail) %q", toolMsg.Content[len(toolMsg.Content)-300:])
	}
}

// fakeMCPTool records executions and returns a fixed output.
type fakeMCPTool struct {
	name   string
	output string
	calls  [][]byte
}

func (t *fakeMCPTool) Spec() llm.ToolDef {
	return llm.ToolDef{Name: t.name, Parameters: map[string]any{"type": "object"}}
}

func (t *fakeMCPTool) Execute(_ context.Context, _ string, args json.RawMessage) (string, error) {
	t.calls = append(t.calls, append([]byte(nil), args...))
	return t.output, nil
}

func TestLoopMCPToolsMountedPerTurn(t *testing.T) {
	fs := &fakeStream{}
	fs.toolCallReply(llm.ToolCall{ID: "c1", Name: "mcp__ping", Arguments: json.RawMessage(`{"x":1}`)})
	fs.textReply("pong done")

	mcpTool := &fakeMCPTool{name: "mcp__ping", output: "pong"}
	closed := false
	l := newTestLoop(LoopConfig{
		ModelName:  "test-model",
		MCPServers: []MCPServerSpec{{Name: "mcp", URL: "http://127.0.0.1:1"}},
	}, fs)
	// Replace the wired connector with a stub serving the fake tool.
	l.mcpConnect = func(context.Context) ([]Tool, func()) {
		return []Tool{mcpTool}, func() { closed = true }
	}

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "ping", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	if !closed {
		t.Fatal("MCP connections must be closed when the turn ends")
	}
	if len(mcpTool.calls) != 1 || string(mcpTool.calls[0]) != `{"x":1}` {
		t.Fatalf("MCP tool not dispatched with its arguments: %+v", mcpTool.calls)
	}
	// The first request of the turn already carries the MCP tool def.
	found := false
	for _, d := range fs.requests[0].Tools {
		if d.Name == "mcp__ping" {
			found = true
		}
	}
	if !found {
		t.Fatalf("MCP tool def missing from the first request: %+v", fs.requests[0].Tools)
	}
	// The tool result was fed back as a tool message.
	req := fs.lastRequest()
	var toolMsg *llm.Message
	for i := range req.Messages {
		if req.Messages[i].Role == "tool" {
			toolMsg = &req.Messages[i]
		}
	}
	if toolMsg == nil || toolMsg.Content != "pong" {
		t.Fatalf("expected MCP tool result fed back, got %+v", req.Messages)
	}
}

func TestLoopMCPToolNameCollisionSkipped(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("ok")

	shadow := &fakeMCPTool{name: "deploy", output: "shadow"}
	l := newTestLoop(LoopConfig{
		ModelName:     "test-model",
		MCPServers:    []MCPServerSpec{{Name: "mcp", URL: "http://127.0.0.1:1"}},
		ExternalTools: []ExternalToolSpec{{Name: "deploy", Parameters: map[string]any{}}},
	}, fs)
	l.mcpConnect = func(context.Context) ([]Tool, func()) {
		return []Tool{shadow}, func() {}
	}

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "hi", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	count := 0
	for _, d := range fs.lastRequest().Tools {
		if d.Name == "deploy" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("colliding MCP tool must not duplicate an existing def: %+v", fs.lastRequest().Tools)
	}
}

func TestLoopWriteAheadSavesUserMessageFirst(t *testing.T) {
	fs := &fakeStream{}
	fs.textReply("answer")
	hist := &fakeHistory{}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, fs)

	r := &fakeReporter{}
	if err := l.RunTurn(context.Background(), "question", r); err != nil {
		t.Fatalf("RunTurn: %v", err)
	}
	// Save sequence: write-ahead (user only) then the completed pair.
	if len(hist.saved) != 2 {
		t.Fatalf("expected write-ahead + final save, got %d saves", len(hist.saved))
	}
	if ahead := hist.saved[0]; len(ahead) != 1 || ahead[0].Role != "user" || ahead[0].Content != "question" {
		t.Fatalf("expected the write-ahead to persist the user message, got %+v", ahead)
	}
	if final := hist.saved[1]; len(final) != 2 || final[1].Role != "assistant" || final[1].Content != "answer" {
		t.Fatalf("expected the final save to persist the pair, got %+v", final)
	}
}

func TestLoopSealInterruptedTurn(t *testing.T) {
	// A history ending with a dangling user message (the node crashed
	// mid-turn) gets the interruption marker as the assistant reply.
	hist := &fakeHistory{msgs: []llm.Message{
		{Role: "user", Content: "earlier"},
		{Role: "assistant", Content: "answered"},
		{Role: "user", Content: "never answered"},
	}}
	l := newTestLoop(LoopConfig{ModelName: "test-model", History: hist}, &fakeStream{})
	if err := l.SealInterruptedTurn(context.Background()); err != nil {
		t.Fatalf("SealInterruptedTurn: %v", err)
	}
	msgs := l.Messages()
	if len(msgs) != 4 || msgs[3].Role != "assistant" || msgs[3].Content != InterruptedTurnMarker {
		t.Fatalf("expected the interruption marker appended, got %+v", msgs)
	}
	if saved := hist.lastSaved(); len(saved) != 4 {
		t.Fatalf("expected the sealed history saved, got %+v", saved)
	}

	// A paired history is left untouched.
	hist2 := &fakeHistory{msgs: []llm.Message{
		{Role: "user", Content: "earlier"},
		{Role: "assistant", Content: "answered"},
	}}
	l2 := newTestLoop(LoopConfig{ModelName: "test-model", History: hist2}, &fakeStream{})
	if err := l2.SealInterruptedTurn(context.Background()); err != nil {
		t.Fatalf("SealInterruptedTurn: %v", err)
	}
	if msgs := l2.Messages(); len(msgs) != 2 {
		t.Fatalf("paired history must be untouched, got %+v", msgs)
	}
	if len(hist2.saved) != 0 {
		t.Fatalf("no save expected for a paired history, got %d", len(hist2.saved))
	}
}
