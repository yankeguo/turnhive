package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/llm"
)

// ErrBusy is returned by RunTurn when a turn is already running on the
// Loop.
var ErrBusy = errors.New("agent: a turn is already running")

// maxTurnSteps bounds the step loop of one turn: each assistant reply
// with tool calls counts as one step.
const maxTurnSteps = 199

// LoopConfig configures a Loop.
type LoopConfig struct {
	// ModelURL is the full OpenAI-compatible chat completion endpoint URL.
	ModelURL string
	// ModelHeaders carries the authentication header and any extra
	// headers sent to the endpoint.
	ModelHeaders map[string]string
	// ModelName is the model name.
	ModelName string
	// SystemPrompt is the composed system prompt; see BuildSystemPrompt.
	SystemPrompt string
	// Sandbox is the session's ironhive sandbox; nil disables the
	// sandbox tools.
	Sandbox *ironhive.Sandbox
	// ExternalTools are the client-defined tools executed externally.
	ExternalTools []ExternalToolSpec
	// Waiter supplies the results of external tool calls.
	Waiter ToolResultWaiter
	// ExternalToolTimeout bounds one external tool call; defaults to
	// 10m when zero.
	ExternalToolTimeout time.Duration
	// History persists the message history; nil keeps history in memory
	// only.
	History HistoryStore
}

// Loop runs agent turns against one model endpoint: it streams chat
// completions, dispatches tool calls to sandbox and external tools, and
// persists the {user, assistant} history between turns.
type Loop struct {
	cfg       LoopConfig
	stream    func(ctx context.Context, req llm.Request, onEvent func(llm.Event)) (llm.Message, llm.Usage, error)
	tools     []Tool
	toolDefs  []llm.ToolDef
	spiller   OutputSpiller
	busy      atomic.Bool
	mu        sync.Mutex
	history   []llm.Message
	histReady bool
}

// NewLoop creates a Loop. Sandbox tools come first, then external tools.
func NewLoop(cfg LoopConfig) *Loop {
	l := &Loop{cfg: cfg, stream: llm.Stream}
	if cfg.Sandbox != nil {
		st := newSandboxTools(cfg.Sandbox)
		l.tools = append(l.tools, st.list()...)
		l.spiller = st
	}
	l.tools = append(l.tools, ExternalTools(cfg.ExternalTools, cfg.Waiter, cfg.ExternalToolTimeout)...)
	l.toolDefs = toolSpecs(l.tools)
	return l
}

// Messages returns the persisted history (for tests/debug).
func (l *Loop) Messages() []llm.Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]llm.Message(nil), l.history...)
}

// RunTurn runs one user turn to completion, streaming progress through r.
// Concurrent RunTurn calls fail immediately with ErrBusy.
func (l *Loop) RunTurn(ctx context.Context, userText string, r Reporter) error {
	if !l.busy.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer l.busy.Store(false)

	if err := l.loadHistory(ctx); err != nil {
		r.Error(err.Error())
		return err
	}

	userMsg := llm.Message{Role: "user", Content: userText}

	// The request messages are [system] + history + the user message +
	// the transient tool exchanges of this turn.
	l.mu.Lock()
	working := make([]llm.Message, 0, len(l.history)+2)
	working = append(working, l.history...)
	l.mu.Unlock()
	if l.cfg.SystemPrompt != "" {
		working = append([]llm.Message{{Role: "system", Content: l.cfg.SystemPrompt}}, working...)
	}
	working = append(working, userMsg)

	for step := 0; step < maxTurnSteps; step++ {
		var stepText strings.Builder
		msg, _, err := l.stream(ctx, llm.Request{
			URL:      l.cfg.ModelURL,
			Headers:  l.cfg.ModelHeaders,
			Model:    l.cfg.ModelName,
			Messages: working,
			Tools:    l.toolDefs,
		}, func(ev llm.Event) {
			switch ev.Type {
			case llm.EventDelta:
				stepText.WriteString(ev.Text)
				r.Delta(ev.Text)
			case llm.EventReasoning:
				r.ReasoningDelta(ev.Text)
			}
		})
		if err != nil {
			return l.failTurn(ctx, r, userMsg, stepText.String(), err)
		}

		if len(msg.ToolCalls) == 0 {
			// Plain text reply: the turn ends. History only ever
			// carries the {user, assistant} pair — the tool exchanges
			// of this turn stay transient.
			l.appendHistory(userMsg, llm.Message{Role: "assistant", Content: msg.Content})
			if err := l.saveHistory(ctx); err != nil {
				r.Error(err.Error())
				return err
			}
			r.Done(msg.Content)
			return nil
		}

		working = append(working, msg)
		for _, tc := range msg.ToolCalls {
			r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallRunning})
			out, err := dispatchTool(ctx, l.tools, l.spiller, tc.ID, tc.Name, tc.Arguments)
			var resultText string
			if err != nil {
				r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallError})
				resultText = "error: " + err.Error()
			} else {
				r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallDone})
				resultText = out
			}
			working = append(working, llm.Message{Role: "tool", ToolCallID: tc.ID, Content: resultText})
		}
	}

	err := errors.New("max steps exceeded")
	r.Error(err.Error())
	return err
}

// loadHistory lazily loads the persisted history on the first turn.
func (l *Loop) loadHistory(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.histReady {
		return nil
	}
	if l.cfg.History != nil {
		msgs, err := l.cfg.History.Load(ctx)
		if err != nil {
			return err
		}
		l.history = msgs
	}
	l.histReady = true
	return nil
}

// appendHistory appends msgs to the persisted history.
func (l *Loop) appendHistory(msgs ...llm.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.history = append(l.history, msgs...)
}

// saveHistory writes the current history to the configured HistoryStore,
// if any.
func (l *Loop) saveHistory(ctx context.Context) error {
	l.mu.Lock()
	if l.cfg.History == nil {
		l.mu.Unlock()
		return nil
	}
	msgs := append([]llm.Message(nil), l.history...)
	l.mu.Unlock()
	return l.cfg.History.Save(ctx, msgs)
}

// failTurn ends a turn after a stream error or cancellation: the partial
// assistant text accumulated so far is persisted, the save is
// best-effort, and the error is reported and returned.
func (l *Loop) failTurn(ctx context.Context, r Reporter, userMsg llm.Message, partialText string, err error) error {
	l.appendHistory(userMsg, llm.Message{Role: "assistant", Content: partialText})
	_ = l.saveHistory(ctx)
	r.Error(err.Error())
	return err
}
