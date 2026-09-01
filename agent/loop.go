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
	// SupportImage enables the load_media tool (requires Sandbox); set it
	// when the model accepts image inputs.
	SupportImage bool
	// PersistStore enables the persist tool (requires Sandbox): files are
	// uploaded to it under sessions/{SessionID}/persisted/.
	PersistStore PersistStore
	// SessionID scopes the persist tool's object keys; required when
	// PersistStore is set.
	SessionID string
	// OnPersisted is called after the persist tool stores a file, so the
	// caller can record it as session state.
	OnPersisted func(PersistedObject)
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
	// MaxContext is the model's context window in tokens; zero disables
	// context window management. When set, the history is trimmed before
	// every turn (TruncateToFit) and compacted after a turn whose usage
	// crosses the overflow threshold (CompactMessages).
	MaxContext int
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
		l.tools = append(l.tools, st.list(cfg.SupportImage)...)
		if cfg.PersistStore != nil {
			l.tools = append(l.tools, sandboxPersist{t: st, store: cfg.PersistStore, sessionID: cfg.SessionID, onPersisted: cfg.OnPersisted})
		}
		l.spiller = st
	}
	l.tools = append(l.tools, ExternalTools(cfg.ExternalTools, cfg.Waiter, cfg.ExternalToolTimeout)...)
	l.toolDefs = toolSpecs(l.tools)
	return l
}

// Messages returns the persisted history. It is the production data
// source of the events sync frame (controller rebuilds the merged
// message list from it), not a tests-only accessor.
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

	// Pre-turn context window management: drop the oldest whole turns
	// when the estimated history no longer fits the window (the incoming
	// user message is appended afterwards and never dropped).
	if l.cfg.MaxContext > 0 {
		l.mu.Lock()
		trimmed, changed := TruncateToFit(l.history, l.cfg.MaxContext, replyReserve+EstimateTokens(l.cfg.SystemPrompt))
		l.history = trimmed
		l.mu.Unlock()
		if changed {
			// History saving is best-effort: the in-memory history is
			// authoritative, so a failed save degrades to continuing
			// the turn instead of aborting it.
			_ = l.saveHistory(ctx)
		}
	}

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

	var stepText strings.Builder
	for step := 0; step < maxTurnSteps; step++ {
		stepText.Reset()
		msg, usage, err := l.stream(ctx, llm.Request{
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
			// Post-turn compaction: a turn that pushed usage past the
			// overflow threshold condenses older turns into a summary.
			if l.cfg.MaxContext > 0 && IsOverflow(usage, l.cfg.MaxContext) {
				l.mu.Lock()
				l.history = CompactMessages(l.history)
				l.mu.Unlock()
			}
			// The client already received every delta: report success
			// first, then persist. A failed save is returned so the
			// caller can log it, but never reported as a turn error —
			// the in-memory history is authoritative.
			r.Done(msg.Content)
			return l.saveHistory(ctx)
		}

		working = append(working, msg)
		for _, tc := range msg.ToolCalls {
			r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallRunning})
			out, images, err := dispatchTool(ctx, l.tools, l.spiller, tc.ID, tc.Name, tc.Arguments)
			var resultText string
			if err != nil {
				r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallError})
				// Bound the error text like any other non-read tool
				// output before feeding it back to the model.
				resultText = "error: " + Truncate(err.Error(), WithMaxLines(StrictMaxLines), WithMaxBytes(StrictMaxBytes), WithHint(strictHint))
			} else {
				r.ToolCall(ToolCallEvent{ID: tc.ID, Name: tc.Name, Status: ToolCallDone})
				resultText = out
			}
			working = append(working, llm.Message{Role: "tool", ToolCallID: tc.ID, Content: resultText})
			// Images a tool produced (load_media) follow their tool
			// message as a user message with image_url parts — chat
			// completions only accepts images on user messages.
			if len(images) > 0 {
				working = append(working, llm.Message{Role: "user", Images: images})
			}
		}
	}

	// Persist the {user, assistant-partial} pair like any other failed
	// turn, so a rebuilt session keeps what was said.
	return l.failTurn(ctx, r, userMsg, stepText.String(), errors.New("max steps exceeded"))
}

// LoadHistory eagerly loads the persisted history. RunTurn also loads
// it lazily; call this when a Loop is rebuilt for a restored session so
// history-dependent reads (e.g. the events sync frame) are correct
// before the next turn.
func (l *Loop) LoadHistory(ctx context.Context) error {
	return l.loadHistory(ctx)
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
// assistant text accumulated so far is persisted (best-effort), and the
// error is reported and returned.
func (l *Loop) failTurn(ctx context.Context, r Reporter, userMsg llm.Message, partialText string, err error) error {
	l.appendHistory(userMsg, llm.Message{Role: "assistant", Content: partialText})
	// ctx may already be cancelled (session deleted, controller closing,
	// turn timeout): save under a detached context so the partial reply
	// still reaches the history store.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_ = l.saveHistory(saveCtx)
	r.Error(err.Error())
	return err
}
