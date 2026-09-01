package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/agent"
)

// ProtocolOpenAICompletions is the only model protocol currently
// supported.
const ProtocolOpenAICompletions = "openai_completions"

// Session is a session owned by this node.
type Session struct {
	ID   string
	Spec CreateSessionRequest
	// Sandbox is the ironhive sandbox allocated for this session.
	Sandbox *ironhive.Sandbox
	// Loop runs the agent turns of this session.
	Loop *agent.Loop
	// hub sequences, buffers and fans out the session's events (see
	// hub.go).
	hub *eventHub
	// stopRenew cancels the sandbox lease renewal loop of this session.
	stopRenew context.CancelFunc

	mu sync.Mutex
	// turnID is the currently running turn ("" when idle); turns run
	// detached from the HTTP request that started them.
	turnID string
	// turnCancel cancels the running turn (DELETE session, node
	// shutdown).
	turnCancel context.CancelFunc
	// lastActivity is the last time the session saw turn activity
	// (message accepted, turn finished). The idle reaper releases the
	// sandbox after idle_timeout without it; the session lives on.
	lastActivity time.Time
	// pending holds tool results that arrived before the agent loop
	// started waiting for them.
	pending map[string]ToolResultRequest
	// waiters holds the channels of tool calls the agent loop is
	// currently blocked on.
	waiters map[string]chan ToolResultRequest
	// persisted records every file the persist tool stored, keyed by
	// in-sandbox path (re-persisting a path replaces the entry).
	persisted map[string]agent.PersistedObject
}

// touch marks turn activity, pushing out the idle reaper.
func (s *Session) touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// idle reports whether the session has been inactive for at least d and
// no turn is running.
func (s *Session) idle(d time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnID == "" && time.Since(s.lastActivity) >= d
}

// hasSandbox reports whether the session currently holds a sandbox.
func (s *Session) hasSandbox() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Sandbox != nil
}

// setSandbox installs a freshly built sandbox and its lease-renewal
// cancel func.
func (s *Session) setSandbox(sb *ironhive.Sandbox, stopRenew context.CancelFunc) {
	s.mu.Lock()
	s.Sandbox = sb
	s.stopRenew = stopRenew
	s.mu.Unlock()
}

// takeSandbox detaches the session's sandbox and its renew cancel func,
// returning them for release (idle reaper, DELETE, shutdown).
func (s *Session) takeSandbox() (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, stop := s.Sandbox, s.stopRenew
	s.Sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// recordPersisted records a persisted file as session state (the
// agent.OnPersisted hook).
func (s *Session) recordPersisted(obj agent.PersistedObject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.persisted == nil {
		s.persisted = make(map[string]agent.PersistedObject)
	}
	s.persisted[obj.Path] = obj
}

// Persisted returns the session's persisted objects, sorted by path.
func (s *Session) Persisted() []agent.PersistedObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]agent.PersistedObject, 0, len(s.persisted))
	for _, obj := range s.persisted {
		out = append(out, obj)
	}
	slices.SortFunc(out, func(a, b agent.PersistedObject) int { return strings.Compare(a.Path, b.Path) })
	return out
}

// startTurn marks a new turn as running, returning false when one is
// already running (the session allows one turn at a time).
func (s *Session) startTurn(turnID string, cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnID != "" {
		return false
	}
	s.turnID = turnID
	s.turnCancel = cancel
	return true
}

// finishTurn clears the running-turn mark when a turn ends.
func (s *Session) finishTurn() {
	s.mu.Lock()
	s.turnID = ""
	s.turnCancel = nil
	s.mu.Unlock()
}

// cancelTurn aborts the running turn, if any.
func (s *Session) cancelTurn() {
	s.mu.Lock()
	cancel := s.turnCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// AddToolResult delivers an externally reported tool result, either to a
// waiting agent loop or into the pending buffer.
func (s *Session) AddToolResult(r ToolResultRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.waiters[r.CallID]; ok {
		delete(s.waiters, r.CallID)
		ch <- r
		return
	}
	if s.pending == nil {
		s.pending = make(map[string]ToolResultRequest)
	}
	s.pending[r.CallID] = r
}

// WaitToolResult implements agent.ToolResultWaiter: it blocks until the
// result of callID is reported via POST /v1/sessions/{id}/tool_results
// or ctx is done.
func (s *Session) WaitToolResult(ctx context.Context, callID string) (json.RawMessage, string, error) {
	s.mu.Lock()
	if r, ok := s.pending[callID]; ok {
		delete(s.pending, callID)
		s.mu.Unlock()
		return r.Result, r.Error, nil
	}
	ch := make(chan ToolResultRequest, 1)
	if s.waiters == nil {
		s.waiters = make(map[string]chan ToolResultRequest)
	}
	s.waiters[callID] = ch
	s.mu.Unlock()

	select {
	case r := <-ch:
		return r.Result, r.Error, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, callID)
		s.mu.Unlock()
		return nil, "", ctx.Err()
	}
}

// CreateSessionRequest is the JSON body of POST /v1/sessions.
type CreateSessionRequest struct {
	Model      ModelSpec       `json:"model"`
	Prompt     PromptSpec      `json:"prompt"`
	Ironhive   IronhiveSpec    `json:"ironhive"`
	Skills     []SkillSpec     `json:"skills"`
	MCPServers []MCPServerSpec `json:"mcp_servers"`
	Tools      []ToolSpec      `json:"tools"`
}

// ModelSpec describes the LLM inference endpoint used by the session.
type ModelSpec struct {
	// URL is the full inference endpoint URL.
	URL string `json:"url"`
	// Protocol is the wire protocol of the endpoint; currently fixed to
	// "openai_completions".
	Protocol string `json:"protocol"`
	// Name is the model name.
	Name string `json:"name"`
	// Headers carries the authentication header and any extra headers
	// sent to the endpoint.
	Headers map[string]string `json:"headers"`
	// MaxContext is the model's context window size in tokens; zero
	// means unspecified.
	MaxContext int `json:"max_context,omitempty"`
	// Features declares model capabilities; see the ModelFeature* constants.
	Features []string `json:"features,omitempty"`
}

// ModelFeatureSupportImage marks a model that accepts image inputs. It
// enables image-related tooling (e.g. load_media) for the session.
const ModelFeatureSupportImage = "support_image"

// modelFeatures is the set of currently recognized ModelSpec.Features values.
var modelFeatures = map[string]bool{
	ModelFeatureSupportImage: true,
}

// PromptSpec holds the session's prompt materials.
type PromptSpec struct {
	// System is the system prompt in plain text.
	System string `json:"system"`
}

// IronhiveSpec selects the sandbox the session executes in.
type IronhiveSpec struct {
	// Pool is the ironhive pool a sandbox is allocated from.
	Pool string `json:"pool"`
}

// SkillSpec references a skill tarball stored in S3.
type SkillSpec struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// ObjectKey is the S3 object key of the skill tarball.
	ObjectKey string `json:"object_key"`
}

// MCPServerSpec describes an MCP server the session may use.
type MCPServerSpec struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// ToolSpec describes an external tool the session may call. Tool calls
// are reported over SSE; results come back via
// POST /v1/sessions/{id}/tool_results.
type ToolSpec struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Parameters is the JSON Schema describing the tool's arguments,
	// following the OpenAI function calling convention.
	Parameters map[string]any `json:"parameters"`
}

// ToolResultRequest is the JSON body of POST /v1/sessions/{id}/tool_results.
type ToolResultRequest struct {
	// CallID identifies the tool call this result belongs to.
	CallID string `json:"call_id"`
	// Result is the tool's output as arbitrary JSON; mutually exclusive
	// with Error.
	Result json.RawMessage `json:"result"`
	// Error reports that the tool call failed; mutually exclusive with
	// Result.
	Error string `json:"error"`
}

// Validate checks the request and returns the first problem found.
func (r *ToolResultRequest) Validate() error {
	if strings.TrimSpace(r.CallID) == "" {
		return fmt.Errorf("call_id is required")
	}
	hasResult := len(r.Result) > 0 && !bytes.Equal(bytes.TrimSpace(r.Result), []byte("null"))
	if hasResult && r.Error != "" {
		return fmt.Errorf("result and error are mutually exclusive")
	}
	if !hasResult && r.Error == "" {
		return fmt.Errorf("one of result or error is required")
	}
	return nil
}

// Validate checks the request and returns the first problem found.
func (r *CreateSessionRequest) Validate() error {
	if r.Model.URL == "" {
		return fmt.Errorf("model.url is required")
	}
	if err := validateHTTPURL("model.url", r.Model.URL); err != nil {
		return err
	}
	if r.Model.Protocol == "" {
		return fmt.Errorf("model.protocol is required")
	}
	if r.Model.Protocol != ProtocolOpenAICompletions {
		return fmt.Errorf("model.protocol must be %q", ProtocolOpenAICompletions)
	}
	if strings.TrimSpace(r.Model.Name) == "" {
		return fmt.Errorf("model.name is required")
	}
	if r.Model.MaxContext < 0 {
		return fmt.Errorf("model.max_context must not be negative")
	}
	for i, f := range r.Model.Features {
		if !modelFeatures[f] {
			return fmt.Errorf("model.features[%d]: unknown feature %q (supported: %s)", i, f, ModelFeatureSupportImage)
		}
	}
	if strings.TrimSpace(r.Prompt.System) == "" {
		return fmt.Errorf("prompt.system is required")
	}
	if strings.TrimSpace(r.Ironhive.Pool) == "" {
		return fmt.Errorf("ironhive.pool is required")
	}
	for i, s := range r.Skills {
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("skills[%d].name is required", i)
		}
		if strings.TrimSpace(s.ObjectKey) == "" {
			return fmt.Errorf("skills[%d].object_key is required", i)
		}
	}
	for i, m := range r.MCPServers {
		if strings.TrimSpace(m.Name) == "" {
			return fmt.Errorf("mcp_servers[%d].name is required", i)
		}
		if m.URL == "" {
			return fmt.Errorf("mcp_servers[%d].url is required", i)
		}
		if err := validateHTTPURL(fmt.Sprintf("mcp_servers[%d].url", i), m.URL); err != nil {
			return err
		}
	}
	for i, t := range r.Tools {
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("tools[%d].name is required", i)
		}
		if t.Parameters == nil {
			return fmt.Errorf("tools[%d].parameters is required (use {} for a tool without arguments)", i)
		}
	}
	return nil
}

// validateHTTPURL reports whether raw is an absolute http(s) URL.
func validateHTTPURL(field, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%s must be an http(s):// URL", field)
	}
	return nil
}
