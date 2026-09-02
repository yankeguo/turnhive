package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
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
	// Sandbox is the ironhive sandbox allocated for this session; access
	// it through the mu-guarded helpers below.
	Sandbox *ironhive.Sandbox
	// hub sequences, buffers and fans out the session's events (see
	// hub.go).
	hub *eventHub

	mu sync.Mutex
	// loop runs the agent turns of this session; it is rebuilt together
	// with the sandbox.
	loop *agent.Loop
	// stopRenew cancels the sandbox lease renewal loop of this session.
	stopRenew context.CancelFunc
	// closed is set when the session is torn down (DELETE, shutdown); a
	// sandbox rebuilt concurrently must not attach to it afterwards.
	closed bool
	// turnID is the currently running turn ("" when idle); turns run
	// detached from the HTTP request that started them.
	turnID string
	// turnCancel cancels the running turn (cancel endpoint, DELETE
	// session, node shutdown).
	turnCancel context.CancelFunc
	// turnCancelCause records why the running turn is being ended; the
	// cancel endpoint sets it to errTurnCancelled so the terminal
	// turn_finished event carries the cancelled status instead of error.
	// Reset on every startTurn.
	turnCancelCause error
	// turnDone is closed by finishTurn once the current turn has been
	// marked finished; the cancel endpoint waits on it so a client can
	// resend immediately after cancelling.
	turnDone chan struct{}
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
	// bgExited holds background-process exits waiting to be reported as
	// a synthesized user turn once the session is idle.
	bgExited []agent.BgProcessExit
}

// touch marks turn activity, pushing out the idle reaper.
func (s *Session) touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// hasSandbox reports whether the session currently holds a sandbox.
func (s *Session) hasSandbox() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Sandbox != nil
}

// setSandbox installs a freshly built sandbox and its lease-renewal
// cancel func. It returns false when the session was closed while the
// sandbox was being built; the caller must then stop the renewal and
// release the sandbox itself.
func (s *Session) setSandbox(sb *ironhive.Sandbox, stopRenew context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.Sandbox = sb
	s.stopRenew = stopRenew
	return true
}

// takeSandbox detaches the session's sandbox and its renew cancel func,
// returning them for release (session creation rollback).
func (s *Session) takeSandbox() (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, stop := s.Sandbox, s.stopRenew
	s.Sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// takeSandboxIfIdle detaches the session's sandbox and renew cancel func
// for release when no turn is running and the session has been inactive
// for at least d; it returns nil otherwise. Checking and detaching in a
// single critical section closes the race with startTurn/ensureSandbox:
// a turn that won the lock first can never lose its sandbox to the
// reaper.
func (s *Session) takeSandboxIfIdle(d time.Duration) (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnID != "" || time.Since(s.lastActivity) < d {
		return nil, nil
	}
	sb, stop := s.Sandbox, s.stopRenew
	s.Sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// closeSession marks the session torn down (DELETE, shutdown) and
// detaches its sandbox and renew cancel func for release. After
// closeSession, setSandbox refuses to attach a new sandbox, so a
// concurrently rebuilding ensureSandbox cannot leak one.
func (s *Session) closeSession() (*ironhive.Sandbox, context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	sb, stop := s.Sandbox, s.stopRenew
	s.Sandbox = nil
	s.stopRenew = nil
	return sb, stop
}

// takeIfCold marks the session closed and detaches its sandbox for
// release when no turn is running and the session has been inactive for
// at least d; it reports false otherwise. Checking, closing and
// detaching in a single critical section closes the race with
// startTurn/ensureSandbox, like takeSandboxIfIdle. Unlike the idle reap,
// eviction retires the whole session (to cold storage) — a later request
// re-adopts it from S3.
func (s *Session) takeIfCold(d time.Duration) (*ironhive.Sandbox, context.CancelFunc, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.turnID != "" || time.Since(s.lastActivity) < d {
		return nil, nil, false
	}
	s.closed = true
	sb, stop := s.Sandbox, s.stopRenew
	s.Sandbox = nil
	s.stopRenew = nil
	return sb, stop, true
}

// getLoop returns the session's agent loop.
func (s *Session) getLoop() *agent.Loop {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loop
}

// setLoop installs the agent loop built alongside a fresh sandbox.
func (s *Session) setLoop(l *agent.Loop) {
	s.mu.Lock()
	s.loop = l
	s.mu.Unlock()
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

// recordBackgroundExit queues a background-process exit for reporting
// as a synthesized user turn (the agent.OnBackgroundExit hook). Exits
// arriving after the session is closed are ignored.
func (s *Session) recordBackgroundExit(info agent.BgProcessExit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.bgExited = append(s.bgExited, info)
}

// takeBackgroundExits pops every queued background-process exit. A
// closed session drops its queue instead of reporting it.
func (s *Session) takeBackgroundExits() []agent.BgProcessExit {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.bgExited) == 0 {
		return nil
	}
	exits := s.bgExited
	s.bgExited = nil
	return exits
}

// requeueBackgroundExits puts exits back at the head of the queue,
// preserving order, when their notification turn could not start
// because the session was busy.
func (s *Session) requeueBackgroundExits(exits []agent.BgProcessExit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.bgExited = append(exits, s.bgExited...)
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
	s.turnCancelCause = nil
	s.turnDone = make(chan struct{})
	return true
}

// finishTurn clears the running-turn mark when a turn ends and drops
// tool results nobody claimed: late results of this turn (or forgeries
// with fabricated call ids) must not leak into the next turn. The
// turnDone channel is closed so anything waiting on it (the cancel
// endpoint) is released as soon as the session is idle again.
func (s *Session) finishTurn() {
	s.mu.Lock()
	s.turnID = ""
	s.turnCancel = nil
	s.pending = nil
	if s.turnDone != nil {
		close(s.turnDone)
		s.turnDone = nil
	}
	s.mu.Unlock()
}

// cancelTurn aborts the running turn, if any (DELETE session, node
// shutdown); the cause stays nil, so the turn still ends with the error
// status rather than cancelled.
func (s *Session) cancelTurn() {
	s.mu.Lock()
	cancel := s.turnCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CancelTurn marks the running turn as a user-initiated interruption
// (errTurnCancelled) and cancels it, returning its id plus the channel
// that closes once finishTurn has marked the session idle; it returns ""
// when no turn is running. Marking and cancelling inside the lock makes
// it race-free: a turn that already finished is never reported as
// cancelled.
func (s *Session) CancelTurn() (string, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnCancel == nil {
		return "", nil
	}
	id := s.turnID
	done := s.turnDone
	s.turnCancelCause = errTurnCancelled
	s.turnCancel()
	return id, done
}

// TurnID returns the id of the currently running turn ("" when idle).
func (s *Session) TurnID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnID
}

// turnCause returns why the current turn is being ended (nil while
// running or when it failed rather than was cancelled).
func (s *Session) turnCause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turnCancelCause
}

// pendingToolResultsCap bounds tool results reported before the agent
// loop waits for them (or never claimed because the waiter timed out).
// Without the cap a client could grow the map forever with fabricated
// call ids.
const pendingToolResultsCap = 256

// AddToolResult delivers an externally reported tool result, either to a
// waiting agent loop or into the pending buffer. It returns an error
// when the pending buffer is full of unclaimed results and the call id
// is unknown.
func (s *Session) AddToolResult(r ToolResultRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.waiters[r.CallID]; ok {
		delete(s.waiters, r.CallID)
		ch <- r
		return nil
	}
	if _, ok := s.pending[r.CallID]; !ok && len(s.pending) >= pendingToolResultsCap {
		return fmt.Errorf("too many pending tool results")
	}
	if s.pending == nil {
		s.pending = make(map[string]ToolResultRequest)
	}
	s.pending[r.CallID] = r
	return nil
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

// mcpServerNamePattern bounds an MCP server name: it prefixes every tool
// of the server as "{name}__{tool}", and the result must satisfy the
// upstream function-name constraint (64 chars max), so the name itself
// is capped at 32.
var mcpServerNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,32}$`)

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

// MCPServerSpec describes an MCP server the session may use. Only
// HTTP-based transports are supported (no stdio).
type MCPServerSpec struct {
	// Name namespaces the server's tools as "{name}__{tool}"; it must
	// match ^[a-zA-Z0-9_-]{1,32}$ and be unique within mcp_servers.
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	// Transport selects the wire transport: "streamable" or "sse".
	// Empty means auto: try streamable HTTP first, fall back to legacy
	// SSE when the connect fails.
	Transport string `json:"transport"`
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
	seenMCP := make(map[string]bool, len(r.MCPServers))
	for i, m := range r.MCPServers {
		if !mcpServerNamePattern.MatchString(m.Name) {
			return fmt.Errorf("mcp_servers[%d].name must match %s (tool namespacing and the upstream function-name limit)", i, mcpServerNamePattern)
		}
		if seenMCP[m.Name] {
			return fmt.Errorf("mcp_servers[%d].name %q duplicates an earlier server", i, m.Name)
		}
		seenMCP[m.Name] = true
		if m.URL == "" {
			return fmt.Errorf("mcp_servers[%d].url is required", i)
		}
		if err := validateHTTPURL(fmt.Sprintf("mcp_servers[%d].url", i), m.URL); err != nil {
			return err
		}
		switch m.Transport {
		case "", agent.MCPTransportStreamable, agent.MCPTransportSSE:
		default:
			return fmt.Errorf("mcp_servers[%d].transport must be %q or %q (empty for auto)", i, agent.MCPTransportStreamable, agent.MCPTransportSSE)
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
