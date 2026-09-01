package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

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
	// stopRenew cancels the sandbox lease renewal loop of this session.
	stopRenew context.CancelFunc

	mu sync.Mutex
	// pending holds tool results that arrived before the agent loop
	// started waiting for them.
	pending map[string]ToolResultRequest
	// waiters holds the channels of tool calls the agent loop is
	// currently blocked on.
	waiters map[string]chan ToolResultRequest
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
