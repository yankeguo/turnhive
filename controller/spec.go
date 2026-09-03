package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/yankeguo/turnhive/agent"
)

// ProtocolOpenAICompletions is the only model protocol currently
// supported.
const ProtocolOpenAICompletions = "openai_completions"

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
