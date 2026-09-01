// Package turnhive is the Go client SDK for the turnhive agent cluster
// service. It exposes the full client surface: creating sessions, running
// turns as SSE streams, reporting external tool results, and deleting
// sessions.
//
//	cli := turnhive.NewClient("http://turnhive:8080")
//	sess, err := cli.CreateSession(ctx, turnhive.CreateSessionRequest{ /* ... */ })
//	defer cli.DeleteSession(ctx, sess.ID)
//	stream, err := cli.SendMessage(ctx, sess.ID, "帮我分析这个仓库的代码结构")
//	for event := range stream.Events() {
//		// handle event.Type: delta / reasoning_delta / tool_call / done / error
//	}
package turnhive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// maxErrorBody caps how much of a non-2xx response body is read for the
// returned error.
const maxErrorBody = 4096

// ErrSessionBusy is returned by SendMessage when the session already has a
// turn in progress (HTTP 409 session_busy).
var ErrSessionBusy = errors.New("turnhive: session busy")

// ErrSessionNotFound is returned when the session does not exist on the
// cluster (HTTP 404 session not found).
var ErrSessionNotFound = errors.New("turnhive: session not found")

// Error is a non-2xx response from the turnhive server that is not one of
// the sentinel errors above.
type Error struct {
	// StatusCode is the HTTP status code.
	StatusCode int
	// Message is the "error" field of the response body.
	Message string
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("turnhive: status %d: %s", e.StatusCode, e.Message)
}

// errorFromResponse converts a non-2xx response into an error, mapping
// well-known statuses onto the sentinel errors.
func errorFromResponse(resp *http.Response) error {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	msg := strings.TrimSpace(string(snippet))
	var envelope struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(snippet, &envelope) == nil && envelope.Error != "" {
		msg = envelope.Error
	}
	switch resp.StatusCode {
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrSessionBusy, msg)
	case http.StatusNotFound:
		if envelope.Error == "session not found" {
			return ErrSessionNotFound
		}
	}
	return &Error{StatusCode: resp.StatusCode, Message: msg}
}

// Client talks to a turnhive node. Session routing across the cluster is
// transparent: any node accepts requests for any session.
type Client struct {
	baseURL string
	hc      *http.Client
}

// Option customizes a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client used for requests. The default is
// http.DefaultClient; note SendMessage responses are long-lived SSE
// streams, so the client must not set a short Timeout.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// NewClient creates a Client for the turnhive node at baseURL, e.g.
// "http://turnhive:8080".
func NewClient(baseURL string, opts ...Option) *Client {
	c := &Client{baseURL: strings.TrimRight(baseURL, "/"), hc: http.DefaultClient}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Session is a created session handle.
type Session struct {
	// ID identifies the session on the cluster.
	ID string `json:"id"`
}

// CreateSessionRequest is the body of Client.CreateSession.
type CreateSessionRequest struct {
	Model      ModelSpec       `json:"model"`
	Prompt     PromptSpec      `json:"prompt"`
	Ironhive   IronhiveSpec    `json:"ironhive"`
	Skills     []SkillSpec     `json:"skills,omitempty"`
	MCPServers []MCPServerSpec `json:"mcp_servers,omitempty"`
	Tools      []ToolSpec      `json:"tools,omitempty"`
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
	Headers map[string]string `json:"headers,omitempty"`
	// MaxContext is the model's context window size in tokens; zero
	// means unspecified.
	MaxContext int `json:"max_context,omitempty"`
	// Features declares model capabilities; see the ModelFeature* constants.
	Features []string `json:"features,omitempty"`
}

// ProtocolOpenAICompletions is the only model protocol currently
// supported.
const ProtocolOpenAICompletions = "openai_completions"

// ModelFeatureSupportImage marks a model that accepts image inputs. It
// enables image-related tooling (e.g. load_media) for the session.
const ModelFeatureSupportImage = "support_image"

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

// SkillSpec references a skill tarball stored in the cluster's S3 bucket.
type SkillSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// ObjectKey is the S3 object key of the skill tarball, relative to the
	// configured bucket prefix.
	ObjectKey string `json:"object_key"`
}

// MCPServerSpec describes an MCP server the session may use.
type MCPServerSpec struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// ToolSpec describes an external tool the session may call. Tool calls
// are reported as tool_call stream events; results are sent back with
// Client.ReportToolResult or Client.ReportToolError.
type ToolSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	// Parameters is the JSON Schema describing the tool's arguments,
	// following the OpenAI function calling convention.
	Parameters map[string]any `json:"parameters"`
}

// CreateSession creates a session and returns its handle.
func (c *Client) CreateSession(ctx context.Context, req CreateSessionRequest) (Session, error) {
	var sess Session
	if err := c.do(ctx, http.MethodPost, "/v1/sessions", req, &sess); err != nil {
		return Session{}, err
	}
	return sess, nil
}

// DeleteSession destroys the session and releases its cluster resources.
// A missing session reports ErrSessionNotFound.
func (c *Client) DeleteSession(ctx context.Context, sessionID string) error {
	return c.do(ctx, http.MethodDelete, "/v1/sessions/"+url.PathEscape(sessionID), nil, nil)
}

// SendMessage sends one user input to the session and returns the turn's
// event stream. The stream must be drained (or closed) to release the
// connection. A session runs one turn at a time; a concurrent turn is
// rejected with ErrSessionBusy.
func (c *Client) SendMessage(ctx context.Context, sessionID, content string) (*Stream, error) {
	body, err := json.Marshal(struct {
		Content string `json:"content"`
	}{Content: content})
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/sessions/"+url.PathEscape(sessionID)+"/messages", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.hc.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("send message: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, errorFromResponse(resp)
	}
	return newStream(resp.Body), nil
}

// ReportToolResult reports the successful result of an external tool call
// (see EventToolCall). result is marshaled to JSON verbatim.
func (c *Client) ReportToolResult(ctx context.Context, sessionID, callID string, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal tool result: %w", err)
	}
	return c.postToolResult(ctx, sessionID, struct {
		CallID string          `json:"call_id"`
		Result json.RawMessage `json:"result"`
	}{CallID: callID, Result: raw})
}

// ReportToolError reports that an external tool call failed.
func (c *Client) ReportToolError(ctx context.Context, sessionID, callID string, toolErr error) error {
	return c.postToolResult(ctx, sessionID, struct {
		CallID string `json:"call_id"`
		Error  string `json:"error"`
	}{CallID: callID, Error: toolErr.Error()})
}

func (c *Client) postToolResult(ctx context.Context, sessionID string, body any) error {
	return c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(sessionID)+"/tool_results", body, nil)
}

// do sends one JSON request and decodes the JSON response into out (when
// out is not nil). Non-2xx responses become errors via errorFromResponse.
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errorFromResponse(resp)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
