// Package turnhive is the Go client SDK for the turnhive agent cluster
// service. It exposes the full client surface: creating sessions, running
// turns as SSE streams, reporting external tool results, and deleting
// sessions.
//
//	cli := turnhive.NewClient("http://turnhive:8080")
//	sess, err := cli.CreateSession(ctx, turnhive.CreateSessionRequest{ /* ... */ })
//	defer cli.DeleteSession(ctx, sess.ID)
//	turnID, err := cli.SendMessage(ctx, sess.ID, "帮我分析这个仓库的代码结构")
//	stream, err := cli.Events(ctx, sess.ID, 0)
//	defer stream.Close()
//	for event := range stream.Events() {
//		// handle event.Type: delta / reasoning_delta / tool_call / done / error
//		// event.TurnID == turnID identifies events of the turn just started
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
	"strconv"
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
		// Wire contract with the controller (routeSession and the
		// handlers): a 404 whose error message is exactly
		// "session not found" maps to the sentinel. Changing the
		// server-side message breaks this mapping.
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

// MCPServerSpec describes an MCP server the session may use. Only
// HTTP-based transports are supported (no stdio).
type MCPServerSpec struct {
	// Name namespaces the server's tools as "{name}__{tool}".
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	// Transport selects the wire transport: "streamable" or "sse".
	// Empty means auto: try streamable HTTP first, fall back to legacy
	// SSE when the connect fails.
	Transport string `json:"transport,omitempty"`
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

// SendMessage sends one user input to the session and returns the id of
// the turn it started. The turn runs asynchronously; its events (and
// those of later turns) are consumed from the session event stream —
// see Events. A session runs one turn at a time; a concurrent turn is
// rejected with ErrSessionBusy.
func (c *Client) SendMessage(ctx context.Context, sessionID, content string) (string, error) {
	var resp struct {
		TurnID string `json:"turn_id"`
	}
	body := struct {
		Content string `json:"content"`
	}{Content: content}
	if err := c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(sessionID)+"/messages", body, &resp); err != nil {
		return "", err
	}
	return resp.TurnID, nil
}

// CancelTurn interrupts the session's running turn and returns its id.
// The turn is aborted (its partial reply is persisted and an error event
// is streamed); to continue, send a new message — an interrupted turn is
// never replayed or resumed.
func (c *Client) CancelTurn(ctx context.Context, sessionID string) (string, error) {
	var resp struct {
		TurnID string `json:"turn_id"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(sessionID)+"/cancel", nil, &resp); err != nil {
		return "", err
	}
	return resp.TurnID, nil
}

// Events opens the session event stream (GET .../events). Events from
// all turns flow over it, sequenced per session; pass the last seen
// Event.Seq as lastSeq to replay what was missed after a reconnect (0
// replays the retained buffer; negative values are treated as 0). The
// sync event that opens every stream is authoritative: when the session
// was recovered from storage (its node crashed, or it was evicted after
// cold_timeout) the numbering restarts, so discard any older seq and
// resume from the sync event's Seq. The stream must be drained (or
// closed) to release the connection.
func (c *Client) Events(ctx context.Context, sessionID string, lastSeq int64) (*Stream, error) {
	u := c.baseURL + "/v1/sessions/" + url.PathEscape(sessionID) + "/events"
	if lastSeq > 0 {
		u += "?last_seq=" + strconv.FormatInt(lastSeq, 10)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.hc.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("session events: %w", err)
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
		// An empty body (e.g. a future 204 endpoint) is a success, not a
		// decode failure.
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
