package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/yankeguo/turnhive/llm"
)

// MCP transports supported by turnhive. Stdio is deliberately not
// supported: MCP servers are remote HTTP services.
const (
	// MCPTransportStreamable is the streamable HTTP transport.
	MCPTransportStreamable = "streamable"
	// MCPTransportSSE is the legacy SSE transport (spec 2024-11-05).
	MCPTransportSSE = "sse"
)

// mcpConnectTimeout bounds one server's connect + list-tools at the start
// of a turn, so a dead upstream degrades to a failed status instead of
// stalling the turn.
const mcpConnectTimeout = 10 * time.Second

// mcpCloseTimeout bounds one client session close on turn teardown.
const mcpCloseTimeout = 5 * time.Second

// mcpToolNamePattern is the OpenAI function name constraint applied to
// namespaced MCP tool names ("{server}__{tool}"); tools that would
// violate it are skipped so they cannot fail the whole turn upstream.
var mcpToolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// MCPServerSpec describes an MCP server the session may use.
type MCPServerSpec struct {
	// Name namespaces the server's tools as "{Name}__{tool}".
	Name string
	// URL is the server's endpoint URL.
	URL string
	// Headers carries authentication and extra headers sent to the server.
	Headers map[string]string
	// Transport selects the wire transport: MCPTransportStreamable or
	// MCPTransportSSE. Empty means auto: try streamable HTTP first, fall
	// back to legacy SSE when the connect fails.
	Transport string
}

// MCPServerStatus reports the outcome of connecting one MCP server at
// the start of a turn.
type MCPServerStatus struct {
	Name string
	// ToolCount is the number of tools actually mounted from the server
	// (invalid or duplicate names are skipped).
	ToolCount int
	// Err is set when the server could not be connected or queried; the
	// turn proceeds without its tools.
	Err error
}

// mcpSession is the subset of the MCP client session a mounted tool
// needs; an interface so tests can stub it.
type mcpSession interface {
	CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
	Close() error
}

// mcpTool is a Tool that delegates execution to a tool of a connected
// MCP server.
type mcpTool struct {
	spec    llm.ToolDef
	session mcpSession
	// origName is the tool's name on the server (without the namespace).
	origName string
}

func (t mcpTool) Spec() llm.ToolDef { return t.spec }

// Execute calls the server tool. Text contents are joined into the tool
// output; non-text contents are noted as omitted. An MCP-level error
// result (IsError) becomes a Go error, fed back to the model as the tool
// result by the loop.
func (t mcpTool) Execute(ctx context.Context, _ string, args json.RawMessage) (string, error) {
	res, err := t.session.CallTool(ctx, &mcp.CallToolParams{Name: t.origName, Arguments: args})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(tc.Text)
		} else {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			fmt.Fprintf(&b, "[mcp: omitted %T content]", c)
		}
	}
	text := b.String()
	if text == "" {
		// No content at all (or only unrecognized shapes): fall back to
		// the JSON of the whole result so structured output is not lost.
		data, merr := json.Marshal(res)
		if merr == nil {
			text = string(data)
		}
	}
	if res.IsError {
		if text == "" {
			text = "mcp tool call failed"
		}
		return "", errors.New(text)
	}
	return text, nil
}

// headerRoundTripper injects the server spec's headers into every
// request, including the SSE/streamable handshake.
type headerRoundTripper struct {
	base    http.RoundTripper
	headers map[string]string
}

func (h headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(h.headers) > 0 {
		req = req.Clone(req.Context())
		for k, v := range h.headers {
			req.Header.Set(k, v)
		}
	}
	base := h.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req)
}

// transports returns the candidate transports of spec in preference
// order; auto mode tries streamable HTTP first, then legacy SSE.
func (s MCPServerSpec) transports(hc *http.Client) []mcp.Transport {
	streamable := &mcp.StreamableClientTransport{
		Endpoint:   s.URL,
		HTTPClient: hc,
		// A turn-scoped client only needs request/response; skip the
		// standalone SSE stream used for server-initiated messages.
		DisableStandaloneSSE: true,
	}
	sse := &mcp.SSEClientTransport{Endpoint: s.URL, HTTPClient: hc}
	switch s.Transport {
	case MCPTransportStreamable:
		return []mcp.Transport{streamable}
	case MCPTransportSSE:
		return []mcp.Transport{sse}
	default:
		return []mcp.Transport{streamable, sse}
	}
}

// ConnectMCPServers connects every configured server in parallel, lists
// its tools and mounts them namespaced as "{server}__{tool}". A server
// that fails to connect or list is reported through onStatus and skipped
// — it never drags down the other servers or the turn. The returned
// closeAll releases every open client session (bounded per session, on a
// detached context, so teardown survives a cancelled turn).
func ConnectMCPServers(ctx context.Context, specs []MCPServerSpec, onStatus func(MCPServerStatus)) (tools []Tool, closeAll func()) {
	var mu sync.Mutex
	var conns []mcpConn
	statuses := make([]MCPServerStatus, len(specs))

	var wg sync.WaitGroup
	for i, spec := range specs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, mounted, err := connectMCPServer(ctx, spec)
			mu.Lock()
			statuses[i] = MCPServerStatus{Name: spec.Name, ToolCount: len(mounted), Err: err}
			if err == nil {
				conns = append(conns, conn)
				tools = append(tools, mounted...)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if onStatus != nil {
		for _, st := range statuses {
			onStatus(st)
		}
	}
	return tools, func() { closeMCPSessions(conns) }
}

// connectMCPServer connects one server, lists its tools following
// pagination and mounts them namespaced.
func connectMCPServer(ctx context.Context, spec MCPServerSpec) (mcpConn, []Tool, error) {
	hc := &http.Client{Transport: headerRoundTripper{headers: spec.Headers}}
	var lastErr error
	for _, tr := range spec.transports(hc) {
		// The session context outlives the connect: the legacy SSE
		// transport ties its hanging GET to the connect context, so a
		// timeout-cancellable context would kill the session right after
		// the handshake. The establish itself is bounded by racing the
		// connect against mcpConnectTimeout.
		sessCtx, sessCancel := context.WithCancel(ctx)
		client := mcp.NewClient(&mcp.Implementation{Name: "turnhive", Version: "1.0.0"}, nil)
		sess, err := connectMCPWithTimeout(sessCtx, client, tr, mcpConnectTimeout)
		if err != nil {
			sessCancel()
			lastErr = err
			continue
		}

		listCtx, listCancel := context.WithTimeout(ctx, mcpConnectTimeout)
		serverTools, err := listMCPTools(listCtx, sess)
		listCancel()
		if err != nil {
			// The session was never handed over; close it so its socket
			// does not leak.
			closeMCPSessions([]mcpConn{{session: sess, cancel: sessCancel}})
			// A server that accepted the connection but cannot list its
			// tools is broken, not a different transport: do not fall
			// through to the next candidate.
			return mcpConn{}, nil, fmt.Errorf("list tools: %w", err)
		}

		mounted := mountMCPTools(spec.Name, serverTools, sess)
		return mcpConn{session: sess, cancel: sessCancel}, mounted, nil
	}
	return mcpConn{}, nil, fmt.Errorf("connect: %w", lastErr)
}

// connectMCPWithTimeout races client.Connect against d, bounding the
// establish without tying the session's lifetime to a timeout context
// (the legacy SSE transport's hanging GET lives on the connect context).
// On timeout the caller cancels the connect context, unblocking the
// abandoned goroutine.
func connectMCPWithTimeout(ctx context.Context, client *mcp.Client, tr mcp.Transport, d time.Duration) (*mcp.ClientSession, error) {
	type result struct {
		sess *mcp.ClientSession
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		sess, err := client.Connect(ctx, tr, nil)
		ch <- result{sess, err}
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res.sess, res.err
	case <-timer.C:
		return nil, fmt.Errorf("timed out after %s", d)
	}
}

// listMCPTools lists every tool of the server, following pagination.
func listMCPTools(ctx context.Context, sess *mcp.ClientSession) ([]*mcp.Tool, error) {
	var serverTools []*mcp.Tool
	params := &mcp.ListToolsParams{}
	for {
		res, err := sess.ListTools(ctx, params)
		if err != nil {
			return nil, err
		}
		serverTools = append(serverTools, res.Tools...)
		if res.NextCursor == "" {
			break
		}
		params.Cursor = res.NextCursor
	}
	return serverTools, nil
}

// mountMCPTools wraps the server's tools as turnhive tools, namespaced
// as "{server}__{tool}". Names that would violate the upstream
// function-name constraint (failing every request of the turn) or
// collide within the same server are skipped.
func mountMCPTools(serverName string, serverTools []*mcp.Tool, sess mcpSession) []Tool {
	mounted := make([]Tool, 0, len(serverTools))
	seen := make(map[string]bool, len(serverTools))
	for _, st := range serverTools {
		name := serverName + "__" + st.Name
		if !mcpToolNamePattern.MatchString(name) || seen[name] {
			continue
		}
		seen[name] = true
		mounted = append(mounted, mcpTool{
			spec: llm.ToolDef{
				Name:        name,
				Description: st.Description,
				Parameters:  toolParameters(st.InputSchema),
			},
			session:  sess,
			origName: st.Name,
		})
	}
	return mounted
}

// toolParameters converts an MCP tool's input schema to the OpenAI
// function calling parameters shape.
func toolParameters(schema any) map[string]any {
	if schema == nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	data, err := json.Marshal(schema)
	if err != nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	var out map[string]any
	if err = json.Unmarshal(data, &out); err != nil || out == nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return out
}

// mcpConn is a live MCP client session plus the cancel func of the
// context the session's transport lives on (the legacy SSE hanging GET).
type mcpConn struct {
	session mcpSession
	cancel  context.CancelFunc
}

// closeMCPSessions closes every connection in parallel, each bounded by
// mcpCloseTimeout on a detached context: teardown must not hang the turn
// even when the turn's context is already cancelled.
func closeMCPSessions(conns []mcpConn) {
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.cancel()
			ctx, cancel := context.WithTimeout(context.Background(), mcpCloseTimeout)
			defer cancel()
			done := make(chan struct{})
			go func() {
				_ = conn.session.Close()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
		}()
	}
	wg.Wait()
}
