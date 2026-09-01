package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// mcpTestArgs is the argument shape of the echo tool every test server
// mounts.
type mcpTestArgs struct {
	Text string `json:"text"`
}

// newMCPEchoServer builds an MCP server with a single echo tool that
// returns its text argument.
func newMCPEchoServer() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "echo", Description: "echo back the text"}, func(_ context.Context, _ *mcp.CallToolRequest, args mcpTestArgs) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: args.Text}}}, nil, nil
	})
	return srv
}

// streamableServer serves srv over streamable HTTP until test end.
func streamableServer(t *testing.T, srv *mcp.Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	return ts
}

// sseServer serves srv over legacy SSE until test end.
func sseServer(t *testing.T, srv *mcp.Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	return ts
}

// collectStatuses returns an onStatus callback recording every status.
func collectStatuses() (func(MCPServerStatus), func() []MCPServerStatus) {
	var statuses []MCPServerStatus
	return func(st MCPServerStatus) { statuses = append(statuses, st) },
		func() []MCPServerStatus { return statuses }
}

func TestConnectMCPServersStreamable(t *testing.T) {
	ts := streamableServer(t, newMCPEchoServer())
	record, statuses := collectStatuses()

	tools, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "test", URL: ts.URL},
	}, record)
	defer closeAll()

	st := statuses()[0]
	if st.Err != nil {
		t.Fatalf("connect failed: %v", st.Err)
	}
	if st.ToolCount != 1 || len(tools) != 1 {
		t.Fatalf("expected 1 mounted tool, got status %+v and %d tools", st, len(tools))
	}
	spec := tools[0].Spec()
	if spec.Name != "test__echo" {
		t.Fatalf("expected namespaced tool name test__echo, got %q", spec.Name)
	}
	if spec.Description == "" || spec.Parameters["type"] != "object" {
		t.Fatalf("unexpected tool spec: %+v", spec)
	}

	out, err := tools[0].Execute(context.Background(), "call-1", json.RawMessage(`{"text":"hello"}`))
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if out != "hello" {
		t.Fatalf("expected echo output, got %q", out)
	}
}

func TestConnectMCPServersSSE(t *testing.T) {
	ts := sseServer(t, newMCPEchoServer())

	// Explicit SSE transport works.
	tools, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "test", URL: ts.URL, Transport: MCPTransportSSE},
	}, nil)
	defer closeAll()
	if len(tools) != 1 {
		t.Fatalf("expected 1 tool over explicit SSE, got %d", len(tools))
	}
	// The session must stay usable after the connect returns (the legacy
	// SSE hanging GET lives on the connect context).
	out, err := tools[0].Execute(context.Background(), "call-0", json.RawMessage(`{"text":"explicit"}`))
	if err != nil || out != "explicit" {
		t.Fatalf("unexpected explicit SSE execute result: %q, %v", out, err)
	}

	// Auto mode falls back from streamable to SSE.
	record, statuses := collectStatuses()
	tools, closeAll2 := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "test", URL: ts.URL},
	}, record)
	defer closeAll2()
	if st := statuses()[0]; st.Err != nil || st.ToolCount != 1 {
		t.Fatalf("auto fallback to SSE failed: %+v", st)
	}
	out, err = tools[0].Execute(context.Background(), "call-1", json.RawMessage(`{"text":"via sse"}`))
	if err != nil || out != "via sse" {
		t.Fatalf("unexpected SSE execute result: %q, %v", out, err)
	}

	// Forcing streamable against an SSE-only server fails, but only
	// marks the status.
	record, statuses = collectStatuses()
	tools, closeAll3 := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "test", URL: ts.URL, Transport: MCPTransportStreamable},
	}, record)
	defer closeAll3()
	if st := statuses()[0]; st.Err == nil {
		t.Fatalf("expected streamable-against-SSE to fail, got %+v", st)
	}
	if len(tools) != 0 {
		t.Fatalf("expected no tools from failed server, got %d", len(tools))
	}
}

func TestConnectMCPServersHeaders(t *testing.T) {
	var gotAuth string
	inner := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return newMCPEchoServer() }, nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a := r.Header.Get("Authorization"); a != "" {
			gotAuth = a
		}
		inner.ServeHTTP(w, r)
	}))
	defer ts.Close()

	_, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "test", URL: ts.URL, Headers: map[string]string{"Authorization": "Bearer secret"}},
	}, nil)
	defer closeAll()
	if gotAuth != "Bearer secret" {
		t.Fatalf("spec headers not forwarded, got Authorization %q", gotAuth)
	}
}

func TestMCPToolErrorResult(t *testing.T) {
	srv := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v0.0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "fail"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "boom"}},
		}, nil, nil
	})
	// A non-text content is reported as omitted, not lost silently.
	mcp.AddTool(srv, &mcp.Tool{Name: "image"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.ImageContent{Data: []byte("png"), MIMEType: "image/png"}},
		}, nil, nil
	})
	ts := streamableServer(t, srv)

	tools, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{{Name: "test", URL: ts.URL}}, nil)
	defer closeAll()
	byName := map[string]Tool{}
	for _, tool := range tools {
		byName[tool.Spec().Name] = tool
	}

	_, err := byName["test__fail"].Execute(context.Background(), "call-1", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected IsError to surface as error, got %v", err)
	}

	out, err := byName["test__image"].Execute(context.Background(), "call-2", json.RawMessage(`{}`))
	if err != nil || !strings.Contains(out, "[mcp: omitted") {
		t.Fatalf("expected non-text content placeholder, got %q, %v", out, err)
	}
}

func TestConnectMCPServersDeadServerDoesNotBlockOthers(t *testing.T) {
	ts := streamableServer(t, newMCPEchoServer())
	record, statuses := collectStatuses()

	tools, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{
		{Name: "dead", URL: "http://127.0.0.1:1"},
		{Name: "live", URL: ts.URL},
	}, record)
	defer closeAll()

	st := statuses()
	if st[0].Err == nil || st[0].Name != "dead" {
		t.Fatalf("expected dead server status error, got %+v", st[0])
	}
	if st[1].Err != nil || st[1].ToolCount != 1 {
		t.Fatalf("expected live server mounted, got %+v", st[1])
	}
	if len(tools) != 1 || tools[0].Spec().Name != "live__echo" {
		t.Fatalf("expected only live__echo mounted, got %+v", tools)
	}
}

func TestConnectMCPServersSkipsInvalidToolNames(t *testing.T) {
	srv := newMCPEchoServer()
	// A tool whose namespaced name violates the upstream function-name
	// constraint must be skipped instead of failing every request.
	mcp.AddTool(srv, &mcp.Tool{Name: "bad.name"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "x"}}}, nil, nil
	})
	ts := streamableServer(t, srv)
	record, statuses := collectStatuses()

	tools, closeAll := ConnectMCPServers(context.Background(), []MCPServerSpec{{Name: "test", URL: ts.URL}}, record)
	defer closeAll()
	st := statuses()[0]
	if st.Err != nil {
		t.Fatalf("connect failed: %v", st.Err)
	}
	// Only the valid echo tool is mounted; the status counts mounted
	// tools, not listed ones.
	if st.ToolCount != 1 || len(tools) != 1 || tools[0].Spec().Name != "test__echo" {
		t.Fatalf("expected only test__echo mounted, got status %+v and %+v", st, tools)
	}
}

func TestMCPToolParametersFallback(t *testing.T) {
	if got := toolParameters(nil); got["type"] != "object" {
		t.Fatalf("nil schema should fall back to empty object schema, got %+v", got)
	}
	if got := toolParameters(map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "string"}}}); got["properties"] == nil {
		t.Fatalf("schema should round-trip, got %+v", got)
	}
}
