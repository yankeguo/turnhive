# turnhive

A horizontally scalable agent cluster service.

turnhive exposes only a minimal Session API: clients **create a session**, then **interact with it** to stream the agent's output. LLM calls, tool calls, shell execution, sub-session creation and orchestration all happen inside the cluster — clients are shielded from every detail of horizontal scaling and scheduling.

## Core Concepts

- **Session** — the complete context of one agent task. Once created, all client interaction revolves around it.
- **Horizontal scaling** — sessions execute on any node in the cluster; scheduling, scale-out and fault tolerance are fully transparent to clients.
- **Sub-sessions** — while running, the agent may internally spawn sub-sessions to process subtasks in parallel and aggregate the results; clients never see this.

## Public API

Clients interact through a small set of endpoints:

| Endpoint | Description |
| --- | --- |
| `POST /v1/sessions` | Create a session; returns the session ID |
| `GET /v1/sessions/{id}` | Session details (attached files, persisted objects, running turn) for stats/auditing |
| `POST /v1/sessions/{id}/messages` | Send input to the session; accepted asynchronously, returns a turn ID |
| `POST /v1/sessions/{id}/files` | Attach user files (object_keys in the shared bucket) to the session; callable at any time |
| `POST /v1/sessions/{id}/cancel` | Interrupt the currently running turn (`409 no_turn_running` when idle); recovery = resend the message |
| `GET /v1/sessions/{id}/events` | Session event stream (SSE): all turn output is delivered over this channel |
| `POST /v1/sessions/{id}/tool_results` | Report the result of an external tool call |
| `DELETE /v1/sessions/{id}` | Destroy the session and release its cluster resources |

### Create a session

JSON body of `POST /v1/sessions`:

```jsonc
{
  "model": {
    "url": "https://api.example.com/v1/chat/completions", // full URL of the inference endpoint
    "protocol": "openai_completions",                     // currently fixed
    "name": "my-model",
    "headers": {"Authorization": "Bearer ..."},           // auth header and any extra headers
    "max_context": 131072,                                // optional; model context window (tokens), drives context-window management
    "features": ["support_image"]                         // optional; capability flags, currently only support_image
  },
  "prompt": {"system": "you are an agent"},               // system prompt
  "ironhive": {"pool": "default"},                        // sandbox pool
  "skills": [                          // optional; tar archives injected via presigned URL into ./.agents/skills/<name>/ in the sandbox
    {"name": "code", "description": "...", "object_key": "skills/code.tar"}
  ],
  "mcp_servers": [                     // optional; MCP servers (SSE / Streamable HTTP only, no stdio)
    {"name": "fs", "url": "http://...", "headers": {}, "transport": "streamable"}
  ],
  "tools": [                           // optional; external tools executed by the client
    {"name": "deploy", "description": "...", "parameters": {"type": "object", "properties": {}}}
  ]
}
```

### Send a message (accepted asynchronously)

`POST /v1/sessions/{id}/messages` with body `{"content": "..."}`. The turn executes asynchronously inside the cluster; the response is `202 {"turn_id": "turn-..."}` and all turn output is delivered over the event stream (see below). Disconnecting this request does not interrupt the turn.

How a message references attached files is entirely up to the caller: files are always located at `./.agents/uploads/<name>` inside the sandbox, and the caller may embed a marker pointing at a file into `content` in any format, at any time — turnhive does not prescribe a marker format.

A session allows only one running turn at a time; concurrent requests get `409 {"error":"session_busy"}`.

All JSON request bodies are capped at 4MB; larger bodies get 400.

### Attach user files

Users may upload files at any time. turnhive and the caller share the same S3 bucket, and files are always passed as **object_keys** — turnhive never proxies bytes. The caller PUTs the user's file into the bucket itself, then registers it at any time via `POST /v1/sessions/{id}/files`:

```jsonc
{"files": [{"name": "data.csv", "object_key": "uploads/sess-xxx/abc-data.csv", "size": 12345}]}
```

- `name` must match `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$` (a bare file name, no path) and is unique within the session (re-registering the same name overwrites it); at most 64 files per session
- Once registered, the file is injected into the sandbox at `./.agents/uploads/<name>`: immediately when a live sandbox exists (the sandbox pulls it from S3 itself via a presigned URL), otherwise together with skills/persisted files on the next sandbox build
- The file list is persisted as session state in `sessions/{id}/files.json`: after sandbox idle reaping and rebuild, node-crash adoption, or cold-swap revival, the files are restored into the new sandbox; the list is also delivered in the `files` field of the `sync` frame
- There is no delete API: re-registering under the same name replaces the file; sandboxes are disposable, so stale files vanish with the sandbox

### Query session details (stats/auditing)

`GET /v1/sessions/{id}` returns `200 {"id", "turn_id", "files", "persisted"}`: the attached user files, the persisted objects written by the persist tool, and the currently running turn ("" when idle). A cold session is revived through the adopt path before responding; a nonexistent session returns 404.

### Interrupt a turn

`POST /v1/sessions/{id}/cancel` interrupts the currently running turn: its context is cancelled, the partial reply is persisted, and the turn is closed out with a `turn_finished` event whose `status` is `cancelled` (distinguishing it from a failure). Returns `202 {"turn_id":"...","status":"cancelled"}` (`409 {"error":"no_turn_running"}` when idle). An interrupted turn is never replayed or resumed — the client "recovers" simply by POSTing the message again; the previously sent user message remains in history (write-ahead).

### Session event stream (SSE)

`GET /v1/sessions/{id}/events`, responded as `text/event-stream`. After connecting, the client first receives a `sync` control event (the currently running turn and the latest sequence number; control frames occupy no event sequence and carry no `id`), followed by buffered historical events and live events. Data events carry `id: <seq>` (standard SSE field, monotonically increasing per session); after a disconnect, reconnect with `?last_seq=<N>` (or the `Last-Event-ID` header) to replay from the point of interruption (the server retains the most recent 2000 events).

| Event | Data | Description |
| --- | --- | --- |
| `sync` | `{"turn_id","seq","messages","persisted","files"}` | First frame after connecting: current turn ("" when idle), latest sequence number, the merged full message history ({user, assistant} pairs), the session's persisted objects (written by the persist tool), and the user-attached files (`files`) — the client completes synchronization with this single frame |
| `turn_started` | `{"turn_id"}` | A turn has started |
| `delta` | `{"turn_id","text"}` | Model output increment |
| `reasoning_delta` | `{"turn_id","text"}` | Reasoning content increment |
| `tool_call` | `{"turn_id","id","name","status"}` | A tool call started (`running`) / ended (`done`/`error`) |
| `turn_finished` | `{"turn_id","status","text"?,"message"?}` | A turn has finished — the single closing event of every turn, paired with `turn_started`. `status`: `done` (succeeded, `text` is the full reply) / `error` (failed, `message` is the reason) / `cancelled` (interrupted via the cancel endpoint by user request, not a failure) |

### External tool results

When the agent invokes an external tool declared in `tools[]`, the SSE emits a `tool_call` event (with an `id`). The client executes it and reports back via `POST /v1/sessions/{id}/tool_results`:

```jsonc
{"call_id": "<id from the tool_call event>", "result": { /* any JSON */ }}
// or, on failure:
{"call_id": "...", "error": "error description"}
```

Results that match no waiting tool call are buffered (up to 256, 429 beyond that) and cleared wholesale when the turn ends — a late result arriving after the turn has ended is never consumed by a later turn.

Cluster-internal capabilities (invisible to clients):

- LLM calls (OpenAI-compatible streaming endpoint; 429 rate-limit responses are retried with `Retry-After` backoff, up to 3 times, with exponential backoff when `Retry-After` is absent — retries only happen before the response is established, never once streaming has begun)
- In-sandbox tools: read / write / edit / apply_patch / shell (all executed inside the ironhive sandbox); load_media is additionally enabled when `model.features` includes `support_image` (injecting sandbox images into context for visual analysis); persist (copies sandbox files to S3 `sessions/{id}/persisted/` and records them as a session attribute, delivered with the sync frame)
- User file injection: files registered via `POST .../files` (object_keys in the shared bucket) are injected via presigned URL into `./.agents/uploads/<name>` when the sandbox is created or rebuilt; the list is persisted to `sessions/{id}/files.json` and delivered with the sync frame — symmetric to persist: persist is sandbox→S3 for intermediate/final artifacts, files is S3→sandbox for user input
- Context-window management (driven by `model.max_context`): before each turn, the oldest history is dropped whole turns at a time by estimate (keeping the most recent turns and a reply budget); when a turn's usage exceeds 0.8× the window, older turns are compacted into a structured `<context-summary>` digest (the most recent 2 turns are kept verbatim)
- MCP tool integration: servers declared in `mcp_servers[]` are connected on the fly at the start of each turn (10s limit per server for connect + list tools); their tools are mounted under the `{name}__{tool}` namespace, valid only for that turn, and all connections are dropped when the turn ends. A single server's failure is only logged and never drags down other servers or the turn. `transport` may be `"streamable"` / `"sse"`, default auto (try streamable HTTP first, fall back to legacy SSE if connect fails); stdio is not supported. `name` must match `^[a-zA-Z0-9_-]{1,32}$` and be unique within `mcp_servers`
- Egress call identity: every LLM request and every MCP connection carries the fixed header `X-Turnhive-Session: <session-id>` (overriding any same-named header in the spec, to prevent impersonation). An upstream gateway may use it to attribute sessions for gating and billing (trusted internal network); an upstream that does no gating can simply ignore the header — turnhive can also talk to providers / MCP servers directly without a gateway
- Unlimited session recovery: a sandbox idle beyond `session.idle_timeout` is reaped but the session is not destroyed; the next message automatically rebuilds the sandbox (reinstalling skills, restoring persisted files, reloading history from S3) and the conversation continues seamlessly
- Crash recovery (hot/cold sessions): the spec is persisted to S3 at session creation (`sessions/{id}/spec.json`), and each turn's user message is written before execution (write-ahead). After a node crash the etcd ownership record disappears, and any node receiving a request for that session adopts it from S3: load the spec and the persisted list, claim ownership (etcd put-if-absent), rebuild history — an interrupted turn is sealed with a `[turn interrupted]` marker and never replayed (tool side effects cannot be replayed); the client simply resends or continues. Sessions idle beyond `session.cold_timeout` (default 0 = disabled) are swapped out wholesale as cold sessions (memory evicted, etcd key deleted, SSE disconnected) and revived via the same adopt path on next access; after swap-out, event sequence numbers restart from 0 and clients must reset from the sync frame. **Note**: the spec contains credentials from model/MCP headers and lands in the S3 bucket along with spec.json (same trust domain as all conversation history)
- Background process exit notifications: when a shell background command (`bg: true` or one that outlives the 30s foreground window) exits, the cluster synthesizes a user message (`<background_processes_exited>`, with pid/command/exit_code/output file paths) and starts a new turn, so the agent immediately cleans up on its own (check output, continue follow-up work); multiple exits accumulated during a running turn are merged into one message and flushed automatically when the turn ends. Fully transparent to clients — a synthesized turn is an ordinary turn, and the message appears as a user message in the event stream and in history

## Client usage

The repository root is a Go client SDK (`package turnhive`) that other projects can import directly:

```go
import "github.com/yankeguo/turnhive"

cli := turnhive.NewClient("http://turnhive:8080")

sess, _ := cli.CreateSession(ctx, turnhive.CreateSessionRequest{ /* ... */ })
defer cli.DeleteSession(ctx, sess.ID)

turnID, _ := cli.SendMessage(ctx, sess.ID, "Analyze the code structure of this repo")

// Attaching user files: the caller PUTs the file into the shared bucket to get an
// object_key, then registers it with the session at any time; how the message
// references the file (marker format and timing) is up to the caller — the file
// is always at ./.agents/uploads/<name> in the sandbox
_ = cli.AttachFiles(ctx, sess.ID, []turnhive.FileRef{{Name: "data.csv", ObjectKey: "uploads/sess-xxx/abc-data.csv", Size: 12345}})
turnID2, _ := cli.SendMessage(ctx, sess.ID, "Analyze the data in ./.agents/uploads/data.csv")
_ = turnID2

// Stats/auditing: query session details
detail, _ := cli.GetSession(ctx, sess.ID) // detail.Files / detail.Persisted / detail.TurnID
_ = detail

stream, _ := cli.Events(ctx, sess.ID, 0) // after a disconnect, reconnect with the last seen event.Seq to replay
for event := range stream.Events() {
    // event.Type: sync / turn_started / delta / reasoning_delta / tool_call / turn_finished
    // turn_finished event.Status: done (event.Text is the full reply) / error (event.Message is the reason) / cancelled
    // after executing an external tool call (event.Type == tool_call with status running):
    //   cli.ReportToolResult(ctx, sess.ID, event.ID, result)
    //   or cli.ReportToolError(ctx, sess.ID, event.ID, err)
    _ = turnID
}
```

## Cluster discovery and routing

At startup each node registers itself in etcd (`{prefix}/nodes/{nodeID}`, kept alive by an attached lease), and when a session is created its ownership is written to `{prefix}/sessions/{sessionID}`. When any node receives a session-related request, it checks locally first, then looks up the owner node in etcd and transparently reverse-proxies the request there — invisible to clients. When a node goes down, its lease expires and its node record, along with all session records under it, is removed automatically.

## Project layout

```
├── cmd/turnhive/    # server entrypoint (HTTP server, graceful shutdown)
├── agent/           # agent turn loop, sandbox/external tools, skills installation, history persistence
├── config/          # config file loading and validation
├── controller/      # HTTP routing and business logic (cross-node forwarding, SSE)
├── llm/             # OpenAI-compatible streaming chat completions client
├── registry/        # etcd-based node discovery, liveness and session ownership
├── storage/         # S3 wrapper (history JSONL, skill tar presign)
└── (repo root)      # Go client SDK (package turnhive)
```

## Quick start

```sh
go run ./cmd/turnhive
# listening on :8080
```

Release images are built multi-stage by CI (`.github/workflows/release.yml`) and pushed to both `ghcr.io/yankeguo/turnhive` and `quay.io/yankeguo/turnhive` (main-branch updates publish `latest` / `latest-<sha>`; a git tag publishes an image tag of the same name).

## License

See [LICENSE](LICENSE).
