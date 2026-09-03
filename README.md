# turnhive

可水平扩展的 Agent 集群服务。

turnhive 对外只暴露极简的 Session API：客户端**创建 session**，然后**与 session 交互**，即可流式获取 Agent 的返回。大模型调用、工具调用、命令行执行、子 session 创建与编排等所有复杂逻辑均在集群内部完成，客户端无需感知水平扩展和调度的任何细节。

## 核心概念

- **Session**：一次完整的 Agent 任务上下文。客户端创建 session 后，所有交互都围绕它进行。
- **水平扩展**：session 在集群内的任意节点上执行，调度、扩容、容错对客户端完全透明。
- **子 session**：Agent 在执行过程中可以内部派生子 session 并行处理子任务，结果汇聚后返回，客户端无需关心。

## 对外 API

客户端只需要五个接口：

| 接口 | 说明 |
| --- | --- |
| `POST /v1/sessions` | 创建 session，返回 session ID |
| `GET /v1/sessions/{id}` | 查询 session 明细（附加文件、持久化对象、运行中 turn），供统计/审计 |
| `POST /v1/sessions/{id}/messages` | 向 session 发送输入，异步受理，返回 turn ID |
| `POST /v1/sessions/{id}/files` | 为 session 附加用户文件（共享 bucket 中的 object_key），任意时刻可调用 |
| `POST /v1/sessions/{id}/cancel` | 中断当前进行中的 turn（空闲时返回 409 `no_turn_running`）；恢复 = 重新发送消息 |
| `GET /v1/sessions/{id}/events` | session 事件流（SSE）：所有 turn 的输出都经此通道下发 |
| `POST /v1/sessions/{id}/tool_results` | 回报外部工具的执行结果 |
| `DELETE /v1/sessions/{id}` | 销毁 session，释放其占用的集群资源 |

### 创建 session

`POST /v1/sessions` 的 JSON body：

```jsonc
{
  "model": {
    "url": "https://api.example.com/v1/chat/completions", // 推理端点完整 URL
    "protocol": "openai_completions",                     // 当前固定
    "name": "my-model",
    "headers": {"Authorization": "Bearer ..."},           // 认证头和其他额外头
    "max_context": 131072,                                // 可选；模型上下文窗口（tokens），驱动上下文窗口管理
    "features": ["support_image"]                            // 可选；能力标记，当前仅 support_image
  },
  "prompt": {"system": "you are an agent"},               // 系统提示词
  "ironhive": {"pool": "default"},                        // 沙箱池
  "skills": [                          // 可选；tar 包会以 presigned URL 注入沙箱 ./.agents/skills/<name>/
    {"name": "code", "description": "...", "object_key": "skills/code.tar"}
  ],
  "mcp_servers": [                     // 可选；MCP server（仅 SSE / Streamable HTTP，不支持 stdio）
    {"name": "fs", "url": "http://...", "headers": {}, "transport": "streamable"}
  ],
  "tools": [                           // 可选；外部工具，由客户端执行
    {"name": "deploy", "description": "...", "parameters": {"type": "object", "properties": {}}}
  ]
}
```

### 发送消息（异步受理）

`POST /v1/sessions/{id}/messages`，body `{"content": "..."}`。turn 在集群内部异步执行，响应为 `202 {"turn_id": "turn-..."}`；turn 的全部输出经事件流（见下）下发。客户端断开此请求不会中断 turn。

消息中引用已附加的文件由调用方自行组织：文件固定位于沙箱 `./.agents/uploads/<name>`，调用方以任意格式、任意时机把指向文件的 marker 文本写进 `content` 即可——turnhive 不规定 marker 格式。

session 同时只允许一个进行中的 turn，并发请求返回 `409 {"error":"session_busy"}`。

所有 JSON 请求体的上限为 4MB，超限返回 400。

### 附加用户文件

用户可能随时上传文件。turnhive 与调用方共享同一个 S3 bucket，文件传递一律以 **object_key** 形式进行，turnhive 不中转字节：调用方收到用户文件后自行 PUT 到 bucket，然后在任意时刻 `POST /v1/sessions/{id}/files` 登记：

```jsonc
{"files": [{"name": "data.csv", "object_key": "uploads/sess-xxx/abc-data.csv", "size": 12345}]}
```

- `name` 须匹配 `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`（纯文件名，不含路径），在 session 内唯一（同名重复登记会覆盖）；每个 session 最多 64 个文件
- 登记后文件会被注入沙箱 `./.agents/uploads/<name>`：有活沙箱时立即注入（沙箱经 presigned URL 自行从 S3 拉取），否则下次沙箱构建时随 skills/persisted 一起注入
- 文件清单作为 session 状态持久化到 `sessions/{id}/files.json`：沙箱 idle 回收重建、节点崩溃收养、冷换出复活后，文件都会恢复到新沙箱；同时随事件流 `sync` 帧的 `files` 字段下发
- 删除文件不提供接口：同名重新登记即替换；沙箱是一次性的，旧文件随沙箱销毁消失

### 查询 session 明细（统计/审计）

`GET /v1/sessions/{id}` 返回 `200 {"id", "turn_id", "files", "persisted"}`：附加的用户文件列表、persist 工具写入的持久化对象列表、当前运行中的 turn（空闲为 ""）。冷 session 会经 adopt 路径恢复后返回，不存在的 session 返回 404。

### 中断 turn

`POST /v1/sessions/{id}/cancel` 中断当前进行中的 turn：其上下文被取消、部分回复落盘并通过 `turn_finished` 事件（`status` 为 `cancelled`）收尾（与失败区分开），返回 `202 {"turn_id":"...","status":"cancelled"}`（空闲时 `409 {"error":"no_turn_running"}`）。中断的 turn 不重放也不可恢复——客户端重新 `POST messages` 即视作恢复，之前已发出的 user 消息仍在历史中（write-ahead）。

### session 事件流（SSE）

`GET /v1/sessions/{id}/events`，响应为 `text/event-stream`。连接建立后先收到一个 `sync` 控制事件（当前进行中的 turn 与最新序号，控制帧不占事件序号、不带 `id`），随后是缓冲的历史事件与实时事件。数据事件带 `id: <seq>`（SSE 标准字段，按 session 单调递增）；断线后带 `?last_seq=<N>`（或 `Last-Event-ID` 头）重连即可从断点重放（服务端保留最近 2000 条事件）。

| 事件 | 数据 | 说明 |
| --- | --- | --- |
| `sync` | `{"turn_id","seq","messages","persisted","files"}` | 连接后的首帧：当前 turn（空闲为 ""）、最新序号、合并后的全部历史消息（{user, assistant} 对）、session 已持久化的对象列表（persist 工具写入）以及用户附加的文件列表（`files`）——客户端凭此一帧完成同步 |
| `turn_started` | `{"turn_id"}` | 一个 turn 开始 |
| `delta` | `{"turn_id","text"}` | 模型输出增量 |
| `reasoning_delta` | `{"turn_id","text"}` | 推理内容增量 |
| `tool_call` | `{"turn_id","id","name","status"}` | 工具调用开始（`running`）/结束（`done`/`error`） |
| `turn_finished` | `{"turn_id","status","text"?,"message"?}` | 一个 turn 结束——每个 turn 的唯一收尾事件，与 `turn_started` 配对。`status`：`done`（成功，`text` 为完整回复）/ `error`（失败，`message` 为原因）/ `cancelled`（被 cancel 端点中断，用户主动中断，非失败） |

### 外部工具回报

Agent 调用 `tools[]` 中声明的外部工具时，SSE 会发出 `tool_call` 事件（含 `id`）。客户端执行后通过 `POST /v1/sessions/{id}/tool_results` 回报：

```jsonc
{"call_id": "<tool_call 事件的 id>", "result": { /* 任意 JSON */ }}
// 或失败时：
{"call_id": "...", "error": "错误描述"}
```

未被等待中的工具调用匹配的回报会暂存（上限 256 条，超限返回 429），turn 结束时全部清空——turn 结束后才回报的迟到结果不会被后续 turn 消费。

集群内部能力（对客户端不可见）：

- 大模型调用（OpenAI-compatible 流式端点；429 限流响应按 `Retry-After` 退避重试，最多 3 次，未带 `Retry-After` 时指数退避——仅在响应建立前重试，流式开始后绝不重试）
- 沙箱内工具：read / write / edit / apply_patch / shell（全部经 ironhive 沙箱执行）；`model.features` 含 `support_image` 时额外启用 load_media（沙箱图片注入上下文供视觉分析）；persist（把沙盒文件转存到 S3 `sessions/{id}/persisted/` 并记录为 session 属性，随 sync 帧下发）
- 用户文件注入：`POST .../files` 登记的文件（共享 bucket 的 object_key）在沙箱创建/重建时经 presigned URL 注入 `./.agents/uploads/<name>`，清单持久化到 `sessions/{id}/files.json` 并随 sync 帧下发——与 persist 对称：persist 是沙箱→S3 的中间/最终产物保存，files 是 S3→沙箱的用户输入
- 上下文窗口管理（`model.max_context` 驱动）：每个 turn 前按估算整轮丢弃最旧历史（保留最近 turn 与回复预算），turn 用量超 0.8×窗口后把旧 turn 压缩为结构化 `<context-summary>` 摘要（保留最近 2 轮原文）
- MCP 工具接入：`mcp_servers[]` 声明的 server 在每个 turn 开始时现场连接（单 server 连接+列工具 10s 上限），其工具以 `{name}__{tool}` 命名空间挂载、仅本 turn 有效，turn 结束全部断开；单个 server 失败只记日志，不拖垮其他 server 与 turn。`transport` 可选 `"streamable"` / `"sse"`，缺省 auto（先试 streamable HTTP，连接失败回退 legacy SSE）；不支持 stdio。`name` 须匹配 `^[a-zA-Z0-9_-]{1,32}$` 且在 `mcp_servers` 内唯一
- 出口调用标识：每次 LLM 请求与每个 MCP 连接固定携带 `X-Turnhive-Session: <session-id>` 头（覆盖 spec 中的同名头，防冒名）。上游网关可据此反推 session 归属做门控与计费归因（内网信任）；不做门控的上游直接忽略该头即可——turnhive 也可以脱离网关直连 provider / MCP server
- session 无限恢复：沙盒空闲超 `session.idle_timeout` 被回收但 session 不销毁；下一条消息时自动重建沙盒（重装 skills、回灌 persisted 文件、从 S3 重载历史），会话无缝继续
- 崩溃恢复（热/冷 session）：session 创建时 spec 落盘 S3（`sessions/{id}/spec.json`），每个 turn 的 user 消息先写盘再执行（write-ahead）。节点崩溃后 etcd 归属记录消失，任意节点收到该 session 的请求时从 S3 收养（adopt）：加载 spec 与 persisted 清单、抢到归属（etcd put-if-absent）、重建历史——中断的 turn 以 `[turn interrupted]` 标记封存，不重放（工具副作用不可重放），客户端重发或继续即可。空闲超 `session.cold_timeout`（默认 0=关闭）的 session 整体换出为冷 session（摘内存、删 etcd、断开 SSE），下次访问经同一条 adopt 路径复活；换出后事件序号从 0 重启，客户端以 sync 帧为准重置。**注意**：spec 含 model/MCP headers 中的凭证，会随 spec.json 进入 S3 bucket（与全部会话历史同一信任域）
- 后台进程退出通知：shell 后台命令（`bg: true` 或超 30s 前台窗口）退出时，集群自动合成一条 user 消息（`<background_processes_exited>`，含 pid/command/exit_code/输出文件路径）并开一个新 turn，Agent 立即自行善后（查输出、继续后续工作）；turn 运行中积累的多个退出会合并为一条消息、turn 结束自动补发。对客户端完全透明——合成 turn 就是普通 turn，消息以 user 身份出现在事件流与历史中

## 客户端用法

根目录为 Go 客户端 SDK（`package turnhive`），其他项目可直接 import：

```go
import "github.com/yankeguo/turnhive"

cli := turnhive.NewClient("http://turnhive:8080")

sess, _ := cli.CreateSession(ctx, turnhive.CreateSessionRequest{ /* ... */ })
defer cli.DeleteSession(ctx, sess.ID)

turnID, _ := cli.SendMessage(ctx, sess.ID, "帮我分析这个仓库的代码结构")

// 附带用户文件：调用方先把文件 PUT 到共享 bucket 得到 object_key，再在任意
// 时刻登记到 session；消息中如何引用（marker 格式与时机）由调用方自行组织，
// 文件固定位于沙箱 ./.agents/uploads/<name>
_ = cli.AttachFiles(ctx, sess.ID, []turnhive.FileRef{{Name: "data.csv", ObjectKey: "uploads/sess-xxx/abc-data.csv", Size: 12345}})
turnID2, _ := cli.SendMessage(ctx, sess.ID, "分析 ./.agents/uploads/data.csv 这份数据")
_ = turnID2

// 统计/审计：查询 session 明细
detail, _ := cli.GetSession(ctx, sess.ID) // detail.Files / detail.Persisted / detail.TurnID
_ = detail

stream, _ := cli.Events(ctx, sess.ID, 0) // 断线后用最后看到的 event.Seq 重连重放
for event := range stream.Events() {
    // event.Type: sync / turn_started / delta / reasoning_delta / tool_call / turn_finished
    // turn_finished 的 event.Status: done（event.Text 为完整回复）/ error（event.Message 为原因）/ cancelled
    // 外部工具调用（event.Type == tool_call 且 status 为 running）执行后：
    //   cli.ReportToolResult(ctx, sess.ID, event.ID, result)
    //   或 cli.ReportToolError(ctx, sess.ID, event.ID, err)
    _ = turnID
}
```

## 集群发现与路由

节点启动时在 etcd 注册自身（`{prefix}/nodes/{nodeID}`，挂 lease 自动 keepalive），创建 session 时把归属关系写入 `{prefix}/sessions/{sessionID}`。任何节点收到 session 相关请求时，先查本地，再查 etcd 定位 owner 节点并透明反向代理过去，客户端无感知。节点宕机后 lease 过期，其节点记录和名下 session 记录自动清除。

## 项目结构

```
├── cmd/turnhive/    # 服务端入口（HTTP server，优雅关闭）
├── agent/           # Agent turn 循环、沙箱/外部工具、skills 安装、历史持久化
├── config/          # 配置文件加载与校验
├── controller/      # HTTP 路由与业务逻辑（含跨节点转发、SSE）
├── llm/             # OpenAI-compatible 流式 chat completions 客户端
├── registry/        # 基于 etcd 的节点发现、存活与 session 归属
├── storage/         # S3 封装（历史 JSONL、skill tar presign）
└── (根目录)          # Go 客户端 SDK（package turnhive）
```

## 快速开始

```sh
go run ./cmd/turnhive
# listening on :8080
```

发布镜像由 CI（`.github/workflows/release.yml`）多阶段构建并同时推送：`ghcr.io/yankeguo/turnhive` 与 `quay.io/yankeguo/turnhive`（main 分支更新 `latest` / `latest-<sha>`，git tag 发布同名镜像标签）。

## License

见 [LICENSE](LICENSE)。
