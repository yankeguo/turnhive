# turnhive

可水平扩展的 Agent 集群服务。

turnhive 对外只暴露极简的 Session API：客户端**创建 session**，然后**与 session 交互**，即可流式获取 Agent 的返回。大模型调用、工具调用、命令行执行、子 session 创建与编排等所有复杂逻辑均在集群内部完成，客户端无需感知水平扩展和调度的任何细节。

## 核心概念

- **Session**：一次完整的 Agent 任务上下文。客户端创建 session 后，所有交互都围绕它进行。
- **水平扩展**：session 在集群内的任意节点上执行，调度、扩容、容错对客户端完全透明。
- **子 session**：Agent 在执行过程中可以内部派生子 session 并行处理子任务，结果汇聚后返回，客户端无需关心。

## 对外 API

客户端只需要四个接口：

| 接口 | 说明 |
| --- | --- |
| `POST /v1/sessions` | 创建 session，返回 session ID |
| `POST /v1/sessions/{id}/messages` | 向 session 发送输入，流式（SSE）返回 Agent 输出 |
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
    "headers": {"Authorization": "Bearer ..."}            // 认证头和其他额外头
  },
  "prompt": {"system": "you are an agent"},               // 系统提示词
  "ironhive": {"pool": "default"},                        // 沙箱池
  "skills": [                          // 可选；tar 包会以 presigned URL 注入沙箱 /skills/<name>/
    {"name": "code", "description": "...", "object_key": "skills/code.tar"}
  ],
  "mcp_servers": [                     // 可选（二期接入）
    {"name": "fs", "url": "http://...", "headers": {}}
  ],
  "tools": [                           // 可选；外部工具，由客户端执行
    {"name": "deploy", "description": "...", "parameters": {"type": "object", "properties": {}}}
  ]
}
```

### 与 session 交互（SSE）

`POST /v1/sessions/{id}/messages`，body `{"content": "..."}`，响应为 `text/event-stream`，事件：

| 事件 | 数据 | 说明 |
| --- | --- | --- |
| `delta` | `{"text"}` | 模型输出增量 |
| `reasoning_delta` | `{"text"}` | 推理内容增量 |
| `tool_call` | `{"id","name","status"}` | 工具调用开始（`running`）/结束（`done`/`error`） |
| `done` | `{"text"}` | 本轮完成，text 为完整回复 |
| `error` | `{"message"}` | 本轮失败 |

session 同时只允许一个进行中的 turn，并发请求返回 `409 {"error":"session_busy"}`。

### 外部工具回报

Agent 调用 `tools[]` 中声明的外部工具时，SSE 会发出 `tool_call` 事件（含 `id`）。客户端执行后通过 `POST /v1/sessions/{id}/tool_results` 回报：

```jsonc
{"call_id": "<tool_call 事件的 id>", "result": { /* 任意 JSON */ }}
// 或失败时：
{"call_id": "...", "error": "错误描述"}
```

集群内部能力（对客户端不可见）：

- 大模型调用（OpenAI-compatible 流式端点）
- 沙箱内工具：read / write / edit / apply_patch / shell（全部经 ironhive 沙箱执行）
- 子 session 创建与结果汇聚（二期）

## 客户端用法（规划）

根目录预留为 Go 客户端 SDK（`package turnhive`），其他项目可直接 import：

```go
import "github.com/yankeguo/turnhive"

cli := turnhive.NewClient("http://turnhive:8080")

sess, _ := cli.CreateSession(ctx, turnhive.CreateSessionRequest{ /* ... */ })
defer cli.DeleteSession(ctx, sess.ID)

stream, _ := cli.SendMessage(ctx, sess.ID, "帮我分析这个仓库的代码结构")
for event := range stream.Events() {
    // 流式处理 Agent 输出
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
└── (根目录)          # 预留给 Go 客户端 SDK
```

## 快速开始

```sh
go run ./cmd/turnhive
# listening on :8080
```

## License

见 [LICENSE](LICENSE)。
