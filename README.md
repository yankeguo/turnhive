# turnhive

可水平扩展的 Agent 集群服务。

turnhive 对外只暴露极简的 Session API：客户端**创建 session**，然后**与 session 交互**，即可流式获取 Agent 的返回。大模型调用、工具调用、命令行执行、子 session 创建与编排等所有复杂逻辑均在集群内部完成，客户端无需感知水平扩展和调度的任何细节。

## 核心概念

- **Session**：一次完整的 Agent 任务上下文。客户端创建 session 后，所有交互都围绕它进行。
- **水平扩展**：session 在集群内的任意节点上执行，调度、扩容、容错对客户端完全透明。
- **子 session**：Agent 在执行过程中可以内部派生子 session 并行处理子任务，结果汇聚后返回，客户端无需关心。

## 对外 API（草案）

客户端只需要三个接口：

| 接口 | 说明 |
| --- | --- |
| `POST /v1/sessions` | 创建 session，返回 session ID |
| `POST /v1/sessions/{id}/messages` | 向 session 发送输入，流式（SSE）返回 Agent 输出 |
| `DELETE /v1/sessions/{id}` | 销毁 session，释放其占用的集群资源 |

集群内部能力（对客户端不可见）：

- 大模型调用（多 provider）
- 工具调用（function calling）
- 命令行执行（沙箱）
- 子 session 创建与结果汇聚

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
├── config/          # 配置文件加载与校验
├── controller/      # HTTP 路由与业务逻辑（含跨节点转发）
├── registry/        # 基于 etcd 的节点发现、存活与 session 归属
└── (根目录)          # 预留给 Go 客户端 SDK
```

## 快速开始

```sh
go run ./cmd/turnhive
# listening on :8080
```

## License

见 [LICENSE](LICENSE)。
