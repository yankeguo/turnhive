# AGENTS.md

turnhive：可水平扩展的 Agent 集群服务。对外只暴露极简 Session API（创建 session、SSE 交互、外部工具回报、销毁），大模型调用、沙箱执行、skills 安装、跨节点路由全部在集群内部完成。

## 构建与测试

```sh
go build ./...     # 编译
go vet ./...       # 静态检查
go test ./...      # 单元测试（agent / llm / storage / 根目录 client）
go run ./cmd/turnhive -config config.yml   # 本地启动（需要 config.yml）
```

提交前必须通过以上全部命令。没有 linter 配置文件，保持 `gofmt`/`go vet` 干净即可。

## 项目结构

```
├── client.go        # Go 客户端 SDK（package turnhive，根目录）
├── stream.go        # 客户端 SSE 流解析
├── cmd/turnhive/    # 服务端入口（HTTP server，优雅关闭）
├── agent/           # Agent turn 循环、沙箱/外部工具、skills 安装、历史持久化
├── config/          # 配置文件加载与校验
├── controller/      # HTTP 路由与业务逻辑（含跨节点转发、SSE）
├── llm/             # OpenAI-compatible 流式 chat completions 客户端
├── registry/        # 基于 etcd 的节点发现、存活与 session 归属
└── storage/         # S3 封装（历史 JSONL、skill tar presign）
```

## 代码约定

- 代码、注释、commit message 一律英文；README 与本文档用中文。
- commit message 用 Conventional Commits 小写格式，如 `feat: ...`、`fix: ...`。
- 新增依赖前先确认现有依赖无法覆盖；`llm/` 刻意只用标准库实现。
- 对外行为（API、事件、配置项）变更时同步更新 README.md 与 config.example.yml 注释。
- 客户端 SDK（根目录）与服务端类型独立定义，SDK 不得 import 服务端内部包（controller/agent 等），避免把 etcd/ironhive 依赖传染给使用方。

## 本地开发依赖

服务端运行需要三个外部服务（本机均已常驻）：

- **etcd**：`127.0.0.1:2379`，无认证。
- **S3（RustFS）**：`127.0.0.1:9000`，无 TLS，需 `force_path_style: true`；凭证从 `~/.config/rustfs/env` 读取。
- **ironhive**：`http://127.0.0.1:30173`（k3s NodePort），default 池有热备沙盒。

`config.yml` 是本地运行配置，**含凭证，已被 .gitignore，严禁提交**；模板见 config.example.yml。

**坑**：skills 通过 S3 presigned URL 由沙盒 Pod 自行下载，`s3.endpoint` 必须是 Pod 可达的地址——`127.0.0.1` 在 k3s Pod 内不可达，本地验证时改用宿主机 LAN IP（如 `192.168.1.133:9000`）。

## 关键设计（改动前必读）

- session 同时只允许一个进行中的 turn，并发返回 409 `session_busy`。
- 沙盒租约在 session 存活期间由 controller 按 lease/2 间隔自动续约；DELETE session 和优雅关闭（`Controller.Close`）都会停止续约并释放沙盒——新增 session 生命周期路径时必须维护这两条清理路径。
- 跨节点路由靠 etcd 归属记录 + `X-Turnhive-Forwarded` 头防转发循环；已转发的请求绝不二次转发。
- SSE 响应头延迟到首个事件才写入，使 `session_busy` 等错误仍能以纯 JSON 返回。
- LLM 历史只持久化 {user, assistant} 对（S3 JSONL），tool 交互是 turn 内瞬态。
