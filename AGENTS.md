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

另外有 `refs/` 目录存放参考项目副本，刻意不被 git 追踪（已在 .gitignore 中忽略）；需要参考信息时先读 `refs/REFS.md`，了解其中有哪些参考项目及其用途。

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
- 沙箱路径一律相对（不假设 WORKDIR），`..` 逃逸词法拒绝；skills 安装在 `./.agents/skills/<name>/`，该树对 write/edit/apply_patch 只读。绝对路径直接透传（沙盒是一次性的）。
- shell 是有状态的：ironhive 每次调用经 cwd/env 事件上报执行后的工作目录与全量环境，turnhive 在下一次前台调用回传（`strict_env`），因此 `cd`/`export` 在 session 内跨调用保持（仅前台完成的调用更新状态）。
- shell 输出从启动即重定向到沙盒 `.agents/shell-logs/<callID>.{stdout,stderr,exit}`（真实退出码在 .exit 文件，SSE exit 事件只是兜底）：前台调用完成后读回，字节精确；30s 前台窗口到期或 `bg: true` 时不杀进程转后台，回报 pid（ironhive pid 事件，即 pgid）+ 输出文件路径，模型用 `tail/cat` 轮询、`kill -- -<pid>` 杀整组。后台进程不回传状态，随沙盒销毁。
- 工具输出分两级截断（参考 agentdesk runner）：`read` 自带 2000 行/50KB 预算自行截断；其他工具走更严格的 500 行/16KB，超限完整输出由 `OutputSpiller` 写入沙盒 `./.agents/tool-results/<tool>-NNNN.txt`，模型只收到 head 预览 + 文件路径 + 读取提示（spill 失败退化为普通截断）。
