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

另外有 `refs/` 目录存放参考项目副本，刻意不被 git 追踪（已在 .gitignore 中忽略）；参考项目说明及与 agentdesk-runner 的功能对比记录见 `AGENTS.local.md`（同样不被 git 追踪的本地文档），需要参考信息时先读它。

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

`config.yml` 是本地运行配置，**含凭证，已被 .gitignore，严禁提交**；模板见 config.example.yml。配置加载是 strict 的（未知字段直接报错，防拼写静默失效）；`etcd.lease_ttl` 最小 1s（registry 按整秒截断给 etcd Grant）。

**坑**：skills 通过 S3 presigned URL 由沙盒 Pod 自行下载，`s3.endpoint` 必须是 Pod 可达的地址——`127.0.0.1` 在 k3s Pod 内不可达，本地验证时改用宿主机 LAN IP（如 `192.168.1.133:9000`）。`s3.endpoint` 为裸 `host:port`（不带 scheme，是否 TLS 由 `use_ssl` 决定）。

## 关键设计（改动前必读）

- session 同时只允许一个进行中的 turn，并发 POST messages 返回 409 `session_busy`。所有 JSON 请求体上限 4MB（`http.MaxBytesReader`）。
- turn 异步执行：`POST messages` 立即返回 202 + turn_id，turn 在后台 goroutine 运行（脱离 POST 请求生命周期）；事件经独立通道 `GET .../events`（SSE）下发——eventHub 按 session 单调 seq、保留最近 2000 条缓冲、连接先发 `sync`（当前 turn + 最新 seq + 合并后的全部历史消息，取自 Loop 的 {user, assistant} 历史）、支持 `last_seq`/`Last-Event-ID` 断点重放、慢订阅者丢弃（客户端带 last_seq 重连自愈）；事件一律带 `turn_id` 供关联。turn 生命周期为 `turn_started` / `turn_finished` 一对：**`turn_finished` 是每个 turn 的唯一收尾事件**（早期 `done`/`error`/`turn_cancelled` 三事件已合并），payload `status` ∈ `done`（带 `text` 完整回复）/ `error`（带 `message`）/ `cancelled`，hub 据此开/合 currentTurn 跟踪。`sync` 是控制帧，**不占事件序号、不写 SSE `id:`**（否则 EventSource 的 lastEventId 会被后续 backlog 帧拉低，重连时重复回放）；客户端解析无 `id` 帧时回退到 payload 内的 seq。
- turn 中断：`POST .../cancel` 把运行中 turn 的 `turnCancelCause` 置为 `errTurnCancelled` 并取消 ctx（`CancelTurn` 在锁内标记+取消，杜绝"取消了个已结束 turn"竞态）→ `RunTurn` 的 stream 返回 ctx 错误 → `failTurn` 持久化 {user, assistant-partial}，`hubReporter.Error` 按 cause 发布 `turn_finished`（status 为 `cancelled`）而非 `error`（DELETE/Close 的 `cancelTurn` 不置 cause，status 仍为 `error`）；**取消端点等 `finishTurn` 清空 busy 后才返回**（`Session.turnDone` 通道），客户端立即重发不撞 `session_busy`——恢复 = 重发，不做 attach/自动重放。cancel 路由用 `routeSessionMode(..., allowAdopt=false)`：**冷 session 没有运行中的 turn，不为取消白跑收养**。历史只存文本、不存 tool_calls，取消后的 working（含未答的 tool-call 对）即弃，无悬空对。
- 外部工具回报（`POST tool_results`）：命中等待中的 call_id 直接唤醒；未匹配的暂存 `pending`（上限 256，超限返回 429），turn 结束时整体清空——迟到结果不会滞留内存，也不会被后续 turn 的同名 call_id 误消费。
- 沙盒租约在 session 存活期间由 controller 按 lease/2 间隔自动续约；DELETE session、优雅关闭（`Controller.Close`）和 cold 换出（`takeIfCold`）三条路径都会停止续约并释放沙盒，DELETE/Close 还会先取消运行中的 turn——新增 session 生命周期路径时必须维护这三条清理路径。
- 跨节点路由靠 etcd 归属记录 + `X-Turnhive-Forwarded` 头防转发循环；已转发的请求绝不二次转发（也绝不 adopt）。节点 keepalive 中断（租约失效）后 registry 会自动退避重试重新注册节点记录（1s 起步、上限 30s，直到成功或 Close），成功后经 `OnReconnected` 回调让 controller 把内存中的 session 归属记录重新注册（`ReregisterSessions`）。
- 崩溃恢复（热/冷 session，`controller/adopt.go`）：session 创建时 spec 落盘 `sessions/{id}/spec.json`（含 model/MCP headers 凭证——与历史同一信任域），DELETE 删 spec（history/persisted 保留）、创建失败回滚删 spec，保持「201 ⇔ 可收养」。`routeSession` 查无归属时经 `adoptSession` 收养：加载 spec + List 重建 persisted 清单（清单无独立对象，key 即路径）→ `ClaimSession`（etcd Txn put-if-absent）抢归属 → 建纯历史 Loop（`buildLoop(sess, nil)` + LoadHistory + `SealInterruptedTurn` 封存 dangling user 消息）；首条消息的 `ensureSandbox` 再建带沙盒的完整 Loop。**并发收敛**：`c.adopting`（id → chan）让同节点的并发请求/转发请求等待首个收养完成，不重复工作、不在赢家中途 404。空闲超 `session.cold_timeout`（默认 0 关闭，须 > idle_timeout）的 session 被 sweeper 换出为冷 session（`takeIfCold` 置 closed、摘沙盒、`hub.closeAll()` 踢订阅者、删 etcd key），下次访问经 adopt 复活；**adopt/换出后事件 seq 从 0 重启，客户端以 sync 帧为准重置**。已知限制：keepalive 间隙内旧节点内存 session 可能被另一节点 adopt 造成短暂双归属，`OnReconnected` 只缩短窗口不消除。
- LLM 历史只持久化 {user, assistant} 对（S3 JSONL），tool 交互是 turn 内瞬态。**write-ahead**：turn 开始先把 user 消息落盘再执行（崩溃不丢用户消息）；失败 turn（流式错误、max steps 超限）补存 assistant-partial，`failTurn` 用 `context.WithoutCancel` + 10s 超时脱离已取消的 turn ctx 保存。历史落盘一律 best-effort：内存为准，保存失败不推翻已发出的 `turn_finished` 事件（错误返回给调用方记日志）；Load 遇坏行/超长行跳过该行继续，不让单行损坏锁死 session。
- 上下文窗口管理（`model.max_context`，0 关闭，参考 agentdesk runner 的 context.ts）：turn 前 `TruncateToFit` 按 chars/4 估算整轮丢弃最旧历史（预算 = max_context − 8000 回复预留 − system 估算；预算再小也保留最后一个完整 turn）；turn 后该 turn 的 usage 超 0.8×窗口时 `CompactMessages` 把最近 2 轮之前的历史压成结构化 `<context-summary>` user 消息（确定性文本摘要，不调 LLM），并回写 S3 历史。
- 沙箱路径一律相对（不假设 WORKDIR），`..` 逃逸词法拒绝；skills 安装在 `./.agents/skills/<name>/`，该树对 write/edit/apply_patch 只读。绝对路径直接透传（沙盒是一次性的）。
- shell 是有状态的：ironhive 每次调用经 cwd/env 事件上报执行后的工作目录与全量环境，turnhive 在下一次前台调用回传（`strict_env`），因此 `cd`/`export` 在 session 内跨调用保持（仅前台完成的调用更新状态）。
- shell 输出从启动即重定向到沙盒 `.agents/shell-logs/<callID>.{stdout,stderr,exit}`（真实退出码在 .exit 文件，SSE exit 事件只是兜底）：前台调用完成后读回，字节精确；30s 前台窗口到期或 `bg: true` 时不杀进程转后台，回报 pid（ironhive pid 事件，即 pgid）+ 输出文件路径，模型用 `tail/cat` 查看输出、`kill -- -<pid>` 杀整组。后台进程随沙盒销毁（idle reap / DELETE / Close 时流断开，watcher 静默丢弃、不发通知）。
- 后台进程退出通知（`BgProcessExit`，对齐 runner 的 bg-notifier）：shell 后台化后 goroutine 本就在 SSE 流上等到进程退出（ironhive 无服务端超时），后台化时挂 `watchBgProcess` 消费 outcome——干净退出回调 `OnBackgroundExit`，流错误（沙盒死亡/断连）静默丢弃。controller 侧退出先 `recordBackgroundExit` 排队再 `drainBackgroundExits`：idle 立即经 `runTurn`（从 handleCreateMessage 提取的 turn 发起路径）开合成 user 消息 turn（`<background_processes_exited>` 格式），busy 则 requeue、由 turn 结束 defer 链末尾的 drain 补发——多退出合并一条消息、每个退出精确上报一次、合成消息以 user 身份入历史（客户端无新增事件类型）。刻意不做 runner 的"每条用户消息前注入在跑进程状态"（污染持久化历史）。
- 工具输出分两级截断（参考 agentdesk runner）：`read` 自带 2000 行/50KB 预算自行截断（目录 listing 同样过该预算；文件读取本身有 8MB 内存上限）；其他工具（含工具错误文本）走更严格的 500 行/16KB，超限完整输出由 `OutputSpiller` 写入沙盒 `./.agents/tool-results/<tool>-NNNN.txt`，模型只收到 head 预览 + 文件路径 + 读取提示（spill 失败退化为普通截断）。shell 前台结果读回有 4MB 内存上限，超限提示模型去 `.agents/shell-logs/` 查完整文件。
- `load_media` 仅在 `model.features` 含 `support_image` 时挂载（`ImageTool.ExecuteImage` 返回 data URI）；图片紧跟其 tool 消息以 user 消息（image_url parts）注入下一轮请求——chat completions 只在 user 消息接受图片；图片是瞬态的，不入历史。
- MCP（`agent/tools_mcp.go`，官方 go-sdk，仅 SSE / Streamable HTTP，不支持 stdio）：**每 turn 现场连接**——`RunTurn` 开始时装载、turn 结束 `closeAll` 断开，不占用 session 清理路径；工具名 `{server}__{tool}` 命名空间（server name 校验 `^[a-zA-Z0-9_-]{1,32}$` 且唯一；namespaced 名违反 OpenAI 函数名约束或与既有工具重名的直接跳过）；`transport` 缺省 auto（先 streamable 后回退 legacy SSE，**仅 connect 失败才回退**，list 失败不回退）；单 server 连接/list 失败只经 `OnMCPStatus` 记日志，不拖垮其他 server 与 turn。**坑**：legacy SSE transport 的挂起 GET 绑在 connect ctx 上，所以连接用独立 `sessCtx`（turn ctx 派生、closeAll 时取消），10s 建立上限靠与 timer 竞速实现，不能给 connect ctx 直接套 timeout/defer cancel；streamable 设 `DisableStandaloneSSE`（turn 级短连接不需要 server 推送通道）。MCP 工具输出走通用 `TruncateSpill`（500 行/16KB + spill），`IsError` 结果转成 Go error 回填。
- `persist` 工具把沙盒文件上传到 S3 `sessions/{id}/persisted/<path>`（同路径重复 persist 原地覆盖），并把 `PersistedObject{path, object_key, size, at}` 记录为 **session 字段**（按 path 去重），随事件流 `sync` 帧下发——不是一次工具调用的旁注；object_key 相对 store prefix（与 SkillSpec.ObjectKey 同约定）。persist 不只面向最终产物，关键中间产物也应 persist——它是 session 恢复的物质基础。**双向传输都走 presigned URL 由沙盒直连 S3**：上传用 `PresignPut` + `/agent/v1/file/upload`（PUT），下载/恢复用 `PresignGet` + `/agent/v1/{file,tar}?url=`，turnhive 不中转字节。
- session 与沙盒解耦（无限恢复）：sweeper 按 `session.idle_timeout` 回收无 turn 活动 session 的沙盒（session/hub/历史保留），idle 判定与沙盒 detach 在单次锁临界区内完成（`takeSandboxIfIdle`，与 startTurn 互斥）；下一条消息经 `ensureSandbox` 重建——分配新沙盒 → 重装 skills → `RestorePersisted` 回灌文件 → 重建 Loop 并 `LoadHistory`（历史本就在 S3）→ 重启续约；session 关闭（DELETE / `Controller.Close` / cold 换出）会置 `closed` 标志，`setSandbox` 对已关闭 session 拒绝并立即释放新建沙盒与续约 goroutine（防孤儿泄漏）。**沙盒文件注入一律走 S3 presigned URL 由沙盒自拉取**（skills tar 用 `/agent/v1/tar?url=`、persisted 文件用 `/agent/v1/file?url=`），turnhive 不中转字节。改动 session 生命周期时必须维护这条重建链路与三条清理路径（DELETE / `Controller.Close` / cold 换出）。
