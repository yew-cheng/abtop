# abtop 会话状态跟踪设计笔记

本文档从 abtop 中提取 PID / 会话 / 状态 跟踪逻辑，供你在其他语言（例如 Go）中重新实现时参考。核心关注的是**状态表**——abtop 如何知道有哪些会话、每个会话属于哪个 PID、以及每个会话当前处于什么状态——以及当状态变化时如何把它推送给 SSE 客户端。

所有数据来源都是本地文件系统 + `ps` / `lsof` 的只读读取，不调用任何 Agent API。

---

## 1. 核心状态表

abtop 的核心是一个按固定周期刷新（TUI 模式下每 2 秒一次）的快照：

```text
sessions: [
  {
    agent_cli:   "claude" | "codex" | "kimi" | "opencode",
    pid:         7336,
    session_id:  "2f029acc-...",
    cwd:         "/Users/graykode/abtop",
    status:      Waiting | Thinking | Executing | Done | Unknown | RateLimited,
    ...
  },
  ...
]
```

SSE 对外推送时只暴露最小子集：

```json
[
  {"session_id":"...","agent_cli":"claude","pid":7336,"status":"Thinking"},
  {"session_id":"...","agent_cli":"kimi",  "pid":9200,"status":"Executing"}
]
```

状态表**每轮 tick 都会从零重建**，然后与上一轮对比，检测变化。没有长期维护的可变会话模型；文件系统和进程表才是唯一真相源。

---

## 2. 共享进程数据（每轮 tick 只收集一次）

为了避免重复工作，每轮 tick 先统一收集一次进程信息，再共享给所有收集器使用：

| 字段 | 含义 |
|------|---------|
| `process_info` | 从 `ps -eo pid,ppid,rss,%cpu,command` 得到的 `pid → {command, rss_kb, start_time}` 映射 |
| `children_map` | 根据 `ppid` 构建的 `pid → [直接子进程 pid]` 映射 |
| `ports` | 从 `lsof -i -P -n -sTCP:LISTEN` 得到的 `pid → [监听端口]` 映射 |
| `slow_tick` | 每 5 轮 tick（约 10 秒）为 true；昂贵的发现逻辑推迟到 slow tick 执行 |

刷新规则：

- 进程信息每轮都拉。
- 端口只在 slow tick 或 PID 集合变化时拉（防止 PID 复用导致端口错配）。
- 各 Agent 的昂贵发现逻辑也在 slow tick 或首次运行时执行。

---

## 3. 各 Agent 的发现逻辑

每个 Agent 有自己的收集器，把 Agent 特有的文件/进程转换成统一的 `AgentSession` 结构。所有收集器在同一批共享进程数据上并行运行。

### 3.1 Claude Code

1. 从 `process_info` 中找到所有存活的 `claude` 进程。
2. 从进程的打开文件/当前目录推断 config root：
   - Linux 上读 `/proc/<pid>/cwd` 和 `/proc/<pid>/fd/*`。
   - macOS 上用 `lsof` / `libproc`。
   - 默认回退：`~/.claude`、`~/.claude-*`、`CLAUDE_CONFIG_DIR` 环境变量、配置文件里指定的目录。
3. 一个合法的 config root 必须同时包含 `sessions/` 和 `projects/`。
4. 读取 `{config-root}/sessions/{PID}.json`，拿到 `pid`、`sessionId`、`cwd`、`startedAt`。
5. 解析 transcript 路径 `{config-root}/projects/{encoded-cwd}/{sessionId}.jsonl`。
   - 路径编码规则：`/Users/foo/bar` → `-Users-foo-bar`。
   - 编码可能冲突，所以以 session JSON 里的 `cwd` 为准。
6. 增量解析 JSONL transcript（按字节偏移 tail），推导 token、model、status。

特殊情况：

- `/clear` 后 Claude 可能只新建 `sessionId` + `.jsonl`，不修改 `sessions/{PID}.json`。abtop 会在项目目录下选最新的 transcript 来修正，除非多个存活 Claude PID 共享同一个 `cwd`。
- PID 是 abtop 自身后代（例如 `claude --print` 生成摘要）的会话会被过滤。
- session 文件被删除或 PID 校验失败时，视为死亡并丢弃。

### 3.2 Kimi Code

1. 读取 `~/.kimi-code/session_index.jsonl`（slow tick 或首次运行）。
   - 每行格式：`{sessionId, sessionDir, workDir}`。
2. 找到所有存活的 `kimi` 进程。
3. 按 `process.cwd == workDir` 把 PID 匹配到会话。
4. 同一个 `cwd` 下多个 PID 时，用三轮分配保证稳定：
   - 先保持上一轮的 PID→session 映射。
   - 新启动的进程按启动时间与 session 创建时间匹配。
   - 剩余 PID 按活动度贪心分配。
   - 活跃但未分配的 session 可以从“已Stale”的 PID 手里接管（处理 `/clear` 场景）。
5. 增量解析 `{sessionDir}/agents/main/wire.jsonl`。
6. 通过回放 wire 事件推导状态（见 §5）。

### 3.3 Codex CLI

1. 找到所有存活的 `codex` 进程。
2. 用 `lsof` 把每个 PID 映射到它打开的 `rollout-*.jsonl` 文件。
3. 解析 JSONL 里的 `session_meta`、`token_count`、`agent_message` 事件。
4. 检测最近结束的会话：扫描 `~/.codex/sessions/YYYY/MM/DD/` 中 5 分钟内修改过但不再属于存活进程的 JSONL。
5. 从 `token_count` 事件里提取 rate-limit 窗口。

### 3.4 OpenCode

1. 找到所有存活的 `opencode` 进程。
2. 通过 `sqlite3 -readonly -json` 读取 `~/.local/share/opencode/opencode.db` 里的最近会话。
3. 按进程 `cwd` 把 PID 匹配到 DB 里的会话。
4. 多个 DB 行共享同一个 `cwd` 时，只把存活 PID 分配出去，避免把旧行显示成存活副本。

---

## 4. PID 校验与 PID 复用防护

永远不要只信 PID。每轮 tick 都要校验会话记录的 PID 仍然存活，并且进程命令仍然是对应 Agent 的可执行文件。

```text
pid_alive = process_info.get(pid)
              .map(|p| command_contains_binary(p.command, agent_name))
              .unwrap_or(false)
```

- `command_contains_binary` 检查命令字符串里的可执行文件名（例如 `claude`、`kimi`、`codex`）。
- 校验失败时，该会话视为死亡/被移除。
- 端口缓存会在 PID 集合变化时失效，避免复用的 PID 继承旧端口。

---

## 5. 状态推断启发式

没有任何一个 Agent 直接暴露干净的“状态”字段。abtop 通过 transcript/wire 事件 + 子进程信号推断状态。

### 5.1 Kimi（事件驱动状态机）

Kimi 的 `wire.jsonl` 被重放成三个内部状态：

- `Idle` —— 等待用户输入。
- `Thinking` —— 模型正在生成。
- `Executing` —— 已发出 `tool.call`，尚未收到 `tool.result`。

映射到公开 `SessionStatus`：

```text
step_state == Executing                         → Executing
step_state == Thinking                          → Thinking（如果子进程活跃则改为 Executing）
step_state == Idle  && 有待处理工具调用         → Executing
step_state == Idle  && 没有待处理工具调用       → Waiting
```

### 5.2 Claude（transcript + 后代进程信号）

从 JSONL transcript 中跟踪：

- `last_user_ts_ms` —— 最后一行是真实用户 prompt，且助手还没回复。
- `current_task` —— 最近一轮助手留下了未回答的 `tool_use`。
- 后代进程 CPU 使用率超过阈值。

```text
active_descendant || pending_tool  → Executing
last_user_ts_ms > 0                → Thinking
否则                                → Waiting
```

### 5.3 通用回退

对所有 Agent，当满足以下任一条件时，会话状态为 `Done`：

- 进程不再存活，**或**
- Agent 特有的 session 文件已被删除。

`Unknown` 很少使用，主要在无法确认所有权时。

---

## 6. 变化检测

变化检测器把本轮收集到的表与上一轮对比，满足以下任一条件就广播：

1. **`session_id` 集合发生变化**。有 session 新增或删除。（只比较数量不够——同一轮里可能一个消失、一个出现。）
2. **某个已有 `session_id` 的 `status` 变了。**

```rust
let current_ids: HashSet<String> = sessions.iter().map(|s| s.session_id.clone()).collect();
let prev_ids:    HashSet<String> = prev_statuses.keys().cloned().collect();

let changed = current_ids != prev_ids
    || sessions.iter().any(|s| prev_statuses.get(&s.session_id) != Some(&s.status));
```

只有 token 数、上下文百分比、子进程详情变化而**状态没变**时，**不会**触发广播，避免 SSE 客户端被刷屏。

广播后，重建上一状态表：

```rust
prev_statuses.clear();
for s in &sessions {
    prev_statuses.insert(s.session_id.clone(), s.status.clone());
}
```

---

## 7. 通过 SSE 广播

检测到变化后，把当前会话状态表序列化成 JSON，推送给每个已连接的 SSE 客户端：

```text
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

data: [{"session_id":"...","status":"Thinking"},...]

data: [{"session_id":"...","status":"Executing"},...]
```

服务端要点：

- 绑定到 `127.0.0.1:8787`，如果被占用则回退到随机临时端口。
- 每个客户端有独立通道，缓冲区很小（2 条消息）。
- 慢/满的客户端保留，只有断开的客户端会被移除。
- 最新 payload 会被缓存，**新连接**的客户端会立刻收到它，不需要等下一次变化。
- 关闭时丢弃所有发送端，客户端线程会解锁并退出。

---

## 8. 边界情况与经验教训

| 场景 | 处理方式 |
|------|----------|
| PID 复用 | 每轮校验命令字符串；PID 集合变化时失效端口缓存。 |
| `/clear` 产生新 sessionId | Claude：多 PID 同 cwd 时不交叉分配，否则选最新 transcript。Kimi：稳定 PID→session 映射 + 活动度 stealing。 |
| 残留 session 文件 | 文件可能在崩溃后残留；PID 死亡时视为 `Done` 并丢弃。 |
| 同 cwd 多个存活 PID | 不要交叉分配 transcript；保持所有权稳定或用活动度启发式。 |
| 增量 transcript 解析 | 记录字节偏移；文件增长时只读新增字节；缓冲不完整行；文件缩小时重置偏移重新扫描。 |
| 慢 I/O | `lsof`/index 读取推迟到 slow tick（约 10 秒）；进程信息每轮都拉。 |
| MCP server 进程 | 检测并抑制，避免被重复计为用户会话。 |
| 无网络 / 隐私 | 所有读取都是本地；SSE 服务器只监听 loopback。 |

---

## 9. 移植检查清单（Go 或其他语言）

1. **进程快照** —— 每轮 tick 执行一次 `ps` 和 `lsof`，缓存结果。
2. **Agent 收集器** —— 为每个 Agent 实现一个收集器，把 Agent 特有文件/进程转成统一 `Session` 结构。
3. **PID 校验** —— 校验每个会话的 PID 仍然存活，且命令匹配预期可执行文件。
4. **状态推导** —— 重放 Agent 特有事件/transcript 文件，推导出 `Waiting | Thinking | Executing | Done`。
5. **状态表** —— 保留上一轮 `session_id → status` 的映射。
6. **变化检测** —— 比较当前 session ID 集合和状态；只在变化时广播。
7. **SSE 服务器** —— 在 loopback 上实现最小 HTTP 服务器，单 `/events` 端点，给所有客户端发送 `data:` JSON，为新客户端缓存最新 payload。
8. **边界处理** —— 处理 PID 复用、`/clear`、残留文件、增量 tail、MCP server 过滤。

核心思路：**文件系统 + 进程表是真相源**，内存中的状态表只是一个可比较差异的快照，用来决定何时推送更新。
