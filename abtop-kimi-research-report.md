# abtop 添加 Kimi Code CLI 支持 — 开发调研报告

> 2026-06-14 | Kimi Code v0.14.3, protocol v1.4
> 源码: [kimi-code](https://github.com/MoonshotAI/kimi-code) | 目标: [abtop](https://github.com/graykode/abtop)

---

## 1. abtop 架构摘要

**核心抽象**: `AgentCollector` trait → `MultiCollector` 统一调度

```rust
// src/collector/mod.rs:97-111
pub trait AgentCollector {
    fn collect(&mut self, shared: &SharedProcessData) -> Vec<AgentSession>;
    fn live_rate_limit(&self) -> Option<RateLimitInfo> { None }
    fn discovered_config_dirs(&self) -> Vec<PathBuf> { Vec::new() }
}
```

`SharedProcessData` 每 tick (2s) 采集，在所有 collector 间共享：`process_info` (ps)、`children_map`、`ports`，每 5 ticks 一次 `slow_tick=true`。

已支持 Agent: `claude` (`*CC`), `codex` (`>CD`), `opencode` (`#OC`)。

---

## 2. Kimi Code 数据架构

| 属性 | 值 |
|------|-----|
| CLI 二进制 | `kimi`，路径 `~/.kimi-code/bin/kimi` |
| 数据根目录 | `~/.kimi-code/`（env `KIMI_CODE_HOME` 可覆盖） |
| 上下文窗口 | 262,144 tokens (256K)，来自 `config.toml` `max_context_size` |

### 2.1 目录结构

```
~/.kimi-code/
├── config.toml                    # 用户配置
├── session_index.jsonl            # ★ 会话索引（每行一个 JSON 对象）
├── sessions/
│   └── wd_<slug>_<sha256前12位>/   # 按 workDir 分桶
│       └── <sessionId>/
│           ├── state.json                  # 会话元数据
│           ├── upcoming-goals.json
│           ├── tasks/                      # 后台任务持久化
│           ├── cron/
│           └── agents/
│               ├── main/wire.jsonl         # ★ 主 Agent 转录
│               └── agent-N/wire.jsonl      # 子 Agent
└── logs/, telemetry/, credentials/, oauth/, user-history/, bin/
```

### 2.2 与 Claude Code 关键差异

| 维度 | Claude Code | Kimi Code |
|------|-------------|-----------|
| 会话发现 | `sessions/{PID}.json` | `session_index.jsonl` + cwd↔workDir 匹配 |
| 转录格式 | `{sid}.jsonl`（`assistant`/`user`/`summary`） | `wire.jsonl`（`step.end`/`usage.record`/`tool.call` 等） |
| Token 字段 | `input_tokens`/`output_tokens`/`cache_read_input_tokens`/`cache_creation_input_tokens` | `inputOther`/`output`/`inputCacheRead`/`inputCacheCreation` |
| 上下文窗口 | 200K/1M（模型相关） | 256K（`config.toml`） |
| 速率限制 | `abtop-rate-limits.json` | 不暴露 |
| 子 Agent | `subagents/agent-{hash}/` | `agents/agent-N/` |
| PID↔会话 | 直接（文件名=PID） | 间接（cwd↔workDir） |

---

## 3. 数据格式详解

### 3.1 session_index.jsonl

**源码**: `packages/agent-core/src/session/store/session-index.ts:4-8`

```typescript
interface SessionIndexEntry {
  readonly sessionId: string;
  readonly sessionDir: string;   // 绝对路径
  readonly workDir: string;      // 绝对路径
}
```

每行一个 JSON，`appendFile` 追加。读取时有安全校验（绝对路径检查、路径遍历防护、basename==sessionId 验证），返回 `Map<sessionId, SessionIndexEntry>`（同 ID 去重）。

### 3.2 state.json

**源码**: `packages/agent-core/src/session/index.ts:152-159`

```json
{
  "createdAt": "2026-06-14T03:47:59.515Z",
  "updatedAt": "2026-06-14T03:48:08.248Z",
  "title": "会话标题",
  "isCustomTitle": false,
  "lastPrompt": "用户最后一条提示",   // 非标准字段，Option 防御性解析
  "agents": {
    "main": {
      "homedir": "/home/.../agents/main",
      "type": "main",
      "parentAgentId": null
    }
  },
  "custom": {},
  "forkedFrom": "source-session-id"   // fork 操作时添加
}
```

| state.json 字段 | → AgentSession |
|---|---|
| `createdAt` | `started_at`（ISO 8601 → epoch ms） |
| `title` / `lastPrompt` | `initial_prompt` |
| `agents` (过滤 `"main"`) | 子 Agent 发现 |
| `agents.<id>.homedir` | 子 Agent wire 路径 |

### 3.3 wire.jsonl — 核心转录文件

**路径**: `{sessionDir}/agents/main/wire.jsonl`（主 Agent）、`{sessionDir}/agents/agent-N/wire.jsonl`（子 Agent）
**源码**: `packages/agent-core/src/agent/records/types.ts:18-103` (`AgentRecordEvents`)

#### 事件类型一览

| `type` | 数据用途 |
|--------|----------|
| `metadata` | 协议版本、App 版本 |
| `config.update` | **模型名** (`modelAlias`)、**thinking level** (`thinkingLevel`) |
| `turn.prompt` | turn 计数、初始 prompt |
| `context.append_loop_event` | **核心**：step.begin / step.end / content.part / tool.call / tool.result |
| `usage.record` | **核心**：累积 token，含 `usageScope` ("turn"/"session") |
| `context.apply_compaction` | compaction 计数 |
| `tools.set_active_tools` | 工具列表 |
| `permission.set_mode` | 权限模式 |

#### TokenUsage（核心数据结构）

**源码**: `packages/protocol/src/events.ts:3-8`

```typescript
interface TokenUsage {
  readonly inputOther: number;          // → total_input_tokens
  readonly output: number;              // → total_output_tokens
  readonly inputCacheRead: number;      // → total_cache_read
  readonly inputCacheCreation: number;  // → total_cache_create
}
```

**注意区分**: REST API 的 `SessionUsage`（`packages/protocol/src/session.ts:20-29`）是 snake_case 且含 `total_cost_usd`/`context_tokens`/`context_limit`，wire.jsonl 中不会出现。

#### 关键事件详细格式

**`config.update`**:
```json
{
  "type": "config.update",
  "modelAlias": "kimi-code/kimi-for-coding",
  "thinkingLevel": "high",
  "time": 1781408879540
}
```
`thinkingLevel` 枚举: `"low"` | `"medium"` | `"high"` | `"xhigh"` | `"max"`（默认 `"high"`）。

**`turn.prompt`**:
```json
{
  "type": "turn.prompt",
  "input": [{"type": "text", "text": "用户输入内容..."}],
  "origin": {"kind": "user"},
  "time": 1781408312840
}
```

**`context.append_loop_event` — 子类型** (源码: `packages/agent-core/src/loop/events.ts:106-111`):

`step.begin`: `{type: "step.begin", uuid, turnId, step}`
`content.part`: `{type: "content.part", part: {type: "think"|"text"|"tool_use", think/text/name+input}}`
`tool.call`: `{type: "tool.call", name, args, toolCallId, description, display}`
`tool.result`: `{type: "tool.result", toolCallId, result: {output}}`

**`step.end` — 最重要的事件** (源码: `packages/agent-core/src/loop/events.ts:15-30`):

```typescript
interface LoopStepEndEvent {
  type: 'step.end';
  uuid: string;
  turnId: string;
  step: number;
  usage?: TokenUsage;                  // ★ Token 用量
  finishReason?: LoopStepStopReason;   // ★ 状态推导
  llmFirstTokenLatencyMs?: number;
  llmStreamDurationMs?: number;
}
```

**`LoopStepStopReason`** (源码: `packages/agent-core/src/loop/types.ts:30-36`):
`'end_turn'` | `'max_tokens'` | `'tool_use'` | `'filtered'` | `'paused'` | `'unknown'`

| finishReason | → SessionStatus |
|---|---|
| `"tool_use"` | `Executing`（等待工具结果） |
| `"end_turn"` | `Thinking`（最近活跃）或 `Waiting` |
| `"max_tokens"` / `"filtered"` | `Thinking` + 等待 compaction |
| `"paused"` / `"unknown"` | `Waiting` |

**`usage.record`** (源码: `packages/agent-core/src/agent/records/types.ts:70-73`):
```json
{
  "type": "usage.record",
  "model": "kimi-code/kimi-for-coding",
  "usage": {"inputOther": 1909, "output": 217, "inputCacheRead": 14336, "inputCacheCreation": 0},
  "usageScope": "turn",
  "time": 1781408233864
}
```
以 `step.end` 为主要数据源（粒度更细），`usage.record` 用于交叉校验。`usageScope` 区分 per-turn 和 cumulative。

### 3.4 AgentStatusUpdatedEvent（仅内存事件，不写入 wire.jsonl）

**源码**: `packages/protocol/src/events.ts:289-299`

```typescript
interface AgentStatusUpdatedEvent {
  type: 'agent.status.updated';
  model?: string;
  contextTokens?: number;       // 当前上下文 token 数
  maxContextTokens?: number;    // 窗口大小
  contextUsage?: number;        // 百分比 (0-100)
  planMode?: boolean;
  permission?: PermissionMode;
  usage?: UsageStatus;          // { byModel?, currentTurn?, total? }
}
```

abtop 无法直接访问内存事件，需通过 wire.jsonl 文件解析获取等效数据。

### 3.5 上下文窗口推导

```rust
fn context_window_for_model(model: &str) -> u64 {
    match model {
        m if m.contains("kimi-for-coding") => 262_144,
        m if m.contains("kimi-k2") => 262_144,
        _ => 200_000,  // 保守 fallback
    }
}
```

wire.jsonl 不包含 `max_context_size`，如需精确值应解析 `config.toml` 的 `[models.*]` 段。

### 3.6 Token 字段映射

| wire.jsonl (TokenUsage) | abtop AgentSession |
|---|---|
| `inputOther` | `total_input_tokens` |
| `output` | `total_output_tokens` |
| `inputCacheRead` | `total_cache_read` |
| `inputCacheCreation` | `total_cache_create` |

上下文 token: `context_tokens = inputOther + inputCacheRead`（如果 `inputCacheRead == 0 && inputCacheCreation > 0`，使用 `inputOther + inputCacheCreation`）。

---

## 4. 实现指导原则：优先复用 ClaudeCollector 模式

> 每段逻辑实现前，先问"ClaudeCollector 是怎么做的？"仅在数据格式根本不同时才自己想办法。

### 4.1 必须复用的模式（9 个）

| # | 模式 | ClaudeCollector 实现 | KimiCollector 适配 |
|---|------|---------------------|-------------------|
| 1 | **增量文件解析** | `parse_transcript_with_previous()` (claude.rs:1292) — offset + file_identity + delta merge + 不完整行处理 | **完全复用**，仅事件类型不同 |
| 2 | **缓存结构** | `TranscriptResult` struct (claude.rs:1206) — 累积值/快照值/偏移量/派生值/历史/对话 | 定义等价 `WireTranscriptState`，字段名和语义对齐 |
| 3 | **缓存驱逐** | `evict_stale_cache()` (claude.rs:175) | **直接照搬** |
| 4 | **slow_tick 控制** | 昂贵操作仅 slow_tick 执行 | session_index/state 刷新在 slow_tick，wire 增量读取不受限 |
| 5 | **PID 安全性检查** | `cmd_has_binary` + `is_descendant_of` + `pid_alive` | **完全照搬**，`"claude"` → `"kimi"` |
| 6 | **子进程递归遍历** | 栈式遍历 `children_map` (claude.rs:597) | **直接照搬** |
| 7 | **三信号状态推导** | active_descendant + pending_tool + model_generating (claude.rs:557) | **完全复用**，信号 2 等价物：最近 tool.call 无匹配 tool.result |
| 8 | **Git 统计** | MultiCollector 统一处理 | **无需实现**，只需填对 `cwd` |
| 9 | **端口/MCP** | MultiCollector 统一处理 | **无需实现** |

### 4.2 Kimi 特有逻辑（3 个差异，无法复用）

| 差异点 | Claude 做法 | Kimi 做法 |
|--------|------------|----------|
| 会话发现 | `sessions/{PID}.json` | `session_index.jsonl` + cwd↔workDir 匹配 |
| 转录路径 | `projects/{encoded-cwd}/{sid}.jsonl` | `sessions/{bucket}/{sid}/agents/main/wire.jsonl` |
| Token 事件 | `assistant.message.usage` | `step.end.usage` / `usage.record` |

### 4.3 开发检查清单

- [ ] 增量解析逻辑与 `parse_transcript_with_previous` 一致（offset, file_identity, delta merge）
- [ ] 缓存结构与 `TranscriptResult` 对齐（字段命名、merge 策略）
- [ ] 状态推导使用三信号模式（active_descendant, pending_tool, model_generating）
- [ ] PID 检查完整（`cmd_has_binary` + `is_descendant_of` + `pid_alive`）
- [ ] `slow_tick` 保护昂贵 I/O
- [ ] 不在 collector 中重复实现 Git stats/ports/MCP

---

## 5. Kimi Collector 实现设计

### 5.1 数据结构（对照 ClaudeCollector）

```rust
// ── 文件解析结构 ──

/// session_index.jsonl 条目 — 对应 Claude 的 sessions/{PID}.json → SessionFile
#[derive(Debug, Deserialize)]
struct SessionIndexEntry {
    #[serde(rename = "sessionId")]   session_id: String,
    #[serde(rename = "sessionDir")]  session_dir: PathBuf,
    #[serde(rename = "workDir")]     work_dir: PathBuf,
}

/// state.json
#[derive(Debug, Deserialize)]
struct KimiStateFile {
    #[serde(rename = "createdAt", default)]       created_at: Option<String>,
    #[serde(rename = "updatedAt", default)]       updated_at: Option<String>,
    #[serde(default)]                             title: Option<String>,
    #[serde(rename = "lastPrompt", default)]      last_prompt: Option<String>,
    #[serde(rename = "isCustomTitle", default)]   is_custom_title: Option<bool>,
    #[serde(default)]                             agents: Option<HashMap<String, AgentEntry>>,
}

#[derive(Debug, Deserialize)]
struct AgentEntry {
    homedir: PathBuf,
    #[serde(rename = "type", default)]             agent_type: Option<String>,
    #[serde(rename = "parentAgentId", default)]    parent_agent_id: Option<String>,
}

/// wire.jsonl 增量解析缓存 — 对应 Claude 的 TranscriptResult (claude.rs:1206)
struct WireTranscriptState {
    // ── 累积 token ──
    model: String,                   // ← config.update.modelAlias
    total_input: u64,                // ← TokenUsage.inputOther
    total_output: u64,               // ← TokenUsage.output
    total_cache_read: u64,           // ← TokenUsage.inputCacheRead
    total_cache_create: u64,         // ← TokenUsage.inputCacheCreation
    last_context_tokens: u64,
    max_context_tokens: u64,

    // ── 偏移量（复用 Claude file_identity 模式）──
    new_offset: u64,
    file_identity: (u64, u64),       // (inode, mtime)

    // ── 派生 ──
    turn_count: u32,
    compaction_count: u32,
    current_task: String,
    app_version: String,
    thinking_level: String,          // ← config.update.thinkingLevel → AgentSession.effort

    // ── 历史 ──
    last_activity: SystemTime,
    token_history: Vec<u64>,
    context_history: Vec<u64>,
    initial_prompt: String,
    tool_calls: Vec<ToolCall>,
    chat_messages: Vec<ChatMessage>,

    // ── 状态信号（三信号模式）──
    last_tool_call_ts_ms: u64,       // ← 等价 Claude.last_assistant_ts_ms
    last_user_ts_ms: u64,            // ← 等价 Claude.last_user_ts_ms
    saw_turn: bool,                  // ← 等价 Claude.saw_turn
}

/// KimiCollector
pub struct KimiCollector {
    code_home: PathBuf,                                    // ← Claude.config_dirs[0]
    session_index_path: PathBuf,
    session_index_cache: Vec<SessionIndexEntry>,
    last_index_load: Option<Instant>,
    transcript_cache: HashMap<String, WireTranscriptState>, // ← Claude.transcript_cache
    state_cache: HashMap<String, KimiStateFile>,            // Kimi 特有
}
```

### 5.2 collect 流程

```
1. 确定 code_home: KIMI_CODE_HOME env 或 ~/.kimi-code

2. 刷新 session_index (slow_tick 或首次)
   → 读取 {code_home}/session_index.jsonl，逐行解析 SessionIndexEntry

3. 查找活跃 kimi 进程
   → 遍历 shared.process_info，过滤 cmd_has_binary(cmd, "kimi")

4. 匹配 PID↔Session: 进程 cwd == SessionIndexEntry.workDir（精确匹配）
   同 cwd 多 session → 选最新的
   同 cwd 多进程 → 按 PID 分配，按创建时间排序

5. 每个匹配的 session 构建 AgentSession:
   a. 读取 state.json → started_at, initial_prompt, 子 Agent 列表
   b. 增量解析 agents/main/wire.jsonl（复用 Claude offset+file_identity+delta merge）
   c. 状态推导: finishReason=="tool_use"→Executing, 最近活跃→Thinking, else→Waiting
   d. 子 Agent: 扫描 agents/agent-N/ 目录，增量解析 wire.jsonl
   e. 上下文: last_context_tokens / context_window * 100
   f. 子进程: 复用 children_map 递归遍历

6. 清理过期缓存 (evict_stale_cache)

7. 返回 sessions
```

### 5.3 增量解析策略（与 ClaudeCollector 完全一致）

```
首次: 全量解析 wire.jsonl → 记录 file_identity (inode+mtime) + new_offset (文件大小)
后续: identity 未变 → seek(new_offset) + 读新字节 → delta merge
      identity 变了 → 重置 offset=0，全量重新解析
      文件缩小      → 重置 offset=0
      行尾无 \n    → break（等下次补全）
```

### 5.4 速率限制

`live_rate_limit()` 返回 `None`。Kimi Code v0.14.3 不暴露速率限制数据。

---

## 6. 文件修改清单

### 6.1 新建

| 文件 | 行数 | 说明 |
|------|------|------|
| `src/collector/kimi.rs` | ~500-700 | KimiCollector 完整实现 |

### 6.2 修改

| 文件 | 行数 | 修改点 |
|------|------|--------|
| `src/collector/mod.rs` | ~15 | ① 添加 `pub mod kimi;` ② 添加 `pub use kimi::KimiCollector;` ③ 注册 collector ④ 更新测试 (collectors.len() 3→4) |
| `src/ui/sessions.rs` | ~3 | agent_label+color match: `"kimi" => ("◆KM", Color::Rgb(100, 180, 255))` |
| `src/locale.rs` | ~1 | `m.insert("agent.kimi", "◆KM");` |
| `src/app.rs` | ~2 | `is_supported_agent_command()` 添加 `cmd_has_binary(cmd, "kimi")` |

### 6.3 修改详情

**`src/collector/mod.rs`** — 注册 collector（`with_hidden_and_claude_config_dirs` 方法中）:
```rust
if !is_hidden("kimi") {
    collectors.push(Box::new(KimiCollector::new()));
}
```

**`src/ui/sessions.rs`** — 标签和颜色:
```rust
"kimi" => ("◆KM", Color::Rgb(100, 180, 255)),  // Moonshot AI brand blue
```

**`src/app.rs`** — 命令识别:
```rust
fn is_supported_agent_command(cmd: &str) -> bool {
    crate::collector::process::cmd_has_binary(cmd, "claude")
        || crate::collector::process::cmd_has_binary(cmd, "codex")
        || crate::collector::process::cmd_has_binary(cmd, "opencode")
        || crate::collector::process::cmd_has_binary(cmd, "kimi")
}
```
`is_killable_agent_command` 无需修改。

---

## 7. 开发阶段

| 阶段 | 内容 | 预估 |
|------|------|------|
| 1. 骨架 | 创建 `kimi.rs` 空壳 + mod.rs/ui/locale/app 注册 + `cargo build` | 1-2h |
| 2. 会话发现 | `load_session_index()` + `find_kimi_pids()` + `match_sessions_to_pids()` + 单测 | 2-3h |
| 3. 元数据 | `read_state()` 解析 state.json → AgentSession + 单测 | 1-2h |
| 4. 转录解析 | `file_identity()` + `parse_wire_delta()` 增量解析 + delta merge + 缓存驱逐 + 单测 | 3-5h |
| 5. 状态推导 | `derive_status()` 三信号 + 子 Agent + 上下文百分比 + 集成测试 | 1-2h |
| 6. 边界情况 | 同 cwd 多进程/session、进程退出、空 wire.jsonl、截断轮转、无 title fallback、orphan port | 2-3h |
| 7. 打磨 | Demo 数据、clippy、全量测试 | 1h |

---

## 8. 附录

### 8.1 关键常量

```rust
const KIMI_CODE_DEFAULT_HOME: &str = ".kimi-code";
const SESSION_INDEX_FILE: &str = "session_index.jsonl";
const STATE_FILE: &str = "state.json";
const MAIN_WIRE_PATH: &str = "agents/main/wire.jsonl";
const KIMI_DEFAULT_CONTEXT_WINDOW: u64 = 262_144;
```

### 8.2 serde 结构体

```rust
#[derive(Debug, Deserialize)]
struct SessionIndexEntry {
    sessionId: String,
    sessionDir: PathBuf,
    workDir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct KimiStateFile {
    createdAt: Option<String>,
    updatedAt: Option<String>,
    title: Option<String>,
    lastPrompt: Option<String>,
    isCustomTitle: Option<bool>,
    agents: Option<HashMap<String, serde_json::Value>>,
    custom: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct WireEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)] model: Option<String>,
    #[serde(default)] usage: Option<WireUsage>,
    #[serde(default)] time: Option<u64>,
}
```

### 8.3 已知限制

1. **无 PID↔会话 直接映射**: 依赖 cwd↔workDir 匹配，同 cwd 多进程需启发式分配
2. **wire.jsonl 格式可能变化**: 通过 `metadata.protocol_version`（当前 `"1.4"`）做版本兼容
3. **wire.jsonl 遗留路径**: 源码同时检查 `{sessionDir}/wire.jsonl`（旧格式），应先检查 `agents/main/wire.jsonl`
4. **上下文窗口需 config.toml**: 需 TOML 解析（可加 `toml` crate 或硬编码 fallback）
5. **无速率限制**: 首次发布不支持，后续若 Kimi 添加可扩展 `live_rate_limit()`
6. **`AgentStatusUpdatedEvent` 仅在内存**: `contextTokens`/`contextUsage` 不持久化，abtop 只能通过 wire 解析
7. **`lastPrompt` 非标准字段**: 源码 `SessionMeta` 未定义，必须 `Option` + `#[serde(default)]`
8. **`usageScope` 区分**: `usage.record` 的 `"turn"` 和 `"session"` scope 需分别处理避免重复计数

### 8.4 参考文档

| 文档 | 路径 |
|------|------|
| Data Locations | `docs/en/configuration/data-locations.md` |
| Config Files | `docs/en/configuration/config-files.md` |
| Sessions | `docs/en/guides/sessions.md` |
| kimi-code 源码 | [github.com/MoonshotAI/kimi-code](https://github.com/MoonshotAI/kimi-code) |
| abtop 源码 | [github.com/graykode/abtop](https://github.com/graykode/abtop) |

### 8.5 源码验证索引

| 数据格式 | 源码位置 |
|----------|---------|
| `TokenUsage` | `packages/protocol/src/events.ts:3-8` |
| `LoopRecordedEvent` 子类型 | `packages/agent-core/src/loop/events.ts:106-111` |
| `LoopStepStopReason` | `packages/agent-core/src/loop/types.ts:30-36` |
| `SessionMeta`/state.json | `packages/agent-core/src/session/index.ts:152-159` |
| `SessionIndexEntry` | `packages/agent-core/src/session/store/session-index.ts:4-8` |
| `AgentStatusUpdatedEvent` | `packages/protocol/src/events.ts:289-299` |
| `SessionSummary` 计算 | `packages/agent-core/src/session/store/session-store.ts:275-302` |
| `SessionStatus` 枚举 | `packages/protocol/src/session.ts:10-16` |
| `SessionUsage` (REST API) | `packages/protocol/src/session.ts:20-29` |

---

> **总结**: KimiCollector 约 500-700 行，其余修改点每处 ≤15 行。核心策略是复用 ClaudeCollector 的增量解析/缓存/状态推导模式，仅会话发现和事件解析需 Kimi 特有实现。
