# abtop Session-State Tracking Design Notes

This document extracts the PID / session / status tracking logic from abtop so it can be re-implemented in another language (e.g. Go). It is focused on the *state table* — how abtop knows which sessions exist, which PID owns each session, and what status each session is in — and how it pushes that table to SSE clients when it changes.

All data sources are read-only from the local filesystem plus `ps`/`lsof`. No agent APIs are called.

---

## 1. The core state table

At the heart of abtop is a snapshot refreshed on a fixed tick (every 2 s in the TUI):

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

The SSE payload intentionally exposes only the minimal subset:

```json
[
  {"session_id":"...","agent_cli":"claude","pid":7336,"status":"Thinking"},
  {"session_id":"...","agent_cli":"kimi",  "pid":9200,"status":"Executing"}
]
```

The state table is **rebuilt from scratch every tick** and then compared against the previous tick to detect changes. There is no long-lived mutable model of sessions; the filesystem and process table are the source of truth.

---

## 2. Shared process data (collect once per tick)

To avoid duplicate work, every tick starts by fetching process information once and sharing it across all collectors:

| Field | Meaning |
|-------|---------|
| `process_info` | Map `pid → {command, rss_kb, start_time}` from `ps -eo pid,ppid,rss,%cpu,command` |
| `children_map` | Map `pid → [direct child pids]` built from `ppid` |
| `ports` | Map `pid → [listening ports]` from `lsof -i -P -n -sTCP:LISTEN` |
| `slow_tick` | True every 5 ticks (~10 s); expensive discovery is deferred to slow ticks |

Refresh rules:

- Process info is fetched every tick.
- Ports are fetched every slow tick **or** whenever the set of PIDs changes (PID-reuse safety).
- Expensive per-agent discovery also runs on slow ticks or first run.

---

## 3. Agent-specific discovery

Each agent has its own collector that translates agent-specific files/processes into the common `AgentSession` shape. The collectors run in parallel over the same shared process data.

### 3.1 Claude Code

1. Find live `claude` processes from `process_info`.
2. Infer config roots from open files/directories:
   - `/proc/<pid>/cwd` and `/proc/<pid>/fd/*` on Linux.
   - `lsof` / `libproc` on macOS.
   - Fallback defaults: `~/.claude`, `~/.claude-*`, `CLAUDE_CONFIG_DIR` env var, configured dirs.
3. A config root must contain both `sessions/` and `projects/`.
4. Read `{config-root}/sessions/{PID}.json` to get `pid`, `sessionId`, `cwd`, `startedAt`.
5. Resolve transcript dir `{config-root}/projects/{encoded-cwd}/{sessionId}.jsonl`.
   - Path encoding: `/Users/foo/bar` → `-Users-foo-bar`.
   - The encoded path can collide, so the session JSON’s `cwd` is the source of truth.
6. Parse the JSONL transcript incrementally (offset-based tailing) to derive tokens, model, and status.

Special cases:

- After `/clear`, Claude may create a new `sessionId` + `.jsonl` without rewriting `sessions/{PID}.json`. abtop resolves this by picking the newest transcript in the project dir, unless multiple live Claude PIDs share the same `cwd`.
- Sessions whose PID descends from abtop itself (e.g. `claude --print` spawned for summary generation) are filtered out.
- Dead sessions are dropped when the session file disappears or PID verification fails.

### 3.2 Kimi Code

1. Read `~/.kimi-code/session_index.jsonl` (slow tick or first run).
   - Each line: `{sessionId, sessionDir, workDir}`.
2. Find live `kimi` processes.
3. Match live PIDs to sessions by `process.cwd == workDir`.
4. Handle multiple PIDs per `cwd` with a three-pass assignment:
   - Keep previous tick’s PID→session mapping stable.
   - Time-based matching for newly launched processes (`proc.start_time` vs session `createdAt`).
   - Greedy activity-based assignment for leftovers.
   - Active unclaimed sessions can steal a PID from a stale session (the `/clear` case).
5. Parse `{sessionDir}/agents/main/wire.jsonl` incrementally.
6. Derive status by replaying wire events (see §5).

### 3.3 Codex CLI

1. Find live `codex` processes.
2. Map each PID to its open `rollout-*.jsonl` via `lsof`.
3. Parse JSONL for `session_meta`, `token_count`, `agent_message` events.
4. Detect recently finished sessions by scanning today’s `~/.codex/sessions/YYYY/MM/DD/` for JSONL files modified < 5 min ago that are no longer owned by a running process.
5. Extract rate-limit windows from `token_count` events.

### 3.4 OpenCode

1. Find live `opencode` processes.
2. Read recent sessions from `~/.local/share/opencode/opencode.db` via `sqlite3 -readonly -json`.
3. Match live PIDs to DB sessions by process `cwd`.
4. When multiple DB rows share one `cwd`, assign only live PIDs and avoid showing stale rows as live duplicates.

---

## 4. PID verification and PID-reuse safety

Never trust a PID alone. Every tick verifies that the session’s recorded PID is still alive and still runs the expected agent binary.

```text
pid_alive = process_info.get(pid)
              .map(|p| command_contains_binary(p.command, agent_name))
              .unwrap_or(false)
```

- `command_contains_binary` checks the executable basename (e.g. `claude`, `kimi`, `codex`).
- If verification fails, the session is treated as dead/removed.
- The port cache is invalidated whenever the PID set changes, so a reused PID does not inherit stale ports.

---

## 5. Status inference heuristics

None of the agents expose a clean “status” field. abtop derives it from transcript/wire events plus child-process signals.

### 5.1 Kimi (event-driven state machine)

Kimi’s `wire.jsonl` is replayed into three internal states:

- `Idle` — waiting for user input.
- `Thinking` — model is generating.
- `Executing` — a `tool.call` has been issued and its `tool.result` has not arrived.

Mapping to the public `SessionStatus`:

```text
step_state == Executing                         → Executing
step_state == Thinking                          → Thinking (or Executing if children active)
step_state == Idle  && pending tool calls       → Executing
step_state == Idle  && no pending tool calls    → Waiting
```

### 5.2 Claude (transcript + descendant signals)

From the JSONL transcript abtop tracks:

- `last_user_ts_ms` — trailing line is a real user prompt with no assistant reply yet.
- `current_task` — latest assistant turn left a `tool_use` unanswered.
- Active descendant CPU usage > threshold.

```text
active_descendant || pending_tool  → Executing
last_user_ts_ms > 0                → Thinking
otherwise                          → Waiting
```

### 5.3 Generic fallback

For all agents, a session is `Done` when:

- The process is no longer alive, **or**
- The agent-specific session file has been deleted.

`Unknown` is used sparingly, mainly when ownership cannot be confirmed.

---

## 6. Detecting state changes

The change detector compares the newly collected table with the previous tick’s table. It broadcasts if either of the following is true:

1. **The set of `session_id`s changed.** A session was added or removed. (Comparing only counts is insufficient — one session can disappear and another appear in the same tick.)
2. **Any existing `session_id` changed `status`.**

```rust
let current_ids: HashSet<String> = sessions.iter().map(|s| s.session_id.clone()).collect();
let prev_ids:    HashSet<String> = prev_statuses.keys().cloned().collect();

let changed = current_ids != prev_ids
    || sessions.iter().any(|s| prev_statuses.get(&s.session_id) != Some(&s.status));
```

Token counts, context percentages, and child-process details changing **without** a status change do **not** trigger a broadcast, to avoid spamming SSE clients.

After broadcasting, the previous-state table is rebuilt:

```rust
prev_statuses.clear();
for s in &sessions {
    prev_statuses.insert(s.session_id.clone(), s.status.clone());
}
```

---

## 7. Broadcasting via SSE

When a change is detected, the current session-status table is serialized to JSON and pushed to every connected SSE client:

```text
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

data: [{"session_id":"...","status":"Thinking"},...]

data: [{"session_id":"...","status":"Executing"},...]
```

Server-side notes:

- Bind to `127.0.0.1:8787`, fall back to an ephemeral port if occupied.
- Each client gets its own channel with a small buffer (2 messages).
- Slow/full clients are retained; only disconnected clients are removed.
- The latest payload is cached and sent to **new** clients immediately upon connection, so they do not have to wait for the next change.
- On shutdown, drop all sender handles so client threads unblock and exit cleanly.

---

## 8. Edge cases and lessons

| Case | Handling |
|------|----------|
| PID reuse | Verify command string every tick; invalidate port cache on PID set changes. |
| `/clear` creating new sessionId | Claude: pick newest transcript in project dir unless multiple PIDs share cwd. Kimi: use stable PID→session mapping + activity stealing. |
| Stale session files | Files may survive crashes. If PID is dead, treat session as `Done` and drop it. |
| Multiple live PIDs in one cwd | Do not cross-assign transcripts; keep ownership stable or use activity-time heuristics. |
| Incremental transcript parsing | Track byte offset; on file growth read only new bytes; buffer partial lines; reset offset on file shrink/rotation. |
| Slow I/O | Defer `lsof`/index reads to slow ticks (~10 s); process info every tick. |
| MCP server processes | Detect and suppress so they are not double-counted as user sessions. |
| No network / privacy | All reads are local; SSE server only listens on loopback. |

---

## 9. Porting checklist (for Go or another language)

1. **Process snapshot** — run `ps` and `lsof` once per tick and cache the results.
2. **Agent collectors** — implement one per agent, each translating agent-specific files/processes into a common `Session` struct.
3. **PID verification** — verify each session’s PID is alive and matches the expected binary.
4. **Status derivation** — replay agent-specific event/transcript files to derive `Waiting | Thinking | Executing | Done`.
5. **State table** — keep a map `session_id → status` from the previous tick.
6. **Change detection** — compare the current session-ID set and statuses; broadcast only on change.
7. **SSE server** — minimal HTTP server on loopback, one `/events` endpoint, send `data:` JSON to all clients, cache latest payload for new clients.
8. **Edge cases** — handle PID reuse, `/clear`, stale files, and incremental file tailing.

The key insight: **the filesystem + process table are the source of truth**, and the in-memory state table is merely a diffable snapshot used to decide when to push updates.
