use super::process::{self, ProcInfo};
use crate::model::{
    AgentSession, ChatMessage, ChatRole, ChildProcess, FileAccess, FileOp, SessionStatus, SubAgent,
    MAX_CHAT_MESSAGES, MAX_FILE_ACCESSES,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
#[cfg(not(target_os = "linux"))]
use std::process::Command;

const KIMI_CODE_DEFAULT_HOME: &str = ".kimi-code";
const SESSION_INDEX_FILE: &str = "session_index.jsonl";
const STATE_FILE: &str = "state.json";
const MAIN_WIRE_PATH: &str = "agents/main/wire.jsonl";
const KIMI_DEFAULT_CONTEXT_WINDOW: u64 = 262_144;

/// A single entry from Kimi Code's `session_index.jsonl`.
#[derive(Debug, Clone, Deserialize)]
struct SessionIndexEntry {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "sessionDir")]
    session_dir: PathBuf,
    #[serde(rename = "workDir")]
    work_dir: PathBuf,
}

/// Parsed contents of `{sessionDir}/state.json`.
#[derive(Debug, Default, Clone, Deserialize)]
struct KimiStateFile {
    #[serde(rename = "createdAt", default)]
    created_at: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(rename = "lastPrompt", default)]
    last_prompt: Option<String>,
}

/// The high-level state of a Kimi step/turn, derived by replaying wire events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepState {
    /// Waiting for a user prompt or between turns.
    Idle,
    /// Inside a step, no pending tool call; the model is thinking/generating.
    Thinking,
    /// A tool.call has been issued and its result has not arrived yet.
    Executing,
}

/// Cached parse state for a Kimi `wire.jsonl` file.
/// Mirrors Claude's `TranscriptResult` semantics but uses Kimi event types.
#[derive(Debug)]
struct WireTranscriptState {
    model: String,
    total_input: u64,
    total_output: u64,
    total_cache_read: u64,
    total_cache_create: u64,
    last_context_tokens: u64,
    max_context_tokens: u64,
    prev_cache_read: u64,
    context_history: Vec<u64>,
    compaction_count: u32,
    turn_count: u32,
    current_task: String,
    app_version: String,
    thinking_level: String,
    last_activity: std::time::SystemTime,
    new_offset: u64,
    file_identity: (u64, u64),
    token_history: Vec<u64>,
    initial_prompt: String,
    first_assistant_text: String,
    chat_messages: Vec<ChatMessage>,
    tool_calls: Vec<crate::model::ToolCall>,
    /// Assistant text buffered across consecutive `content.part` events within
    /// a single step. Flushed to `chat_messages` on step.end, tool.call, or
    /// a new user prompt so one assistant reply renders as one chat line.
    pending_assistant_text: String,
    /// Current high-level state of the session, driven by wire events.
    step_state: StepState,
    /// Timestamp (event time) when the current thinking period started.
    /// Used only for UI duration display.
    thinking_since_ms: u64,
    /// Pending tool calls within the current step, keyed by toolCallId.
    /// Persisted across ticks so tool.result can clear the pending state even
    /// when it arrives in a later delta than tool.call.
    pending_tool_calls: HashMap<String, u64>,
    file_accesses: Vec<FileAccess>,
}

pub struct KimiCollector {
    code_home: PathBuf,
    session_index_path: PathBuf,
    session_index_cache: Vec<SessionIndexEntry>,
    last_index_load: Option<std::time::Instant>,
    transcript_cache: HashMap<String, WireTranscriptState>,
    state_cache: HashMap<String, KimiStateFile>,
    /// Persistent PID -> session_id mapping across ticks. Used to keep
    /// session ownership stable when multiple live Kimi processes share the
    /// same cwd (e.g. after `/clear` creates a new session, the other
    /// process should not jump to the stale session).
    pid_session_map: HashMap<u32, String>,
}

impl KimiCollector {
    pub fn new() -> Self {
        let code_home = std::env::var("KIMI_CODE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| dirs::home_dir().unwrap_or_default().join(KIMI_CODE_DEFAULT_HOME));
        Self {
            session_index_path: code_home.join(SESSION_INDEX_FILE),
            code_home,
            session_index_cache: Vec::new(),
            last_index_load: None,
            transcript_cache: HashMap::new(),
            state_cache: HashMap::new(),
            pid_session_map: HashMap::new(),
        }
    }

    fn collect_sessions(&mut self, shared: &super::SharedProcessData) -> Vec<AgentSession> {
        // Refresh the session index on slow ticks or the first run.
        if shared.slow_tick
            || self.last_index_load.is_none()
            || self
                .last_index_load
                .is_some_and(|t| t.elapsed() >= std::time::Duration::from_secs(10))
        {
            self.load_session_index();
        }

        let self_pid = std::process::id();
        let mut kimi_pids = Self::find_kimi_pids(&shared.process_info, self_pid);
        // Sort PIDs so that multiple live PIDs sharing the same cwd are assigned
        // to sessions in a stable order across ticks. Otherwise HashMap iteration
        // order can swap which PID gets the newest session.
        kimi_pids.sort();

        // Map live PIDs to sessions by cwd == workDir. When multiple PIDs share
        // a cwd we first try to keep the previous tick's assignment stable, then
        // assign remaining PIDs greedily, and finally let very active unclaimed
        // sessions take over a PID whose current session has gone stale (handles
        // `/clear` where one process starts a fresh session while the other
        // process should stay on its existing session).
        let mut pid_sessions: Vec<(u32, SessionIndexEntry)> = Vec::new();
        let mut claimed_sids = HashSet::new();
        let mut new_pid_session_map: HashMap<u32, String> = HashMap::new();

        // Pre-compute cwd for each live PID to avoid repeated /proc reads.
        let pid_cwds: HashMap<u32, PathBuf> = kimi_pids
            .iter()
            .filter_map(|&pid| get_process_cwd(pid).map(|c| (pid, PathBuf::from(c))))
            .collect();

        // Group sessions by workDir so we can assign newest session to each PID.
        let mut by_cwd: HashMap<PathBuf, Vec<SessionIndexEntry>> = HashMap::new();
        for entry in self.session_index_cache.iter().cloned() {
            by_cwd.entry(entry.work_dir.clone()).or_default().push(entry);
        }
        for list in by_cwd.values_mut() {
            list.sort_by(|a, b| {
                // Use the wire transcript mtime when available: it reflects
                // actual session activity, whereas the session directory mtime
                // only changes when files are created/deleted.
                let a_time = session_activity_time(a).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                let b_time = session_activity_time(b).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                b_time.cmp(&a_time)
            });
        }

        // First pass: keep stable assignments from the previous tick.
        for &pid in &kimi_pids {
            let Some(sid) = self.pid_session_map.get(&pid) else {
                continue;
            };
            let Some(cwd_path) = pid_cwds.get(&pid) else {
                continue;
            };
            if let Some(entry) = by_cwd
                .get(cwd_path)
                .and_then(|list| list.iter().find(|e| e.session_id == *sid))
            {
                if claimed_sids.insert(entry.session_id.clone()) {
                    pid_sessions.push((pid, entry.clone()));
                    new_pid_session_map.insert(pid, entry.session_id.clone());
                }
            }
        }

        // Second pass: time-based matching for newly launched processes. When a
        // Kimi process was started within a small window of a session's creation
        // time, it is very likely the owner of that session. This only handles
        // "new process + new session"; existing processes that create a new
        // session via `/clear` fall through to the stable/steal logic below.
        const START_TIME_THRESHOLD_MS: u64 = 2000;
        let mut created_at_cache: HashMap<String, Option<u64>> = HashMap::new();
        let mut time_matches: Vec<(u32, SessionIndexEntry)> = Vec::new();
        for &pid in &kimi_pids {
            if new_pid_session_map.contains_key(&pid) {
                continue;
            }
            let Some(cwd_path) = pid_cwds.get(&pid) else {
                continue;
            };
            let Some(proc) = shared.process_info.get(&pid) else {
                continue;
            };
            if proc.start_time == 0 {
                continue;
            }
            let candidates = by_cwd.get(cwd_path).cloned().unwrap_or_default();
            let mut best: Option<(u64, SessionIndexEntry)> = None;
            for candidate in candidates {
                if claimed_sids.contains(&candidate.session_id) {
                    continue;
                }
                let created = *created_at_cache
                    .entry(candidate.session_id.clone())
                    .or_insert_with(|| session_created_at_ms(&candidate));
                let Some(created) = created else {
                    continue;
                };
                if created == 0 {
                    continue;
                }
                let delta = proc.start_time.abs_diff(created);
                if delta <= START_TIME_THRESHOLD_MS
                    && best.as_ref().map(|(bd, _)| delta < *bd).unwrap_or(true)
                {
                    best = Some((delta, candidate));
                }
            }
            if let Some((_, entry)) = best {
                if claimed_sids.insert(entry.session_id.clone()) {
                    time_matches.push((pid, entry));
                }
            }
        }
        for (pid, entry) in time_matches {
            pid_sessions.push((pid, entry.clone()));
            new_pid_session_map.insert(pid, entry.session_id.clone());
        }

        // Third pass: assign remaining PIDs greedily by activity.
        for &pid in &kimi_pids {
            if new_pid_session_map.contains_key(&pid) {
                continue;
            }
            let Some(cwd_path) = pid_cwds.get(&pid) else {
                continue;
            };
            let candidates = by_cwd.get(cwd_path).cloned().unwrap_or_default();
            for candidate in candidates {
                if claimed_sids.insert(candidate.session_id.clone()) {
                    pid_sessions.push((pid, candidate.clone()));
                    new_pid_session_map.insert(pid, candidate.session_id.clone());
                    break;
                }
            }
        }

        // Third pass: active unclaimed sessions may take over a PID whose
        // current session has gone stale. This is the `/clear` case: the
        // cleared process starts a new session while the other process keeps
        // its own session.
        const STALE_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(30);
        let now = std::time::SystemTime::now();
        let mut unclaimed: Vec<SessionIndexEntry> = by_cwd
            .values()
            .flatten()
            .filter(|e| !claimed_sids.contains(&e.session_id))
            .cloned()
            .collect();
        unclaimed.sort_by(|a, b| {
            let a_time = session_activity_time(a).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let b_time = session_activity_time(b).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            b_time.cmp(&a_time)
        });

        for new_entry in unclaimed {
            let new_activity =
                session_activity_time(&new_entry).unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            if now.duration_since(new_activity).unwrap_or(STALE_THRESHOLD) > STALE_THRESHOLD {
                continue;
            }

            // Find the PID mapped to the stalest session in the same cwd.
            let victim = pid_sessions
                .iter()
                .filter(|(pid, _)| {
                    pid_cwds
                        .get(pid)
                        .is_some_and(|c| c == &new_entry.work_dir)
                })
                .filter(|(_, old_entry)| {
                    let old_activity = session_activity_time(old_entry)
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                    now.duration_since(old_activity).unwrap_or(STALE_THRESHOLD) > STALE_THRESHOLD
                })
                .min_by_key(|(_, old_entry)| {
                    session_activity_time(old_entry).unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                })
                .map(|(pid, old_entry)| (*pid, old_entry.session_id.clone()));

            if let Some((pid, old_sid)) = victim {
                pid_sessions.retain(|(p, _)| *p != pid);
                claimed_sids.remove(&old_sid);
                pid_sessions.push((pid, new_entry.clone()));
                claimed_sids.insert(new_entry.session_id.clone());
                new_pid_session_map.insert(pid, new_entry.session_id.clone());
            }
        }

        self.pid_session_map = new_pid_session_map;

        let mut sessions = Vec::new();
        for (pid, entry) in pid_sessions {
            if let Some(session) = self.load_session(pid, &entry, shared) {
                sessions.push(session);
            }
        }

        self.evict_stale_cache(&sessions);
        sessions.sort_by_key(|s| std::cmp::Reverse(s.started_at));
        sessions
    }

    /// Read and parse `session_index.jsonl` into the cache.
    fn load_session_index(&mut self) {
        self.last_index_load = Some(std::time::Instant::now());
        self.session_index_cache.clear();
        if is_symlink(&self.session_index_path) || !self.session_index_path.exists() {
            return;
        }
        let content = match fs::read_to_string(&self.session_index_path) {
            Ok(c) => c,
            Err(_) => return,
        };
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<SessionIndexEntry>(line) {
                // Basic validation: sessionDir basename must equal sessionId.
                if entry
                    .session_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n == entry.session_id)
                {
                    self.session_index_cache.push(entry);
                }
            }
        }
    }

    fn find_kimi_pids(
        process_info: &HashMap<u32, ProcInfo>,
        self_pid: u32,
    ) -> Vec<u32> {
        process_info
            .iter()
            .filter(|(pid, info)| {
                process::cmd_has_binary(&info.command, "kimi")
                    && !process::is_descendant_of(**pid, self_pid, process_info)
            })
            .map(|(pid, _)| *pid)
            .collect()
    }

    fn load_session(
        &mut self,
        pid: u32,
        entry: &SessionIndexEntry,
        shared: &super::SharedProcessData,
    ) -> Option<AgentSession> {
        let proc_cmd = shared.process_info.get(&pid).map(|p| p.command.as_str());
        let pid_alive = proc_cmd
            .map(|c| process::cmd_has_binary(c, "kimi"))
            .unwrap_or(false);
        if !pid_alive {
            return None;
        }

        // Read state.json (metadata).
        let state_path = entry.session_dir.join(STATE_FILE);
        let state: KimiStateFile = self
            .state_cache
            .get(&entry.session_id)
            .cloned()
            .or_else(|| read_state_file(&state_path))
            .unwrap_or_default();
        self.state_cache
            .insert(entry.session_id.clone(), state.clone());

        let started_at = parse_iso_or_zero(
            state
                .created_at
                .as_deref()
                .unwrap_or(""),
        );
        let initial_prompt = state
            .title
            .clone()
            .or(state.last_prompt.clone())
            .map(|s| super::sanitize_terminal_text(&super::redact_secrets(&s)))
            .unwrap_or_default();

        // Parse main agent wire.jsonl incrementally.
        let main_wire = entry.session_dir.join(MAIN_WIRE_PATH);
        let fallback_wire = entry.session_dir.join("wire.jsonl");
        let wire_path = if main_wire.exists() {
            main_wire
        } else {
            fallback_wire
        };

        if wire_path.exists() && !is_symlink(&wire_path) {
            let identity = file_identity(&wire_path);
            let mut cached = self
                .transcript_cache
                .remove(&entry.session_id)
                .unwrap_or_else(empty_wire_state);
            // Reset when the file identity changed or we have no previous offset.
            if cached.file_identity != identity || cached.new_offset == 0 {
                cached = empty_wire_state();
                cached.file_identity = identity;
            }
            let from_offset = cached.new_offset;

            parse_wire_delta(&wire_path, &mut cached, from_offset);
            if from_offset == 0 {
                // Initial discovery (or file reset): the historical step state is
                // not representative of the live session. Start from a clean
                // Waiting state and let subsequent incremental events drive status.
                reset_state(&mut cached);
                cached.current_task.clear();
            }
            self.transcript_cache.insert(entry.session_id.clone(), cached);
        }

        let empty = empty_wire_state();
        let cached = self
            .transcript_cache
            .get(&entry.session_id)
            .unwrap_or(&empty);

        let model = if cached.model != "-" {
            cached.model.clone()
        } else {
            "kimi-code/kimi-for-coding".to_string()
        };
        let context_window = context_window_for_model(&model, cached.max_context_tokens);
        let context_percent = if context_window > 0 {
            (cached.last_context_tokens as f64 / context_window as f64) * 100.0
        } else {
            0.0
        };

        let proc = shared.process_info.get(&pid);
        let mem_mb = proc.map(|p| p.rss_kb / 1024).unwrap_or(0);

        // Children with cycle guard.
        let mut children = Vec::new();
        let mut stack: Vec<u32> = shared
            .children_map
            .get(&pid)
            .cloned()
            .unwrap_or_default();
        let mut visited = HashSet::new();
        while let Some(cpid) = stack.pop() {
            if !visited.insert(cpid) {
                continue;
            }
            if let Some(cproc) = shared.process_info.get(&cpid) {
                let port = shared.ports.get(&cpid).and_then(|v| v.first().copied());
                children.push(ChildProcess {
                    pid: cpid,
                    command: cproc.command.clone(),
                    mem_kb: cproc.rss_kb,
                    port,
                });
            }
            if let Some(grandchildren) = shared.children_map.get(&cpid) {
                stack.extend(grandchildren);
            }
        }

        // Status: event-driven state machine with child-process auxiliary signal.
        // Kimi batches writes to wire.jsonl, so the event stream is replayed in
        // order to derive the current state. Child processes are used only as a
        // secondary Executing signal while inside a step.
        let has_children = !children.is_empty();
        let status = match cached.step_state {
            StepState::Executing => SessionStatus::Executing,
            StepState::Thinking => {
                if has_children {
                    SessionStatus::Executing
                } else {
                    SessionStatus::Thinking
                }
            }
            StepState::Idle => {
                if !cached.pending_tool_calls.is_empty() || has_children {
                    SessionStatus::Executing
                } else {
                    SessionStatus::Waiting
                }
            }
        };

        let current_task = cached.current_task.clone();
        let current_tasks = if !current_task.is_empty() {
            vec![current_task]
        } else if matches!(status, SessionStatus::Waiting) {
            vec!["waiting for input".to_string()]
        } else {
            vec!["thinking...".to_string()]
        };

        let project_name = process::last_path_segment(&entry.work_dir.to_string_lossy())
            .unwrap_or("?")
            .to_string();

        // Subagents.
        let subagents = self.collect_subagents(&entry.session_dir.join("agents"));

        let cwd = entry.work_dir.to_string_lossy().to_string();
        Some(AgentSession {
            agent_cli: "kimi",
            pid,
            session_id: entry.session_id.clone(),
            cwd,
            project_name,
            started_at,
            status,
            model,
            effort: cached.thinking_level.clone(),
            context_percent,
            total_input_tokens: cached.total_input,
            total_output_tokens: cached.total_output,
            total_cache_read: cached.total_cache_read,
            total_cache_create: cached.total_cache_create,
            turn_count: cached.turn_count,
            current_tasks,
            mem_mb,
            version: cached.app_version.clone(),
            git_branch: String::new(),
            git_added: 0,
            git_modified: 0,
            token_history: cached.token_history.clone(),
            context_history: cached.context_history.clone(),
            compaction_count: cached.compaction_count,
            context_window,
            subagents,
            mem_file_count: 0,
            mem_line_count: 0,
            children,
            initial_prompt: if cached.initial_prompt.is_empty() {
                initial_prompt
            } else {
                cached.initial_prompt.clone()
            },
            first_assistant_text: cached.first_assistant_text.clone(),
            chat_messages: cached.chat_messages.clone(),
            tool_calls: cached.tool_calls.clone(),
            pending_since_ms: cached.pending_tool_calls.values().copied().max().unwrap_or(0),
            thinking_since_ms: cached.thinking_since_ms,
            file_accesses: cached.file_accesses.clone(),
            config_root: super::abbrev_path(&self.code_home),
        })
    }

    fn collect_subagents(&self, agents_dir: &Path) -> Vec<SubAgent> {
        let mut subagents = Vec::new();
        let entries = match fs::read_dir(agents_dir) {
            Ok(e) => e,
            Err(_) => return subagents,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("agent")
                .to_string();
            if name == "main" {
                continue;
            }
            let wire = path.join("wire.jsonl");
            let (tokens, status) = if wire.exists() && !is_symlink(&wire) {
                let mut state = empty_wire_state();
                parse_wire_delta(&wire, &mut state, 0);
                let tokens = state.total_input
                    + state.total_output
                    + state.total_cache_read
                    + state.total_cache_create;
                let status = if last_activity_within(&wire, std::time::Duration::from_secs(30)) {
                    "working".to_string()
                } else {
                    "done".to_string()
                };
                (tokens, status)
            } else {
                (0, "done".to_string())
            };
            subagents.push(SubAgent { name, status, tokens });
        }
        subagents
    }

    fn evict_stale_cache(&mut self, sessions: &[AgentSession]) {
        let active_ids: HashSet<&str> = sessions.iter().map(|s| s.session_id.as_str()).collect();
        self.transcript_cache
            .retain(|sid, _| active_ids.contains(sid.as_str()));
        self.state_cache
            .retain(|sid, _| active_ids.contains(sid.as_str()));
    }
}

impl Default for KimiCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl super::AgentCollector for KimiCollector {
    fn collect(&mut self, shared: &super::SharedProcessData) -> Vec<AgentSession> {
        self.collect_sessions(shared)
    }
}

/// Enter the `Thinking` state. Records the event timestamp as the start of a
/// thinking period only when transitioning from `Idle`.
fn enter_thinking(state: &mut WireTranscriptState, entry_ts_ms: u64) {
    if state.step_state == StepState::Idle {
        state.thinking_since_ms = entry_ts_ms;
    }
    state.step_state = StepState::Thinking;
}

/// Enter the `Executing` state and record a pending tool call.
fn enter_executing(state: &mut WireTranscriptState, tool_call_id: String, entry_ts_ms: u64) {
    state.step_state = StepState::Executing;
    state.pending_tool_calls.insert(tool_call_id, entry_ts_ms);
}

/// Mark a single pending tool call as finished. Returns to `Thinking` when no
/// pending tools remain.
fn finish_tool(state: &mut WireTranscriptState, tool_call_id: &str) {
    state.pending_tool_calls.remove(tool_call_id);
    if state.pending_tool_calls.is_empty() && state.step_state == StepState::Executing {
        state.step_state = StepState::Thinking;
    }
}

/// Close the current step and return to `Idle`.
fn finish_step(state: &mut WireTranscriptState) {
    state.pending_tool_calls.clear();
    state.step_state = StepState::Idle;
    state.thinking_since_ms = 0;
}

/// Reset to a known idle state, used when a session/agent is initialized.
fn reset_state(state: &mut WireTranscriptState) {
    state.pending_tool_calls.clear();
    state.step_state = StepState::Idle;
    state.thinking_since_ms = 0;
}

/// Parse new lines from a Kimi `wire.jsonl` and update `state` in place.
/// `from_offset` is the byte position where the previous parse stopped; on
/// first read it should be 0.
fn parse_wire_delta(path: &Path, state: &mut WireTranscriptState, from_offset: u64) {
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return,
    };
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if file_len == from_offset {
        state.new_offset = file_len;
        return;
    }
    let effective_offset = if file_len < from_offset { 0 } else { from_offset };

    let mut prev_context_tokens = if effective_offset > 0 {
        state.last_context_tokens
    } else {
        0
    };
    let mut prev_cache_read = if effective_offset > 0 {
        state.prev_cache_read
    } else {
        0
    };

    if let Ok(m) = fs::metadata(path) {
        if let Ok(mtime) = m.modified() {
            state.last_activity = mtime;
        }
    }

    let mut reader = BufReader::new(file);
    if effective_offset > 0 {
        let _ = reader.seek(SeekFrom::Start(effective_offset));
    }

    const MAX_LINE_BYTES: usize = 10 * 1024 * 1024;
    let mut bytes_read = effective_offset;
    let mut line_buf = String::new();

    loop {
        line_buf.clear();
        match reader
            .by_ref()
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_line(&mut line_buf)
        {
            Ok(0) => break,
            Ok(n) => {
                if line_buf.len() > MAX_LINE_BYTES && !line_buf.ends_with('\n') {
                    bytes_read = file_len;
                    break;
                }
                let has_newline = line_buf.ends_with('\n');
                let line = line_buf.trim();
                if line.is_empty() {
                    if has_newline {
                        bytes_read += n as u64;
                    }
                    continue;
                }
                let val = match serde_json::from_str::<Value>(line) {
                    Ok(v) => v,
                    Err(_) => {
                        if has_newline {
                            bytes_read += n as u64;
                        }
                        if !has_newline {
                            break;
                        }
                        continue;
                    }
                };
                bytes_read += n as u64;

                let entry_ts_ms = val.get("time").and_then(|t| t.as_u64()).unwrap_or(0);
                let event = event_payload(&val);

                match event.get("type").and_then(|v| v.as_str()) {
                    Some("config.update") => {
                        if let Some(m) = event.get("modelAlias").and_then(|v| v.as_str()) {
                            state.model = m.to_string();
                        }
                        if let Some(t) = event.get("thinkingLevel").and_then(|v| v.as_str()) {
                            state.thinking_level = t.to_string();
                        }
                    }
                    Some("turn.prompt") => {
                        flush_pending_assistant(state);
                        state.current_task.clear();
                        reset_state(state);
                        enter_thinking(state, entry_ts_ms);
                        if state.initial_prompt.is_empty() {
                            state.initial_prompt = extract_prompt_text(event);
                        }
                        let user_text = extract_chat_text_from_value(event, "input");
                        if !user_text.is_empty() {
                            push_chat_message(&mut state.chat_messages, ChatRole::User, user_text);
                        }
                    }
                    Some("turn.cancel") => {
                        reset_state(state);
                    }
                    Some("step.begin") => {
                        // A new step begins: clear stale pending-tool state from
                        // any previous step that never got a result.
                        state.pending_tool_calls.clear();
                        enter_thinking(state, entry_ts_ms);
                    }
                    Some("content.part") => {
                        if let Some(part) = event.get("part") {
                            let part_type = part.get("type").and_then(|v| v.as_str());
                            // Internal reasoning indicates the model is actively
                            // working on this turn, even between tool calls.
                            if part_type == Some("think") {
                                enter_thinking(state, entry_ts_ms);
                            }
                            if let Some(text) = extract_content_part_text(part) {
                                // Buffer actual assistant text so a multi-part reply
                                // renders as a single chat line; skip reasoning/thinking.
                                if part_type == Some("text") {
                                    if state.first_assistant_text.is_empty() {
                                        state.first_assistant_text = truncate(&text, 200);
                                    }
                                    if !state.pending_assistant_text.is_empty() {
                                        state.pending_assistant_text.push(' ');
                                    }
                                    state.pending_assistant_text.push_str(&text);
                                }
                            }
                        }
                    }
                    Some("tool.call") => {
                        flush_pending_assistant(state);
                        let name = event
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("?")
                            .to_string();
                        let arg = extract_kimi_tool_arg(event);
                        let tool_call_id = event
                            .get("toolCallId")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        state.current_task = format!("{} {}", name, arg);
                        if state.tool_calls.len() < 500 {
                            state.tool_calls.push(crate::model::ToolCall {
                                name: name.clone(),
                                arg: truncate(&arg, 40),
                                duration_ms: 0,
                            });
                        }
                        if !tool_call_id.is_empty() {
                            enter_executing(state, tool_call_id, entry_ts_ms);
                        }
                        // File access audit.
                        if let Some(file_path) = event
                            .get("args")
                            .and_then(|a| a.get("file_path"))
                            .and_then(|f| f.as_str())
                        {
                            let op = match name.as_str() {
                                "Read" => Some(FileOp::Read),
                                "Edit" => Some(FileOp::Edit),
                                "Write" => Some(FileOp::Write),
                                _ => None,
                            };
                            if let Some(op) = op {
                                state.file_accesses.push(FileAccess {
                                    path: file_path.to_string(),
                                    operation: op,
                                    turn_index: state.turn_count,
                                });
                            }
                        }
                    }
                    Some("tool.result") => {
                        if let Some(id) = event.get("toolCallId").and_then(|v| v.as_str()) {
                            finish_tool(state, id);
                        }
                    }
                    Some("step.end") => {
                        state.turn_count += 1;
                        flush_pending_assistant(state);
                        if let Some(usage) = event.get("usage") {
                            apply_usage(usage, state, &mut prev_context_tokens, &mut prev_cache_read);
                        }
                        finish_step(state);
                    }
                    Some("usage.record") => {
                        let scope = event
                            .get("usageScope")
                            .and_then(|v| v.as_str())
                            .unwrap_or("turn");
                        if scope == "session" {
                            if let Some(usage) = event.get("usage") {
                                reconcile_session_usage(usage, state);
                            }
                        }
                        if let Some(m) = event.get("model").and_then(|v| v.as_str()) {
                            if state.model == "-" {
                                state.model = m.to_string();
                            }
                        }
                    }
                    Some("context.apply_compaction") => {
                        state.compaction_count += 1;
                    }
                    Some("metadata") => {
                        if let Some(v) = event.get("version").and_then(|v| v.as_str()) {
                            state.app_version = v.to_string();
                        }
                        // A metadata record means the agent/session just started or
                        // resumed; the agent cannot be mid-step at this point.
                        reset_state(state);
                    }
                    _ => {}
                }
            }
            Err(_) => break,
        }
    }

    let len = state.file_accesses.len();
    if len > MAX_FILE_ACCESSES {
        state.file_accesses.drain(..len - MAX_FILE_ACCESSES);
    }

    state.new_offset = bytes_read;
}

/// Return the inner event payload, unwrapping `context.append_loop_event` if needed.
fn event_payload(val: &Value) -> &Value {
    if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
        if t == "context.append_loop_event" {
            if let Some(inner) = val.get("event").or_else(|| val.get("loopEvent")) {
                return inner;
            }
        }
    }
    val
}

fn apply_usage(
    usage: &Value,
    result: &mut WireTranscriptState,
    prev_context_tokens: &mut u64,
    prev_cache_read: &mut u64,
) {
    let inp = usage.get("inputOther").and_then(|v| v.as_u64()).unwrap_or(0);
    let out = usage.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
    let cr = usage
        .get("inputCacheRead")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let cc = usage
        .get("inputCacheCreation")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    result.total_input += inp;
    result.total_output += out;
    result.total_cache_read += cr;
    result.total_cache_create += cc;

    let current_context = if cr == 0 && cc > 0 { inp + cc } else { inp + cr };
    result.last_context_tokens = current_context;
    if result.last_context_tokens > result.max_context_tokens {
        result.max_context_tokens = result.last_context_tokens;
    }
    if *prev_context_tokens > 0
        && result.last_context_tokens < *prev_context_tokens * 7 / 10
        && *prev_cache_read > 1000
        && cr < *prev_cache_read / 5
    {
        result.compaction_count += 1;
    }
    *prev_context_tokens = current_context;
    *prev_cache_read = cr;
    result.prev_cache_read = cr;

    if result.context_history.len() < 10_000 {
        result.context_history.push(result.last_context_tokens);
    }
    if result.token_history.len() < 10_000 {
        result.token_history.push(inp + out + cr + cc);
    }
}

fn reconcile_session_usage(usage: &Value, result: &mut WireTranscriptState) {
    let inp = usage.get("inputOther").and_then(|v| v.as_u64()).unwrap_or(0);
    let out = usage.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
    let cr = usage
        .get("inputCacheRead")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let cc = usage
        .get("inputCacheCreation")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if inp > result.total_input {
        result.total_input = inp;
    }
    if out > result.total_output {
        result.total_output = out;
    }
    if cr > result.total_cache_read {
        result.total_cache_read = cr;
    }
    if cc > result.total_cache_create {
        result.total_cache_create = cc;
    }
}

fn empty_wire_state() -> WireTranscriptState {
    WireTranscriptState {
        model: "-".to_string(),
        total_input: 0,
        total_output: 0,
        total_cache_read: 0,
        total_cache_create: 0,
        last_context_tokens: 0,
        max_context_tokens: 0,
        prev_cache_read: 0,
        context_history: Vec::new(),
        compaction_count: 0,
        turn_count: 0,
        current_task: String::new(),
        app_version: String::new(),
        thinking_level: String::new(),
        last_activity: std::time::UNIX_EPOCH,
        new_offset: 0,
        file_identity: (0, 0),
        token_history: Vec::new(),
        initial_prompt: String::new(),
        first_assistant_text: String::new(),
        chat_messages: Vec::new(),
        tool_calls: Vec::new(),
        pending_assistant_text: String::new(),
        step_state: StepState::Idle,
        thinking_since_ms: 0,
        pending_tool_calls: HashMap::new(),
        file_accesses: Vec::new(),
    }
}

fn read_state_file(path: &Path) -> Option<KimiStateFile> {
    if is_symlink(path) || !path.exists() {
        return None;
    }
    let content = fs::read_to_string(path).ok()?;
    let mut state: KimiStateFile = serde_json::from_str(&content).ok()?;
    if let Some(ref mut t) = state.title {
        truncate_field(t, 512);
    }
    if let Some(ref mut p) = state.last_prompt {
        truncate_field(p, 4096);
    }
    Some(state)
}

fn parse_iso_or_zero(s: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis().max(0) as u64)
        .unwrap_or(0)
}

/// Read a session's `state.json` and return its `createdAt` timestamp in
/// milliseconds since the Unix epoch. Returns `None` when the file is missing
/// or the timestamp cannot be parsed.
fn session_created_at_ms(entry: &SessionIndexEntry) -> Option<u64> {
    let state_path = entry.session_dir.join(STATE_FILE);
    let state = read_state_file(&state_path)?;
    let ms = parse_iso_or_zero(state.created_at.as_deref().unwrap_or(""));
    if ms == 0 {
        None
    } else {
        Some(ms)
    }
}

fn context_window_for_model(model: &str, max_context_tokens: u64) -> u64 {
    if model.contains("kimi-for-coding") || model.contains("kimi-k2") {
        KIMI_DEFAULT_CONTEXT_WINDOW
    } else if max_context_tokens > 200_000 {
        max_context_tokens
    } else {
        200_000
    }
}

fn extract_kimi_tool_arg(val: &Value) -> String {
    if let Some(args) = val.get("args") {
        if let Some(fp) = args.get("file_path").and_then(|v| v.as_str()) {
            return shorten_path(fp);
        }
        if let Some(cmd) = args.get("command").and_then(|v| v.as_str()) {
            let short = cmd.lines().next().unwrap_or(cmd);
            return super::redact_secrets(&truncate(short, 40));
        }
        if let Some(pat) = args.get("pattern").and_then(|v| v.as_str()) {
            return truncate(pat, 40);
        }
        // Fallback: first string-valued arg.
        if let Some(obj) = args.as_object() {
            for v in obj.values() {
                if let Some(s) = v.as_str() {
                    return truncate(s, 40);
                }
            }
        }
    }
    if let Some(display) = val.get("display").and_then(|v| v.as_str()) {
        return truncate(display, 40);
    }
    String::new()
}

fn extract_content_part_text(part: &Value) -> Option<String> {
    let part_type = part.get("type").and_then(|v| v.as_str())?;
    match part_type {
        "text" => part.get("text").and_then(|v| v.as_str()).map(|s| s.to_string()),
        "think" => part.get("think").and_then(|v| v.as_str()).map(|s| s.to_string()),
        _ => None,
    }
}

fn extract_prompt_text(val: &Value) -> String {
    let raw = match val.get("input") {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                    item.get("text").and_then(|t| t.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let cleaned: String = raw
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("```"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut result = cleaned;
    while let Some(start) = result.find("[Image") {
        if let Some(end) = result[start..].find(']') {
            result = format!(
                "{}{}",
                &result[..start],
                result[start + end + 1..].trim_start()
            );
        } else {
            break;
        }
    }
    let clean = result.trim().to_string();
    if clean.contains("You are a conversation title generator") {
        return String::new();
    }
    truncate(&clean, 50)
}

fn extract_chat_text_from_value(val: &Value, key: &str) -> String {
    let raw = match val.get(key) {
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                    item.get("text").and_then(|t| t.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    clean_chat_text(&raw, 500)
}

fn clean_chat_text(raw: &str, max: usize) -> String {
    let cleaned = raw
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with("```"))
        .collect::<Vec<_>>()
        .join(" ");
    let terminal_safe = super::sanitize_terminal_text(&cleaned);
    let redacted = super::redact_secrets(&terminal_safe);
    truncate(&redacted, max)
}

fn shorten_path(path: &str) -> String {
    #[cfg(windows)]
    let parts: Vec<&str> = path.rsplit(['/', '\\']).collect();
    #[cfg(not(windows))]
    let parts: Vec<&str> = path.rsplit('/').collect();
    if parts.len() <= 2 {
        path.to_string()
    } else {
        format!("{}/{}", parts[1], parts[0])
    }
}

fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max - 1).collect();
        format!("{}…", truncated)
    }
}

fn truncate_field(s: &mut String, max_bytes: usize) {
    if s.len() > max_bytes {
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
}

#[cfg(unix)]
fn file_identity(path: &Path) -> (u64, u64) {
    fs::metadata(path)
        .ok()
        .map(|m| (m.dev(), m.ino()))
        .unwrap_or((0, 0))
}

#[cfg(windows)]
fn file_identity(path: &Path) -> (u64, u64) {
    fs::metadata(path)
        .ok()
        .map(|m| {
            let size = m.len();
            let mtime_ns = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            (size, mtime_ns)
        })
        .unwrap_or((0, 0))
}

fn is_symlink(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(true)
}

fn flush_pending_assistant(result: &mut WireTranscriptState) {
    if result.pending_assistant_text.is_empty() {
        return;
    }
    let text = std::mem::take(&mut result.pending_assistant_text);
    push_chat_message(&mut result.chat_messages, ChatRole::Assistant, text);
}

fn push_chat_message(messages: &mut Vec<ChatMessage>, role: ChatRole, text: String) {
    if text.is_empty() {
        return;
    }
    messages.push(ChatMessage { role, text });
    let len = messages.len();
    if len > MAX_CHAT_MESSAGES {
        messages.drain(..len - MAX_CHAT_MESSAGES);
    }
}

fn session_activity_time(entry: &SessionIndexEntry) -> Option<std::time::SystemTime> {
    let wire = entry.session_dir.join(MAIN_WIRE_PATH);
    let fallback = entry.session_dir.join("wire.jsonl");
    let path = if wire.exists() { wire } else { fallback };
    fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .or_else(|| entry.session_dir.metadata().ok().and_then(|m| m.modified().ok()))
}

fn last_activity_within(path: &Path, within: std::time::Duration) -> bool {
    fs::metadata(path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
        .is_some_and(|d| d < within)
}

#[cfg(target_os = "linux")]
fn get_process_cwd(pid: u32) -> Option<String> {
    std::fs::read_link(format!("/proc/{}/cwd", pid))
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

#[cfg(not(target_os = "linux"))]
fn get_process_cwd(pid: u32) -> Option<String> {
    let output = Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "-d", "cwd", "-Fn"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find(|l| l.starts_with('n') && l.len() > 1)
        .map(|l| l[1..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        (dir, path)
    }

    #[test]
    fn parse_wire_delta_counts_tokens() {
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"config.update","modelAlias":"kimi-code/kimi-for-coding","thinkingLevel":"high","time":1000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"turn.prompt","input":[{{"type":"text","text":"hello"}}],"time":2000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"step.end","usage":{{"inputOther":100,"output":50,"inputCacheRead":200,"inputCacheCreation":10}},"finishReason":"end_turn","time":3000}}"#
        )
        .unwrap();

        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert_eq!(state.total_input, 100);
        assert_eq!(state.total_output, 50);
        assert_eq!(state.total_cache_read, 200);
        assert_eq!(state.total_cache_create, 10);
        assert_eq!(state.last_context_tokens, 300);
        assert_eq!(state.turn_count, 1);
        assert_eq!(state.model, "kimi-code/kimi-for-coding");
        assert_eq!(state.thinking_level, "high");
    }

    #[test]
    fn parse_wire_delta_tool_use_status() {
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"turn.prompt","input":[{{"type":"text","text":"run"}}],"time":1000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"tool.call","name":"Bash","args":{{"command":"sleep 1"}},"toolCallId":"t1","time":2000}}"#
        )
        .unwrap();
        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert_eq!(state.step_state, StepState::Executing);
        assert!(state.pending_tool_calls.contains_key("t1"));
        assert!(state.current_task.contains("Bash"));
    }

    #[test]
    fn parse_wire_delta_tool_result_clears_pending() {
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"tool.call","name":"Bash","args":{{"command":"sleep 1"}},"toolCallId":"t1","time":2000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"tool.result","toolCallId":"t1","result":{{"output":"done"}},"time":3000}}"#
        )
        .unwrap();
        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert!(state.pending_tool_calls.is_empty());
        // Without a step.end the turn is still in progress, so state stays Thinking.
        assert_eq!(state.step_state, StepState::Thinking);
    }

    #[test]
    fn parse_wire_delta_incremental_update() {
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"step.end","usage":{{"inputOther":10,"output":5,"inputCacheRead":0,"inputCacheCreation":0}},"finishReason":"end_turn","time":1000}}"#
        )
        .unwrap();
        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert_eq!(state.total_input, 10);

        let mut f = fs::OpenOptions::new().append(true).open(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"step.end","usage":{{"inputOther":20,"output":10,"inputCacheRead":0,"inputCacheCreation":0}},"finishReason":"end_turn","time":2000}}"#
        )
        .unwrap();
        drop(f);

        let from_offset = state.new_offset;
        parse_wire_delta(&wire, &mut state, from_offset);
        assert_eq!(state.total_input, 30);
        assert_eq!(state.total_output, 15);
    }

    #[test]
    fn find_kimi_pids_skips_abtop_descendants() {
        let mut info = HashMap::new();
        info.insert(
            10,
            ProcInfo {
                pid: 10,
                ppid: 1,
                rss_kb: 1000,
                cpu_pct: 0.0,
                start_time: 0,
                command: "/home/user/.kimi-code/bin/kimi".to_string(),
            },
        );
        info.insert(
            11,
            ProcInfo {
                pid: 11,
                ppid: 5,
                rss_kb: 1000,
                cpu_pct: 0.0,
                start_time: 0,
                command: "/home/user/.kimi-code/bin/kimi".to_string(),
            },
        );
        info.insert(
            5,
            ProcInfo {
                pid: 5,
                ppid: 1,
                rss_kb: 1000,
                cpu_pct: 0.0,
                start_time: 0,
                command: "abtop".to_string(),
            },
        );
        let pids = KimiCollector::find_kimi_pids(&info, 5);
        assert!(pids.contains(&10));
        assert!(!pids.contains(&11));
    }

    #[test]
    fn context_window_for_known_models() {
        assert_eq!(
            context_window_for_model("kimi-code/kimi-for-coding", 0),
            262_144
        );
        assert_eq!(context_window_for_model("kimi-k2", 0), 262_144);
        assert_eq!(context_window_for_model("unknown", 0), 200_000);
    }

    #[test]
    fn parse_wire_delta_handles_wrapped_loop_events() {
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"config.update","modelAlias":"kimi-code/kimi-for-coding","thinkingLevel":"medium"}},"time":1000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"turn.prompt","input":[{{"type":"text","text":"hello"}}],"origin":{{"kind":"user"}}}},"time":2000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"content.part","part":{{"type":"think","think":"internal reasoning"}},"turnId":"0","step":1}},"time":3000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"content.part","part":{{"type":"text","text":"Hi"}},"turnId":"0","step":1}},"time":3001}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"content.part","part":{{"type":"text","text":"there"}},"turnId":"0","step":1}},"time":3002}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"context.append_loop_event","event":{{"type":"step.end","usage":{{"inputOther":50,"output":20,"inputCacheRead":0,"inputCacheCreation":0}},"finishReason":"end_turn"}},"time":4000}}"#
        )
        .unwrap();

        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert_eq!(state.model, "kimi-code/kimi-for-coding");
        assert_eq!(state.thinking_level, "medium");
        assert_eq!(state.initial_prompt, "hello");
        assert_eq!(state.first_assistant_text, "Hi");
        assert_eq!(state.total_input, 50);
        assert_eq!(state.total_output, 20);
        assert_eq!(state.chat_messages.len(), 2);
        assert_eq!(state.chat_messages[0].role, ChatRole::User);
        assert_eq!(state.chat_messages[1].role, ChatRole::Assistant);
        // Multiple text parts within one step are merged into a single chat line.
        assert_eq!(state.chat_messages[1].text, "Hi there");
    }

    #[test]
    fn incremental_update_preserves_running_tool_before_step_end() {
        // Simulates the live screenshot scenario: a tool.call is written and
        // the child process is running, but step.end has not arrived yet. The
        // in-place parser must keep current_task and the executing step_state
        // across ticks.
        let (_dir, path) = tmp_dir();
        let wire = path.join("wire.jsonl");
        let mut f = fs::File::create(&wire).unwrap();
        writeln!(
            f,
            r#"{{"type":"turn.prompt","input":[{{"type":"text","text":"run"}}],"time":1000}}"#
        )
        .unwrap();
        writeln!(
            f,
            r#"{{"type":"tool.call","name":"Bash","args":{{"command":"cd /tmp && sleep 8"}},"toolCallId":"t1","time":2000}}"#
        )
        .unwrap();

        let mut state = empty_wire_state();
        parse_wire_delta(&wire, &mut state, 0);
        assert_eq!(state.step_state, StepState::Executing);
        assert!(state.pending_tool_calls.contains_key("t1"));
        assert!(state.current_task.contains("Bash"));

        // On the next tick no new bytes have been appended; state stays the same.
        let saved_offset = state.new_offset;
        parse_wire_delta(&wire, &mut state, saved_offset);
        assert_eq!(state.new_offset, saved_offset);
        assert_eq!(
            state.step_state,
            StepState::Executing,
            "executing state lost after no-op tick"
        );
        assert!(
            state.current_task.contains("Bash"),
            "current_task lost after no-op tick: got {:?}",
            state.current_task
        );
    }
}
