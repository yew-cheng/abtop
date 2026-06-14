//! Headless HTTP server exposing abtop monitor state.
//!
//! Runs the data collector on a dedicated thread and serves the latest
//! serialized [`Snapshot`] over HTTP. This keeps the non-`Send` [`App`] on the
//! collector thread while request handlers only touch an `Arc<Mutex<String>>`.

use crate::app::App;
use crate::{config, theme::Theme};
use std::io::{self, Read};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tiny_http::{Header, Response, Server, StatusCode};

/// Latest monitor state shared between the collector and HTTP threads.
struct ServerState {
    /// Full JSON snapshot from the last successful tick.
    json: String,
    /// `generated_at_ms` of the snapshot; 0 before the first tick.
    updated_at_ms: u64,
    /// Error message from the last failed tick, if any.
    last_error: Option<String>,
}

impl ServerState {
    fn empty() -> Self {
        Self {
            json: String::new(),
            updated_at_ms: 0,
            last_error: None,
        }
    }
}

/// Active SSE clients. Each entry is a sender that pushes a full JSON snapshot
/// whenever the collector finishes a tick. Dead senders are lazily cleaned up
/// on the next broadcast.
type Subscribers = Arc<Mutex<Vec<Sender<String>>>>;

fn content_type_json() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
}

fn text_response(status: StatusCode, body: impl Into<Vec<u8>>) -> Response<io::Cursor<Vec<u8>>> {
    Response::new(
        status,
        vec![Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..]).unwrap()],
        io::Cursor::new(body.into()),
        None,
        None,
    )
}

fn json_response(json: String) -> Response<io::Cursor<Vec<u8>>> {
    Response::new(
        StatusCode(200),
        vec![content_type_json()],
        io::Cursor::new(json.into_bytes()),
        None,
        None,
    )
}

fn sse_response(body: SseBody) -> Response<SseBody> {
    Response::new(
        StatusCode(200),
        vec![
            Header::from_bytes(&b"Content-Type"[..], &b"text/event-stream"[..]).unwrap(),
            Header::from_bytes(&b"Cache-Control"[..], &b"no-cache"[..]).unwrap(),
        ],
        body,
        None,
        None,
    )
}

/// Push a fresh JSON snapshot to every connected SSE client, removing any whose
/// receiver has dropped.
fn broadcast(subscribers: &Subscribers, json: String) {
    let mut subs = subscribers.lock().unwrap();
    subs.retain(|tx| tx.send(json.clone()).is_ok());
}

/// Start the collector thread and block serving HTTP on `addr`.
pub fn run_http(addr: &str) -> io::Result<()> {
    let state = Arc::new(Mutex::new(ServerState::empty()));
    let subscribers: Subscribers = Arc::new(Mutex::new(Vec::new()));

    // Collector thread: owns the App (which is !Send) and refreshes the snapshot.
    let state_for_collector = Arc::clone(&state);
    let subs_for_collector = Arc::clone(&subscribers);
    let cfg = config::load_config();
    let theme = Theme::by_name(&cfg.theme).unwrap_or_default();
    thread::spawn(move || {
        let mut app = App::new_with_config_and_claude_dirs(
            theme,
            &cfg.hidden_agents,
            cfg.panels,
            &cfg.claude_config_dirs,
        );

        loop {
            app.tick_no_summaries();
            match serde_json::to_string(&app.to_snapshot(2_000)) {
                Ok(json) => {
                    let mut st = state_for_collector.lock().unwrap();
                    st.json = json.clone();
                    st.updated_at_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    st.last_error = None;
                    drop(st);
                    broadcast(&subs_for_collector, json);
                }
                Err(e) => {
                    let mut st = state_for_collector.lock().unwrap();
                    st.last_error = Some(e.to_string());
                }
            }
            thread::sleep(Duration::from_secs(2));
        }
    });

    let server = Server::http(addr).map_err(|e| {
        io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("failed to bind HTTP server to {}: {}", addr, e),
        )
    })?;

    println!("abtop HTTP server listening on http://{}", addr);
    println!("endpoints: GET /status  GET /health  GET /events");

    for request in server.incoming_requests() {
        // SSE is handled separately because its response body type differs from
        // the simple in-memory responses used by the other endpoints.
        if request.url() == "/events" {
            let (tx, rx) = mpsc::channel::<String>();
            subscribers.lock().unwrap().push(tx);
            if let Err(e) = request.respond(sse_response(SseBody::new(rx))) {
                eprintln!("abtop http: failed to respond: {}", e);
            }
            continue;
        }

        let response = match request.url() {
            "/status" => {
                let st = state.lock().unwrap();
                if st.json.is_empty() {
                    text_response(
                        StatusCode(503),
                        "snapshot not ready yet".as_bytes().to_vec(),
                    )
                } else {
                    json_response(st.json.clone())
                }
            }
            "/health" | "/" => {
                let st = state.lock().unwrap();
                // Minimal health payload derived from the cached snapshot.
                let (session_count, sessions) = if st.json.is_empty() {
                    (0, Vec::new())
                } else {
                    parse_minimal_sessions(&st.json)
                };

                let payload = serde_json::json!({
                    "running": true,
                    "snapshot_ready": !st.json.is_empty(),
                    "updated_at_ms": st.updated_at_ms,
                    "session_count": session_count,
                    "sessions": sessions,
                    "error": st.last_error,
                });
                json_response(payload.to_string())
            }
            _ => text_response(StatusCode(404), "not found".as_bytes().to_vec()),
        };

        if let Err(e) = request.respond(response) {
            eprintln!("abtop http: failed to respond: {}", e);
        }
    }

    Ok(())
}

/// Extract just enough session info for `/health` without re-parsing the whole
/// snapshot into typed structs (keeps the endpoint cheap and self-contained).
fn parse_minimal_sessions(json: &str) -> (usize, Vec<serde_json::Value>) {
    let value: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return (0, Vec::new()),
    };

    let sessions = value.get("sessions").and_then(|s| s.as_array());
    let list = match sessions {
        Some(arr) => arr
            .iter()
            .map(|s| {
                serde_json::json!({
                    "pid": s.get("pid").cloned().unwrap_or(serde_json::Value::Null),
                    "status": s.get("status").cloned().unwrap_or(serde_json::Value::Null),
                    "project_name": s.get("project_name").cloned().unwrap_or(serde_json::Value::Null),
                    "agent_cli": s.get("agent_cli").cloned().unwrap_or(serde_json::Value::Null),
                })
            })
            .collect(),
        None => Vec::new(),
    };
    (list.len(), list)
}

/// SSE response body backed by a channel. Blocks until the collector broadcasts
/// the next snapshot, then emits it as an `data: ...` event.
struct SseBody {
    rx: mpsc::Receiver<String>,
    buf: Vec<u8>,
    pos: usize,
}

impl SseBody {
    fn new(rx: mpsc::Receiver<String>) -> Self {
        Self {
            rx,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for SseBody {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.buf.len() {
            match self.rx.recv() {
                Ok(json) => {
                    self.buf = format!("data: {}\n\n", json).into_bytes();
                    self.pos = 0;
                }
                Err(_) => return Ok(0), // Receiver dropped; end the stream.
            }
        }
        let remaining = &self.buf[self.pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        self.pos += n;
        Ok(n)
    }
}
