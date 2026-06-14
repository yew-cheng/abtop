use crate::model::{AgentSession, SessionStatus};
use serde::Serialize;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Per-session status payload pushed to every connected SSE client.
#[derive(Serialize)]
pub struct SessionStatusEvent {
    pub session_id: String,
    pub agent_cli: String,
    pub pid: u32,
    pub status: SessionStatus,
}

/// A minimal HTTP server that exposes a single `/events` SSE endpoint.
///
/// The server runs in a background thread and shuts down automatically when
/// dropped, so it does not block process exit.
pub struct SseServer {
    addr: SocketAddr,
    clients: Arc<Mutex<Vec<SyncSender<String>>>>,
    /// The most recently broadcast payload. Sent to new clients immediately
    /// so they do not have to wait for the next status change.
    last_payload: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
}

impl SseServer {
    /// Start the SSE server.
    ///
    /// Tries `127.0.0.1:8787` first; if that port is unavailable, falls back
    /// to an OS-assigned ephemeral port on the loopback interface.
    pub fn start() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:8787")
            .or_else(|_| TcpListener::bind("127.0.0.1:0"))?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;

        let clients: Arc<Mutex<Vec<SyncSender<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let last_payload: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let shutdown = Arc::new(AtomicBool::new(false));

        let clients_for_thread = Arc::clone(&clients);
        let last_payload_for_thread = Arc::clone(&last_payload);
        let shutdown_for_thread = Arc::clone(&shutdown);

        thread::spawn(move || {
            server_loop(
                listener,
                clients_for_thread,
                last_payload_for_thread,
                shutdown_for_thread,
            );
        });

        Ok(Self {
            addr,
            clients,
            last_payload,
            shutdown,
        })
    }

    /// The address the server is actually listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Serialize the current sessions and broadcast them to all connected
    /// SSE clients. Slow clients get dropped; dead clients are cleaned up.
    pub fn broadcast_sessions(&self, sessions: &[AgentSession]) {
        let events: Vec<SessionStatusEvent> = sessions
            .iter()
            .map(|s| SessionStatusEvent {
                session_id: s.session_id.clone(),
                agent_cli: s.agent_cli.to_string(),
                pid: s.pid,
                status: s.status.clone(),
            })
            .collect();

        if let Ok(json) = serde_json::to_string(&events) {
            self.broadcast(json);
        }
    }

    fn broadcast(&self, payload: String) {
        // Remember the latest payload for clients that connect between broadcasts.
        if let Ok(mut last) = self.last_payload.lock() {
            *last = Some(payload.clone());
        }

        let mut clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        clients.retain(|tx| match tx.try_send(payload.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Disconnected(_)) => false,
        });
    }
}

impl Drop for SseServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Drop all sender handles so blocked client threads wake up and exit.
        let mut clients = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        clients.clear();
    }
}

fn server_loop(
    listener: TcpListener,
    clients: Arc<Mutex<Vec<SyncSender<String>>>>,
    last_payload: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let (tx, rx) = sync_channel(2);
                {
                    let mut clients = clients.lock().unwrap_or_else(|p| p.into_inner());
                    clients.push(tx);
                }
                let last_payload = Arc::clone(&last_payload);
                thread::spawn(move || handle_client(stream, rx, last_payload));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::time::Duration;

    fn make_session(session_id: &str, status: SessionStatus) -> AgentSession {
        AgentSession {
            agent_cli: "claude",
            pid: 1234,
            session_id: session_id.to_string(),
            cwd: "/tmp".to_string(),
            project_name: "test".to_string(),
            started_at: 0,
            status,
            model: "claude-sonnet".to_string(),
            effort: String::new(),
            context_percent: 0.0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read: 0,
            total_cache_create: 0,
            turn_count: 0,
            current_tasks: Vec::new(),
            mem_mb: 0,
            version: String::new(),
            git_branch: String::new(),
            git_added: 0,
            git_modified: 0,
            token_history: Vec::new(),
            context_history: Vec::new(),
            compaction_count: 0,
            context_window: 0,
            subagents: Vec::new(),
            mem_file_count: 0,
            mem_line_count: 0,
            children: Vec::new(),
            initial_prompt: String::new(),
            first_assistant_text: String::new(),
            chat_messages: Vec::new(),
            tool_calls: Vec::new(),
            pending_since_ms: 0,
            thinking_since_ms: 0,
            file_accesses: Vec::new(),
            config_root: String::new(),
        }
    }

    #[test]
    fn test_sse_broadcasts_session_status() {
        let server = SseServer::start().unwrap();

        let mut stream = TcpStream::connect(server.addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line);
        }
        assert!(headers.contains("200 OK"));
        assert!(headers.contains("text/event-stream"));

        server.broadcast_sessions(&[
            make_session("sess-1", SessionStatus::Thinking),
            make_session("sess-2", SessionStatus::Waiting),
        ]);

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("data: "));
        let json = line.strip_prefix("data: ").unwrap().trim();
        assert!(json.contains("sess-1"));
        assert!(json.contains("sess-2"));
        assert!(json.contains("Thinking"));
        assert!(json.contains("Waiting"));
    }

    #[test]
    fn test_new_client_receives_last_payload() {
        let server = SseServer::start().unwrap();

        // Broadcast once without any client connected.
        server.broadcast_sessions(&[make_session("sess-a", SessionStatus::Executing)]);

        // A client connecting afterwards should still receive the latest snapshot
        // before blocking for future updates.
        let mut stream = TcpStream::connect(server.addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line);
        }
        assert!(headers.contains("200 OK"));

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("data: "));
        let json = line.strip_prefix("data: ").unwrap().trim();
        assert!(json.contains("sess-a"));
        assert!(json.contains("Executing"));
    }

    #[test]
    fn test_non_events_path_returns_404() {
        let server = SseServer::start().unwrap();
        let mut stream = TcpStream::connect(server.addr()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(b"GET /foo HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("404 Not Found"));
    }
}

fn handle_client(
    mut stream: TcpStream,
    rx: Receiver<String>,
    last_payload: Arc<Mutex<Option<String>>>,
) {
    // Guard against dead peers hanging the TUI-side server thread.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    let mut buf = [0u8; 1024];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");
    if !first_line.starts_with("GET /events") {
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n");
        return;
    }

    let headers = b"HTTP/1.1 200 OK\r\n\
                     Content-Type: text/event-stream\r\n\
                     Cache-Control: no-cache\r\n\
                     Connection: keep-alive\r\n\
                     \r\n";
    if stream.write_all(headers).is_err() {
        return;
    }

    // Send the latest known snapshot immediately so the client has data right
    // away, even if no status change occurs in the next few seconds.
    if let Ok(last) = last_payload.lock() {
        if let Some(payload) = last.as_ref() {
            let event = format!("data: {}\n\n", payload);
            if stream.write_all(event.as_bytes()).is_err() {
                return;
            }
        }
    }

    if stream.flush().is_err() {
        return;
    }

    while let Ok(payload) = rx.recv() {
        let event = format!("data: {}\n\n", payload);
        if stream.write_all(event.as_bytes()).is_err() {
            break;
        }
        if stream.flush().is_err() {
            break;
        }
    }
}
