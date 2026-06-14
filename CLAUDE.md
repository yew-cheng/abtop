# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                          # Build
cargo run                            # Run TUI
cargo run -- --once                  # Print snapshot and exit
cargo run -- --json                  # Print one JSON snapshot and exit
cargo run -- --setup                 # Install StatusLine hook for rate limit collection
cargo run -- --exit-on-jump          # Quit after Enter-jumping to a tmux pane
cargo run -- --theme dracula         # Launch with a specific theme
cargo test                           # Run tests
cargo clippy                         # Lint
cargo clippy -- -D warnings          # Strict lint
```

## Architecture

Rust TUI app (ratatui + crossterm). Read-only from local filesystem + `ps` + `lsof`. No API calls, no auth.

Supports four AI agent CLIs: **Claude Code**, **Codex CLI**, **OpenCode**, and **Kimi Code**.

Full architecture, data sources, session status detection, context window calculation, gotchas, and release process are in **[AGENTS.md](./AGENTS.md)** — read that before making changes.
