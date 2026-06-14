#!/usr/bin/env python3
"""Minimal SSE client for abtop session-status events.

Connects to http://127.0.0.1:8787/events by default, reconnects on
disconnection, and prints each event with a local timestamp.

Usage:
    ./scripts/abtop-sse-client.py
    ./scripts/abtop-sse-client.py http://127.0.0.1:8787/events
    ABTOP_SSE_URL=http://127.0.0.1:8787/events ./scripts/abtop-sse-client.py
"""

import http.client
import json
import os
import sys
import time
import urllib.parse

DEFAULT_URL = "http://127.0.0.1:8787/events"


def _timestamp() -> str:
    return time.strftime("%H:%M:%S")


def _connect(url: str):
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "http":
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80)
    elif parsed.scheme == "https":
        conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443)
    else:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme}")

    path = parsed.path or "/events"
    if parsed.query:
        path += "?" + parsed.query

    conn.request("GET", path, headers={"Accept": "text/event-stream"})
    return conn.getresponse()


def _stream_events(url: str):
    retry_delay = 1.0
    while True:
        try:
            resp = _connect(url)
            if resp.status != 200:
                print(
                    f"[{_timestamp()}] HTTP {resp.status} {resp.reason}",
                    file=sys.stderr,
                )
                time.sleep(min(retry_delay, 5.0))
                retry_delay = min(retry_delay * 1.5, 5.0)
                continue

            # Reset backoff on a successful connection.
            retry_delay = 1.0
            buffer = ""
            while True:
                chunk = resp.read(1024)
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="replace")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.rstrip("\r")
                    if line.startswith("data: "):
                        yield line[6:]
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"[{_timestamp()}] connection error: {exc}", file=sys.stderr)
            time.sleep(min(retry_delay, 5.0))
            retry_delay = min(retry_delay * 1.5, 5.0)


def main() -> None:
    url = os.environ.get("ABTOP_SSE_URL", DEFAULT_URL)
    if len(sys.argv) > 1:
        url = sys.argv[1]

    print(f"[{_timestamp()}] connecting to {url} ...", file=sys.stderr)
    for payload in _stream_events(url):
        try:
            data = json.loads(payload)
            pretty = json.dumps(data, ensure_ascii=False, indent=2)
            print(f"[{_timestamp()}]\n{pretty}\n")
        except json.JSONDecodeError:
            print(f"[{_timestamp()}] {payload}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
