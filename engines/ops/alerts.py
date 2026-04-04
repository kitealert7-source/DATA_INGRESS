"""
alerts.py — Observer-only Telegram alerting for DATA_INGRESS pipeline.

Public API:
    send_alert(event_type: str, message: str) -> None

Rules:
    - Never raises. All failures are silent.
    - Non-blocking: urllib with 2s timeout.
    - Rate limit: 1 message per event_type per 60s (in-memory).
    - NO-OP if TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in environment.

CLI usage (from PowerShell or bat):
    python alerts.py <EVENT_TYPE> <message>
"""

import os
import sys
import time
import urllib.request
import urllib.parse

_BOT_TOKEN: str | None = os.environ.get("TELEGRAM_BOT_TOKEN")
_CHAT_ID:   str | None = os.environ.get("TELEGRAM_CHAT_ID")
_ENABLED:   bool       = bool(_BOT_TOKEN and _CHAT_ID)

_RATE_LIMIT_S: int = 60
_TIMEOUT_S:    int = 2
_last_sent:    dict[str, float] = {}


def send_alert(event_type: str, message: str) -> None:
    """Send a Telegram alert. Silent no-op on any failure."""
    if not _ENABLED:
        return
    try:
        now = time.monotonic()
        if now - _last_sent.get(event_type, 0.0) < _RATE_LIMIT_S:
            return
        _last_sent[event_type] = now
        _send_telegram(f"[DATA_INGRESS][{event_type}] {message}")
    except Exception:
        pass


def _send_telegram(text: str) -> None:
    url  = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": _CHAT_ID, "text": text}).encode()
    req  = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=_TIMEOUT_S):
        pass


if __name__ == "__main__":
    # CLI: python alerts.py EVENT_TYPE message words here
    if len(sys.argv) < 3:
        print("Usage: python alerts.py <EVENT_TYPE> <message>")
        sys.exit(1)
    event  = sys.argv[1]
    msg    = " ".join(sys.argv[2:])
    send_alert(event, msg)
