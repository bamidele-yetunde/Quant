#!/usr/bin/env python3
"""notify.py - Telegram alerts. Alpha IDs and statistics only, never formulas.

Needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID. Without them every call is a silent no-op.

    python notify.py chats            # after messaging your bot: prints your chat ID
    python notify.py test             # sends a test message
    python notify.py failed <run-url> # used by the workflow
    python notify.py stopped <run-url>
    python notify.py send "text"
"""
from __future__ import annotations

import os
import sys

import requests


def _token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def _chat() -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "").strip()


def configured() -> bool:
    return bool(_token() and _chat())


def send(text: str) -> bool:
    """Send a message. Never raises: a broken notifier must not stop the miner."""
    if not configured():
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{_token()}/sendMessage",
            data={"chat_id": _chat(), "text": text[:4000], "disable_web_page_preview": "true"},
            timeout=30,
        )
        return r.ok
    except requests.RequestException:
        return False


def chats() -> None:
    token = _token() or (sys.argv[2] if len(sys.argv) > 2 else "")
    if not token:
        sys.exit("set TELEGRAM_BOT_TOKEN (or pass it: python notify.py chats <token>)")
    r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=30)
    r.raise_for_status()
    seen = {}
    for u in r.json().get("result", []):
        msg = u.get("message") or u.get("channel_post") or u.get("my_chat_member") or {}
        chat = msg.get("chat") or {}
        if chat.get("id") is not None:
            seen[chat["id"]] = chat.get("username") or chat.get("title") or chat.get("first_name") or ""
    if not seen:
        print("No chats yet. Send your bot any message in Telegram, then run this again.")
    for cid, name in seen.items():
        print(f"TELEGRAM_CHAT_ID = {cid}   ({name})")


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    arg = sys.argv[2] if len(sys.argv) > 2 else ""
    if cmd == "chats":
        chats()
    elif cmd == "test":
        print("sent" if send("✅ Quant miner: Telegram notifications work.") else "not sent (check token / chat ID)")
    elif cmd == "failed":
        send(f"🔴 Run failed: an error stopped the miner.\n{arg}")
    elif cmd == "stopped":
        send(f"⏹️ Run stopped from outside (manual restart or GitHub's 6-hour limit). "
             f"Progress is saved and the chain restarts by itself.\n{arg}")
    elif cmd == "send":
        send(arg)
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
