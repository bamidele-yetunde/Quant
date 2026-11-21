#!/usr/bin/env python3
"""vault.py - keeps the miner's memory private.

    python vault.py newkey   # print a new key -> store it as MINER_STATE_KEY (GitHub secret + your PC)
    python vault.py pack     # miner/  ->  miner_state.enc   (encrypted, safe to commit)
    python vault.py unpack   # miner_state.enc  ->  miner/

The key is read from the MINER_STATE_KEY environment variable, or from a local,
git-ignored file named .miner_key next to this script.
"""
from __future__ import annotations

import io
import os
import sys
import tarfile
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "miner"
VAULT = ROOT / "miner_state.enc"
KEY_FILE = ROOT / ".miner_key"
LOG_KEEP_LINES = 5000


def _key() -> bytes:
    k = os.environ.get("MINER_STATE_KEY", "").strip()
    if not k and KEY_FILE.exists():
        k = KEY_FILE.read_text(encoding="utf-8").strip()
    if not k:
        sys.exit("MINER_STATE_KEY is not set (and no .miner_key file). Create one with: python vault.py newkey")
    return k.encode()


def _trim_log() -> None:
    log = STATE_DIR / "miner.log"
    if log.exists():
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        if len(lines) > LOG_KEEP_LINES:
            log.write_text("".join(lines[-LOG_KEEP_LINES:]), encoding="utf-8")


def pack() -> None:
    if not STATE_DIR.exists():
        sys.exit("nothing to pack: miner/ does not exist")
    f = Fernet(_key())
    _trim_log()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for p in sorted(STATE_DIR.rglob("*")):
            if p.is_file() and not p.name.endswith(".tmp"):
                tar.add(p, arcname=p.relative_to(ROOT).as_posix())
    token = f.encrypt(buf.getvalue())
    tmp = VAULT.with_suffix(".enc.tmp")
    tmp.write_bytes(token)
    os.replace(tmp, VAULT)
    print(f"packed miner/ -> {VAULT.name} ({len(token) // 1024} KB)")


def unpack() -> None:
    if not VAULT.exists():
        STATE_DIR.mkdir(exist_ok=True)
        print("no miner_state.enc yet: starting with an empty miner/")
        return
    try:
        data = Fernet(_key()).decrypt(VAULT.read_bytes())
    except InvalidToken:
        sys.exit("cannot decrypt miner_state.enc: MINER_STATE_KEY is wrong")
    STATE_DIR.mkdir(exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        members = []
        for m in tar.getmembers():
            target = (ROOT / m.name).resolve()
            if not (m.isfile() and str(target).startswith(str(STATE_DIR.resolve()))):
                continue  # only plain files inside miner/
            members.append(m)
        try:
            tar.extractall(ROOT, members=members, filter="data")
        except TypeError:  # Python < 3.12
            tar.extractall(ROOT, members=members)
    print(f"unpacked {VAULT.name} -> miner/ ({len(members)} files)")


def newkey() -> None:
    print(Fernet.generate_key().decode())


if __name__ == "__main__":
    cmds = {"pack": pack, "unpack": unpack, "newkey": newkey}
    if len(sys.argv) != 2 or sys.argv[1] not in cmds:
        print(__doc__)
        sys.exit(1)
    cmds[sys.argv[1]]()
