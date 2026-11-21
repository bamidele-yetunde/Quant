#!/usr/bin/env python3
"""brain.py - a small WorldQuant BRAIN API client.

Covers what the miner needs: login (including the identity check), simulate, alpha
statistics, recordsets (daily PnL, yearly stats), the official pre-submission check,
submitted alphas, and the dataset / data-field catalogue.

Also a CLI for running hand-written ideas:

    python brain.py ideas/batch1.json            # results go to results/batch1.json

The ideas file is a JSON list. Each item is either an expression string or an object
{"expr": "...", "settings": {...overrides...}, "name": "..."}.
Credentials come from BRAIN_EMAIL / BRAIN_PASSWORD (asked for interactively if missing).
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin

import requests

API = "https://api.worldquantbrain.com"

DEFAULT_SETTINGS = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 0,
    "neutralization": "SUBINDUSTRY",
    "truncation": 0.08,
    "pasteurization": "ON",
    "unitHandling": "VERIFY",
    "nanHandling": "OFF",
    "language": "FASTEXPR",
    "visualization": False,
}


class BrainError(Exception):
    """An API call failed in a way retrying will not fix."""


class NetworkError(BrainError):
    """The connection kept failing (internet outage, BRAIN down)."""


class AuthError(BrainError):
    """Wrong credentials, or login refused."""


class IdentityCheckRequired(BrainError):
    """BRAIN wants a one-off identity check (new login location). `url` must be opened by a human."""

    def __init__(self, url: str):
        super().__init__("BRAIN asks for an identity check")
        self.url = url


def _retry_after(resp: requests.Response) -> float:
    try:
        return float(resp.headers.get("Retry-After", 0) or 0)
    except ValueError:
        return 0.0


class Brain:
    def __init__(self, email: str, password: str, timeout: float = 60.0):
        self.email = email
        self.password = password
        self.timeout = timeout
        self.s = requests.Session()
        self._auth_lock = threading.Lock()
        self._auth_gen = 0  # bumps on each successful login, so threads don't re-login in a stampede

    # ------------------------------------------------------------------ auth
    def login(self) -> None:
        with self._auth_lock:
            self._login_locked()

    def _login_locked(self) -> None:
        try:
            r = self.s.post(API + "/authentication", auth=(self.email, self.password), timeout=self.timeout)
        except requests.RequestException as e:
            raise NetworkError(f"login: {e}") from e
        if r.status_code == 201:
            self._auth_gen += 1
            return
        if r.status_code == 401 and "persona" in r.headers.get("WWW-Authenticate", "").lower():
            raise IdentityCheckRequired(urljoin(r.url, r.headers.get("Location", "")))
        raise AuthError(f"login failed ({r.status_code}): {r.text[:200]}")

    def complete_identity_check(self, url: str) -> None:
        """Call after a human has completed the check at `url`."""
        r = self.s.post(url, timeout=self.timeout)
        if r.status_code != 201:
            raise AuthError(f"identity check not completed ({r.status_code}): {r.text[:200]}")
        self._auth_gen += 1

    # --------------------------------------------------------------- request
    def request(self, method: str, path: str, *, tries: int = 6, **kw) -> requests.Response:
        """HTTP call with re-login on 401, back-off on 429/5xx and on dropped connections."""
        url = path if path.startswith("http") else API + path
        kw.setdefault("timeout", self.timeout)
        net_fail = 0
        relogged = False
        attempt = 0
        while True:
            attempt += 1
            gen = self._auth_gen
            try:
                r = self.s.request(method, url, **kw)
            except requests.RequestException as e:
                net_fail += 1
                if net_fail >= tries:
                    raise NetworkError(f"{method} {path}: {e}") from e
                time.sleep(min(10 * 2 ** (net_fail - 1), 120))
                continue
            if r.status_code == 401 and not relogged:
                relogged = True
                with self._auth_lock:
                    if self._auth_gen == gen:  # nobody else re-logged meanwhile
                        self._login_locked()
                continue
            if r.status_code == 429 or r.status_code >= 500:
                if attempt >= tries * 3:
                    raise BrainError(f"{method} {path}: HTTP {r.status_code} {r.text[:200]}")
                time.sleep(max(_retry_after(r), 15 if r.status_code == 429 else 10))
                continue
            return r

    def get_json(self, path: str, **kw):
        r = self.request("GET", path, **kw)
        if r.status_code >= 400:
            raise BrainError(f"GET {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def wait_json(self, path: str, max_wait: float = 1800.0):
        """GET a resource BRAIN computes asynchronously (it answers with Retry-After until ready)."""
        start = time.time()
        while True:
            r = self.request("GET", path)
            if r.status_code >= 400:
                raise BrainError(f"GET {path}: HTTP {r.status_code} {r.text[:200]}")
            ra = _retry_after(r)
            if ra > 0 or not r.content.strip():
                if time.time() - start > max_wait:
                    raise BrainError(f"GET {path}: still not ready after {max_wait:.0f}s")
                time.sleep(ra if ra > 0 else 3)
                continue
            return r.json()

    # -------------------------------------------------------------- simulate
    def simulate(self, expr: str, settings: dict | None = None, max_wait: float = 3600.0) -> dict:
        """Run one simulation. Returns {"status", "alpha" (id or None), "message", "sim_id"}."""
        full = dict(DEFAULT_SETTINGS)
        full.update(settings or {})
        body = {"type": "REGULAR", "settings": full, "regular": expr}
        while True:
            r = self.request("POST", "/simulations", json=body)
            if r.status_code == 201:
                break
            text = r.text[:300]
            if r.status_code == 429 or "CONCURRENT_SIMULATION_LIMIT" in text.upper():
                time.sleep(max(_retry_after(r), 20))
                continue
            return {"status": "ERROR", "alpha": None, "message": f"HTTP {r.status_code}: {text}", "sim_id": None}
        loc = r.headers.get("Location", "")
        sim_url = urljoin(API + "/", loc)
        start = time.time()
        while True:
            p = self.request("GET", sim_url)
            ra = _retry_after(p)
            if ra > 0:
                if time.time() - start > max_wait:
                    return {"status": "TIMEOUT", "alpha": None, "message": "simulation timed out", "sim_id": loc}
                time.sleep(ra)
                continue
            try:
                j = p.json()
            except ValueError:
                time.sleep(5)
                continue
            status = j.get("status", "")
            if status in ("", "PENDING", "RUNNING", "IN_PROGRESS") and not j.get("alpha"):
                if time.time() - start > max_wait:
                    return {"status": "TIMEOUT", "alpha": None, "message": "simulation timed out", "sim_id": j.get("id")}
                time.sleep(5)
                continue
            return {
                "status": status or "COMPLETE",
                "alpha": j.get("alpha"),
                "message": j.get("message", "") or "",
                "sim_id": j.get("id"),
            }

    # ---------------------------------------------------------------- alphas
    def alpha(self, alpha_id: str) -> dict:
        return self.get_json(f"/alphas/{alpha_id}")

    def recordset(self, alpha_id: str, name: str) -> list[dict]:
        j = self.wait_json(f"/alphas/{alpha_id}/recordsets/{name}")
        cols = [p["name"] for p in j.get("schema", {}).get("properties", [])]
        return [dict(zip(cols, row)) for row in j.get("records", [])]

    def yearly_stats(self, alpha_id: str) -> list[dict]:
        return self.recordset(alpha_id, "yearly-stats")

    def daily_pnl(self, alpha_id: str) -> list[tuple[str, float]]:
        """Daily (not cumulative) PnL as [(date, pnl)]."""
        rows = self.recordset(alpha_id, "pnl")
        out, prev = [], None
        for row in rows:
            d = row.get("date")
            v = row.get("pnl")
            if d is None or v is None:
                continue
            v = float(v)
            if prev is not None:
                out.append((str(d), v - prev))
            prev = v
        return out

    def check(self, alpha_id: str, max_wait: float = 1800.0) -> list[dict]:
        """BRAIN's official pre-submission check ("Check Submission"). Returns the list of checks."""
        start = time.time()
        while True:
            j = self.wait_json(f"/alphas/{alpha_id}/check", max_wait=max_wait)
            checks = (j.get("is") or {}).get("checks", [])
            if any(c.get("result") == "PENDING" for c in checks) and time.time() - start < max_wait:
                time.sleep(10)
                continue
            return checks

    def patch_alpha(self, alpha_id: str, **fields) -> None:
        r = self.request("PATCH", f"/alphas/{alpha_id}", json=fields)
        if r.status_code >= 400:
            raise BrainError(f"PATCH alpha {alpha_id}: HTTP {r.status_code} {r.text[:200]}")

    def user_alphas(self, **params) -> list[dict]:
        out, offset = [], 0
        while True:
            q = dict(params, limit=100, offset=offset)
            j = self.get_json("/users/self/alphas", params=q)
            res = j.get("results", [])
            out.extend(res)
            offset += len(res)
            if not res or offset >= j.get("count", 0):
                return out

    def submitted_alphas(self) -> list[dict]:
        return self.user_alphas(stage="OS", order="-dateSubmitted")

    # ------------------------------------------------------------- catalogue
    def _paged(self, path: str, params: dict) -> list[dict]:
        out, offset = [], 0
        while True:
            j = self.get_json(path, params=dict(params, limit=50, offset=offset))
            res = j.get("results", [])
            out.extend(res)
            offset += len(res)
            if not res or offset >= j.get("count", 0):
                return out
            time.sleep(0.3)

    def datasets(self, region="USA", delay=1, universe="TOP3000") -> list[dict]:
        return self._paged("/data-sets", {"instrumentType": "EQUITY", "region": region,
                                          "delay": delay, "universe": universe})

    def data_fields(self, dataset_id: str, region="USA", delay=1, universe="TOP3000") -> list[dict]:
        return self._paged("/data-fields", {"instrumentType": "EQUITY", "region": region, "delay": delay,
                                            "universe": universe, "dataset.id": dataset_id})


# ---------------------------------------------------------------- helpers
def is_stats(alpha_json: dict) -> dict:
    """Pull the in-sample numbers and check results out of an /alphas/{id} response."""
    s = alpha_json.get("is") or {}
    return {
        "sharpe": s.get("sharpe"),
        "fitness": s.get("fitness"),
        "turnover": s.get("turnover"),
        "returns": s.get("returns"),
        "drawdown": s.get("drawdown"),
        "margin": s.get("margin"),
        "long": s.get("longCount"),
        "short": s.get("shortCount"),
        "checks": {c.get("name"): c.get("result") for c in s.get("checks", []) if c.get("name")},
    }


def failed_checks(checks) -> list[str]:
    """Names of failed checks. Accepts a {name: result} dict or BRAIN's list of check objects."""
    if isinstance(checks, dict):
        return sorted(n for n, r in checks.items() if r == "FAIL")
    return sorted(c.get("name") for c in checks if c.get("result") == "FAIL")


def credentials() -> tuple[str, str]:
    email = os.environ.get("BRAIN_EMAIL", "").strip()
    pw = os.environ.get("BRAIN_PASSWORD", "")
    if (not email or not pw) and sys.stdin.isatty():
        import getpass
        email = email or input("BRAIN email: ").strip()
        pw = pw or getpass.getpass("BRAIN password: ")
    if not email or not pw:
        raise AuthError("set BRAIN_EMAIL and BRAIN_PASSWORD")
    return email, pw


def connect(interactive: bool = True) -> Brain:
    b = Brain(*credentials())
    try:
        b.login()
    except IdentityCheckRequired as e:
        if not (interactive and sys.stdin.isatty()):
            raise
        print(f"BRAIN asks for an identity check. Open this link, complete it, then press Enter:\n{e.url}")
        input()
        b.complete_identity_check(e.url)
    return b


# -------------------------------------------------------------------- CLI
def _run_ideas(path: str) -> None:
    src = Path(path)
    ideas = json.loads(src.read_text(encoding="utf-8"))
    items = [{"expr": i} if isinstance(i, str) else i for i in ideas]
    b = connect()
    print(f"{len(items)} ideas, 3 at a time")

    def one(item):
        settings = item.get("settings", {})
        sim = b.simulate(item["expr"], settings)
        res = {"name": item.get("name", ""), "expr": item["expr"], "settings": settings, **sim}
        if sim["alpha"]:
            res.update(is_stats(b.alpha(sim["alpha"])))
        return res

    results = []
    with ThreadPoolExecutor(3) as pool:
        for res in pool.map(one, items):
            results.append(res)
            if res.get("alpha"):
                fails = ",".join(failed_checks(res.get("checks", {}))) or "none"
                print(f"{res['alpha']}  Sharpe {res.get('sharpe')}  Fitness {res.get('fitness')}  "
                      f"TO {res.get('turnover')}  fails: {fails}  | {res['expr']}")
            else:
                print(f"ERROR {res['status']}: {res['message'][:150]}  | {res['expr']}")
    out = Path("results") / src.name
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"saved {out}")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] not in ("-h", "--help"):
        _run_ideas(sys.argv[1])
    else:
        print(__doc__)
        sys.exit(0 if len(sys.argv) == 2 else 1)
