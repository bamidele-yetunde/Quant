#!/usr/bin/env python3
"""miner.py - autonomous, restart-proof alpha miner for WorldQuant BRAIN (see README.md).

    python miner.py            # mine (forever locally; MINER_MINUTES in the cloud)
    python miner.py ready      # re-check the ready list: OK / NOW FAILS <check>  (shows formulas: local use)
    python miner.py status     # offline summary of progress
    python miner.py issue      # markdown for the GitHub issue (new winners; IDs + stats only)

Everything the miner learns is written to miner/ as it happens, so a run can stop at
any moment and the next one continues where it left off. Only this process's main
thread writes state; worker threads only talk to BRAIN.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import threading
import time
import traceback
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import brain as B
import notify

# =============================================================== configuration
ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "miner"
NY = ZoneInfo("America/New_York")
CLOUD = os.environ.get("GITHUB_ACTIONS") == "true"

# Gates (section 4 of the README)
MIN_FITNESS = 1.0          # gate 1 (Delay 1)
MIN_FITNESS_D0 = 1.3       # gate 1 (Delay 0 has stricter cutoffs)
MIN_STABILITY = 0.34       # gate 2: share of family variants with Sharpe >= SCREEN_SHARPE
MAX_BAD_YEARS = 1          # gate 3: losing calendar years allowed
MIN_RECENT_SHARPE = 0.5    # gate 3: Sharpe over the last 2 years
MAX_CORRELATION = 0.7      # gate 4: daily-PnL correlation with submitted alphas
SAME_IDEA_CORR = 0.6       # "same idea" / "near-copy" threshold for the Telegram headers

SCREEN_SHARPE = 1.0        # |Sharpe| needed to open a tuning family
GATE_TOP_N = 3             # best variants of a finished family that go through the gates
CONCURRENCY = 3            # BRAIN's simulation concurrency limit
MIN_COVERAGE = 0.30
SPARSE_COVERAGE = 0.75     # below this a field gets ts_backfill
FIELDS_MAX_AGE_DAYS = 7
IDLE_RECHECK_HOURS = 6
STOP_MARGIN_MIN = 6        # stop starting simulations this long before the time limit
LOG_KEEP_LINES = 5000
SUB_UNIVERSES = ["TOP1000", "TOP500", "TOPSP500", "TOP200"]
SIGNAL_CHECKS = {"LOW_SHARPE", "LOW_FITNESS", "LOW_SUB_UNIVERSE_SHARPE", "LOW_2Y_SHARPE", "IS_LADDER_SHARPE"}
PICK_REMINDER_HOUR_NY = 18


def _env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


MINER_MINUTES = int(_env("MINER_MINUTES", "335" if CLOUD else "0"))
MAX_SIMS_PER_DAY = int(_env("MINER_MAX_SIMS_PER_DAY", "5000" if CLOUD else "600"))
TELEGRAM_MODE = _env("MINER_TELEGRAM", "all").lower()
SCOPES = _env("MINER_SCOPES", "USA/1/TOP3000,USA/1/TOP1000,USA/0/TOP3000,USA/1/TOP500")
PRIMARY_SCOPE = "USA/1/TOP3000"

# ------------------------------------------------------------------ templates
# {x} = prepared field expression, {w} = lookback window.
# "windows" are the three lookbacks tried when tuning (the screen uses "w").
TEMPLATES = {
    "revision": {"pass": 1, "w": 66, "windows": [22, 126, 252],
                 "expr": "group_rank(ts_delta({x}, {w})/(abs(ts_delay({x}, {w})) + 0.0001), industry)",
                 "unitfree": "group_rank(ts_delta({x}, {w})/abs(ts_delay({x}, {w})), industry)"},
    "volnorm": {"pass": 1, "w": 66, "windows": [22, 126, 252],
                "expr": "group_rank(ts_delta({x}, {w})/(ts_std_dev({x}, {w}) + 0.0001), subindustry)",
                "unitfree": "group_rank(ts_delta({x}, {w})/ts_std_dev({x}, {w}), subindustry)"},
    "avdiff": {"pass": 1, "w": 66, "windows": [22, 126, 252],
               "expr": "group_neutralize(ts_av_diff({x}, {w}), subindustry)"},
    "detrend": {"pass": 1, "w": 126, "windows": [66, 252, 504],
                "expr": "group_rank(ts_regression({x}, ts_step(1), {w}), industry)",
                "unitfree": "group_rank(ts_regression(ts_zscore({x}, 252), ts_step(1), {w}), industry)"},
    "recency": {"pass": 1, "w": 66, "windows": [22, 126, 252],
                "expr": "group_rank(-ts_arg_max({x}, {w}), industry)"},
    "volgate": {"pass": 1, "w": 66, "windows": [22, 126, 252],
                "expr": "trade_when(volume > ts_mean(volume, 20), group_rank(ts_zscore({x}, {w}), industry), -1)"},
    "zscore": {"pass": 2, "w": 126, "windows": [66, 252, 504],
               "expr": "ts_zscore({x}, {w})"},
    "level": {"pass": 2, "w": None, "windows": [None],
              "expr": "group_rank({x}, subindustry)"},
    "to_cap": {"pass": 2, "w": None, "windows": [None],
               "expr": "group_rank({x}/cap, subindustry)"},
    "tsrank": {"pass": 3, "w": 252, "windows": [66, 126, 504],
               "expr": "group_rank(ts_rank({x}, {w}), industry)"},
}

# category -> ([screen neutralization, alternative], screening decay)
CATEGORY_SETTINGS = {
    "fundamental": (["SUBINDUSTRY", "INDUSTRY"], 2),
    "analyst": (["INDUSTRY", "SUBINDUSTRY"], 2),
    "model": (["INDUSTRY", "SUBINDUSTRY"], 2),
    "option": (["SECTOR", "MARKET"], 4),
    "news": (["INDUSTRY", "SUBINDUSTRY"], 6),
    "socialmedia": (["INDUSTRY", "SUBINDUSTRY"], 6),
    "pv": (["INDUSTRY", "MARKET"], 10),
    "other": (["INDUSTRY", "SUBINDUSTRY"], 4),
}

# corporate-action bookkeeping and static relationship counts: ~1% turnover caps Fitness
TRAP_FIELDS = ("adjfactor", "split", "dividend", "rel_num", "num_rel", "relationship_count",
               "_num_cust", "_num_supp", "_num_comp", "_num_part")


def tune_decays(turnover: float) -> list[int]:
    t = abs(turnover or 0)
    if t > 0.50:
        return [12, 20]
    if t > 0.30:
        return [6, 12]
    if t > 0.125:
        return [2, 6]
    return [0, 3]  # below 12.5% turnover decay cannot raise Fitness; keep returns


def normalize_category(cat_id: str) -> str:
    c = (cat_id or "").lower()
    for key, name in (("fundamental", "fundamental"), ("analyst", "analyst"), ("earnings", "analyst"),
                      ("model", "model"), ("risk", "model"), ("option", "option"), ("news", "news"),
                      ("sentiment", "news"), ("social", "socialmedia"), ("pv", "pv"), ("price", "pv"),
                      ("volume", "pv"), ("universe", "pv")):
        if key in c:
            return name
    return "other"


def category_settings(cat: str) -> tuple[list[str], int]:
    return CATEGORY_SETTINGS.get(cat, CATEGORY_SETTINGS["other"])


def field_expr(f: dict) -> str | None:
    """Turn a catalogue field into an expression usable inside the templates (or None to skip it)."""
    name = f["id"]
    low = name.lower()
    if f.get("type") not in ("MATRIX", "VECTOR"):
        return None
    if any(t in low for t in TRAP_FIELDS):
        return None
    if f.get("type") == "VECTOR":
        summed = any(k in low for k in ("count", "buzz", "num", "cnt", "volume", "number", "total"))
        x = f"vec_sum({name})" if summed else f"vec_avg({name})"
    else:
        x = name
    cat = f.get("category")
    if cat == "option" and any(k in low for k in ("price", "strike", "premium", "_px")):
        x = f"{x}/close"
    if (f.get("coverage") or 1) < SPARSE_COVERAGE:
        days = 504 if cat == "news" else 252 if cat == "fundamental" else 66
        x = f"ts_backfill({x}, {days})"
    return x


def build_expr(template: str, x: str, w, sign: int, unitfree: bool) -> str:
    t = TEMPLATES[template]
    body = (t["unitfree"] if unitfree else t["expr"]).format(x=x, w=w)
    return body if sign > 0 else f"-({body})"


# ===================================================================== utils
_LOG_LOCK = threading.Lock()


def log(msg: str, private: bool = False) -> None:
    """Full line to miner/miner.log. Console too, except private lines (formulas) in the public cloud log."""
    line = f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}Z {msg}"
    with _LOG_LOCK:
        STATE_DIR.mkdir(exist_ok=True)
        with open(STATE_DIR / "miner.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        if not (CLOUD and private):
            print(line, flush=True)


def trim_log() -> None:
    p = STATE_DIR / "miner.log"
    if p.exists():
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
        if len(lines) > LOG_KEEP_LINES:
            p.write_text("".join(lines[-LOG_KEEP_LINES:]), encoding="utf-8")


def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, UnicodeDecodeError):
        log(f"warning: {path.name} is unreadable, starting it fresh")
        return default


def save_json(path: Path, obj) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def now_ny() -> datetime:
    return datetime.now(NY)


def ny_today() -> str:
    return now_ny().strftime("%Y-%m-%d")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_scope(s: str) -> dict:
    region, delay, universe = s.strip().split("/")
    return {"region": region.upper(), "delay": int(delay), "universe": universe.upper()}


def scope_id(sc: dict) -> str:
    return f"{sc['region']}/{sc['delay']}/{sc['universe']}"


def fields_path(sid: str) -> Path:
    if sid == PRIMARY_SCOPE:
        return STATE_DIR / "fields.json"
    return STATE_DIR / ("fields_" + sid.replace("/", "_") + ".json")


def spec_key(expr: str, settings: dict) -> str:
    raw = json.dumps({"e": expr, "s": settings}, sort_keys=True)
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def make_spec(kind: str, expr: str, sid: str, decay: int, neut: str, trunc: float = 0.08, **meta) -> dict:
    sc = parse_scope(sid)
    settings = {"region": sc["region"], "universe": sc["universe"], "delay": sc["delay"],
                "decay": int(decay), "neutralization": neut, "truncation": trunc}
    spec = {"kind": kind, "expr": expr, "scope": sid, "settings": settings}
    spec.update({k: v for k, v in meta.items() if v is not None})
    spec["key"] = spec_key(expr, settings)
    return spec


def pearson(a: dict, b: dict, last: int = 4 * 252) -> float:
    common = sorted(set(a) & set(b))[-last:]
    n = len(common)
    if n < 60:
        return 0.0
    xs = [a[d] for d in common]
    ys = [b[d] for d in common]
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return 0.0
    return sxy / math.sqrt(sxx * syy)


def sharpe_of(daily: list[float]) -> float:
    n = len(daily)
    if n < 20:
        return 0.0
    m = sum(daily) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in daily) / (n - 1))
    return 0.0 if sd == 0 else math.sqrt(252) * m / sd


def pct(v) -> str:
    return "n/a" if v is None else f"{100 * v:.1f}%"


def num(v, nd=2) -> str:
    return "n/a" if v is None else f"{v:.{nd}f}"


def settings_line(s: dict) -> str:
    return (f"{s['region']} · D{s['delay']} · {s['universe']} · decay {s['decay']} · "
            f"{s['neutralization']} · trunc {s['truncation']}")


# ===================================================================== miner
class Miner:
    def __init__(self, connect: bool = True):
        STATE_DIR.mkdir(exist_ok=True)
        self.scopes = [scope_id(parse_scope(s)) for s in SCOPES.split(",") if s.strip()]
        self.tried: dict[str, dict] = {}
        self._load_tried()
        fam = load_json(STATE_DIR / "families.json", {})
        self.fam_active: dict[str, dict] = fam.get("active", {})
        self.fam_closed: dict[str, str] = fam.get("closed", {})
        self.ready: list[dict] = load_json(STATE_DIR / "ready.json", [])
        self.sent: dict[str, dict] = load_json(STATE_DIR / "sent.json", {})
        self.state: dict = load_json(STATE_DIR / "state.json", {})
        self.pick: dict = load_json(STATE_DIR / "pick.json", {})
        self.seeds: list[dict] = load_json(STATE_DIR / "seeds.json", [])
        self.pnl: dict[str, dict] = {k: dict(v) for k, v in load_json(STATE_DIR / "pnl_cache.json", {}).items()}
        self.submitted: dict[str, dict] = {}
        self.submitted_fields: set[str] = set()
        self.catalogs: dict[str, dict[str, dict]] = {}
        self.order: list[str] = []
        self.inflight: set[str] = set()
        self.priority: deque = deque()
        self.fail_counts: dict[str, int] = {}
        self.gen = None
        self.last_hourly = 0.0
        self.last_idle_refresh = 0.0
        self._cat_queue: list[str] = []
        self._cat_done: dict[str, dict] = {}
        self._cat_thread: threading.Thread | None = None
        self.brain: B.Brain | None = None
        if connect:
            self.brain = B.Brain(*B.credentials())

    # ---------------------------------------------------------- persistence
    def _load_tried(self) -> None:
        p = STATE_DIR / "tried.jsonl"
        if not p.exists():
            return
        with open(p, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue  # a line cut off by a hard stop
                self.tried[r["key"]] = r

    def record(self, rec: dict) -> None:
        self.tried[rec["key"]] = rec
        with open(STATE_DIR / "tried.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def save_families(self) -> None:
        save_json(STATE_DIR / "families.json", {"active": self.fam_active, "closed": self.fam_closed})

    def save_ready(self) -> None:
        self.ready.sort(key=lambda w: -(w.get("fitness") or 0))
        save_json(STATE_DIR / "ready.json", self.ready)

    def save_misc(self) -> None:
        save_json(STATE_DIR / "state.json", self.state)
        save_json(STATE_DIR / "pick.json", self.pick)
        save_json(STATE_DIR / "sent.json", self.sent)

    def save_pnl(self) -> None:
        keep = set(self.submitted) | set(self.sent) | {w["id"] for w in self.ready}
        self.pnl = {k: v for k, v in self.pnl.items() if k in keep}
        save_json(STATE_DIR / "pnl_cache.json", {k: sorted(v.items()) for k, v in self.pnl.items()})

    # ---------------------------------------------------------- daily count
    def _roll_day(self) -> None:
        today = ny_today()
        if self.state.get("date") != today:
            hist = self.state.setdefault("history", {})
            if self.state.get("date"):
                hist[self.state["date"]] = {"sims": self.state.get("sims", 0),
                                            "families": self.state.get("families", 0)}
                for d in sorted(hist)[:-30]:
                    hist.pop(d)
            self.state.update({"date": today, "sims": 0, "families": 0})

    def sims_today(self) -> int:
        self._roll_day()
        return self.state.get("sims", 0)

    def count_sim(self) -> None:
        self._roll_day()
        self.state["sims"] = self.state.get("sims", 0) + 1
        save_json(STATE_DIR / "state.json", self.state)

    # ---------------------------------------------------------------- login
    def login(self) -> None:
        try:
            self.brain.login()
        except B.IdentityCheckRequired as e:
            self.identity_check(e)

    def identity_check(self, e: B.IdentityCheckRequired) -> None:
        if not CLOUD and sys.stdin.isatty():
            print(f"BRAIN asks for an identity check. Open this link, complete it, then press Enter:\n{e.url}")
            input()
            self.brain.complete_identity_check(e.url)
            return
        sent = notify.send("🪪 BRAIN asks for an identity check (new login location).\n"
                           f"Open this link, complete the check, then re-run the workflow:\n{e.url}")
        log("BRAIN asks for an identity check. " +
            ("The link was sent to Telegram." if sent else "Set up Telegram to receive the link, "
             "or log in to BRAIN from a browser and re-run."))
        if not CLOUD:
            log(f"identity check link: {e.url}", private=True)
        sys.exit(2)

    # ------------------------------------------------------------ catalogue
    def load_catalog(self, sid: str, force: bool = False) -> dict[str, dict]:
        path = fields_path(sid)
        data = load_json(path, None)
        fresh = data and time.time() - data.get("fetched_ts", 0) < FIELDS_MAX_AGE_DAYS * 86400
        if data and fresh and not force:
            return {f["id"]: f for f in data["fields"]}
        sc = parse_scope(sid)
        log(f"[{sid}] refreshing dataset and field catalogue")
        try:
            datasets = self.brain.datasets(sc["region"], sc["delay"], sc["universe"])
            per_ds: list[list[dict]] = []
            ds_info = []
            for ds in sorted(datasets, key=lambda d: d.get("alphaCount") or 0):
                cat = normalize_category((ds.get("category") or {}).get("id", ""))
                fs = self.brain.data_fields(ds["id"], sc["region"], sc["delay"], sc["universe"])
                rows = []
                for f in fs:
                    if (f.get("coverage") or 0) < MIN_COVERAGE or f.get("type") not in ("MATRIX", "VECTOR"):
                        continue
                    row = {"id": f["id"], "dataset": ds["id"], "category": cat, "type": f.get("type"),
                           "coverage": f.get("coverage"), "alphaCount": f.get("alphaCount") or 0}
                    if field_expr(row) is not None:  # drops trap fields
                        rows.append(row)
                rows.sort(key=lambda f: f["alphaCount"])
                per_ds.append(rows)
                ds_info.append({"id": ds["id"], "name": ds.get("name"), "category": cat, "fields": len(rows)})
            ordered = []  # interleave datasets, least-crowded field first in each
            for i in range(max((len(r) for r in per_ds), default=0)):
                for rows in per_ds:
                    if i < len(rows):
                        ordered.append(rows[i])
        except B.BrainError as e:
            if data:
                log(f"[{sid}] catalogue refresh failed ({e}); using the cached one")
                return {f["id"]: f for f in data["fields"]}
            raise
        save_json(path, {"fetched_ts": time.time(), "fetched": utc_now(), "datasets": ds_info, "fields": ordered})
        log(f"[{sid}] {len(ds_info)} datasets, {len(ordered)} usable fields")
        return {f["id"]: f for f in ordered}

    def load_catalogs(self, force: bool = False) -> None:
        """Use cached catalogues; fetch missing/stale ones in a background thread so mining starts at once.
        (BRAIN's field listing is slow: ~9 minutes per scope.) Only when there's nothing at all to mine
        with is one catalogue fetched before starting."""
        todo = []
        for sid in self.scopes:
            data = load_json(fields_path(sid), None)
            fresh = data and time.time() - data.get("fetched_ts", 0) < FIELDS_MAX_AGE_DAYS * 86400
            if data:
                self.catalogs[sid] = {f["id"]: f for f in data["fields"]}  # stale is fine meanwhile
            if not data or not fresh or force:
                todo.append(sid)
        if todo and not any(self.catalogs.values()):
            sid = todo.pop(0)
            try:
                self.catalogs[sid] = self.load_catalog(sid, force=True)
            except B.BrainError as e:
                log(f"[{sid}] cannot load catalogue: {e}")
        for sid in self.scopes:
            self.catalogs.setdefault(sid, {})
        todo = [s for s in todo if s not in self._cat_queue]
        if todo:
            self._cat_queue.extend(todo)
            if not (self._cat_thread and self._cat_thread.is_alive()):
                self._cat_thread = threading.Thread(target=self._fetch_catalogs, daemon=True)
                self._cat_thread.start()

    def _fetch_catalogs(self) -> None:
        """Background thread: fetch queued catalogues one at a time; results are picked up by poll_catalogs."""
        while self._cat_queue:
            sid = self._cat_queue[0]
            try:
                self._cat_done[sid] = self.load_catalog(sid, force=True)
            except Exception as e:  # keep the cached (or empty) one
                log(f"[{sid}] cannot load catalogue: {e}")
            self._cat_queue.pop(0)

    def poll_catalogs(self) -> None:
        """Main thread: merge catalogues the background thread finished, and widen the search."""
        if not self._cat_done:
            return
        for sid in list(self._cat_done):
            self.catalogs[sid] = self._cat_done.pop(sid)
        self.build_order()
        self.gen = self.candidates()  # restarts the search order; finished work is skipped

    def build_order(self) -> None:
        """Global field order: primary scope's interleaving, then fields only other scopes have.
        Fields near what you've already submitted go last."""
        seen, order = set(), []
        for sid in [PRIMARY_SCOPE] + self.scopes:
            for fid in self.catalogs.get(sid, {}):
                if fid not in seen:
                    seen.add(fid)
                    order.append(fid)
        ds_of = {}
        for cat in self.catalogs.values():
            for fid, f in cat.items():
                ds_of.setdefault(fid, f["dataset"])
        ds_count: dict[str, int] = {}
        for a in self.submitted.values():
            for ds in {ds_of[f] for f in a["fields"] if f in ds_of}:
                ds_count[ds] = ds_count.get(ds, 0) + 1
        crowded = {ds for ds, n in ds_count.items() if n >= 4}

        def bucket(fid):
            if fid in self.submitted_fields:
                return 2
            return 1 if ds_of.get(fid) in crowded else 0

        self.order = sorted(order, key=bucket)  # sort is stable: keeps the interleaving

    # ------------------------------------------------------------ submitted
    def refresh_submitted(self) -> None:
        try:
            alphas = self.brain.submitted_alphas()
        except B.BrainError as e:
            log(f"could not read submitted alphas: {e}")
            return
        known = set().union(*self.catalogs.values()) if self.catalogs else set()
        subs = {}
        for a in alphas:
            code = ((a.get("regular") or {}).get("code")) or ""
            tokens = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", code))
            subs[a["id"]] = {"date": a.get("dateSubmitted"), "fields": sorted(tokens & known) if known else [],
                             "tokens": sorted(tokens)}
        new = [i for i in subs if i not in self.submitted]
        self.submitted = subs
        self.submitted_fields = set().union(*(set(s["fields"]) for s in subs.values())) if subs else set()
        for aid in subs:
            if aid not in self.pnl:
                try:
                    self.pnl[aid] = dict(self.brain.daily_pnl(aid))
                except B.BrainError as e:
                    log(f"PnL of submitted alpha {aid} unavailable: {e}")
        before = len(self.ready)
        self.ready = [w for w in self.ready if w["id"] not in subs]
        if len(self.ready) != before:
            log(f"{before - len(self.ready)} ready alpha(s) were submitted and leave the ready list")
            self.save_ready()
        if new:
            log(f"{len(subs)} submitted alphas ({len(new)} new)")
        self.save_pnl()

    def submitted_today(self) -> bool:
        today = ny_today()
        for s in self.submitted.values():
            d = s.get("date")
            if not d:
                continue
            try:
                if datetime.fromisoformat(d.replace("Z", "+00:00")).astimezone(NY).strftime("%Y-%m-%d") == today:
                    return True
            except ValueError:
                continue
        return False

    # ----------------------------------------------------------------- work
    def template_spec(self, tname: str, f: dict, sid: str, unitfree: bool = False) -> dict | None:
        t = TEMPLATES[tname]
        if unitfree and not t.get("unitfree"):
            return None
        x = field_expr(f)
        if x is None:
            return None
        neuts, decay = category_settings(f["category"])
        expr = build_expr(tname, x, t["w"], 1, unitfree)
        return make_spec("screen", expr, sid, decay, neuts[0], template=tname, field=f["id"],
                         dataset=f["dataset"], category=f["category"], x=x, w=t["w"], sign=1,
                         unitfree=unitfree, idea=f["id"])

    def seed_specs(self):
        for seed in self.seeds:
            cat = seed.get("category", "other")
            neuts, decay = category_settings(cat)
            for sid in self.scopes:
                yield make_spec("seed", seed["expr"], sid, seed.get("decay", decay),
                                seed.get("neutralization", neuts[0]), seed.get("truncation", 0.08),
                                category=cat, idea=seed.get("name") or seed["expr"][:40],
                                fields_used=seed.get("fields"))

    def combo_specs(self) -> list[dict]:
        """Pairs of near misses from different datasets, rank(A) + rank(B)."""
        if PRIMARY_SCOPE not in self.scopes:
            return []
        best: dict[str, dict] = {}
        for r in self.tried.values():
            if r.get("scope") != PRIMARY_SCOPE or r.get("kind") not in ("screen", "variant", "seed"):
                continue
            if r.get("error") or r.get("sharpe") is None or r.get("fitness") is None:
                continue
            sh, fit = abs(r["sharpe"]), abs(r["fitness"])
            if sh < SCREEN_SHARPE or not (0.5 <= fit < 1.0):
                continue
            fails = set(B.failed_checks(r.get("checks", {})))
            if not fails or not fails <= SIGNAL_CHECKS:
                continue
            if r.get("field") and r["field"] in self.submitted_fields:
                continue
            idea = r.get("idea") or r.get("field") or r["key"]
            if idea not in best or fit > abs(best[idea]["fitness"]):
                best[idea] = r
        by_ds: dict[str, list[dict]] = {}
        for r in best.values():
            by_ds.setdefault(r.get("dataset") or "seed", []).append(r)
        pool = []
        for rs in by_ds.values():
            pool.extend(sorted(rs, key=lambda r: -abs(r["fitness"]))[:5])
        pairs = []
        for i, a in enumerate(pool):
            for b in pool[i + 1:]:
                if (a.get("dataset") or "seed") != (b.get("dataset") or "seed"):
                    pairs.append((abs(a["fitness"]) + abs(b["fitness"]), a, b))
        pairs.sort(key=lambda p: -p[0])
        specs = []
        for _, a, b in pairs[:90]:
            ea = a["expr"] if a["sharpe"] > 0 else f"-({a['expr']})"
            eb = b["expr"] if b["sharpe"] > 0 else f"-({b['expr']})"
            s = a["settings"]
            specs.append(make_spec("combo", f"rank({ea}) + rank({eb})", PRIMARY_SCOPE,
                                   max(s["decay"], b["settings"]["decay"]), s["neutralization"],
                                   category=a.get("category"), dataset=a.get("dataset"),
                                   idea=f"combo:{a['key'][:6]}+{b['key'][:6]}",
                                   fields_used=[x for x in (a.get("field"), b.get("field")) if x]))
        return specs

    def candidates(self):
        """Every screen, in search order: seeds, combinations, then templates pass by pass."""
        yield from self.seed_specs()
        yield from self.combo_specs()
        passes = sorted({t["pass"] for t in TEMPLATES.values()})
        for p in passes:
            names = [n for n, t in TEMPLATES.items() if t["pass"] == p]
            for fid in self.order:
                for tname in names:
                    for sid in self.scopes:
                        f = self.catalogs.get(sid, {}).get(fid)
                        if f:
                            spec = self.template_spec(tname, f, sid)
                            if spec:
                                yield spec

    def resolve(self, spec: dict | None) -> dict | None:
        """The spec to run for this candidate, or None if it's done (or running)."""
        if spec is None:
            return None
        k = spec["key"]
        if k in self.inflight:
            return None
        rec = self.tried.get(k)
        if rec is None:
            return spec
        if rec.get("unit_issue") and spec.get("template") and not spec.get("unitfree"):
            f = self.catalogs.get(spec["scope"], {}).get(spec.get("field"))
            if f:
                return self.resolve(self.template_spec(spec["template"], f, spec["scope"], unitfree=True))
        return None

    def next_spec(self) -> dict | None:
        for fam in self.fam_active.values():  # finish promising ideas first
            for s in fam["specs"]:
                if s["key"] not in self.tried and s["key"] not in self.inflight:
                    return s
        while self.priority:
            s = self.resolve(self.priority.popleft())
            if s:
                return s
        if self.gen is None:
            return None
        while True:
            try:
                s = self.resolve(next(self.gen))
            except StopIteration:
                self.gen = None
                return None
            if s:
                return s

    def run_spec(self, spec: dict) -> dict | None:
        """Worker thread: simulate one spec. Returns the record, or None to put it back in the queue."""
        try:
            sim = self.brain.simulate(spec["expr"], spec["settings"])
        except B.NetworkError as e:
            log(f"connection problem, idea goes back in the queue: {e}")
            return None
        rec = dict(spec)
        rec["ts"] = utc_now()
        rec["alpha"] = sim.get("alpha")
        rec["status"] = sim.get("status")
        if not sim.get("alpha"):
            msg = sim.get("message") or sim.get("status") or "error"
            if sim.get("status") == "TIMEOUT":
                return None
            rec["error"] = msg[:300]
            rec["unit_issue"] = "unit" in msg.lower()
            return rec
        try:
            rec.update(B.is_stats(self.brain.alpha(sim["alpha"])))
        except B.NetworkError:
            return None
        if rec.get("checks", {}).get("UNITS") == "FAIL":
            rec["unit_issue"] = True
        return rec

    # -------------------------------------------------------------- results
    def handle(self, spec: dict, rec: dict | None) -> None:
        if rec is None:
            n = self.fail_counts.get(spec["key"], 0) + 1
            self.fail_counts[spec["key"]] = n
            if n >= 3:  # don't loop forever on one idea
                rec = dict(spec, ts=utc_now(), error="gave up after 3 failed attempts")
            else:
                self.priority.append(spec)  # back in the queue
                return
        sid = rec["scope"]
        if rec.get("error"):
            log(f"[{sid}] {rec['kind']} error: {rec['error'][:120]}")
            log(f"    expr: {rec['expr']}", private=True)
        else:
            log(f"[{sid}] {rec['kind']} {rec['alpha']}  Sharpe {num(rec.get('sharpe'))}  "
                f"Fitness {num(rec.get('fitness'))}  TO {pct(rec.get('turnover'))}")
            log(f"    {rec.get('template') or rec.get('idea', '')}: {rec['expr']}  "
                f"(decay {rec['settings']['decay']}, {rec['settings']['neutralization']})", private=True)
        # open a tuning family before recording, so a hard stop cannot lose it
        promising = (rec["kind"] in ("screen", "seed", "combo") and not rec.get("error")
                     and abs(rec.get("sharpe") or 0) >= SCREEN_SHARPE)
        if promising:
            self.open_family(rec)
        self.record(rec)
        if rec.get("unit_issue") and rec["kind"] == "screen" and not rec.get("unitfree"):
            self.priority.append(spec)  # resolve() turns it into the unit-free form
        self.check_families()

    def tuning_variants(self, rec: dict) -> list[dict]:
        sign = 1 if (rec.get("sharpe") or 0) > 0 else -1
        sid = rec["scope"]
        neuts, _ = category_settings(rec.get("category", "other"))
        decays = tune_decays(rec.get("turnover"))
        s0 = rec["settings"]
        meta = {k: rec.get(k) for k in ("template", "field", "dataset", "category", "x", "unitfree", "idea",
                                        "fields_used")}
        specs = []
        if rec.get("template"):
            t = TEMPLATES[rec["template"]]
            uf = bool(rec.get("unitfree"))
            specs.append(make_spec("variant", build_expr(rec["template"], rec["x"], rec.get("w"), sign, uf), sid,
                                   s0["decay"], s0["neutralization"], s0["truncation"], w=rec.get("w"), sign=sign,
                                   **meta))
            for w in t["windows"]:
                expr = build_expr(rec["template"], rec["x"], w, sign, uf)
                for d in decays:
                    for n in neuts:
                        specs.append(make_spec("variant", expr, sid, d, n, w=w, sign=sign, **meta))
        else:
            base = rec["expr"] if sign > 0 else f"-({rec['expr']})"
            specs.append(make_spec("variant", base, sid, s0["decay"], s0["neutralization"], s0["truncation"],
                                   sign=sign, **meta))
            for d in decays:
                for n in neuts:
                    for tr in (0.08, 0.05):
                        specs.append(make_spec("variant", base, sid, d, n, tr, sign=sign, **meta))
        seen, out = set(), []
        for s in specs:
            if s["key"] not in seen:
                seen.add(s["key"])
                out.append(s)
        return out

    def open_family(self, rec: dict, kind: str = "tune", specs: list[dict] | None = None,
                    stability: float | None = None, parent: str | None = None, fid: str | None = None) -> None:
        fid = fid or (rec["key"] if kind == "tune" else f"{kind}-{rec['key']}")
        if fid in self.fam_active or fid in self.fam_closed:
            return
        specs = specs if specs is not None else self.tuning_variants(rec)
        for s in specs:
            s["family"] = fid
        self.fam_active[fid] = {
            "id": fid, "kind": kind, "scope": specs[0]["scope"] if specs else rec["scope"],
            "idea": rec.get("idea") or fid[:8], "origin": rec["key"], "opened": utc_now(),
            "stability": stability, "parent": parent, "specs": specs,
        }
        self._roll_day()
        self.state["families"] = self.state.get("families", 0) + 1
        self.save_families()
        log(f"[{rec['scope']}] promising ({kind}): family {fid[:12]} with {len(specs)} variants")

    def check_families(self) -> None:
        done = [f for f in self.fam_active.values()
                if all(s["key"] in self.tried for s in f["specs"])]
        for fam in done:
            try:
                outcome = self.gate_family(fam)
            except B.IdentityCheckRequired:
                raise
            except B.BrainError as e:
                log(f"family {fam['id'][:12]}: gating interrupted ({e}); will retry")
                continue
            self.fam_active.pop(fam["id"], None)
            self.fam_closed[fam["id"]] = outcome
            self.save_families()

    # ---------------------------------------------------------------- gates
    @staticmethod
    def min_fitness(rec: dict) -> float:
        return MIN_FITNESS_D0 if rec["settings"]["delay"] == 0 else MIN_FITNESS

    def gate1(self, r: dict) -> bool:
        return (not r.get("error") and (r.get("sharpe") or 0) > 0
                and (r.get("fitness") or 0) >= self.min_fitness(r)
                and not B.failed_checks(r.get("checks", {})))

    def gate_family(self, fam: dict) -> str:
        recs = [self.tried[s["key"]] for s in fam["specs"]]
        ok = [r for r in recs if not r.get("error")]
        if fam.get("stability") is not None:
            stab = fam["stability"]
        else:
            stab = sum(1 for r in ok if (r.get("sharpe") or 0) >= SCREEN_SHARPE) / max(len(recs), 1)
        tag = f"family {fam['id'][:12]}"

        if fam["kind"] == "subuni":  # each smaller-universe version stands on its own
            cands = [r for r in ok if self.gate1(r)]
            if not cands:
                return "subuni: gate 1"
            won = self.evaluate(cands[0], fam, stab)
            return "winner" if won else "subuni: rejected"

        cands = sorted((r for r in ok if self.gate1(r)), key=lambda r: -(r.get("fitness") or 0))[:GATE_TOP_N]
        if not cands:
            if ok and fam["kind"] == "tune":
                best = max(ok, key=lambda r: r.get("fitness") or -9)
                if ((best.get("fitness") or 0) >= self.min_fitness(best) and stab >= MIN_STABILITY
                        and B.failed_checks(best.get("checks", {})) == ["CONCENTRATED_WEIGHT"]):
                    self.open_repair(best, fam, stab)
                    return "repairing"
            log(f"{tag}: no variant passes gate 1 (stability {stab:.0%})")
            return "gate 1"
        if stab < MIN_STABILITY:
            log(f"{tag}: rejected by gate 2, stability {stab:.0%} < {MIN_STABILITY:.0%}")
            return "gate 2"
        for c in cands:
            if self.evaluate(c, fam, stab):
                return "winner"
        return "gates 3-5"

    def open_repair(self, best: dict, fam: dict, stab: float) -> None:
        e, s = best["expr"], best["settings"]
        meta = {k: best.get(k) for k in ("category", "dataset", "field", "idea", "template")}
        specs = [make_spec("repair", f"ts_backfill({e}, 5)", best["scope"], s["decay"], s["neutralization"],
                           s["truncation"], **meta),
                 make_spec("repair", e, best["scope"], s["decay"], s["neutralization"], 0.05, **meta),
                 make_spec("repair", f"ts_backfill({e}, 5)", best["scope"], s["decay"], s["neutralization"],
                           0.05, **meta)]
        log(f"family {fam['id'][:12]}: strong but over-concentrated; trying 3 repairs")
        self.open_family(best, kind="repair", specs=specs, stability=stab)

    def pnl_of(self, alpha_id: str) -> dict:
        if alpha_id not in self.pnl:
            self.pnl[alpha_id] = dict(self.brain.daily_pnl(alpha_id))
        return self.pnl[alpha_id]

    def evaluate(self, r: dict, fam: dict, stab: float) -> bool:
        """Gates 3-5 for one candidate; on success it becomes a winner. Returns True if it did."""
        aid = r["alpha"]
        if aid in self.sent or aid in self.submitted:  # same simulation reached through another family
            return False
        # gate 3: year consistency
        years = {}
        for row in self.brain.yearly_stats(aid):
            y, sh = row.get("year"), row.get("sharpe")
            if y is not None and sh is not None:
                years[str(y)] = round(float(sh), 2)
        bad = sum(1 for v in years.values() if v < 0)
        daily = self.pnl_of(aid)
        recent = sharpe_of([daily[d] for d in sorted(daily)[-504:]])
        if bad > MAX_BAD_YEARS:
            log(f"{aid}: rejected by gate 3, {bad} losing years")
            return False
        if recent < MIN_RECENT_SHARPE:
            log(f"{aid}: rejected by gate 3, last-2-years Sharpe {recent:.2f}")
            return False
        # gate 4: correlation with submitted alphas
        corr_sub, near_sub = 0.0, None
        for sid in self.submitted:
            if sid in self.pnl:
                c = pearson(daily, self.pnl[sid])
                if c > corr_sub:
                    corr_sub, near_sub = c, sid
        if corr_sub >= MAX_CORRELATION:
            log(f"{aid}: rejected by gate 4, correlation {corr_sub:.2f} with submitted {near_sub}")
            return False
        # gate 5: BRAIN's official pre-submission check
        checks = self.brain.check(aid)
        fails = B.failed_checks(checks)
        pending = [c.get("name") for c in checks if c.get("result") == "PENDING"]
        if fails or pending:
            log(f"{aid}: rejected by gate 5 (official check): {', '.join(fails + pending)}")
            return False

        # passed everything: how does it relate to what was already sent?
        corr_sent, near_sent = 0.0, None
        for sid in self.sent:
            if sid in self.pnl and sid != aid and sid != fam.get("parent"):
                c = pearson(daily, self.pnl[sid])
                if c > corr_sent:
                    corr_sent, near_sent = c, sid
        add = True
        related = None
        if fam["kind"] == "subuni":
            header = f"📉 Smaller-universe version ({r['settings']['universe']}) of {fam.get('parent')}"
            related = fam.get("parent")
        elif corr_sent >= SAME_IDEA_CORR:
            other = self.sent[near_sent]
            related = near_sent
            if (r.get("fitness") or 0) > (other.get("fitness") or 0):
                header = f"🔁 Better version of {near_sent}"
                self.ready = [w for w in self.ready if w["id"] != near_sent]
            else:
                header = f"🔂 Same idea as {near_sent}"
                add = False
        elif corr_sub >= SAME_IDEA_CORR:
            header = f"⚠️ Near-copy of your submitted {near_sub}"
            related = near_sub
        else:
            header = "🆕 New idea"

        w = {"id": aid, "key": r["key"], "family": fam["id"], "idea": fam.get("idea"), "kind": fam["kind"],
             "scope": r["scope"], "settings": r["settings"], "expr": r["expr"],
             "sharpe": r.get("sharpe"), "fitness": r.get("fitness"), "turnover": r.get("turnover"),
             "returns": r.get("returns"), "drawdown": r.get("drawdown"), "margin": r.get("margin"),
             "stability": round(stab, 3), "years": years, "recent_sharpe": round(recent, 2),
             "max_corr_submitted": round(corr_sub, 3), "max_corr_sent": round(corr_sent, 3),
             "relation": header, "related": related, "found": utc_now(), "issued": False}
        self.sent[aid] = {"fitness": r.get("fitness"), "found": w["found"], "relation": header}
        if add:
            self.ready.append(w)
            self.save_ready()
        else:
            self.archive_same_idea(w)
        self.save_misc()
        self.save_pnl()
        self.label(aid, fam)
        log(f"WINNER {aid} ({header})  Sharpe {num(w['sharpe'])}  Fitness {num(w['fitness'])}  "
            f"TO {pct(w['turnover'])}  stability {stab:.0%}  corr {corr_sub:.2f}")
        distinct = header.startswith(("🆕", "🔁"))
        if TELEGRAM_MODE == "all" or distinct:
            notify.send(self.winner_message(w))
        # every TOP3000 winner is re-tried on smaller universes
        if fam["kind"] != "subuni" and r["settings"]["universe"] == "TOP3000":
            s = r["settings"]
            for u in SUB_UNIVERSES:
                sid = f"{s['region']}/{s['delay']}/{u}"
                spec = make_spec("subuni", r["expr"], sid, s["decay"], s["neutralization"], s["truncation"],
                                 category=r.get("category"), idea=fam.get("idea"))
                self.open_family(r, kind="subuni", specs=[spec], stability=stab, parent=aid,
                                 fid=f"sub{u}-{r['key']}")
        return True

    def archive_same_idea(self, w: dict) -> None:
        p = STATE_DIR / "same_idea.json"
        items = load_json(p, [])
        items.append(w)
        save_json(p, items[-200:])

    def label(self, aid: str, fam: dict) -> None:
        try:
            self.brain.patch_alpha(aid, name=aid, tags=["passing", f"idea-{fam['id'].split('-')[-1][:5]}"],
                                   color="GREEN")
        except B.BrainError as e:
            log(f"could not label {aid} in BRAIN: {e}")

    @staticmethod
    def winner_message(w: dict) -> str:
        years = " · ".join(f"{y} {v:.1f}" for y, v in sorted(w.get("years", {}).items()))
        return (f"{w['relation']}\n"
                f"🟢 New alpha ready: {w['id']}\n"
                f"{settings_line(w['settings'])}\n"
                f"Sharpe {num(w['sharpe'])} · Fitness {num(w['fitness'])} · Turnover {pct(w['turnover'])}\n"
                f"Returns {pct(w['returns'])} · Drawdown {pct(w['drawdown'])} · Stability {w['stability']:.0%}\n"
                f"Sharpe by year: {years}\n"
                f"Last 2 years Sharpe {w['recent_sharpe']:.2f} · max corr with your alphas "
                f"{w['max_corr_submitted']:.2f}")

    # ------------------------------------------------------- hourly / daily
    def hourly(self, force: bool = False) -> None:
        if not force and time.time() - self.last_hourly < 3600:
            return
        self.last_hourly = time.time()
        self.refresh_submitted()
        try:
            self.daily_pick()
        except B.IdentityCheckRequired:
            raise
        except B.BrainError as e:
            log(f"daily pick postponed: {e}")
        self.daily_report()
        self.check_families()
        self.save_misc()

    def daily_pick(self) -> None:
        today = ny_today()
        if self.pick.get("date") != today:
            self.pick = {"date": today}
        if self.submitted_today():
            self.pick["submitted"] = True
            return
        if not self.pick.get("alpha") and time.time() - self.pick.get("last_try", 0) >= 3600 - 60:
            self.pick["last_try"] = time.time()
            scored = []
            for w in self.ready:
                d = self.pnl.get(w["id"]) or self.pnl_of(w["id"])
                c = max((pearson(d, self.pnl[s]) for s in self.submitted if s in self.pnl), default=0.0)
                scored.append((c, -(w.get("fitness") or 0), w))
            scored.sort(key=lambda t: (t[0], t[1]))
            dropped = []
            for c, _, w in scored:
                fails = B.failed_checks(self.brain.check(w["id"]))
                if fails:
                    dropped.append(w["id"])
                    log(f"{w['id']} now fails {', '.join(fails)}; dropped from the ready list")
                    continue
                self.pick.update({"alpha": w["id"], "corr": round(c, 3)})
                notify.send(f"📅 Today's submission ({today}): {w['id']}\n"
                            f"{settings_line(w['settings'])}\n"
                            f"Sharpe {num(w['sharpe'])} · Fitness {num(w['fitness'])} · "
                            f"Turnover {pct(w['turnover'])}\n"
                            f"Max corr with your submitted alphas {c:.2f}. Passes the official check right now.\n"
                            f"BRAIN → Alphas → Unsubmitted → {w['id']} → Submit")
                log(f"today's pick: {w['id']}")
                break
            if dropped:
                self.ready = [w for w in self.ready if w["id"] not in dropped]
                self.save_ready()
            if not self.pick.get("alpha") and self.ready:
                log("no ready alpha passes the official check right now; trying again in an hour")
        if (self.pick.get("alpha") and not self.pick.get("reminded")
                and now_ny().hour >= PICK_REMINDER_HOUR_NY):
            self.pick["reminded"] = True
            notify.send(f"⏰ Reminder: you haven't submitted today. Today's pick is {self.pick['alpha']}. "
                        f"The BRAIN day ends at midnight New York time (5 AM in Nigeria).")
        save_json(STATE_DIR / "pick.json", self.pick)

    def daily_report(self) -> None:
        today = ny_today()
        if self.state.get("report_date") == today:
            return
        self._roll_day()
        hist = self.state.get("history", {})
        yday = hist.get(max(hist)) if hist else None
        screened = sum(1 for r in self.tried.values() if r.get("kind") == "screen")
        msg = (f"⚪ Daily report ({today})\n"
               f"Simulations: {yday['sims'] if yday else 0} yesterday, {self.state.get('sims', 0)} today so far "
               f"({len(self.tried)} in total, {screened} template screens)\n"
               f"Promising ideas yesterday: {yday['families'] if yday else 0}\n"
               f"Alphas waiting to be submitted: {len(self.ready)}\n"
               f"Ideas being tuned: {len(self.fam_active)}\n"
               f"Submitted alphas on your account: {len(self.submitted)}")
        if notify.send(msg) or not notify.configured():
            self.state["report_date"] = today
        save_json(STATE_DIR / "state.json", self.state)

    # ------------------------------------------------------------------ run
    def start_search(self, force_catalog: bool = False) -> None:
        self.load_catalogs(force_catalog)
        self.refresh_submitted()  # needs the catalogue to map fields
        self.build_order()
        self.gen = self.candidates()

    def run(self) -> None:
        start = time.time()
        deadline = start + MINER_MINUTES * 60 if MINER_MINUTES > 0 else None
        trim_log()
        log(f"miner starting: scopes {', '.join(self.scopes)}; cap {MAX_SIMS_PER_DAY}/day; "
            f"{'%d min' % MINER_MINUTES if deadline else 'no time limit'}; {len(self.tried)} simulations on record, "
            f"{len(self.fam_active)} families open, {len(self.ready)} ready")
        self.login()
        self.start_search()
        self.hourly(force=True)
        pool = ThreadPoolExecutor(CONCURRENCY)
        futures: dict = {}
        idle_logged = cap_logged = False
        try:
            while True:
                now = time.time()
                stopping = deadline is not None and now >= deadline - STOP_MARGIN_MIN * 60
                if deadline is not None and now >= deadline:
                    if futures:
                        log(f"time limit: {len(futures)} simulation(s) in flight go back in the queue")
                    break
                if not stopping:
                    self.poll_catalogs()
                    self.hourly()
                capped = self.sims_today() >= MAX_SIMS_PER_DAY
                if capped and not cap_logged:
                    log(f"daily cap of {MAX_SIMS_PER_DAY} simulations reached; waiting for the next New York day")
                    cap_logged = True
                if not capped:
                    cap_logged = False
                while not stopping and not capped and len(futures) < CONCURRENCY:
                    spec = self.next_spec()
                    if spec is None:
                        break
                    idle_logged = False
                    self.inflight.add(spec["key"])
                    self.count_sim()
                    futures[pool.submit(self.run_spec, spec)] = spec
                    capped = self.sims_today() >= MAX_SIMS_PER_DAY
                if not futures:
                    if stopping:
                        break
                    if self.gen is None and not capped and not self.fam_active:
                        if not idle_logged:
                            log(f"search space exhausted; idling, re-checking for new datasets every "
                                f"{IDLE_RECHECK_HOURS} hours")
                            idle_logged = True
                        if time.time() - self.last_idle_refresh > IDLE_RECHECK_HOURS * 3600:
                            self.last_idle_refresh = time.time()
                            self.start_search(force_catalog=self.last_idle_refresh - start > 60)
                            continue
                    time.sleep(60)
                    continue
                done, _ = wait(list(futures), timeout=30, return_when=FIRST_COMPLETED)
                for fut in done:
                    spec = futures.pop(fut)
                    self.inflight.discard(spec["key"])
                    try:
                        rec = fut.result()
                    except B.IdentityCheckRequired as e:
                        self.identity_check(e)
                        rec = None
                    except B.AuthError:
                        raise
                    except Exception as e:  # never let one odd response kill the run
                        log(f"unexpected error on one simulation: {e!r}")
                        log(traceback.format_exc(), private=True)
                        rec = None
                    self.handle(spec, rec)
        except KeyboardInterrupt:
            log("stopped by user; progress is saved")
        finally:
            self.save_misc()
            self.save_families()
            log(f"run finished: {self.state.get('sims', 0)} simulations today, {len(self.ready)} ready, "
                f"{len(self.fam_active)} families open")
            pool.shutdown(wait=False, cancel_futures=True)
        # in-flight simulations are abandoned (they'll be re-run next time); don't wait for them
        sys.stdout.flush()
        os._exit(0)


# ================================================================ commands
def cmd_ready() -> None:
    m = Miner()
    m.login()
    if not m.ready:
        print("ready list is empty")
        return
    for w in m.ready:
        try:
            fails = B.failed_checks(m.brain.check(w["id"]))
        except B.BrainError as e:
            fails = [f"(check unavailable: {e})"]
        status = "OK" if not fails else "NOW FAILS " + ", ".join(fails)
        print(f"{w['id']}  {status}\n  {w['relation']}\n  {settings_line(w['settings'])}\n"
              f"  Sharpe {num(w['sharpe'])}  Fitness {num(w['fitness'])}  TO {pct(w['turnover'])}  "
              f"stability {w['stability']:.0%}  corr {w['max_corr_submitted']:.2f}\n  {w['expr']}\n")


def cmd_status() -> None:
    m = Miner(connect=False)
    kinds: dict[str, int] = {}
    for r in m.tried.values():
        kinds[r.get("kind", "?")] = kinds.get(r.get("kind", "?"), 0) + 1
    outcomes: dict[str, int] = {}
    for o in m.fam_closed.values():
        outcomes[o] = outcomes.get(o, 0) + 1
    print(f"simulations on record: {len(m.tried)}  {kinds}")
    print(f"today ({m.state.get('date')}): {m.state.get('sims', 0)} simulations")
    print(f"families: {len(m.fam_active)} open, {len(m.fam_closed)} closed {outcomes}")
    print(f"ready: {len(m.ready)}   sent so far: {len(m.sent)}   seeds: {len(m.seeds)}")
    for w in m.ready:
        print(f"  {w['id']}  Fitness {num(w['fitness'])}  Sharpe {num(w['sharpe'])}  {w['scope']}  {w['relation']}")


def cmd_issue() -> None:
    m = Miner(connect=False)
    new = [w for w in m.ready if not w.get("issued")]
    if not new:
        return
    rows = ["| Alpha ID | Scope | Sharpe | Fitness | Turnover | Stability | Max corr | Note |",
            "|---|---|---|---|---|---|---|---|"]
    for w in sorted(new, key=lambda w: -(w.get("fitness") or 0)):
        rows.append(f"| `{w['id']}` | {w['scope']} | {num(w['sharpe'])} | {num(w['fitness'])} | "
                    f"{pct(w['turnover'])} | {w['stability']:.0%} | {w['max_corr_submitted']:.2f} | {w['relation']} |")
        w["issued"] = True
    print(f"{len(new)} new alpha(s) passed every gate. {len(m.ready)} in the ready list in total.\n")
    print("\n".join(rows))
    print("\nOpen BRAIN → Alphas → Unsubmitted, find the ID, check it and submit. "
          "Submit one per BRAIN day (midnight to midnight New York time).")
    m.save_ready()


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        try:
            Miner().run()
        except B.AuthError as e:
            log(f"login failed: {e}")
            sys.exit(1)
    elif cmd == "ready":
        cmd_ready()
    elif cmd == "status":
        cmd_status()
    elif cmd == "issue":
        cmd_issue()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
