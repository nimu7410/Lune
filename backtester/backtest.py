"""Backtests and tuning of the HH/LL engine on Lune's ticks.

Every result is a replay of `engine.Book` over whole finished sessions, one (index, day) at a
time, each from a fresh book exactly as Lune starts each session. Sessions are independent:
nothing of one day reaches the next, so a day's trades are the same whichever days are run.

Tuning (`sweep`) replays every combination of up to two settings on every day. `walk_forward`
then picks, for each day, the combination that made the most on the days BEFORE it only, and
books what that pick made on the day itself: an out-of-sample result. The in-sample table
(best total over all days) is shown beside it and is labelled as hindsight.
"""

from __future__ import annotations

import itertools
import math
import os
import re
import threading
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, fields, replace
from datetime import date
from multiprocessing import get_context
from typing import Callable, Optional

import engine
import ticks
from engine import SPECS, Book, Params

NAMES = list(SPECS)
PIVOT_KEYS = {"R": 1, "threshold": 2, "min_leg": 3}     # position in an index_pivots row
GLOBAL_KEYS = ("chop_k", "leg_k", "lock_at", "lock_trail", "stop", "target", "exit_on", "exit_wide",
               "max_hold_s", "last_entry", "order", "limit_wait", "needle", "needle_arm", "needle_give",
               "needle_wait_s")
TUNE_KEYS = tuple(PIVOT_KEYS) + GLOBAL_KEYS
MAX_COMBOS = 400
#: Big legs, the backtester's own default: each index's threshold, x cost, with no minimum leg (the
#: one-threshold zigzag). About a quarter of the index's median day range (sessions of Nov 2026).
BIG_LEG = {"NIFTY": 46.0, "BANKNIFTY": 48.0, "SENSEX": 48.0, "FINNIFTY": 42.0, "MIDCPNIFTY": 30.0, "NIFTYNXT50": 30.0}
#: Fast confirmation: (threshold, min leg) x cost, a quarter of the big leg and the big leg itself.
FAST_LEG = {n: (round(k / 4, 1), k) for n, k in BIG_LEG.items()}
#: Scaled legs, the backtester's default: one rule for every index in units of its own BIG_LEG
#: (volatility-scaled): threshold 0.4x, min leg 4x (about a day range), with the leg filter at 2x R.
SCALED_LEG = {n: (round(0.4 * k, 1), round(4 * k, 1)) for n, k in BIG_LEG.items()}
SCALED_LEG_K = 2.0


# ── settings in and out ──────────────────────────────────────────────────────────
def _num(v, key: str, lo: float, hi: float, integer: bool = False) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"{key}: not a number") from None
    if not math.isfinite(x) or not lo <= x <= hi:
        raise ValueError(f"{key}: {v} is outside {lo:g}..{hi:g}")
    if integer:
        if x != int(x):
            raise ValueError(f"{key}: must be a whole number")
        return int(x)
    return x


_RULES = {   # key -> (low, high) for numbers, or the allowed values
    "thr_cost": (0.05, 1000), "fast_cost": (0, 1000), "leg_cost": (0, 5000),
    "chop_k": (0, 50), "leg_k": (0, 50), "lock_at": (0, 50), "lock_trail": (0.01, 50), "stop": (0, 50),
    "target": (0, 50), "exit_wide": (0, 50), "max_hold_s": (0, 6 * 3600), "limit_wait": (0.1, 3600),
    "latency_ms": (0, 60_000), "lots": (1, 50), "needle_arm": (0, 50), "needle_give": (0.01, 1),
    "needle_wait_s": (0, 6 * 3600), "needle": ("off", "giveback", "entry", "stall"),
    "order": ("market", "limit"), "exit_on": ("pivot", "signal"), "fill": ("next", "same"),
}


def _hhmm(v, key: str) -> str:
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", str(v).strip())
    if not m or not (engine.OPEN_S <= int(m[1]) * 3600 + int(m[2]) * 60 <= engine.LAST_ENTRY_S) or int(m[2]) > 59:
        raise ValueError(f"{key}: {v} is not a time between 09:15 and 15:00")
    return f"{int(m[1]):02d}:{m[2]}"


def check(key: str, v):
    """One setting from outside, validated and typed."""
    if key == "last_entry":
        return _hhmm(v, key)
    rule = _RULES[key]
    if isinstance(rule[0], str):
        if v not in rule:
            raise ValueError(f"{key}: {v!r} is not one of {', '.join(rule)}")
        return v
    return _num(v, key, *rule, integer=key == "lots")


def params_from(d: dict) -> Params:
    """Settings as the UI sends them: the trade rules plus `pivots`, {index: [R, threshold, min leg]}."""
    kw = {}
    for f in fields(Params):
        if f.name in d and f.name not in ("index_pivots", "thr_cost", "fast_cost", "leg_cost"):
            kw[f.name] = check(f.name, d[f.name])
    piv = d.get("pivots") or {}
    rows = []
    for n in NAMES:
        r = piv.get(n)
        if r is None or len(r) != 3:
            raise ValueError(f"{n}: R, threshold and min leg are all needed")
        rows.append((n, check("thr_cost", r[0]), check("fast_cost", r[1]), check("leg_cost", r[2])))
    return Params(index_pivots=tuple(rows), **kw)


def params_to(p: Params) -> dict:
    d = {k: v for k, v in asdict(p).items() if k not in ("index_pivots", "thr_cost", "fast_cost", "leg_cost")}
    d["pivots"] = {n: [*pivots_of(p, n)] for n in NAMES}
    return d


def pivots_of(p: Params, name: str) -> tuple:
    """(R, threshold, min leg) of an index, x cost, as the engine reads them."""
    return next((tuple(x[1:4]) for x in p.index_pivots if x[0] == name), (p.thr_cost, p.fast_cost, p.leg_cost))


def live() -> Params:
    """Lune's settings in force at the last sync, every index's pivots spelled out."""
    d = ticks.live_params()
    p = Params(**{k: v for k, v in d.items() if k in {f.name for f in fields(Params)}}) if d else Params()
    return replace(p, index_pivots=tuple((n, *pivots_of(p, n)) for n in NAMES))


def big() -> Params:
    """Lune's trade rules on big legs: every leg is at least the index's `BIG_LEG`, and a pivot is
    confirmed on the first tick that far back from its extreme. No rule can confirm sooner and keep
    every leg that long: a pivot confirmed c from its extreme can be passed again from there, and
    the answer to that is a leg of c, a pivot redrawn, or none at all (see README). Nothing is forced."""
    return _with_pivots({n: (k, 0.0) for n, k in BIG_LEG.items()})


def fast() -> Params:
    """Lune's trade rules on a short threshold (`FAST_LEG`: pivots print about a minute after their
    extreme) with the big leg as the minimum leg. A pivot price breaks before the next leg is that
    long forces the next one there and then, on a short leg (about half of them on Nov 2026's
    sessions): the only way left to keep both the print and the chain unchanged and moving."""
    return _with_pivots(FAST_LEG)


def scaled() -> Params:
    """The default: `SCALED_LEG` pivots and the leg filter on Lune's default trade rules, pinned here
    (not read from the last sync) so the default never drifts. Why this one (README, "Scaled legs"):
    tick data has memory only at short thresholds; one dimensionless rule for all six indices, chosen as
    the best neighbourhood of a grid (not its best cell), holds its drawdown when nudged 0.8-1.25x."""
    return Params(leg_k=SCALED_LEG_K, index_pivots=tuple((n, 10.0, *SCALED_LEG[n]) for n in NAMES))


def _with_pivots(rows: dict) -> Params:
    p = live()
    return replace(p, index_pivots=tuple((n, pivots_of(p, n)[0], *rows[n]) for n in NAMES))


def with_value(p: Params, key: str, v, name: Optional[str]) -> Params:
    """`p` with one tunable setting changed: a pivot key changes `name`'s row only."""
    if key in PIVOT_KEYS:
        i = PIVOT_KEYS[key]
        v = check(("thr_cost", "fast_cost", "leg_cost")[i - 1], v)
        return replace(p, index_pivots=tuple((*x[:i], v, *x[i + 1:]) if x[0] == name else x for x in p.index_pivots))
    return replace(p, **{key: check(key, v)})


# ── replay ───────────────────────────────────────────────────────────────────────
def replay(name: str, p: Params, day: date, detail: bool = False) -> dict:
    """One index, one finished session, from a fresh book: its trades (and pivots if `detail`)."""
    ms, px = ticks.load(day, name)
    b = Book(SPECS[name], p)
    for m, x in zip(ms, px):
        b.on_tick(x, m)
    b.finish("data_end")
    out = {"trades": b.trades, "blocked": dict(b.blocked), "R": b.R, "threshold": b.zz.R, "leg": b.zz.leg,
           "cost": round(b.cost, 4), "ticks": len(ms)}
    if detail:
        out["pivots"] = [{"kind": v.kind, "price": v.price, "ms": v.ms, "conf_price": v.conf_price,
                          "conf_ms": v.conf_ms, "leg": v.leg, "forced": v.forced} for v in b.pivots]
        out["needles"] = list(b.needles)
    return out


def _day_rows(name: str, day: date, combos: list) -> list:
    """Worker: per combination, (net Rs, trades, wins, gross Rs, charges Rs) of one (index, day)."""
    out = []
    for p in combos:
        tr = replay(name, p, day)["trades"]
        out.append((round(sum(t["net_rs"] for t in tr), 2), len(tr), sum(t["net_rs"] > 0 for t in tr),
                    round(sum(t["gross_pts"] * t["units"] for t in tr), 2), round(sum(t["charges_rs"] for t in tr), 2)))
    return out


def _day_trades(name: str, day: date, p: Params) -> list:
    return replay(name, p, day)["trades"]


_POOL: Optional[ProcessPoolExecutor] = None
_POOL_LOCK = threading.Lock()


def pool() -> ProcessPoolExecutor:
    global _POOL
    with _POOL_LOCK:
        if _POOL is None:
            _POOL = ProcessPoolExecutor(max_workers=max(1, os.cpu_count() or 1), mp_context=get_context("spawn"))
        return _POOL


def run(p: Params, days: list[date], names: list[str]) -> list[dict]:
    """Every closed trade of `p` on `names` x `days`, in entry order."""
    jobs = [(n, d) for d in days for n in names if n in ticks.names_on(d)]
    futs = [pool().submit(_day_trades, n, d, p) for n, d in jobs]
    trades = [t for f in futs for t in f.result()]
    return sorted(trades, key=lambda t: (t["entry_ms"], t["index"]))


# ── statistics ───────────────────────────────────────────────────────────────────
def stats(trades: list[dict], days: list[date]) -> dict:
    net = [t["net_rs"] for t in trades]
    won = sum(x for x in net if x > 0)
    lost = -sum(x for x in net if x < 0)
    eq = peak = dd = 0.0
    for t in sorted(trades, key=lambda t: (t["exit_ms"], t["index"])):
        eq += t["net_rs"]
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    by_day = {d.isoformat(): [0, 0.0] for d in days}
    by_index: dict = {}
    by_reason: dict = {}
    for t in trades:
        for agg, k in ((by_day, t["day"]), (by_index, t["index"]), (by_reason, t["reason"])):
            a = agg.setdefault(k, [0, 0.0])
            a[0] += 1
            a[1] += t["net_rs"]
    keys = [d.isoformat() for d in days]
    h = len(keys) // 2
    return {
        "trades": len(trades), "wins": sum(x > 0 for x in net), "net": round(sum(net), 2),
        "gross": round(sum(t["gross_pts"] * t["units"] for t in trades), 2),
        "charges": round(sum(t["charges_rs"] for t in trades), 2),
        "friction": round(sum(t["friction_pts"] * t["units"] for t in trades), 2),
        "pf": round(won / lost, 3) if lost else None, "max_dd": round(dd, 2),
        "avg": round(sum(net) / len(net), 2) if net else None,
        "h1": round(sum(by_day[k][1] for k in keys[:h]), 2), "h2": round(sum(by_day[k][1] for k in keys[h:]), 2),
        "by_day": [{"day": k, "trades": v[0], "net": round(v[1], 2)} for k, v in sorted(by_day.items())],
        "by_index": [{"index": k, "trades": v[0], "net": round(v[1], 2)} for k, v in sorted(by_index.items())],
        "by_reason": [{"reason": k, "trades": v[0], "net": round(v[1], 2)} for k, v in sorted(by_reason.items())],
    }


# ── tuning ───────────────────────────────────────────────────────────────────────
_SWEEP_CACHE: dict = {}      # (index, settings, day) -> day row; finished days never change
_CACHE_LOCK = threading.Lock()


def combos_of(base: Params, axes: list[tuple[str, list]], name: Optional[str]) -> list[tuple[tuple, Params]]:
    """Every combination of the axes' values on top of `base`, as (values, settings)."""
    if not 1 <= len(axes) <= 2:
        raise ValueError("sweep one or two settings")
    if len({k for k, _ in axes}) != len(axes):
        raise ValueError("the two settings must differ")
    for k, vals in axes:
        if k not in TUNE_KEYS:
            raise ValueError(f"{k} cannot be tuned")
        if k in PIVOT_KEYS and name is None:
            raise ValueError(f"{k} is per index: pick one index to tune it")
        if not vals:
            raise ValueError(f"{k}: no values")
    out = []
    for vals in itertools.product(*[v for _, v in axes]):
        p = base
        for (k, _), v in zip(axes, vals):
            p = with_value(p, k, v, name)
        out.append((tuple(vals), p))
    if len(out) > MAX_COMBOS:
        raise ValueError(f"{len(out)} combinations, at most {MAX_COMBOS}")
    return out


def sweep(base: Params, axes: list[tuple[str, list]], name: Optional[str], days: list[date],
          progress: Callable[[float], None] = lambda f: None) -> dict:
    """Every combination on every day of `name` (None: every index). The settings in force
    (`base`) run beside them as the yardstick. Rows hold per-day nets for `walk_forward`."""
    combos = combos_of(base, axes, name)
    allp = list(dict.fromkeys([base] + [p for _, p in combos]))
    names = [name] if name else NAMES
    cells = [(n, d) for d in days for n in names if n in ticks.names_on(d)]
    have = set(cells)
    todo = {}
    for n, d in cells:
        with _CACHE_LOCK:
            need = [p for p in allp if (n, p, d) not in _SWEEP_CACHE]
        if need:
            todo[(n, d)] = need
    size = {c: ticks.count(*c) * len(v) for c, v in todo.items()}
    total, done = sum(size.values()) or 1, 0
    futs = {pool().submit(_day_rows, n, d, need): (n, d) for (n, d), need in todo.items()}
    for f in as_completed(futs):
        n, d = futs[f]
        rows = f.result()
        with _CACHE_LOCK:
            for p, r in zip(todo[(n, d)], rows):
                _SWEEP_CACHE[(n, p, d)] = r
        done += size[(n, d)]
        progress(done / total)

    def per_day(p: Params) -> list:
        out = []
        for d in days:
            rs = [_SWEEP_CACHE[(n, p, d)] for n in names if (n, d) in have]
            out.append([round(sum(r[0] for r in rs), 2), sum(r[1] for r in rs), sum(r[2] for r in rs),
                        round(sum(r[3] for r in rs), 2), round(sum(r[4] for r in rs), 2)])
        return out

    h = len(days) // 2
    def row(vals, p):
        pd = per_day(p)
        return {"values": list(vals), "daily": [x[0] for x in pd], "net": round(sum(x[0] for x in pd), 2),
                "trades": sum(x[1] for x in pd), "wins": sum(x[2] for x in pd),
                "gross": round(sum(x[3] for x in pd), 2), "charges": round(sum(x[4] for x in pd), 2),
                "h1": round(sum(x[0] for x in pd[:h]), 2), "h2": round(sum(x[0] for x in pd[h:]), 2),
                "up_days": sum(x[0] > 0 for x in pd), "is_base": p == base}

    return {"index": name, "axes": [k for k, _ in axes], "days": [d.isoformat() for d in days],
            "rows": [row(v, p) for v, p in combos], "base": row((), base)}


def walk_forward(sw: dict, train: int = 0, min_train: int = 1) -> dict:
    """Out of sample: each day d traded on the combination with the best net over the `train`
    days before d (0: every day before d, from the `min_train`-th day), none of them d or later.
    With a window, a day is tested only once a whole window precedes it. Ties go to the earlier row."""
    rows, days, base = sw["rows"], sw["days"], sw["base"]
    out, oos, yard = [], 0.0, 0.0
    for k in range(len(days)):
        lo = 0 if train <= 0 else max(0, k - train)
        if k - lo < (train if train > 0 else max(1, min_train)):
            continue
        pick = max(range(len(rows)), key=lambda c: sum(rows[c]["daily"][lo:k]))
        got = rows[pick]["daily"][k]
        oos += got
        yard += base["daily"][k]
        out.append({"day": days[k], "train": days[lo:k], "pick": rows[pick]["values"],
                    "train_net": round(sum(rows[pick]["daily"][lo:k]), 2), "net": got, "base": base["daily"][k]})
    return {"rows": out, "net": round(oos, 2), "base": round(yard, 2), "train": train, "test_days": len(out)}
