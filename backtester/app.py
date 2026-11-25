"""Lune tick backtester: the HH/LL engine replayed on Lune's ticks, in Lune's own UI.

    ../venv/bin/python app.py      http://127.0.0.1:8060  (PORT / HOST to change)
"""

from __future__ import annotations

import gzip
import logging
import os
import threading
import time
import uuid
from datetime import date

from flask import Flask, jsonify, render_template, request, send_from_directory

import backtest as bt
import engine
import ticks

LOG = logging.getLogger("backtester")
app = Flask(__name__)
LUNE_STATIC = engine.LUNE / "static"
TRADE_KEYS = ("index", "day", "side", "entry", "exit", "entry_ms", "exit_ms", "signal_ms", "exit_signal_ms",
              "entry_px", "exit_px", "pivot", "pivot_px", "pivot_lag_s", "reason", "units", "gross_pts",
              "friction_pts", "charges_rs", "net_rs", "net_pts", "mfe_pts", "mae_pts", "lag_pts", "R")


class Refused(ValueError):
    pass


@app.after_request
def _gzip(response):
    """Read from a phone over Tailscale on mobile data: the page's scripts and styles shrink ~4x."""
    if (response.status_code != 200 or "Content-Encoding" in response.headers
            or response.mimetype.startswith("font/")
            or "gzip" not in request.headers.get("Accept-Encoding", "")):
        return response
    response.direct_passthrough = False
    data = response.get_data()
    if len(data) < 1024:
        return response
    response.set_data(gzip.compress(data, 6))
    response.headers["Content-Encoding"] = "gzip"
    response.vary.add("Accept-Encoding")
    return response


@app.errorhandler(ValueError)
def _bad(exc):
    return jsonify(error=str(exc)), 400


@app.errorhandler(KeyError)
def _missing(exc):
    return jsonify(error=f"not found: {exc}"), 404


def _days(body: dict) -> list[date]:
    have = ticks.days()
    lo = date.fromisoformat(body.get("from") or have[0].isoformat())
    hi = date.fromisoformat(body.get("to") or have[-1].isoformat())
    out = [d for d in have if lo <= d <= hi]
    if not out:
        raise ValueError("no sessions in that range")
    return out


def _settings(body: dict):
    """The request's settings; what differs from the default (scaled legs) is logged, so a
    result someone sees in the UI can be traced to the exact settings that made it."""
    p = bt.params_from(body.get("settings") or {})
    d, base = bt.params_to(p), bt.params_to(bt.scaled())
    diff = {k: v for k, v in d.items() if base.get(k) != v}
    LOG.info("%s %s settings vs default: %s", request.path, body.get("index") or "all", diff or "none")
    return p


def _index(v) -> str:
    if v not in bt.NAMES:
        raise ValueError(f"unknown index {v!r}")
    return v


def _meta() -> dict:
    inv = ticks.inventory()
    days = ticks.days()
    cost = {}
    if days:                                  # what one x cost is in points, at the latest session's open
        for n in bt.NAMES:
            if n in ticks.names_on(days[-1]):
                ms, px = ticks.load(days[-1], n)
                cost[n] = round(engine.cost_pts(engine.SPECS[n], px[0], ms[0]), 4)
    return {
        "indices": bt.NAMES, "days": [d.isoformat() for d in days],
        "files": [{k: r[k] for k in ("day", "index", "ticks")} for r in inv],
        "ticks": sum(r["ticks"] for r in inv), "live": bt.params_to(bt.live()),
        "big": bt.params_to(bt.big()), "fast": bt.params_to(bt.fast()), "scaled": bt.params_to(bt.scaled()), "cost_pts": cost,
        "tick_size": {n: s.tick for n, s in engine.SPECS.items()}, "lot": {n: s.lot for n, s in engine.SPECS.items()},
        "tune_keys": list(bt.TUNE_KEYS), "pivot_keys": list(bt.PIVOT_KEYS), "grid": engine.GRID,
        "max_combos": bt.MAX_COMBOS,
    }


@app.get("/")
def index():
    return render_template("index.html", indices=bt.NAMES)


@app.get("/lune/<path:path>")
def lune_static(path):
    """Lune's own stylesheet and fonts, served from Lune itself: one UI, never a stale copy."""
    return send_from_directory(LUNE_STATIC, path)


@app.get("/api/meta")
def meta():
    return jsonify(_meta())


@app.post("/api/sync")
def sync():
    with _JOB_LOCK:
        if _JOB.get("state") == "running":
            raise Refused("a tuning run is in progress")
        got = ticks.sync()
        bt._SWEEP_CACHE.clear()
    return jsonify(sync=got, meta=_meta())


@app.post("/api/run")
def run():
    body = request.get_json(force=True) or {}
    p = _settings(body)
    days = _days(body)
    names = [_index(body["index"])] if body.get("index") else bt.NAMES
    t0 = time.time()
    trades = bt.run(p, days, names)
    return jsonify(stats=bt.stats(trades, days), trades=[{k: t[k] for k in TRADE_KEYS} for t in trades],
                   days=[d.isoformat() for d in days], seconds=round(time.time() - t0, 2))


@app.post("/api/day")
def day():
    """One index's session: every tick, its pivots and its trades under the settings."""
    body = request.get_json(force=True) or {}
    p = _settings(body)
    name, d = _index(body.get("index")), date.fromisoformat(body.get("day") or "")
    r = bt.replay(name, p, d, detail=True)
    ms, px = ticks.load(d, name)
    return jsonify(index=name, day=d.isoformat(), t0=ms[0], dt=[b - a for a, b in zip([ms[0], *ms[:-1]], ms)],
                   p=list(px), pivots=r["pivots"], needles=r["needles"], trades=[{k: t[k] for k in TRADE_KEYS} for t in r["trades"]],
                   R=r["R"], threshold=r["threshold"], leg=r["leg"], cost=r["cost"], blocked=r["blocked"],
                   net=round(sum(t["net_rs"] for t in r["trades"]), 2))


# ── tuning jobs: one at a time, in the background, polled ────────────────────────
_JOB: dict = {}
_JOB_LOCK = threading.Lock()


def _tune(job: dict, base, axes, name, days, train) -> None:
    try:
        sw = bt.sweep(base, axes, name, days, progress=lambda f: job.update(progress=round(f, 4)))
        job.update(state="done", progress=1.0, result={"sweep": sw, "walk": bt.walk_forward(sw, train)},
                   seconds=round(time.time() - job["started"], 1))
    except Exception as exc:  # noqa: BLE001 - reported to the page
        LOG.exception("tuning failed")
        job.update(state="error", error=str(exc))


@app.post("/api/tune")
def tune():
    body = request.get_json(force=True) or {}
    base = bt.params_from(body.get("settings") or {})
    name = _index(body["index"]) if body.get("index") else None
    axes = [(a[0], list(a[1])) for a in body.get("axes") or [] if a and a[0]]
    train = int(bt._num(body.get("train", 0), "train", 0, 1000, integer=True))
    days = _days(body)
    bt.combos_of(base, axes, name)          # refuse bad axes now, not in the background
    with _JOB_LOCK:
        if _JOB.get("state") == "running":
            raise Refused("a tuning run is already in progress")
        _JOB.clear()
        _JOB.update(id=uuid.uuid4().hex, state="running", progress=0.0, started=time.time())
        threading.Thread(target=_tune, args=(_JOB, base, axes, name, days, train), daemon=True).start()
        return jsonify(id=_JOB["id"])


@app.get("/api/job")
def job():
    return jsonify({k: v for k, v in _JOB.items() if k != "started"})


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if not ticks.days():
        LOG.info("no local ticks yet: syncing from Lune's database")
        ticks.sync()
    app.run(host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8060")), threaded=True)
