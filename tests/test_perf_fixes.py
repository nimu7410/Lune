"""The performance fixes of 2026-11-15, each against the behaviour it must not change.

  1  TickTrader rebuilds a book for new settings off the websocket thread, to the same book.
  2  ForecastLogger coalesces tick wake-ups.
  3  price/tick streams wait on their own instrument.
  4/5  the HH/LL store batches commits; wait_ticks says whether anything was committed.
  6  evaluate() replays the day in progress incrementally, to the same trades.
  7  report.md lists the newest reviews and is rewritten at most every REPORT_EVERY_S.
  8  the desk's tick stream resumes from its last id and has a compact encoding.
  9  cached pivots / settled trades equal the uncached ones.
  11 the REST fallback quotes only the keys asked for.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import threading
import time
from datetime import date, datetime, time as dt_time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from flask import Flask

import hhll_store
import hhll_tick
import ist
import live_server
import manual_desk
from db_loggers import ForecastLogger
from live_websocket import LiveWebSocket
from manual_desk import ManualDesk, TickTape, TickTrader, _tick_payload

ROOT = Path(__file__).resolve().parent.parent
DAY = date(2026, 11, 3)


def _ticks(n: int, start_hour: int = 10):
    """NIFTY swinging 60 pts every 20 minutes, a tick a second, from `start_hour` IST."""
    t0 = int(datetime(2026, 11, 3, start_hour, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
    ms, px = [], []
    for i in range(n):
        k = i % 1200
        ms.append(t0 + i * 1000)
        px.append(round(25000 + (k if k < 600 else 1200 - k) * 0.1, 2))
    return ms, px


# -- 8: the tick stream's payload ----------------------------------------------------
def _decode(m: dict):
    """Decodes the tick stream's compact payload (`enc=d1`), as a client does."""
    if m.get("enc") != "d1":
        return m["t"], m["p"]
    if "t0" not in m:
        return [], []
    t = [m["t0"]]
    for d in m["dt"]:
        t.append(t[-1] + d)
    if "ps" in m:
        q = [m["q0"]]
        for d in m["dq"]:
            q.append(q[-1] + d)
        return t, [x / m["ps"] for x in q]
    return t, m["p"]


@pytest.mark.parametrize("prices", [
    [25000.05, 25000.1, 24999.95, 25010.0],             # index paise
    [94.5250, 94.5275, 94.5200],                         # finer than paise: sent as they are
    [0.1 + 0.2, 1 / 3, 7.0],                             # no exact scale: sent as they are
    [],
])
def test_compact_payload_decodes_to_the_very_same_ticks(prices) -> None:
    times = [1_794_551_966_982 + 250 * i - (i == 2) * 400 for i in range(len(prices))]   # a clock step back too
    plain = _tick_payload("NIFTY", True, 0, times, prices, False)
    assert (plain["t"], plain["p"], plain["from"]) == (times, prices, 0)
    compact = json.loads(json.dumps(_tick_payload("NIFTY", True, 0, times, prices, True)))
    t, p = _decode(compact)
    assert t == times and p == prices and all(type(x) is float for x in p)


# -- 8 and 3: the stream resumes, per index ------------------------------------------
def _stream_desk():
    desk = ManualDesk.__new__(ManualDesk)
    desk.names, desk.closing = ("NIFTY", "SENSEX"), False
    desk.tape = TickTape({"NIFTY": "NSE|26000", "SENSEX": "BSE|1"})
    app = Flask(__name__)
    desk.register_routes(app)
    return desk, app.test_client()


def _events(res, n):
    """The first `n` SSE events (id, data) of a streamed response, skipping keep-alives."""
    out = []
    for chunk in res.response:
        if b"data:" not in chunk:
            continue
        fields = dict(line.split(b": ", 1) for line in chunk.strip().split(b"\n"))
        out.append((fields.get(b"id", b"").decode(), json.loads(fields[b"data"])))
        if len(out) == n:
            return out
    return out


def test_tick_stream_resumes_from_its_last_id_and_resends_on_a_stale_one() -> None:
    noon = datetime.combine(ist.today(), dt_time(12, 0), tzinfo=ist.IST)
    with mock.patch.object(manual_desk.ist, "now", return_value=noon):
        desk, client = _stream_desk()
        for x in (24000.05, 24000.1, 24000.15):
            desk.tape.on_tick({"e": "NSE", "tk": "26000", "lp": str(x)})
        res = client.get("/manual/tick_stream?index=NIFTY&enc=d1", buffered=False)
        [(last_id, first)] = _events(res, 1)
        res.close()
        assert first["reset"] and _decode(first)[1] == [24000.05, 24000.1, 24000.15]
        assert last_id == f"{desk.tape.epoch}.0.3"
        desk.tape.on_tick({"e": "NSE", "tk": "26000", "lp": "24000.2"})
        desk.tape.on_tick({"e": "BSE", "tk": "1", "lp": "80000"})          # another index: not in this stream
        # a reconnect (Last-Event-ID) and a reopened stream (`from`) both go on from 3
        for url, headers in (("/manual/tick_stream?index=NIFTY&enc=d1", {"Last-Event-ID": last_id}),
                             (f"/manual/tick_stream?index=NIFTY&enc=d1&from={last_id}", {})):
            res = client.get(url, headers=headers, buffered=False)
            [(nid, more)] = _events(res, 1)
            res.close()
            assert (more["reset"], more["from"], _decode(more)[1], nid) == (False, 3, [24000.2], f"{desk.tape.epoch}.0.4")
        # another process's id, a future count, junk: the whole session again
        for stale in (f"other.0.3", f"{desk.tape.epoch}.0.99", f"{desk.tape.epoch}.5.3", "junk"):
            res = client.get("/manual/tick_stream?index=NIFTY", headers={"Last-Event-ID": stale}, buffered=False)
            [(_, again)] = _events(res, 1)
            res.close()
            assert again["reset"] and again["t"] and len(again["p"]) == 4 and "enc" not in again


def test_tape_wakes_only_its_own_index_waiter() -> None:
    noon = datetime.combine(ist.today(), dt_time(12, 0), tzinfo=ist.IST)
    with mock.patch.object(manual_desk.ist, "now", return_value=noon):
        tape = TickTape({"NIFTY": "NSE|26000", "SENSEX": "BSE|1"})
        woken = []

        def wait(name):
            w = tape.waiter(name)
            with w:
                woken.append((name, w.wait(2.0)))

        threads = [threading.Thread(target=wait, args=(n,)) for n in ("NIFTY", "SENSEX")]
        for t in threads:
            t.start()
        time.sleep(0.1)
        tape.on_tick({"e": "BSE", "tk": "1", "lp": "80000"})
        threads[1].join(1.0)
        assert woken == [("SENSEX", True)]                # NIFTY's waiter slept through SENSEX's tick
        tape.on_tick({"e": "NSE", "tk": "26000", "lp": "24000"})
        threads[0].join(1.0)
        assert woken[-1] == ("NIFTY", True)


def test_price_stream_waits_on_its_own_token() -> None:
    prices = {"NIFTY": 100.0, "BANKNIFTY": 500.0}
    listeners = []
    app = live_server.create_app(
        None, "NSE", "1", lambda: None, None, None, pivot_service=object(),
        price_getters={name: (lambda n=name: prices[n]) for name in prices},
        add_price_listener=listeners.append, chart_index="NIFTY",
        price_keys={"NIFTY": "NSE|26000", "BANKNIFTY": "NSE|26009"})
    res = app.test_client().get("/live_price_stream?index=NIFTY", headers={"Host": "localhost:5000"}, buffered=False)
    chunks = res.response
    try:
        assert next(chunks) == b"data: 100.0\n\n"
        prices["NIFTY"] = 100.05
        threading.Timer(0.05, listeners[0], args=({"tk": "26000", "lp": "100.05"},)).start()
        t0 = time.monotonic()
        assert next(chunks) == b"data: 100.05\n\n" and time.monotonic() - t0 < 0.5
    finally:
        res.close()


# -- 1: settings rebuilds off the websocket thread ----------------------------------
class _Store:
    """The slice of hhll_store.Store TickTrader reads: settings, and a pivot sink."""

    def __init__(self, params):
        self.state = {"active": hhll_tick.asdict(params), "active_day": DAY.isoformat(),
                      "next": hhll_tick.asdict(params), "applied_ms": 0}
        self.version = 1
        self.writer = SimpleNamespace(put_pivot=lambda row: None)

    def load_state(self, name="engine"):
        return json.loads(json.dumps(self.state)) if name == "engine" else {}

    def state_version(self):
        return self.version


def test_settings_rebuild_runs_off_the_tick_thread_and_lands_on_the_same_book() -> None:
    ms, px = _ticks(4000)
    fed = {"n": 0}

    def history(name):
        n = fed["n"]
        return ms[:n], px[:n], n - 1

    def since(name, start):
        return ms[start:fed["n"]], px[start:fed["n"]]

    old, new = hhll_tick.Params(), hhll_tick.Params(thr_cost=6)
    store = _Store(old)
    saved = hhll_tick.STORE
    hhll_tick.use_store(store)
    replays = []
    try:
        with mock.patch.object(hhll_tick, "today_ist", lambda: DAY), \
                mock.patch.object(manual_desk, "SETTINGS_POLL_SEC", 0.05):
            tt = TickTrader(["NIFTY"], history, since=since)
            tt.attach(store)
            real = tt._replay
            tt._replay = lambda name, p: replays.append(threading.current_thread().name) or real(name, p)
            tt.start()
            try:
                def tick(i):
                    fed["n"] = i + 1
                    tt.on_tick("NIFTY", px[i], ms[i] / 1000, i)

                for i in range(2000):
                    tick(i)
                store.state.update(active=hhll_tick.asdict(new), next=hhll_tick.asdict(new))
                store.version += 1
                end = time.monotonic() + 10
                i = 2000
                while tt.books["NIFTY"].p != new and time.monotonic() < end:
                    tick(i)                                  # the tape keeps ticking during the rebuild
                    i += 1
                    time.sleep(0.001)
                assert tt.books["NIFTY"].p == new
                assert replays and all(name != threading.current_thread().name for name in replays)
                for j in range(i, 4000):
                    tick(j)
            finally:
                tt.stop()
    finally:
        hhll_tick.use_store(saved)
    ref = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], new)
    for m, x in zip(ms, px):
        ref.on_tick(x, m)
    b = tt.books["NIFTY"]
    assert (b.last_ms, b.R, b.zz.hp, b.zz.lp, b.zz.dir, b.zz.n) == (ref.last_ms, ref.R, ref.zz.hp, ref.zz.lp, ref.zz.dir, ref.zz.n)
    assert b.zz.last == ref.zz.last and tt._side(b) == tt._side(ref)
    assert tt._fed["NIFTY"] == 3999


def test_an_unchanged_index_keeps_its_book_and_a_pending_one_is_not_entered() -> None:
    a = hhll_tick.Params(index_pivots=(("NIFTY", 10, 19.0, 46.0), ("SENSEX", 10, 22.0, 50.0)))
    b = hhll_tick.Params(index_pivots=(("NIFTY", 10, 19.0, 46.0), ("SENSEX", 10, 30.0, 50.0)))
    assert TickTrader._same_for(a, b, "NIFTY") and not TickTrader._same_for(a, b, "SENSEX")
    assert not TickTrader._same_for(a, hhll_tick.replace(b, thr_cost=12), "NIFTY")   # a global setting
    ms, _ = _ticks(1)
    with mock.patch.object(hhll_tick, "today_ist", lambda: DAY), mock.patch.object(manual_desk, "AUTO_ORDER", ("NIFTY",)):
        tt = TickTrader(["NIFTY"], lambda name: ([], [], -1))
        tt.books["NIFTY"].pos = hhll_tick._Pos(1, 100.0, ms[0], 0, None, 0.0, None)
        with mock.patch.object(manual_desk.time, "time", lambda: ms[0] / 1000):
            tt._swap.add("NIFTY")
            assert tt.desired(None, None, True) is None      # superseded settings: not entered
            tt._swap.clear()
            assert tt.desired(None, None, True) == ("BUY", "NIFTY", 1)


# -- 2: the forecast logger coalesces ticks -------------------------------------------
def test_forecast_logger_sends_one_wake_up_for_a_burst_of_ticks() -> None:
    logger = ForecastLogger(None, None, pivot_provider=lambda: {})
    loop = mock.Mock()
    logger._loop, logger._wake_event = loop, asyncio.Event()
    for _ in range(50):
        logger.notify_tick({})
    assert loop.call_soon_threadsafe.call_count == 1
    logger._wake_pending = False                     # the loop took it: the next tick sends another
    logger.notify_tick({})
    assert loop.call_soon_threadsafe.call_count == 2


# -- 6: incremental replay of the day in progress --------------------------------------
class _TickStore:
    """hhll_store's tick reads over in-memory rows that grow as a session does."""

    def __init__(self):
        self.rows = {}                                     # name -> [(seq, ms, price)]

    def add(self, name, ms, px):
        rows = self.rows.setdefault(name, [])
        rows += [(len(rows) + i, m, x) for i, (m, x) in enumerate(zip(ms, px))]

    def ticks(self, day, name, start=0, stop=None):
        rows = [r for r in self.rows.get(name, []) if r[0] >= start and (stop is None or r[0] < stop)]
        return [r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows]

    def ticks_after(self, day, after, limit=50_000):
        out = [(n, *r) for n in sorted(after) for r in self.rows.get(n, []) if r[0] > after[n]]
        return out[:limit]

    def day_ticks(self, day):
        return {n: ([r[1] for r in rows], [r[2] for r in rows]) for n, rows in sorted(self.rows.items()) if rows}


def test_evaluate_on_the_open_day_equals_a_full_replay() -> None:
    ms, px = _ticks(5400, start_hour=9)
    ms = [m + 15 * 60_000 for m in ms]                      # from 09:15
    sx = [round(x * 3.2, 2) for x in px]
    st = _TickStore()
    configs = [hhll_tick.Params(), hhll_tick.Params(thr_cost=6), hhll_tick.Params(thr_cost=6, lock_at=1.0)]
    saved = hhll_tick.STORE
    hhll_tick.use_store(st)
    try:
        with mock.patch.object(hhll_tick, "session_over", lambda d: False):
            prev = 0
            for cut in (700, 701, 2500, 5400):
                st.add("NIFTY", ms[prev:cut], px[prev:cut])
                st.add("SENSEX", ms[prev:cut], sx[prev:cut])
                prev = cut
                got = hhll_tick.evaluate(configs, [DAY])
                data = {n: v for n, v in st.day_ticks(DAY).items() if n in hhll_tick.SPECS}
                for c in configs:
                    want = [{k: t[k] for k in hhll_tick.REPLAY_KEYS} for t in hhll_tick.replay_day(c, DAY, data)]
                    assert got[c] == want, (cut, c)
            assert any(got[c] for c in configs)             # the comparison saw trades
            # a skipped seq: that index is read whole from then on, still exact
            st.rows["NIFTY"].append((len(st.rows["NIFTY"]) + 5, ms[-1] + 1000, px[-1] + 1))
            got = hhll_tick.evaluate(configs[:1], [DAY])
            data = {n: v for n, v in st.day_ticks(DAY).items() if n in hhll_tick.SPECS}
            assert got[configs[0]] == [{k: t[k] for k in hhll_tick.REPLAY_KEYS}
                                       for t in hhll_tick.replay_day(configs[0], DAY, data)]
            assert "NIFTY" in hhll_tick._OPEN_GAPPY
    finally:
        hhll_tick.use_store(saved)
        with hhll_tick._OPEN_LOCK:
            hhll_tick._OPEN_DAY = None
            hhll_tick._OPEN_TICKS.clear()
            hhll_tick._OPEN_GAPPY.clear()
            hhll_tick._OPEN_BOOKS.clear()


# -- 7: the report ---------------------------------------------------------------------
def test_report_lists_the_newest_reviews_and_is_rewritten_at_most_every_few_minutes(tmp_path) -> None:
    revs = [{"kind": "retest", "at": f"r{i}", "setting": "x", "fix": {}, "accepted": False, "why": ["no"]}
            for i in range(30)]
    store = SimpleNamespace(trades=lambda: [], load_state=lambda name="engine": {},
                            recent_reviews=lambda n: (revs[-n:], len(revs)), latest_review=lambda kind: None)
    saved = hhll_tick.STORE
    hhll_tick.use_store(store)
    try:
        with mock.patch.object(hhll_tick, "DATA", tmp_path), mock.patch.object(hhll_tick, "REPORT", tmp_path / "r.md"), \
                mock.patch.object(hhll_tick, "REPORT_REVIEWS", 10):
            hhll_tick.write_report()
            text = (tmp_path / "r.md").read_text()
            assert "The newest 10 of 30" in text and "r29" in text and "r19 " not in text and "r20" in text
            live = hhll_tick.Live.__new__(hhll_tick.Live)
            with mock.patch.object(hhll_tick, "write_report") as w:
                live._report()
                assert w.call_count == 0                    # nothing waiting
                live._report_due = True
                live._report()
                live._report_due = True
                live._report()                              # within REPORT_EVERY_S: later
                assert w.call_count == 1
                live._report(force=True)
                assert w.call_count == 2
    finally:
        hhll_tick.use_store(saved)


# -- 9: caches return what an uncached read would --------------------------------------
def test_pivot_cache_follows_the_strategy_snapshot_and_the_candles() -> None:
    from strategy_logic import IndexView
    marker = {"time": "2026-11-03T10:05:00+05:30", "low": 24990.0, "price": 24990.0}
    engine = SimpleNamespace(snapshot=SimpleNamespace(markers={"NIFTY": [marker]}))
    candles = [{"time": f"2026-11-03T10:{m:02d}:00+05:30", "open": 1, "high": 1, "low": 1, "close": 1} for m in (0, 5)]
    rev = {"n": 1}
    handler = SimpleNamespace(get_latest_data_if_changed=lambda known: (rev["n"], None if known == rev["n"] else list(candles)))
    svc = live_server.PivotSnapshotService(handler, IndexView(engine, "NIFTY"))
    first = svc.latest_pivots()
    assert first["ll_points"] == [marker]
    first["ll_points"].clear()                              # a caller's copy: the cache is untouched
    assert svc.latest_pivots()["ll_points"] == [marker]
    engine.snapshot = SimpleNamespace(markers={"NIFTY": []})   # a new replay
    assert svc.latest_pivots()["ll_points"] == []
    engine.snapshot = SimpleNamespace(markers={"NIFTY": [marker]})
    candles[:] = candles[:1]                                 # new candles end before the marker
    rev["n"] += 1
    assert svc.latest_pivots()["ll_points"] == []


def test_settled_trades_are_cached_by_everything_they_depend_on() -> None:
    desk = ManualDesk.__new__(ManualDesk)
    desk._settled = {}
    row = {"log_time": datetime(2026, 11, 3, 10, 0, tzinfo=ist.IST), "exit_time": datetime(2026, 11, 3, 10, 30, tzinfo=ist.IST)}
    trade = {"id": 1, "direction": "LONG", "entry": 25000.0, "exit": 25040.0, "quantity": 65, "lots": 1}
    fills = [{"phase": "entry", "symbol": "A", "side": "B", "qty": 65, "premium": 120.0},
             {"phase": "entry", "symbol": "B", "side": "S", "qty": 65, "premium": 110.0}]
    with mock.patch.object(manual_desk, "_settle", wraps=manual_desk._settle) as raw:
        a = desk._settle("NIFTY", dict(trade), row, fills)
        b = desk._settle("NIFTY", dict(trade), row, fills)
        assert raw.call_count == 1 and a == b == manual_desk._settle("NIFTY", dict(trade), row, fills)
        desk._settle("NIFTY", dict(trade), row, fills + [{"phase": "exit", "symbol": "A", "side": "S", "qty": 65,
                                                           "premium": 140.0}])
        desk._settle("NIFTY", {**trade, "exit": 25050.0}, row, fills)
        assert raw.call_count == 4


def test_chart_pivots_are_read_again_only_after_a_new_pivot() -> None:
    from tests.test_hhll_tick import _StateStore, _day_ticks
    ms, px = _day_ticks()
    st = _StateStore()
    reads = []
    real = st.pivots
    st.pivots = lambda *a: reads.append(a) or real(*a)
    saved = hhll_tick.STORE
    hhll_tick.use_store(st)
    try:
        with mock.patch.object(hhll_tick, "today_ist", lambda: date(2026, 11, 3)):
            hist = {"NIFTY": (ms[:1500], px[:1500], 1499)}
            tt = TickTrader(["NIFTY"], lambda name: hist[name])
            tt.attach(st)
            a = tt.points("NIFTY")
            assert tt.points("NIFTY") == a and len(reads) == 1
            n0 = len(st.writer.pivots)
            i = 1500
            while len(st.writer.pivots) == n0:
                tt.on_tick("NIFTY", px[i], ms[i] / 1000, i)
                i += 1
            b = tt.points("NIFTY")
            assert len(reads) == 2 and b != a
            assert sorted(p["extreme_at"] for p in b["hh_points"] + b["ll_points"]) == \
                sorted(x[6] for x in st.writer.pivots)
    finally:
        hhll_tick.use_store(saved)


# -- 11: the REST fallback quotes only what is read -------------------------------------
def test_rest_fallback_quotes_only_the_keys_asked_for() -> None:
    calls = []
    ws = LiveWebSocket.__new__(LiveWebSocket)
    ws.feed_opened, ws._prices, ws._price_stamps = False, {}, {}
    ws._lock = threading.Lock()
    ws._rest_prices, ws._rest_stamps, ws._rest_next, ws._rest_failures, ws._rest_serving = {}, {}, {}, {}, set()
    ws._failure_throttle = mock.Mock(should_log=lambda key: (False, 0))
    ws.api = SimpleNamespace(get_quotes=lambda e, t, timeout=30: calls.append(f"{e}|{t}") or {"stat": "Ok", "lp": "100"})
    ws.resubscribe_symbols = ["NSE|26000", "NFO|5678"]
    ws._rest_keys = frozenset({"NSE|26000"})
    with mock.patch("live_websocket.ist.now_naive", return_value=datetime(2026, 11, 3, 12, 0)):
        ws._rest_quote_once()
    assert calls == ["NSE|26000"]
    ws._rest_keys, calls[:] = None, []                       # unset: every subscription, as before
    ws._rest_next.clear()
    with mock.patch("live_websocket.ist.now_naive", return_value=datetime(2026, 11, 3, 12, 0)):
        ws._rest_quote_once()
    assert calls == ["NSE|26000", "NFO|5678"]


def test_store_batches_its_commits() -> None:
    assert hhll_store.Store(SimpleNamespace(maxconn=1)).writer.interval == hhll_store.WRITE_EVERY_S > 0
