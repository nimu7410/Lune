"""HH/LL tick trader: pivot rules, charges, session rules, acceptance rule (hhll_tick.selfcheck),
and the desk's TickTrader following that engine."""

import dataclasses
import json
from datetime import date, datetime
from unittest import mock

import pytest

import hhll_store
import hhll_tick

hhll_tick.MANUAL = False   # these tests adopt settings themselves
import manual_desk


def test_selfcheck() -> None:
    hhll_tick.selfcheck(real=False)


def _day_ticks():
    """NIFTY swinging 60 pts every 10 minutes from 10:00 IST: pivots at R ~ 15."""
    t0 = int(datetime(2026, 11, 3, 10, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
    ms, px = [], []
    for i in range(3600):                       # one tick a second for an hour
        k = i % 1200
        ms.append(t0 + i * 1000)
        px.append(25000 + (k if k < 600 else 1200 - k) * 0.1)
    return ms, px


class _Writer:
    def __init__(self):
        self.pivots = []

    def put_pivot(self, row):
        self.pivots.append(row)


class _StateStore:
    """The slice of hhll_store.Store that TickTrader and current_params use, in memory."""

    def __init__(self, state=None):
        self.state, self.version, self.writer = state or {}, 1, _Writer()

    def load_state(self, name="engine"):
        return json.loads(json.dumps(self.state)) if name == "engine" else {}

    def save_state(self, st):
        self.state, self.version = st, self.version + 1

    def state_version(self):
        return self.version

    def pivots(self, day, name, r, limit):
        rows = [x for x in self.writer.pivots if x[0] == name and x[1] == day and x[2] == round(r, 4)]
        rows = sorted({x[3]: x for x in rows}.values(), key=lambda x: x[3])[-limit:]
        return [(x[4], x[5], x[6], x[8], x[9], x[10]) for x in rows]


def test_min_leg_holds_the_pivot_and_a_broken_pivot_prints_the_opposite_one() -> None:
    z = hhll_tick.ZigZag(2.0, leg=20.0)
    feed = lambda *xs: [p for p in (z.update(x, i) for i, x in enumerate(xs)) if p]
    piv = feed(100, 98,               # no leg to measure: the threshold alone prints the first pivot, and 105 breaks it
               105, 103,              # swing 7 < 20: a retrace prints nothing
               120, 118,              # swing 22 >= 20: the HH prints on the low threshold
               115, 121,              # past the HH: it is invalidated, the LL at 115 keeps us with the trend
               119, 136, 134,         # swing 21 from that LL: the next HH
               114)                   # one tick drops 22 past it all: an LL needs its bounce, nothing yet
    assert [(p.kind, p.price, p.conf_price, p.forced) for p in piv] == [
        ("HH", 100, 98, False), ("LL", 98, 105, True), ("HH", 120, 118, False), ("LL", 115, 121, True),
        ("HH", 136, 134, False)], piv
    assert feed(113, 112, 114)[0].price == 112                  # the extreme, not the tick that confirmed it


def test_broken_low_prints_the_last_minor_high_and_a_tie_does_not_break() -> None:
    z = hhll_tick.ZigZag(2.0, leg=20.0)
    piv = [p for p in (z.update(x, i) for i, x in enumerate([100, 102, 108, 100, 104, 100, 99.95])) if p]
    assert [(p.kind, p.price, p.forced) for p in piv] == [("LL", 100, False), ("HH", 104, True)], piv   # 108 is further back
    assert piv[1].conf_price == 99.95 and z.lp == 99.95          # the tie at 100 printed nothing


def test_pivots_keep_printing_after_square_off_to_the_close() -> None:
    t0 = int(datetime(2026, 11, 3, 15, 15, tzinfo=hhll_tick.IST).timestamp() * 1000)
    b = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(thr_cost=4))
    for i, x in enumerate([25000, 25010, 25020, 25000, 24980, 25000]):
        b.on_tick(x, t0 + i * 1000)
    assert [p.kind for p in b.pivots] == ["LL", "HH", "LL"] and not b.trades and b.pos is None


def test_book_prices_pivot_settings_in_cost_multiples() -> None:
    ms, px = _day_ticks()
    b = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(thr_cost=18, fast_cost=4, leg_cost=24))
    b.on_tick(px[0], ms[0])
    t = hhll_tick.SPECS["NIFTY"].tick
    assert abs(b.R - 18 * b.cost) <= t and abs(b.zz.R - 4 * b.cost) <= t and abs(b.zz.leg - 24 * b.cost) <= t


def test_index_pivots_override_one_index_and_survive_the_state_round_trip() -> None:
    ms, px = _day_ticks()
    p = hhll_tick.Params(thr_cost=18, fast_cost=4, leg_cost=24, index_pivots=(("NIFTY", 10, 2, 14),))
    back = hhll_tick.Params(**json.loads(json.dumps(dataclasses.asdict(p))))
    assert back == p and hash(back) == hash(p)
    mine, other = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], back), hhll_tick.Book(hhll_tick.SPECS["SENSEX"], back)
    mine.on_tick(px[0], ms[0])
    other.on_tick(px[0], ms[0])
    t = hhll_tick.SPECS["NIFTY"].tick
    assert abs(mine.R - 10 * mine.cost) <= t and abs(mine.zz.R - 2 * mine.cost) <= t and abs(mine.zz.leg - 14 * mine.cost) <= t
    assert abs(other.R - 18 * other.cost) <= t and abs(other.zz.leg - 24 * other.cost) <= t


def test_tick_trader_enters_only_auto_order_but_exits_any_held_index() -> None:
    ms, px = _day_ticks()
    with mock.patch.object(hhll_tick, "today_ist", lambda: date(2026, 11, 3)), \
            mock.patch.object(manual_desk, "AUTO_ORDER", ("SENSEX", "FINNIFTY")):
        tt = manual_desk.TickTrader(["NIFTY", "FINNIFTY", "SENSEX"], lambda name: ([], [], -1))
        pos = hhll_tick._Pos(1, 100.0, ms[0], 0, None, 0.0, None)
        with mock.patch.object(manual_desk.time, "time", lambda: ms[0] / 1000):
            tt.books["NIFTY"].pos = pos
            assert tt.desired(None, None, True) is None                  # outside AUTO_ORDER: never entered
            tt.books["FINNIFTY"].pos = tt.books["SENSEX"].pos = pos
            assert tt.desired(None, None, True) == ("BUY", "SENSEX", 1)  # first in the order wins
            tt._managed = {"NIFTY"}
            tt.books["NIFTY"].pos = None
            assert tt.desired("LONG", "NIFTY", True) == ("SELL", "NIFTY", 1)


def test_tick_trader_is_always_on_for_every_index() -> None:
    ms, px = _day_ticks()
    with mock.patch.object(hhll_tick, "today_ist", lambda: date(2026, 11, 3)), \
            mock.patch.object(manual_desk, "AUTO_ORDER", ("NIFTY", "SENSEX")):
        tt = manual_desk.TickTrader(["NIFTY", "SENSEX"], lambda name: ([], [], -1))
        assert tt.names == ("NIFTY", "SENSEX")
        pos = hhll_tick._Pos(1, 100.0, ms[0], 0, None, 0.0, None)
        tt.books["SENSEX"].pos = pos
        with mock.patch.object(manual_desk.time, "time", lambda: ms[0] / 1000):
            assert tt.desired(None, None, True) == ("BUY", "SENSEX", 1)   # no index has to be picked
            tt.issued("a")
            tt.settle("a", False, "no legs")                              # failed entry: flat, keeps trading
            tt.books["NIFTY"].pos = pos
            assert tt.desired(None, None, True) == ("BUY", "NIFTY", 1)
            tt.books["NIFTY"].pos = None
            tt.issued("b")
            tt.settle("b", False, "exit not flat")                        # failed exit: held, no more orders
            assert tt.desired("LONG", "NIFTY", True) is None and tt.halted == "exit not flat"
            assert tt.desired(None, None, True) is None and tt.halted is None   # flat again: running
    assert "/manual/auto" not in (hhll_tick.BASE / "manual_desk.py").read_text(encoding="utf-8")
    for f in ("templates/index.html", "static/Lune-v6.js"):
        assert "autoButton" not in (hhll_tick.BASE / f).read_text(encoding="utf-8")


def test_tick_trader_follows_engine() -> None:
    ms, px = _day_ticks()
    with mock.patch.object(hhll_tick, "today_ist", lambda: date(2026, 11, 3)), \
            mock.patch.object(manual_desk, "AUTO_ORDER", ("NIFTY",)):
        n = 2000
        hist = {"NIFTY": (ms[:n], px[:n], n - 1)}
        tt = manual_desk.TickTrader(["NIFTY"], lambda name: hist.get(name, ([], [], -1)))
        ref = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params())
        for m, x in zip(ms[:n], px[:n]):
            ref.on_tick(x, m)
        assert [p.ms for p in tt.books["NIFTY"].pivots] == [p.ms for p in ref.pivots]   # same engine, same pivots
        held = None
        actions = []
        for i, (m, x) in enumerate(zip(ms[n:], px[n:])):
            with mock.patch.object(manual_desk.time, "time", lambda: m / 1000):
                tt.on_tick("NIFTY", x, m / 1000, n + i)
                tt.on_tick("NIFTY", x, m / 1000, n + i)                 # a tick seen twice is fed once
                ref.on_tick(x, m)
                step = tt.desired(held, "NIFTY" if held else None, True)
                if step:
                    actions.append((m, step[0]))
                    held = None if step[0] == "SELL" else ("LONG" if step[0] == "BUY" else "SHORT")
                    # the next step in the same tick: a reversal opens at once
                    again = tt.desired(held, "NIFTY" if held else None, True)
                    if again:
                        actions.append((m, again[0]))
                        held = "LONG" if again[0] == "BUY" else "SHORT"
                if actions:
                    assert held == tt._side(ref), (m, actions[-3:])   # the desk is where the engine is
        assert len(actions) >= 3 and all(a in ("BUY", "SHORT", "SELL") for _, a in actions), actions
        assert [p.ms for p in tt.books["NIFTY"].pivots] == [p.ms for p in ref.pivots]
        # one entry per engine entry: asking again never re-enters
        tt.books["NIFTY"].pos = ref.pos
        assert tt.desired(None, None, True) is None
        # a clicked position against the engine is not closed until the engine flips fresh
        with mock.patch.object(manual_desk.time, "time", lambda: (ms[-1] + 60_000) / 1000):
            other = "SHORT" if tt._side(ref) == "LONG" else "LONG"
            tt._managed.clear()
            assert tt.desired(other, "NIFTY", True) is None
        # attached to a store: pivots go there (and the chart reads them back), the book keeps
        # only its latest, and adopted settings reach the book when it is not held
        hist["NIFTY"] = (ms, px, len(ms) - 1)
        st = _StateStore()
        saved = hhll_tick.STORE
        hhll_tick.use_store(st)
        try:
            tt.attach(st)
            assert len(tt.books["NIFTY"].pivots) == 1 and len(st.writer.pivots) == len(ref.pivots)
            pts = tt.points("NIFTY")
            assert sorted(p["extreme_at"] for p in pts["hh_points"] + pts["ll_points"]) == [p.ms for p in ref.pivots]
            assert sorted((p["confirmed_at"], p["confirmed_price"]) for p in pts["hh_points"] + pts["ll_points"]) == \
                sorted((p.conf_ms, p.conf_price) for p in ref.pivots)
            tt.poll_settings()
            assert tt.books["NIFTY"].p.thr_cost == 10                    # nothing adopted yet: the prior
            st.save_state({"active": hhll_tick.asdict(hhll_tick.Params(thr_cost=20)), "active_day": "2026-11-03",
                           "next": hhll_tick.asdict(hhll_tick.Params(thr_cost=20)), "applied_ms": ms[-1]})
            tt.poll_settings()
            tt._held = "NIFTY"
            tt.on_tick("NIFTY", px[-1], ms[-1] / 1000, len(ms))
            assert tt.books["NIFTY"].p.thr_cost == 10                    # held: not yet
            tt._held = None
            hist["NIFTY"] = (ms + [ms[-1] + 1000], px + [px[-1]], len(ms))
            tt.on_tick("NIFTY", px[-1], (ms[-1] + 1000) / 1000, len(ms) + 1)
            assert tt.books["NIFTY"].p.thr_cost == 20 and tt._fed["NIFTY"] == len(ms)
            tt.on_tick("NIFTY", px[-1], (ms[-1] + 2000) / 1000, len(ms) + 1)   # the next tick is fed again
            assert tt._fed["NIFTY"] == len(ms) + 1
        finally:
            hhll_tick.use_store(saved)


def test_current_params_and_split() -> None:
    saved = hhll_tick.STORE
    st = _StateStore()
    hhll_tick.use_store(st)
    try:
        assert hhll_tick.current_params(date(2026, 11, 3)) == (hhll_tick.Params(thr_cost=10), 0)
        a, b = hhll_tick.asdict(hhll_tick.Params(thr_cost=12)), hhll_tick.asdict(hhll_tick.Params(thr_cost=15))
        at = int(datetime(2026, 11, 3, 11, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
        st.save_state({"active": a, "active_day": "2026-11-03", "next": b, "applied_ms": at})
        assert hhll_tick.current_params(date(2026, 11, 3)) == (hhll_tick.Params(thr_cost=12), at)
        assert hhll_tick.current_params(date(2026, 11, 4)) == (hhll_tick.Params(thr_cost=15), 0)   # next session
    finally:
        hhll_tick.use_store(saved)


def test_replayed_book_keeps_only_an_entry_on_this_tick() -> None:
    ms, px = _day_ticks()
    ref = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params())
    for i, (m, x) in enumerate(zip(ms, px)):
        ref.on_tick(x, m)
        if ref.trades and ref.trades[-1]["exit_ms"] == m:     # first reversal tick
            break
    ticks = (ms[:i + 1], px[:i + 1])
    day = date(2026, 11, 3)
    b = hhll_tick.replayed_book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(), day, keep_ms=ms[i], ticks=ticks)
    assert b.pos is not None and b.pos.ms == ms[i] and b.trades == []
    b = hhll_tick.replayed_book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(), day, keep_ms=ms[i] - 1, ticks=ticks)
    assert b.pos is None
    seen = []
    b = hhll_tick.replayed_book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(), day, ticks=ticks,
                                on_pivot=lambda bk, pv: seen.append(pv), keep=False)
    assert seen == ref.pivots and list(b.pivots) == ref.pivots[-1:] and len(b.trades) == 0


def test_desk_charges_from_leg_quotes() -> None:
    from decimal import Decimal
    from types import SimpleNamespace
    # what a paper trade records: the bought leg at the ask, the sold leg at the bid, and the reverse at exit
    rows = []
    quotes = {"P": {"stat": "Ok", "sp1": "120.5", "bp1": "120.0"}, "C": {"stat": "Ok", "sp1": "99.5", "bp1": "99.0"}}
    P, C = "NIFTY10NOV26P25000", "NIFTY10NOV26C25000"
    scrip = {P: "P", C: "C"}
    # the real paper manager (no live-only helpers), with only the broker and ledger faked
    mgr = manual_desk.ManualTradeManagerTest.__new__(manual_desk.ManualTradeManagerTest)
    mgr._tokens = {}
    mgr.api = SimpleNamespace(
        searchscrip=lambda exchange, searchtext: {"stat": "Ok", "values": [{"tsym": searchtext, "token": scrip[searchtext]}]},
        get_quotes=lambda exchange, token: quotes[token])
    mgr.db = SimpleNamespace(execute=lambda q, a: rows.append(a))
    mgr.last_price_getter = lambda name: 25000.0
    mgr._record_legs(7, "entry", (P, C), 65)
    mgr._record_legs(7, "exit", (P, C), 65)
    assert [(r[2], r[3], r[5]) for r in rows] == [(P, "B", 120.5), (C, "S", 99.0), (P, "S", 120.0), (C, "B", 99.5)], rows
    fills = [dict(zip(("trade_id", "phase", "symbol", "side", "qty", "premium"), r[:6])) for r in rows]
    trade = {"direction": "SHORT", "lots": 1, "entry": 25000.0, "exit": 24990.0}
    row = {"log_time": datetime(2026, 11, 3, 11, 0, tzinfo=hhll_tick.IST),
           "exit_time": datetime(2026, 11, 3, 11, 5, tzinfo=hhll_tick.IST)}
    total, basis = manual_desk._charges("NIFTY", trade, row, fills)
    want = sum(sum(hhll_tick.order_charges("NFO", b, 65, p).values(), Decimal(0))
               for b, p in ((True, 120.5), (False, 99.0), (False, 120.0), (True, 99.5)))
    assert basis == "quotes" and abs(total - float(want)) < 1e-9
    assert 20 <= total - 4 * 5 * 0.18 - 20 <= 80        # Rs 20 brokerage (4 x Rs 5) plus STT/exchange/GST/stamp
    # a missing quote falls back to the model, and says so
    assert manual_desk._charges("NIFTY", trade, row, fills[:3])[1] == "model"


def test_target_and_leg_filter() -> None:
    ms, px = _day_ticks()
    spec = hhll_tick.SPECS["NIFTY"]
    def run(**kw):
        b = hhll_tick.Book(spec, hhll_tick.Params(**kw))
        for m, x in zip(ms, px):
            b.on_tick(x, m)
        return b
    base, tgt = run(), run(target=1.5)
    hits = [t for t in tgt.trades if t["reason"] == "target"]
    assert hits and len(tgt.trades) >= len(base.trades)
    for t in hits:   # closed on the first tick at or beyond 1.5 R of open profit, after friction
        assert t["gross_pts"] >= 1.5 * t["R"] - t["friction_pts"] - 0.11
    legs = [t["leg_R"] for t in base.trades if t["leg_R"] is not None]
    assert legs and min(legs) > 1.0                     # every leg here is the same 60-pt swing
    kept = run(leg_k=min(legs) - 0.01)                   # just under it: every entry after the first pivot
    assert len(kept.trades) == len(legs) and all(t["leg_R"] >= min(legs) - 0.01 for t in kept.trades)
    none = run(leg_k=max(legs) + 0.01)                   # just over it: no entry at all
    assert none.trades == [] and none.blocked.get("leg", 0) > 0


def test_winner_is_reviewed_for_a_target() -> None:
    p = hhll_tick.Params()
    t = {"R": 10.0, "net_pts": 3.0, "gross_pts": 5.0, "mfe_pts": 25.0, "chop": 3.0, "charges_pts": 1.0,
         "friction_pts": 1.0, "reason": "reverse", "entry_sod": 40000, "leg_R": 3.0}
    w, s = hhll_tick.weakness(t, p)
    assert w == "left profit at the peak" and s[w] == 15.0, s       # a 2R target keeps 20 of the 25
    assert hhll_tick.fix_for(p, w) == ("target", hhll_tick.Params(target=2.0))


def test_forward_test_reverts_a_change_that_does_not_live_up() -> None:
    import threading
    from types import SimpleNamespace
    old, new = hhll_tick.Params(), hhll_tick.Params(target=2.0)
    at = int(datetime(2026, 11, 3, 11, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
    adopted, logged = [], []
    fake = SimpleNamespace(today=date(2026, 11, 3), params=new, _state_lock=threading.Lock(),
                           state={"trial": {"at_ms": at, "prev": hhll_tick.asdict(old), "new": hhll_tick.asdict(new)}},
                           _adopt=lambda p, why, trial=True: adopted.append((p, trial)))
    def trades(n, rs):
        return [{"entry_ms": at + 60_000 * i, "net_rs": rs} for i in range(n)] + [{"entry_ms": at - 1, "net_rs": 9e9}]
    with mock.patch.object(hhll_tick, "log_review", logged.append), mock.patch.object(hhll_tick, "save_state", lambda st: None), \
            mock.patch.object(hhll_tick.time, "time", lambda: (at + 10 * 60_000) / 1000):
        with mock.patch.object(hhll_tick, "evaluate", lambda cs, ds: {new: trades(3, -100), old: trades(3, 50)}):
            hhll_tick.Live._forward(fake)                  # 3 trades, 10 minutes: too early to judge
        assert adopted == [] and fake.state["trial"]
        with mock.patch.object(hhll_tick, "evaluate", lambda cs, ds: {new: trades(8, -100), old: trades(8, 50)}):
            hhll_tick.Live._forward(fake)                  # 8 trades and behind (ticks before adoption ignored)
        assert adopted == [(old, False)] and fake.state["trial"] is None
        assert logged[-1]["why"] == ["did not live up"] and logged[-1]["base"]["net"] == 400
        assert fake.state["forward_failed"] == {"day": "2026-11-03", "params": [hhll_tick.asdict(new)]}
    # and a re-test that picks it again the same day is refused
    live = SimpleNamespace(today=date(2026, 11, 3), params=old, state={"forward_failed": fake.state["forward_failed"]},
                           _state_lock=threading.Lock())
    with mock.patch.object(hhll_tick, "save_state", lambda st: (_ for _ in ()).throw(AssertionError("adopted"))):
        hhll_tick.Live._adopt(live, new, "intraday re-test")
    assert live.params == old


def test_signal_exit_holds_through_filtered_pivots() -> None:
    import random
    rnd = random.Random(3)
    t0 = int(datetime(2026, 11, 3, 9, 20, tzinfo=hhll_tick.IST).timestamp() * 1000)
    ms, px, x = [], [], 25000.0
    for i in range(6 * 3600):                          # 09:20-15:20 at 1 tick/s, swings of every size
        x = round(x + rnd.choice([-1, 1]) * rnd.choice([0.05, 0.5, 1, 2, 4]), 2)
        ms.append(t0 + i * 1000); px.append(x)
    spec = hhll_tick.SPECS["NIFTY"]
    def run(**kw):
        b = hhll_tick.Book(spec, hhll_tick.Params(chop_k=1.5, leg_k=2.0, **kw))
        for m, y in zip(ms, px):
            b.on_tick(y, m)
        return b
    pivot, sig, wide = run(), run(exit_on="signal"), run(exit_on="signal", exit_wide=3.0)
    assert pivot.trades and sig.trades and wide.trades
    cut = hhll_tick.hhmm("15:00")
    for b in (sig, wide):
        entries = {t["entry_ms"] for t in b.trades}
        for t in b.trades:
            exit_sod = (t["exit_ms"] // 1000 + hhll_tick.IST_OFFSET_S) % 86400
            if t["reason"] == "reverse" and exit_sod < cut:
                assert t["exit_ms"] in entries, t        # a signal exit is always a reversal into a new trade
    assert len(sig.trades) < len(pivot.trades)          # held through pivots the filters turn down
    assert any(t["reason"] == "wide" for t in wide.trades)
    for t in wide.trades:                                # a wide exit never also reverses: flat until a signal
        if t["reason"] == "wide":
            assert t["exit_ms"] not in {u["entry_ms"] for u in wide.trades}


def test_leg_quote_retries_the_index_reply(monkeypatch) -> None:
    from types import SimpleNamespace
    monkeypatch.setattr(manual_desk.time, "sleep", lambda s: None)
    good = {"stat": "Ok", "tsym": "X", "sp1": "120.5", "bp1": "120.0"}
    index = {"stat": "Ok", "tsym": "Nifty 50", "exch": "NSE", "token": "26000", "lp": "25000"}
    searches = []
    mgr = manual_desk.ManualTradeManagerTest.__new__(manual_desk.ManualTradeManagerTest)
    mgr._spot_now = lambda s: 25000.0
    mgr._leg_token = lambda s: searches.append(s) or ("NFO", "1")
    replies = [index, None, good]
    mgr._tokens = {}
    mgr.api = SimpleNamespace(get_quotes=lambda exchange, token: replies.pop(0))
    assert mgr._leg_quote("X", "B") == 120.5 and len(searches) == 3
    # never a good reply: it fails, naming what was asked, and writes nothing
    replies[:] = [index] * 4
    rows = []
    mgr.db = SimpleNamespace(execute=lambda q, a: rows.append(a))
    with pytest.raises(ValueError, match=r"asked NFO\|1, 4 tries"):
        mgr._leg_quote("X", "B")
    replies[:] = [index] * 4
    mgr._record_legs(7, "entry", ("X", None), 65)
    assert rows == [] and not replies


def test_leg_premium_refuses_a_wrong_quote() -> None:
    import pytest
    ok = {"stat": "Ok", "tsym": "X", "sp1": "180.5", "bp1": "180.0", "lp": "180.2"}
    assert manual_desk._leg_premium("X", "B", ok, 25000.0) == 180.5
    assert manual_desk._leg_premium("X", "S", ok, 25000.0) == 180.0
    for bad in ({**ok, "tsym": "Y"}, {**ok, "sp1": "25385.2"}, {"stat": "Not_Ok"}, None, {"stat": "Ok", "sp1": "0", "lp": ""}):
        with pytest.raises(ValueError):
            manual_desk._leg_premium("X", "B", bad, 25000.0)


def test_shadow_tournament_adopts_only_out_of_sample_winners() -> None:
    import threading
    from types import SimpleNamespace
    live, a, b = hhll_tick.Params(), hhll_tick.Params(thr_cost=8), hhll_tick.Params(lock_at=2.0)
    adopted, logged, ok = [], [], [True]
    def fake(day, picked):
        return SimpleNamespace(today=day, params=live, _state_lock=threading.Lock(),
                               state={"shadows": {"picked": picked, "at_ms": 0, "params": [hhll_tick.asdict(a), hhll_tick.asdict(b)]}},
                               _adopt=lambda p, why, trial=True: adopted.append(p) or ok[0])
    d = date(2026, 11, 4)
    open_ms = int(datetime(2026, 11, 4, 9, 15, tzinfo=hhll_tick.IST).timestamp() * 1000)
    def trades(n, rs):
        return [{"entry_ms": open_ms + 60_000 * i, "net_rs": rs} for i in range(n)]
    res = {live: trades(10, -10), a: trades(10, 50), b: trades(10, 20)}
    with mock.patch.object(hhll_tick, "log_review", logged.append), mock.patch.object(hhll_tick, "save_state", lambda st: None), \
            mock.patch.object(hhll_tick, "evaluate", lambda cs, ds: {c: res[c] for c in cs}):
        same_day = fake(d, "2026-11-04")
        with mock.patch.object(hhll_tick.time, "time", lambda: open_ms / 1000 + 7200):
            hhll_tick.Live._tournament(same_day)          # picked today: in sample, no verdict
        assert adopted == [] and logged == []
        f = fake(d, "2026-11-03")
        with mock.patch.object(hhll_tick.time, "time", lambda: open_ms / 1000 + 1800):
            hhll_tick.Live._tournament(f)                 # 30 minutes in: too early
        assert adopted == [] and f.state["shadows"]["at_ms"] == open_ms
        with mock.patch.object(hhll_tick.time, "time", lambda: open_ms / 1000 + 7200):
            hhll_tick.Live._tournament(f)
        assert adopted == [a] and logged[-1]["accepted"]
        assert hhll_tick.Params(**f.state["shadows"]["params"][0]) == live   # the old live settings become a shadow
        ok[0], g = False, fake(d, "2026-11-03")           # the winner is turned down (manual tuning): it stays a shadow
        with mock.patch.object(hhll_tick.time, "time", lambda: open_ms / 1000 + 7200):
            hhll_tick.Live._tournament(g)
        assert [hhll_tick.Params(**c) for c in g.state["shadows"]["params"]] == [a, b]


def test_retune_switch_waits_for_the_close_while_a_session_is_traded() -> None:
    live = hhll_tick.Live.__new__(hhll_tick.Live)
    live._auto_switch = mock.Mock()
    with mock.patch.object(hhll_tick, "stored_days", lambda: [date(2026, 11, 9)]), \
            mock.patch.object(hhll_tick, "refresh_health", lambda p: {"rows": ["r"]}), \
            mock.patch.object(hhll_tick, "current_params", lambda d: (hhll_tick.Params(), 0)), \
            mock.patch.object(hhll_tick, "write_report"):
        with mock.patch.object(hhll_tick, "in_session", lambda: True):
            live._health()
        live._auto_switch.assert_not_called()
        live._health_key = None                           # the close changes the key
        with mock.patch.object(hhll_tick, "in_session", lambda: False):
            live._health()
        live._auto_switch.assert_called_once_with(["r"], (date(2026, 11, 9),))
        # a check that fails (the database was away at the close) is tried again, after a pause, until it works
        live._health_key, boom = None, mock.Mock(side_effect=RuntimeError("db away"))
        with mock.patch.object(hhll_tick, "in_session", lambda: False), mock.patch.object(hhll_tick, "refresh_health", boom):
            import pytest
            with pytest.raises(RuntimeError):
                live._health()
            live._health()                                # within the pause: not hammered
            assert boom.call_count == 1 and live._health_key is None
        live._health_retry = 0.0                          # the pause is over
        with mock.patch.object(hhll_tick, "in_session", lambda: False):
            live._health()
        assert live._auto_switch.call_count == 2 and live._health_key is not None


def test_max_hold_closes_a_trade():
    b = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params(thr_cost=6, max_hold_s=30))
    t0 = 1793766000000
    for i, p in enumerate([20000.0, 20040.0, 20040.5] + [20041.0] * 40):
        b.on_tick(p, t0 + i * 1000)
    assert b.pos is None and b.trades[-1]["reason"] == "time"


def test_thresholds_set_on_the_desk_are_adopted_once_past_manual_tuning() -> None:
    live = hhll_tick.Live.__new__(hhll_tick.Live)
    live.store = mock.Mock()
    live.store.load_state.return_value = {"fixed": {"NIFTY": 18, "SENSEX": 20}}
    live.state = {"next": dataclasses.asdict(hhll_tick.Params(thr_cost=18, fast_cost=3))}
    live._adopt = mock.Mock()
    live._requested()
    p = live._adopt.call_args.args[0]
    assert p.fixed_of("SENSEX") == 20.0 and p.fixed_of("BANKNIFTY") is None
    assert live._adopt.call_args.kwargs == {"trial": False, "by_hand": True}
    live.state["next"] = json.loads(json.dumps(dataclasses.asdict(p)))    # as the state reads back
    live._adopt.reset_mock()
    live._requested()
    live._adopt.assert_not_called()


def test_desk_thresholds_keep_each_index_minimum_leg() -> None:
    live = hhll_tick.Live.__new__(hhll_tick.Live)
    live.store, live._adopt = mock.Mock(), mock.Mock()
    p = hhll_tick.Params(thr_cost=10, index_pivots=(("NIFTY", 10, 18.0, 0.0), ("SENSEX", 10, 22.0, 48.0)))
    live.state = {"next": dataclasses.asdict(p)}
    live.store.load_state.return_value = {"fixed": {"NIFTY": 20, "SENSEX": 22}}
    live._requested()                                    # a desk save leaves SENSEX's leg alone
    assert dict((x[0], x[3]) for x in live._adopt.call_args.args[0].index_pivots) == {"NIFTY": 0.0, "SENSEX": 48.0}
    live._adopt.reset_mock()
    live.store.load_state.return_value = {"fixed": {"NIFTY": 18, "SENSEX": 22}, "legs": {"NIFTY": 40}}
    live._requested()                                    # and a request may set one
    assert ("NIFTY", 10, 18.0, 40.0) in live._adopt.call_args.args[0].index_pivots


@mock.patch.object(hhll_tick, "AUTO_SWITCH", True)
def test_retune_switch_is_applied_by_itself_once_per_finished_session() -> None:
    import threading
    live = hhll_tick.Live.__new__(hhll_tick.Live)
    live.store, live._state_lock = mock.Mock(), threading.RLock()
    p = hhll_tick.Params(thr_cost=18, index_pivots=(("NIFTY", 18.0, 18.0, 0.0), ("SENSEX", 18.0, 20.0, 0.0)))
    live.state = {"next": dataclasses.asdict(p)}
    row = lambda n, v, thr, to: {"index": n, "verdict": v, "threshold": thr, "switch_to": to}
    rows = [row("NIFTY", "SWITCH", 18.0, 25), row("SENSEX", "SWITCH", 30.0, 40), row("BANKNIFTY", "ALARM", 18.0, None)]
    done = (date(2026, 11, 9),)
    with mock.patch.object(hhll_tick, "save_state") as save:
        live._auto_switch(rows, ())                               # no finished session: nothing
        live.store.save_state.assert_not_called()
        live._auto_switch(rows, done)
        req, name = live.store.save_state.call_args.args
        # NIFTY moves; SENSEX's verdict is on 30, not the 20 in force; the others keep their thresholds
        assert name == "request" and req["fixed"]["NIFTY"] == 25.0 and req["fixed"]["SENSEX"] == 20.0
        assert req["fixed"]["BANKNIFTY"] == 18.0 and set(req["fixed"]) == set(hhll_tick.SPECS)
        assert req["why"] == "retune check: NIFTY 18 -> 25" and live.state["auto_switch"] == {"NIFTY": "2026-11-09"}
        save.assert_called_once_with(live.state)
        assert live.state["switches"] == [{"index": "NIFTY", "from": 18.0, "to": 25.0, "after": "2026-11-09"}]
        live.store.save_state.reset_mock()
        live._auto_switch(rows, done)                             # the same session: not again
        live.store.save_state.assert_not_called()
        live._auto_switch(rows, done + (date(2026, 11, 10),))     # a new finished session: again
        live.store.save_state.assert_called_once()
    # the request is adopted under its own reason, as a desk save is, MANUAL or not
    live.store.load_state.return_value = req
    live._adopt = mock.Mock()
    live._requested()
    assert live._adopt.call_args.args[1] == "retune check: NIFTY 18 -> 25"
    assert hhll_tick.pivot_value(live._adopt.call_args.args[0], "NIFTY") == 25.0
    assert live._adopt.call_args.kwargs == {"trial": False, "by_hand": True}
    with mock.patch.object(hhll_tick, "AUTO_SWITCH", False), mock.patch.object(hhll_tick, "save_state"):
        live.store.save_state.reset_mock()
        live._auto_switch(rows, done + (date(2026, 11, 11),))
        live.store.save_state.assert_not_called()
