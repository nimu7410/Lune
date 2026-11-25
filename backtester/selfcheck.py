"""Self-validation of the backtester. `python selfcheck.py` (Lune's venv); exits non-zero on any failure.

 1 data       every local file decodes to exactly the ticks in Lune's database (when reachable)
 2 pivots     ZigZag against Lune's literal pivot rules (reference_pivots / check_pivots), on
              random walks and on real sessions; HH/LL strictly alternate, nothing repaints
   biglegs    the default pivots (bt.big), every session: every leg >= the threshold, each pivot
              the true extreme printed on the first tick that far back, nothing forced, no
              stall, strict alternation, and a replay cut anywhere printed the same pivots
   fastlegs   the fast preset (bt.fast), every session: Lune's literal rules (check_pivots: no
              stall, no late print), strict alternation, no repaint at any cut
   scaledlegs the default (bt.scaled), every session: the same proofs as fastlegs
 3 parity     with fill="same" this engine makes exactly the trades and pivots of Lune's own
              hhll_tick.Book, on every (index, day), under many different settings
 4 lookahead  a replay cut at any tick, or with every later tick replaced by noise, makes the
              very same decisions, fills and pivots up to the cut
 5 fills      fill="next": every fill is on the first tick stamped after decision + latency, at
              that tick's price +/- friction; a fill never shares its decision's millisecond
 6 money      each trade's gross, charges and net re-derived independently; stats add up
 7 sessions   one book fed every day back to back = a fresh book per day (nothing leaks)
 8 tuning     the sweep's cached numbers equal direct runs; a walk-forward pick for a day
              never changes when that day or any later one changes
 9 inputs     settings from the UI are validated; bad ones are refused
10 needle     every needle rule: one needle per pivot, each re-derived from the ticks in exact
              paise (give back / back to entry / stall once armed, or the next pivot), its trade never
              outlives it, pivots unchanged, no lookahead; no entry from 15:00, flat at 15:10

`python selfcheck.py parity lookahead` runs only the named sections.
"""

from __future__ import annotations

import random
import sys
from fractions import Fraction
import time
from bisect import bisect_right
from dataclasses import asdict, replace

import backtest as bt
import engine
import ticks
from engine import SPECS, Book, Params

import hhll_tick as lune   # Lune's live engine (engine.py put Lune on sys.path)

FAIL = []


def ok(cond: bool, what: str) -> None:
    if not cond:
        FAIL.append(what)
        print("  FAIL", what)


def section(title: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {title}")


VARIANTS = [   # every branch of the book: filters, exits, limit entries, wide exit, time exit
    {},
    dict(stop=0.75, lock_at=1.5, lock_trail=0.5),
    dict(target=2.0, chop_k=1.5),
    dict(leg_k=2.0, exit_on="signal", exit_wide=3.0),
    dict(order="limit", limit_wait=5.0, last_entry="14:00"),
    dict(max_hold_s=600, lots=3, stop=1.0),
    dict(index_pivots=(), thr_cost=6, chop_k=2.0, lock_at=2.0, lock_trail=0.25, exit_wide=2.0, target=4.0),
    dict(index_pivots=(), thr_cost=8, fast_cost=4, leg_cost=12, stop=0.5),
]


def lune_params(p: Params) -> "lune.Params":
    d = asdict(p)
    for k in ("fill", "latency_ms", "needle", "needle_arm", "needle_give", "needle_wait_s"):
        d.pop(k)
    return lune.Params(**d)


def full(name, day, p, ms=None, px=None):
    if ms is None:
        ms, px = ticks.load(day, name)
    b = Book(SPECS[name], p)
    for m, x in zip(ms, px):
        b.on_tick(x, m)
    b.finish("data_end")
    return b


def check_data(days) -> None:
    section("1 data: local files == Lune database")
    try:
        import hhll_store
        st = hhll_store.connect(maxconn=1)
    except Exception as exc:  # noqa: BLE001
        print(f"  database not reachable ({exc}); files were checked against their SHA-256 on load")
        for d in days:
            for n in ticks.names_on(d):
                ticks.load(d, n)
        return
    try:
        n_ticks = 0
        for d in days:
            db = st.day_ticks(d)
            ok(set(db) >= set(ticks.names_on(d)), f"{d}: index missing in the database")
            for n in ticks.names_on(d):
                ms, px = ticks.load(d, n)
                ok(list(ms) == db[n][0] and list(px) == db[n][1], f"{d} {n}: ticks differ from the database")
                n_ticks += len(ms)
        print(f"  {len(days)} days, {n_ticks:,} ticks identical")
    finally:
        st.close()


def check_pivots(days) -> None:
    section("2 pivots: Lune's literal rules, alternation, no repaint")
    rnd = random.Random(11)
    for trial in range(40):
        R = rnd.choice([0.5, 1.0, 2.5])
        px, x = [], 100.0
        for _ in range(rnd.randint(50, 900)):
            x = round(x + rnd.choice([-1, -0.5, 0, 0.5, 1]) * rnd.choice([0.5, 1, 4]), 2)
            px.append(x)
        for leg in (0.0, 2 * R, 6 * R):
            z, piv = engine.ZigZag(R, leg), []
            for i, v in enumerate(px):
                pv = z.update(v, 1_700_000_000_000 + i)
                if pv:
                    piv.append(pv)
            ok([(p.kind, p.price, p.idx, p.conf_idx, p.forced) for p in piv] == lune.reference_pivots(px, R, leg),
               f"random walk {trial} leg {leg}: differs from the literal rules")
            try:
                lune.check_pivots(px, R, piv, leg)
            except AssertionError as exc:
                ok(False, f"random walk {trial}: {exc}")
    p = bt.live()
    for d in days[-2:]:
        for n in ticks.names_on(d):
            ms, px = ticks.load(d, n)
            b = full(n, d, p, ms, px)
            kinds = [v.kind for v in b.pivots]
            ok(all(a != c for a, c in zip(kinds, kinds[1:])), f"{d} {n}: two pivots of a kind in a row")
            seg = list(px[:30_000])           # literal checker is O(n^2): a real slice
            z, piv = engine.ZigZag(b.zz.R, b.zz.leg), []
            for i, v in enumerate(seg):
                pv = z.update(v, ms[i])
                if pv:
                    piv.append(pv)
            try:
                lune.check_pivots(seg, b.zz.R, piv, b.zz.leg)
            except AssertionError as exc:
                ok(False, f"{d} {n}: {exc}")
            ok([(v.kind, v.price, v.conf_ms) for v in b.pivots if v.conf_idx < len(seg)]
               == [(v.kind, v.price, v.conf_ms) for v in piv], f"{d} {n}: pivots of a prefix differ (repaint)")
    print("  ok")


def check_biglegs(days) -> None:
    section("2b big legs: every leg >= threshold, earliest print, no repaint, no stall, alternation")
    p, rnd, n_piv, small = bt.big(), random.Random(23), 0, None
    ok(all(bt.pivots_of(p, n)[1:] == (bt.BIG_LEG[n], 0.0) for n in bt.NAMES) and set(bt.BIG_LEG) == set(bt.NAMES),
       "big legs: not one threshold per index with no minimum leg")
    ok(replace(p, index_pivots=()) == replace(bt.live(), index_pivots=()), "big legs: a trade rule differs from Lune's")
    for d in days:
        for n in ticks.names_on(d):
            ms, px = ticks.load(d, n)
            b = full(n, d, p, ms, px)
            piv, R = b.pivots, b.zz.R
            ok(b.zz.leg == 0 and b.zz.minor is None, f"{d} {n}: big legs has a minimum leg")
            kinds = [v.kind for v in piv]
            ok(all(a != c for a, c in zip(kinds, kinds[1:])), f"{d} {n}: two pivots of a kind in a row")
            ok(not any(v.forced for v in piv), f"{d} {n}: a forced pivot")
            ok(all(v.leg >= R - 1e-9 for v in piv[1:]), f"{d} {n}: a leg shorter than the threshold")
            ok(all(abs(v.price - v.conf_price) >= R - 1e-9 for v in piv), f"{d} {n}: confirmed short of the threshold")
            # the rule written out again on the session's ticks (the book feeds 09:15-15:30 only): the true
            # extreme, on the first tick R back from it, and nothing due is ever left unprinted (no stall)
            seg = [(m, x) for m, x in zip(ms, px) if engine.OPEN_S <= (m // 1000 + engine.IST_OFFSET_S) % 86400 < engine.CLOSE_S]
            hi = lo = seg[0][1]
            want, k = None, 0                    # walk the ticks: hi/lo = the open swing's extremes
            for m, x in seg:
                hi, lo = max(hi, x), min(lo, x)
                due = "HH" if want != "LL" and hi - x >= R - 1e-9 else "LL" if want != "HH" and x - lo >= R - 1e-9 else None
                got = piv[k] if k < len(piv) and piv[k].conf_ms == m and piv[k].conf_price == x else None
                if due is None and got is None:
                    continue
                if got is None or got.kind != due or got.price != (hi if due == "HH" else lo):
                    ok(False, f"{d} {n} {engine.clock(m)}: pivot late, early, of the wrong kind or off the extreme")
                    break
                k, want, hi, lo = k + 1, "LL" if due == "HH" else "HH", x, x
            else:
                ok(k == len(piv), f"{d} {n}: a pivot printed by no rule")
            for cut in rnd.sample(range(2, len(ms)), 3):          # a printed pivot never changes, not even the newest
                c = full(n, d, p, ms[:cut], px[:cut])
                ok(c.pivots == [v for v in piv if v.conf_idx < c.zz.n], f"{d} {n} cut {cut}: pivots of a prefix differ (repaint)")
            n_piv += len(piv)
            if len(piv) > 1:
                m = min(v.leg / R for v in piv[1:])
                small = m if small is None else min(small, m)
    print(f"  {n_piv} pivots, shortest leg {small:.3f} x threshold")


def check_fastlegs(days) -> None:
    section("2c fast + min leg: literal rules, alternation, no repaint, no stall")
    p = bt.fast()
    ok(all(bt.pivots_of(p, n)[1:] == bt.FAST_LEG[n] for n in bt.NAMES), "fast: not the FAST_LEG pivots")
    _minleg_rules(days, p, random.Random(29))


def check_scaledlegs(days) -> None:
    section("2d scaled legs (the default): literal rules, alternation, no repaint, no stall")
    p = bt.scaled()
    ok(all(bt.pivots_of(p, n) == (10.0, *bt.SCALED_LEG[n]) for n in bt.NAMES) and p.leg_k == bt.SCALED_LEG_K,
       "scaled: not the SCALED_LEG pivots and leg filter")
    ok(replace(p, index_pivots=(), leg_k=0.0) == Params(), "scaled: a trade rule other than the leg filter is not Lune's default")
    _minleg_rules(days, p, random.Random(31))


def _minleg_rules(days, p, rnd) -> None:
    n_piv, n_big, n_forced = 0, 0, 0
    for d in days:
        for n in ticks.names_on(d):
            ms, px = ticks.load(d, n)
            b = full(n, d, p, ms, px)
            piv = b.pivots
            ok(b.zz.leg > b.zz.R, f"{d} {n}: min leg not above the threshold")
            kinds = [v.kind for v in piv]
            ok(all(a != c for a, c in zip(kinds, kinds[1:])), f"{d} {n}: two pivots of a kind in a row")
            seg = [x for m, x in zip(ms, px) if engine.OPEN_S <= (m // 1000 + engine.IST_OFFSET_S) % 86400 < engine.CLOSE_S]
            try:
                lune.check_pivots(seg, b.zz.R, piv, b.zz.leg)
            except AssertionError as exc:
                ok(False, f"{d} {n}: {exc}")
            for cut in rnd.sample(range(2, len(ms)), 3):
                c = full(n, d, p, ms[:cut], px[:cut])
                ok(c.pivots == [v for v in piv if v.conf_idx < c.zz.n], f"{d} {n} cut {cut}: pivots of a prefix differ (repaint)")
            n_piv += len(piv)
            n_forced += sum(v.forced for v in piv)
            n_big += sum(v.leg is not None and v.leg >= b.zz.leg - 1e-9 for v in piv)
    print(f"  {n_piv} pivots, {n_big} legs at least the min leg, {n_forced} forced")


def check_parity(days) -> None:
    section("3 parity: fill='same' == Lune's hhll_tick.Book, every index and day")
    base = bt.live()
    n_tr = 0
    for k, v in enumerate(VARIANTS):
        before = len(FAIL)
        p = replace(base, fill="same", **v)
        lp = lune_params(p)
        for d in days:
            for n in ticks.names_on(d):
                ms, px = ticks.load(d, n)
                a = full(n, d, p, ms, px)
                b = lune.Book(lune.SPECS[n], lp)
                for m, x in zip(ms, px):
                    b.on_tick(x, m)
                b.finish("data_end")
                strip = lambda t: {key: val for key, val in t.items() if key not in ("signal_ms", "exit_signal_ms")}
                ok([strip(t) for t in a.trades] == b.trades, f"variant {k} {d} {n}: trades differ from Lune")
                ok([tuple(asdict(v).values()) for v in a.pivots] == [tuple(asdict(v).values()) for v in b.pivots]
                   and len(a.pivots) > 0, f"variant {k} {d} {n}: pivots differ from Lune")
                ok(a.blocked == b.blocked, f"variant {k} {d} {n}: blocked counts differ from Lune")
                ok(all(t["signal_ms"] == t["entry_ms"] and t["exit_signal_ms"] == t["exit_ms"] for t in a.trades),
                   f"variant {k} {d} {n}: same-tick fill not on its decision tick")
                n_tr += len(a.trades)
        print(f"  variant {k} {v or 'live settings'}: {'identical' if len(FAIL) == before else 'DIFFERENT'}")
    print(f"  {n_tr:,} trades compared")


def _cut_equal(name, day, p, ref, cut, noise: bool) -> None:
    ms, px = ticks.load(day, name)
    ms2, px2 = list(ms[:cut]), list(px[:cut])
    if noise:                                       # a different future: random walk from the cut
        rnd, x = random.Random(cut), px2[-1]
        for m in ms[cut:]:
            x = max(1.0, round(x + rnd.choice([-3, -1, -0.05, 0, 0.05, 1, 3]) * rnd.random(), 2))
            ms2.append(m)
            px2.append(x)
    b = full(name, day, p, ms2, px2)
    last = ms[cut - 1]
    # everything decided AND filled before the cut's last tick must be identical
    sure = lambda tr: [t for t in tr if t["exit_ms"] < last]
    ok(sure(b.trades) == sure(ref.trades), f"{day} {name} cut {cut} noise={noise}: trades before the cut differ")
    ok([v for v in b.pivots if v.conf_idx < cut] == [v for v in ref.pivots if v.conf_idx < cut],
       f"{day} {name} cut {cut} noise={noise}: pivots before the cut differ")
    entries = lambda bk: [(t["signal_ms"], t["entry_ms"], t["entry_px"], t["side"]) for t in bk.trades if t["entry_ms"] < last]
    ok(entries(b) == entries(ref), f"{day} {name} cut {cut} noise={noise}: entries before the cut differ")


def check_lookahead(days) -> None:
    section("4 lookahead: cut / replaced future never changes the past")
    rnd = random.Random(5)
    n = 0
    for v in (VARIANTS[1], VARIANTS[3], VARIANTS[4]):
        for fill in ("next", "same"):
            p = replace(bt.live(), fill=fill, **v)
            for d in days:
                for name in ticks.names_on(d):
                    size = ticks.count(name, d)
                    ref = full(name, d, p)
                    for cut in sorted(rnd.sample(range(2, size), 2)):
                        _cut_equal(name, d, p, ref, cut, noise=False)
                        _cut_equal(name, d, p, ref, cut, noise=True)
                        n += 2
    print(f"  {n} cuts identical")


def check_fills(days) -> None:
    section("5 fills: next tick after decision + latency, at its price +/- friction")
    n = 0
    for lat in (0, 250, 2000):
        for v in (VARIANTS[0], VARIANTS[1], VARIANTS[4], VARIANTS[5]):
            p = replace(bt.live(), fill="next", latency_ms=lat, **v)
            for d in days:
                for name in ticks.names_on(d):
                    ms, px = ticks.load(d, name)
                    b = full(name, d, p, ms, px)
                    fr = engine.friction_pts(SPECS[name])
                    for t in b.trades:
                        side = 1 if t["side"] == "LONG" else -1
                        forced = t["reason"] == "data_end"
                        limit_fill = p.order == "limit" and t["friction_pts"] == round(fr, 2)  # resting limit: fills itself
                        if not limit_fill and not (forced and t["entry_ms"] == ms[-1]):
                            j = bisect_right(ms, t["signal_ms"] + lat)
                            ok(j < len(ms) and ms[j] == t["entry_ms"] and t["entry_ms"] > t["signal_ms"] + lat,
                               f"{d} {name} {t['entry']}: entry not on the first tick after decision + {lat} ms")
                            ok(j < len(ms) and abs(t["entry_px"] - round(px[j] + side * fr, 2)) < 1e-9,
                               f"{d} {name} {t['entry']}: entry price is not that tick's price + friction")
                        if not forced:
                            j = bisect_right(ms, t["exit_signal_ms"] + lat)
                            ok(j < len(ms) and ms[j] == t["exit_ms"] and t["exit_ms"] > t["exit_signal_ms"] + lat,
                               f"{d} {name} {t['exit']}: exit not on the first tick after decision + {lat} ms")
                            ok(j < len(ms) and abs(t["exit_px"] - round(px[j] - side * fr, 2)) < 1e-9,
                               f"{d} {name} {t['exit']}: exit price is not that tick's price - friction")
                        ok(t["exit_ms"] >= t["entry_ms"], f"{d} {name} {t['entry']}: exit before entry")
                        n += 1
    print(f"  {n:,} trades checked")


def check_money(days) -> None:
    section("6 money: every trade and the totals re-derived")
    p = bt.live()
    tr = bt.run(p, days, bt.NAMES)
    for t in tr:
        side = 1 if t["side"] == "LONG" else -1
        gross = round(side * (t["exit_px"] - t["entry_px"]), 2)
        ch = engine.round_trip_charges(SPECS[t["index"]], p.lots, side, t["entry_px"], t["exit_px"], t["entry_ms"], t["exit_ms"])
        ok(abs(t["gross_pts"] - gross) < 1e-9, f"{t['index']} {t['day']} {t['entry']}: gross")
        ok(abs(t["charges_rs"] - round(ch["total"], 2)) < 1e-9 and ch["orders"] == 4 * -(-t["units"] // SPECS[t["index"]].freeze),
           f"{t['index']} {t['day']} {t['entry']}: charges")
        ok(abs(t["net_rs"] - round(gross * t["units"] - ch["total"], 2)) < 1e-6, f"{t['index']} {t['day']} {t['entry']}: net")
        ok(t["charges_rs"] > 0 and t["units"] == p.lots * SPECS[t["index"]].lot, f"{t['index']} {t['day']}: units / charges")
    s = bt.stats(tr, days)
    ok(abs(s["net"] - sum(t["net_rs"] for t in tr)) < 0.05, "stats net")
    ok(abs(s["net"] - sum(x["net"] for x in s["by_day"])) < 0.05, "by_day adds up")
    ok(abs(s["net"] - sum(x["net"] for x in s["by_index"])) < 0.05, "by_index adds up")
    ok(abs(s["net"] - (s["h1"] + s["h2"])) < 0.05, "halves add up")
    ok(s["trades"] == sum(x["trades"] for x in s["by_reason"]), "by_reason adds up")
    # hand check of one order (Lune's own selfcheck figures)
    b = engine.order_charges("NFO", False, 65, 100.0)
    ok(sum(b.values()) == engine.Decimal("18.39"), "order charges by hand")
    print(f"  {len(tr)} trades, net Rs {s['net']:,.2f}")


def check_sessions(days) -> None:
    section("7 sessions: back to back == one fresh book a day")
    for v in (VARIANTS[0], VARIANTS[1]):
        p = replace(bt.live(), **v)
        for name in bt.NAMES:
            b = Book(SPECS[name], p)
            per_day = []
            for d in days:
                if name not in ticks.names_on(d):
                    continue
                ms, px = ticks.load(d, name)
                for m, x in zip(ms, px):
                    b.on_tick(x, m)
                per_day += full(name, d, p, ms, px).trades
            b.finish("data_end")
            ok(b.trades == per_day, f"{name}: a day's trades depend on the day before")
    print("  ok")


def check_tuning(days) -> None:
    section("8 tuning: sweep == direct runs; walk-forward never sees the day it trades")
    base = bt.live()
    name = "MIDCPNIFTY"
    thr = bt.pivots_of(base, name)[1]
    sw = bt.sweep(base, [("threshold", [thr * 0.75, thr, thr * 1.5]), ("stop", [0, 1.0])], name, days)
    for r in sw["rows"]:
        p = bt.with_value(bt.with_value(base, "threshold", r["values"][0], name), "stop", r["values"][1], name)
        direct = bt.stats(bt.run(p, days, [name]), days)
        ok([x["net"] for x in direct["by_day"]] == r["daily"] and direct["trades"] == r["trades"],
           f"sweep row {r['values']} differs from a direct run")
    direct = bt.stats(bt.run(base, days, [name]), days)
    ok([x["net"] for x in direct["by_day"]] == sw["base"]["daily"], "sweep base differs from a direct run")
    ok(any(r["is_base"] for r in sw["rows"]), "the settings in force are in the grid")
    wf = bt.walk_forward(sw)
    rnd = random.Random(3)
    for k, row in enumerate(wf["rows"]):
        i = sw["days"].index(row["day"])
        best = max(range(len(sw["rows"])), key=lambda c: sum(sw["rows"][c]["daily"][:i]))
        ok(row["pick"] == sw["rows"][best]["values"], f"walk-forward {row['day']}: not the best of the days before")
        ok(row["day"] not in row["train"] and all(t < row["day"] for t in row["train"]), f"walk-forward {row['day']}: trained on itself or later")
        fake = {**sw, "rows": [{**r, "daily": r["daily"][:i] + [rnd.uniform(-1e6, 1e6) for _ in r["daily"][i:]]}
                               for r in sw["rows"]]}
        again = bt.walk_forward(fake)["rows"][k]
        ok(again["pick"] == row["pick"], f"walk-forward {row['day']}: the pick moved when that day or later changed")
    for train in (1, 2, 3):
        for row in bt.walk_forward(sw, train)["rows"]:
            ok(len(row["train"]) == train and row["train"][-1] < row["day"], f"walk-forward train={train}: window")
    print(f"  {len(sw['rows'])} combinations x {len(days)} days; walk-forward {len(wf['rows'])} test days, "
          f"out of sample Rs {wf['net']:,.0f} vs settings in force Rs {wf['base']:,.0f}")


def check_inputs(days) -> None:
    section("9 inputs: bad settings are refused")
    good = bt.params_to(bt.live())
    ok(bt.params_from(good) == bt.live(), "settings do not survive a round trip through the UI form")
    for key, bad in (("needle", "avg"), ("needle", 100), ("needle_give", 0), ("needle_give", 1.5), ("needle_arm", -1), ("needle_wait_s", -5), ("stop", -1), ("stop", "x"), ("last_entry", "16:00"), ("last_entry", "15:05"), ("lots", 1.5), ("order", "stop"),
                     ("fill", "prev"), ("latency_ms", float("nan")), ("lock_trail", 0)):
        try:
            bt.params_from({**good, key: bad})
            ok(False, f"{key}={bad!r} was accepted")
        except ValueError:
            pass
    try:
        bt.params_from({**good, "pivots": {**good["pivots"], "NIFTY": [0, 1, 1]}})
        ok(False, "R = 0 was accepted")
    except ValueError:
        pass
    try:
        bt.combos_of(bt.live(), [("threshold", [1, 2])], None)
        ok(False, "a per-index setting swept over every index")
    except ValueError:
        pass
    print("  ok")


NEEDLES = [("giveback", 1.0, 0.5, 0), ("giveback", 0.5, 0.3, 0), ("entry", 2.0, 0.5, 0), ("entry", 0.0, 0.5, 0),
           ("stall", 1.0, 0.7, 1800), ("stall", 1.0, 1.0, 600)]


def needle_ends(mode: str, arm: float, give: float, wait_s: float, R: float, side: int, D: float, E: float,
                y: float, ms: int, ext_ms: int) -> bool:
    """The needle rules in exact paise (no floats): the move |E - D| has reached arm x R and
    the price y has given back `give` of it (all of it for "entry"; "stall": and E is wait_s old)."""
    paise = lambda x: round(x * 100)
    move, back = side * (paise(E) - paise(D)), side * (paise(E) - paise(y))
    g = Fraction(1) if mode == "entry" else Fraction(str(give))
    if mode == "stall" and ms - ext_ms < wait_s * 1000:
        return False
    return move >= Fraction(str(arm)) * paise(R) and back >= g * move


def check_needle(days) -> None:
    section("10 needle: re-derived from the ticks, trades end with it, no lookahead")
    rnd = random.Random(17)
    n_needles = n_trades = 0
    sod = lambda ms_: (ms_ // 1000 + engine.IST_OFFSET_S) % 86400
    for mode, arm, give, wait in NEEDLES:
        for v in (VARIANTS[0], VARIANTS[3], VARIANTS[4]):
            for fill in ("next", "same"):
                p = replace(bt.scaled(), fill=fill, needle=mode, needle_arm=arm, needle_give=give, needle_wait_s=wait, **v)
                tag = f"{mode} {arm} {give} {wait} {fill}"
                for d in days:
                    for name in ticks.names_on(d):
                        ms, px = ticks.load(d, name)
                        b = full(name, d, p, ms, px)
                        sess = [(m, x) for m, x in zip(ms, px) if engine.OPEN_S <= sod(m) < engine.CLOSE_S]
                        pv, nd = b.pivots, b.needles
                        ok(len(nd) == len(pv), f"{d} {name} {tag}: {len(nd)} needles for {len(pv)} pivots")
                        for k, (a, x) in enumerate(zip(pv, nd)):
                            ok((x["kind"], x["ms"], x["price"], x["conf_ms"], x["conf_px"]) == (a.kind, a.ms, a.price, a.conf_ms, a.conf_price),
                               f"{d} {name} {tag}: needle {k} is not its pivot's")
                            side, ext, ext_ms = (-1 if a.kind == "HH" else 1), a.conf_price, a.conf_ms
                            stop = pv[k + 1].conf_idx if k + 1 < len(pv) else len(sess) - 1
                            why, end = None, None
                            for j in range(a.conf_idx + 1, stop + 1):
                                m, y = sess[j]
                                if side * (y - ext) > 0:
                                    ext, ext_ms = y, m
                                elif needle_ends(mode, arm, give, wait, b.R, side, a.conf_price, ext, y, m, ext_ms):
                                    why, end = "fade", (m, y)
                                    break
                            if why is None:
                                why, end = ("pivot", sess[stop]) if k + 1 < len(pv) else ("end", (ms[-1], px[-1]))
                            ok((x["why"], x["end_ms"], x["end_px"], x["ext"], x["ext_ms"]) == (why, *end, ext, ext_ms),
                               f"{d} {name} {tag} {v}: needle {k} {(x['why'], x['end_ms'], x['ext'])} "
                               f"!= re-derived {(why, end[0], ext)}")
                            ok(side * (x["ext"] - x["price"]) >= 0, f"{d} {name} {tag}: needle {k} extreme beyond its pivot")
                            n_needles += 1
                        conf = [x["conf_ms"] for x in nd]
                        for t in b.trades:
                            x = nd[bisect_right(conf, t["signal_ms"]) - 1]
                            ok(t["pivot"] == x["kind"] and t["pivot_px"] == x["price"],
                               f"{d} {name} {tag} {t['entry']}: trade is not on its needle's pivot")
                            ok(t["exit_signal_ms"] <= x["end_ms"], f"{d} {name} {tag} {t['entry']}: trade outlives its needle")
                            ok((t["reason"] == "needle") == (x["why"] == "fade" and t["exit_signal_ms"] == x["end_ms"]
                                                             and t["reason"] not in ("stop", "lock", "target", "time", "wide", "circuit", "square_off")),
                               f"{d} {name} {tag} {t['entry']}: exit reason {t['reason']} vs needle {x['why']}")
                            ok(sod(t["signal_ms"]) < engine.LAST_ENTRY_S, f"{d} {name} {t['entry']}: entry from 15:00")
                            ok(sod(t["exit_signal_ms"]) <= engine.SQUARE_OFF_S or t["reason"] == "data_end",
                               f"{d} {name} {t['exit']}: still open after 15:10")
                            n_trades += 1
                        # the needle only ever ends trades sooner: pivots never change
                        ok([(q.kind, q.price, q.conf_ms) for q in b.pivots]
                           == [(q.kind, q.price, q.conf_ms) for q in full(name, d, replace(p, needle="off"), ms, px).pivots],
                           f"{d} {name} {tag}: the needle changed a pivot")
                        if v is VARIANTS[0] and fill == "next" and arm == 1.0 and give in (0.5, 0.7):
                            for cut in sorted(rnd.sample(range(2, len(ms)), 2)):
                                _cut_equal(name, d, p, b, cut, noise=False)
                                _cut_equal(name, d, p, b, cut, noise=True)
    ok(not full("NIFTY", days[-1], bt.scaled()).needles, "needle off made needles")
    print(f"  {n_needles:,} needles and {n_trades:,} trades checked")


def main() -> int:
    t0 = time.time()
    days = ticks.days()
    if not days:
        print("no tick data: run `python ticks.py` (sync) first")
        return 1
    print(f"{len(days)} sessions {days[0]}..{days[-1]}, {sum(r['ticks'] for r in ticks.inventory()):,} ticks")
    only = set(sys.argv[1:])        # e.g. `python selfcheck.py parity tuning`; none: everything
    for f in (check_data, check_pivots, check_biglegs, check_fastlegs, check_scaledlegs, check_parity, check_lookahead, check_fills, check_money, check_sessions,
              check_tuning, check_inputs, check_needle):
        if not only or f.__name__[6:] in only:
            f(days)
    print(f"\n{'FAILED: ' + str(len(FAIL)) if FAIL else 'ALL CHECKS PASSED'} in {time.time() - t0:.0f}s")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
