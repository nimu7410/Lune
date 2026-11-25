"""Strategy v3: v2's index waterfall + an overnight-shock guard + a two-candle carry window.  One position, one fixed
lot, long only, entries strictly before 15:00, intraday exits before 15:00, overnight exits at the next 09:15 open.

Setups (signals on COMPLETED candles, filled at the NEXT candle's open; gates, waterfall and costs are v2's):
  A  momentum-hold   11:15-14:15 candle: close - open >= 0.40 ATR14, close > running mean, close > SMA200, drop and
                     cost gates.  Entry-day stop entry - 1 ATR; exit at the next 09:15 open.
  B  strict carry    14:45 or 14:50 candle (the first that qualifies; v2 used 14:50 only): close > session open,
                     close > running mean, SMA200, gates.  Filled 14:50 / 14:55, exit at the next 09:15 open.
Overnight-shock guard: a session whose NIFTY 09:15 open is >= SHOCK_ATR x NIFTY ATR14 below NIFTY's previous close
  is a shock session (known at the open).  On it, A trades are closed at the 14:55 open instead of being held
  overnight, and B is off: no new overnight risk right after an overnight shock (volatility clusters).
Edge-health monitor (edge_ok) is a live alert only: it flags an index whose last 120 B-condition nights averaged
  <= 0 net.  Both changes were pre-registered and checked once on Validate/Hidden (results/preregistration_v3*.json).
"""
import numpy as np
import pandas as pd
import strategy_v2 as V2
from strategy_v2 import ROOT, SPLITS, LOTS, P, WATERFALL, Market, charges, metrics

SHOCK_ATR = 0.75
B_CANDLES = (66, 67)                                   # 14:45 and 14:50 candles
MONITOR = dict(nights=120, threshold=0.0)
CARRY, STOP, INTRADAY = 1, 2, 3                        # holds: no stop | entry-day stop | entry-day stop + 14:55 exit


def shock_sessions(m):
    """True on sessions whose NIFTY open gapped >= SHOCK_ATR x ATR14 below the previous close (prior-session data)."""
    k = m.idx['NIFTY']
    with np.errstate(invalid='ignore', divide='ignore'):
        gap = (m.O[k, :, 0] - m.pC[k]) / m.atr[k]
    return np.nan_to_num(gap, nan=0.0) <= -SHOCK_ATR


def setup_signal(m, setup, k, gated=True):
    """A: v2's signal.  B: v2's conditions on the first of the 14:45 / 14:50 candles that meets them."""
    if setup == 'A':
        return V2.setup_signal(m, 'A', k, gated)
    sig = np.zeros(m.C[k].shape, bool)
    for b in B_CANDLES:
        sig |= V2.setup_signal(m, 'B', k, gated, params=dict(carry_bar=b))
    first = np.zeros_like(sig)
    has = sig.any(1)
    first[np.flatnonzero(has), np.argmax(sig, 1)[has]] = True
    return first


def build_slots(m, waterfall=WATERFALL):
    """(setup, k, hold, signal) in waterfall order.  Each A slot is split into its overnight form (normal sessions)
    and its intraday form (shock sessions); the two never fire on the same session."""
    sh = shock_sessions(m)[:, None]
    slots = []
    for setup, s in waterfall:
        if s not in m.idx:
            continue
        k = m.idx[s]
        sig = setup_signal(m, setup, k)
        if setup == 'A':
            slots += [('A', k, STOP, sig & ~sh), ('A', k, INTRADAY, sig & sh)]
        else:
            slots.append(('B', k, CARRY, sig & ~sh))
    return slots


def edge_ok(m, nights=MONITOR['nights'], threshold=MONITOR['threshold']):
    """Live alert: False where the mean net INR of k's last `nights` B-condition nights before d is <= threshold."""
    nI, nD = m.valid.shape
    ok = np.ones((nI, nD), bool)
    b0 = P['carry_bar'] + 1
    for k, s in enumerate(m.names):
        sig = V2.setup_signal(m, 'B', k, gated=False)[:, P['carry_bar']]
        d = np.flatnonzero(sig[:-1] & m.valid[k, 1:])
        e, x = m.O[k, d, b0], m.O[k, d + 1, 0]
        pnl = (x - e) * m.lot[k] - np.array([charges(s, m.cal[i], p, m.lot[k]) for i, p in zip(d, e)])
        done = np.searchsorted(d + 1, np.arange(nD), side='right')
        csum = np.r_[0.0, np.cumsum(pnl)]
        mean = (csum[done] - csum[np.maximum(done - nights, 0)]) / nights
        ok[k] = (done < nights) | (mean > threshold)
    return ok


class Engine:
    """Two-phase event engine for historical replay and live operation (strategy_v2.Engine + the intraday hold).

    on_open(d, b): exits due at this open (next-session 09:15 exit; 14:55 exit of an intraday hold; gap through an
    entry-day stop), then the pending entry.  on_close(d, b): the entry-day resting stop, then - if flat - the first
    live slot schedules an entry at the next open.  A missing session of the held index blocks the book until it can
    be exited at the next observed open ('data_gap_recovery')."""

    def __init__(self, market, slots, entry_ok=None, extra_points=0.0):
        self.m, self.slots = market, slots
        self.entry_ok = np.ones(len(market.cal), bool) if entry_ok is None else entry_ok
        self.extra = float(extra_points)
        self.position, self.pending, self.gap, self.trades, self.last = None, None, False, [], (-1, 74)
        self.by_bar = {}
        for j, (_, k, _, sig) in enumerate(slots):
            for d, b in zip(*np.nonzero(sig)):
                self.by_bar.setdefault((int(d), int(b)), []).append(j)

    def _close(self, d, b, price, reason):
        p, m = self.position, self.m
        k = p['k']
        exit_price = price - self.extra
        cost = charges(m.names[k], m.cal[p['d0']], p['entry'], m.lot[k])
        gross = (exit_price - p['entry']) * m.lot[k]
        self.trades.append(dict(sym=m.names[k], setup=p['setup'], d0=p['d0'], b0=p['b0'], d1=d, b1=b,
                                entry=p['entry'], exit=exit_price, gross=gross, cost=cost, pnl=gross - cost,
                                reason=reason, data_gap=self.gap))
        self.position, self.gap = None, False

    def on_open(self, d, b):
        if (d, b) <= self.last:
            raise ValueError('events must be strictly increasing')
        self.last = (d, b)
        m, p = self.m, self.position
        pending, self.pending = self.pending, None
        if p is not None:
            k = p['k']
            if not m.valid[k, d]:
                self.gap = True
                return
            o = m.O[k, d, b]
            if d > p['d0'] and b == 0:
                self._close(d, b, o, 'data_gap_recovery' if self.gap else 'next_open')
            elif p['intraday'] and d == p['d0'] and b == P['last_entry_bar']:
                self._close(d, b, o, 'intraday_exit')
            elif p['stop'] is not None and d == p['d0'] and o <= p['stop']:
                self._close(d, b, o, 'stop_gap')
            if self.position is not None:
                return
        if pending is not None and pending[0] == (d, b) and b <= P['last_entry_bar']:
            setup, k, hold, _ = self.slots[pending[1]]
            if m.valid[k, d]:
                entry = m.O[k, d, b] + self.extra
                stop = entry - P['stop'] * m.atr[k, d] if hold in (STOP, INTRADAY) else None
                self.position = dict(k=k, setup=setup, entry=entry, d0=d, b0=b, stop=stop, intraday=hold == INTRADAY)

    def on_close(self, d, b):
        m, p = self.m, self.position
        if p is not None and m.valid[p['k'], d] and p['stop'] is not None and d == p['d0'] \
                and m.L[p['k'], d, b] <= p['stop']:
            self._close(d, b + 1, p['stop'], 'stop')
        if self.position is not None or b + 1 > P['last_entry_bar'] or not self.entry_ok[d]:
            return
        for j in self.by_bar.get((d, b), ()):
            if m.valid[self.slots[j][1], d]:
                self.pending = ((d, b + 1), j)
                break


def backtest(market, slots, d_lo=0, d_hi=None, entry_ok=None, extra_points=0.0):
    d_hi = len(market.cal) - 1 if d_hi is None else d_hi
    e = Engine(market, slots, entry_ok, extra_points)
    for d in range(d_lo, d_hi + 1):
        for b in range(75):
            e.on_open(d, b)
            e.on_close(d, b)
    t = pd.DataFrame(e.trades, columns=['sym', 'setup', 'd0', 'b0', 'd1', 'b1', 'entry', 'exit', 'gross', 'cost',
                                        'pnl', 'reason', 'data_gap'])
    t.attrs['open_position'] = e.position
    t.attrs['execution_complete'] = e.position is None
    return t
