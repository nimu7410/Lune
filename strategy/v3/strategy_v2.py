"""Strategy v2: core-first index waterfall on 5-minute data - one position, one fixed lot, long only.

v2 = the original v1 momentum/carry core, unchanged, plus warm-up history (indicators ready from the first
session) and one more index traded with the same rules (Nifty Next 50).  A wider expansion (91 stock-option
underlyings plus a quiet-day index carry) looked strong on Tune but lost to v1 on Validate/Hidden, so it was
rejected (README.md, research record).

Setups (signals on COMPLETED candles, filled at the NEXT candle's open, entries strictly before 15:00):
  A  momentum-hold  11:15-14:15 candle: close-open >= 0.40 x prior daily ATR, close > running mean price,
                    close > SMA200 of prior closes, drop gate (close-prev close > -1 ATR), cost gate.
                    Resting stop entry - 1 ATR on the entry day only; else exit at the next 09:15 open.
  B  strict carry   14:50 candle: close > session open, close > running mean price, SMA200, gates.
                    Filled 14:55, exit at the next session's 09:15 open (strictly before 09:16).
Waterfall (first live slot wins at every completed candle; A slots fire 11:15-14:15, B slots at 14:50):
  instruments with Tune history are ordered by Tune expectancy lower-confidence-bound (research_v2.py);
  MIDCPNIFTY (no Tune history) keeps the position v1 assigned it (first);
  NIFTYNXT50 (added, no Tune history, options since 2024-04-24) is appended last.
Costs per round trip = 2 x Zerodha option charges (premium estimated as 0.8% of spot) + slippage:
  v1 indices 5 points (before 2021) / 10 points x lot as in v1; NIFTYNXT50 max(10 pts, 0.05%).
PnL (INR) = (exit - entry) x lot - costs, with 1 index point = 1 INR per unit of lot.
"""
from pathlib import Path
import numpy as np
import pandas as pd
import strategy as BASE

ROOT = Path(__file__).resolve().parent
GIVEN = list(BASE.PRIORITY)                          # v1 order: MIDCPNIFTY, SENSEX, BANKNIFTY, FINNIFTY, NIFTY
EXTRA = {'NIFTYNXT50': dict(lot=25, available_from='2024-04-24', slip_pct=0.0005)}
LOTS = {**dict(BASE.LOTS), **{s: v['lot'] for s, v in EXTRA.items()}}
AVAILABLE_FROM = {**{s: pd.Timestamp(v) for s, v in BASE.AVAILABLE_FROM.items()},
                  **{s: pd.Timestamp(v['available_from']) for s, v in EXTRA.items()}}
BSE = {'SENSEX'}
SPLITS = {'tune': ('2015-01-09', '2022-01-07'), 'validate': ('2022-01-10', '2024-05-09'),
          'hidden': ('2024-05-10', '2026-09-04')}
P = dict(k=0.40, stop=1.0, m_lo=24, m_hi=60, carry_bar=67, last_entry_bar=68, drop=1.0, cov=20.0,
         idx_prem=0.008)
WATERFALL = [('A', 'MIDCPNIFTY'), ('A', 'SENSEX'), ('A', 'BANKNIFTY'), ('A', 'FINNIFTY'), ('A', 'NIFTY'),
             ('A', 'NIFTYNXT50'),
             ('B', 'MIDCPNIFTY'), ('B', 'SENSEX'), ('B', 'BANKNIFTY'), ('B', 'NIFTY'), ('B', 'FINNIFTY'),
             ('B', 'NIFTYNXT50')]
HOLD = {'A': 2, 'B': 1}                             # 2: entry-day resting stop, 1: plain carry


class Market:
    """Validated 5-minute index bars on the v1-index master calendar plus causal daily features."""

    def __init__(self, data_root=ROOT / 'data', through=None, live=False, extra=tuple(EXTRA)):
        root = Path(data_root)
        frames = BASE.load_frames(str(root), live=live, through=through)
        self.cal = pd.DatetimeIndex(sorted(set().union(
            *(set(pd.DatetimeIndex(f.Datetime).normalize()) for f in frames.values()))))
        for s in extra:
            f = self._extra(root / 'indices_5m' / f'{s}.parquet', through, live)
            if len(f):
                frames[s] = f
        self.names = [s for s in GIVEN + list(extra) if s in frames]
        self.idx = {s: k for k, s in enumerate(self.names)}
        self.lot = np.array([LOTS[s] for s in self.names], float)
        nI, nD = len(self.names), len(self.cal)
        self.O, self.H, self.L, self.C = (np.full((nI, nD, 75), np.nan) for _ in range(4))
        for k, s in enumerate(self.names):
            ts = pd.DatetimeIndex(frames[s].Datetime)
            di = self.cal.get_indexer(ts.normalize())
            b = ((ts.hour * 60 + ts.minute - 555) // 5).to_numpy()
            ok = (di >= 0) & (b >= 0) & (b < 75)
            for A, col in ((self.O, 'Open'), (self.H, 'High'), (self.L, 'Low'), (self.C, 'Close')):
                A[k, di[ok], b[ok]] = frames[s][col].to_numpy(float)[ok]
        self.valid = np.isfinite(self.O[:, :, 0]) & (np.isfinite(self.C).all(2) | live)
        for A in (self.O, self.H, self.L, self.C):
            A[~self.valid] = np.nan
        self.available = np.ones((nI, nD), bool)
        for s, first in AVAILABLE_FROM.items():
            if s in self.idx:
                self.available[self.idx[s]] = self.cal >= first
        self._daily(root)

    @staticmethod
    def _extra(path, through, live):
        """Extra index: keep only exact 75-bar days (a gap-free prefix for the live last day)."""
        if not path.exists():
            return pd.DataFrame(columns=['Datetime', 'Open', 'High', 'Low', 'Close'])
        df = pd.read_parquet(path)
        if through is not None:
            df = df[df.Datetime < pd.Timestamp(through) + pd.Timedelta(days=1)]
        day = df.Datetime.dt.normalize()
        tod = df.Datetime.dt.hour * 60 + df.Datetime.dt.minute
        p = df[['Open', 'High', 'Low', 'Close']].to_numpy(float)
        ok = (np.isfinite(p).all(1) & (p > 0).all(1) & (p[:, 1] >= p[:, [0, 2, 3]].max(1))
              & (p[:, 2] <= p[:, [0, 1, 3]].min(1)) & (tod % 5 == 0))
        g = pd.DataFrame({'day': day, 'ok': ok, 'tod': tod}).groupby('day').agg(
            n=('tod', 'size'), u=('tod', 'nunique'), ok=('ok', 'all'), t0=('tod', 'min'), t1=('tod', 'max'))
        good = (g.n == g.u) & g.ok & (g.t0 == 555) & ((g.n == 75) & (g.t1 == 925)
                                                      | (live & (g.index == g.index.max()) & (g.t1 == 555 + 5 * (g.n - 1))))
        return df[day.isin(g.index[good])]

    def _daily(self, root):
        """ATR(14, Wilder), SMA200 and previous close from COMPLETED sessions only (shifted one session).

        Daily bars come from the validated 5-minute sessions; other sessions (warm-up years, dropped
        dates) come from the independent daily reference, never from repaired 5-minute data."""
        nI, nD = len(self.names), len(self.cal)
        self.atr, self.sma, self.pC = (np.full((nI, nD), np.nan) for _ in range(3))
        for k, s in enumerate(self.names):
            v = self.valid[k] & np.isfinite(self.C[k, :, 74]) & np.isfinite(self.H[k]).all(1) & np.isfinite(self.L[k]).all(1)
            d5 = pd.DataFrame({'Open': self.O[k, v, 0], 'High': self.H[k, v].max(1), 'Low': self.L[k, v].min(1),
                               'Close': self.C[k, v, 74]}, index=self.cal[v])
            parts = [d5]
            for f in (root / 'daily_ref' / f'{s}.csv', root / 'warmup_extra' / f'{s}.csv'):
                if f.exists():
                    ref = pd.read_csv(f, parse_dates=['Date']).set_index('Date')
                    for col in ('Open', 'High', 'Low'):
                        if col not in ref:
                            ref[col] = ref['Close']            # close-only history: range unknown
                    have = pd.DatetimeIndex(sorted(set().union(*(set(p.index) for p in parts))))
                    parts.append(ref.loc[(ref.index <= self.cal[-1]) & ~ref.index.isin(have), ['Open', 'High', 'Low', 'Close']])
            dd = pd.concat(parts).sort_index()
            prev = dd.Close.shift(1)
            tr = np.maximum(dd.High - dd.Low, np.maximum((dd.High - prev).abs(), (dd.Low - prev).abs()))
            f = pd.DataFrame({'atr': tr.ewm(alpha=1 / 14, adjust=False).mean().shift(1),
                              'sma': dd.Close.rolling(200).mean().shift(1), 'pC': prev}).reindex(self.cal)
            self.atr[k], self.sma[k], self.pC[k] = f.atr.to_numpy(), f.sma.to_numpy(), f.pC.to_numpy()


def setup_signal(m, setup, k, gated=True, params=None):
    """Boolean (session, candle) signals for one instrument from completed candles only.
    gated=False ignores option-listing dates (only to rank on the underlying's own Tune history)."""
    Q = P if params is None else {**P, **params}
    O, H, L, C = m.O[k], m.H[k], m.L[k], m.C[k]
    vw = np.cumsum((H + L + C) / 3.0, axis=1) / np.arange(1, 76)       # running mean typical price
    atr, sma, pC, dO = m.atr[k], m.sma[k], m.pC[k], O[:, 0]
    ok = m.valid[k] & np.isfinite(atr) & (atr > 0) & np.isfinite(sma)
    if gated:
        ok &= m.available[k]
    cost_pts = np.where(m.cal.year < 2021, 5.0, 10.0)
    sig = np.zeros(C.shape, bool)
    with np.errstate(invalid='ignore', divide='ignore'):
        if setup == 'A':
            bars = np.arange(75)
            cond = ok[:, None] & (bars >= Q['m_lo']) & (bars <= Q['m_hi'])
            cond &= (C - dO[:, None] >= Q['k'] * atr[:, None]) & (C > vw) & (C > sma[:, None])
            cond &= (C - pC[:, None]) / atr[:, None] > -Q['drop']
            cond &= (atr >= Q['cov'] * cost_pts)[:, None]
            first, has = np.argmax(cond, 1), cond.any(1)
            sig[np.flatnonzero(has), first[has]] = True
        elif setup == 'B':
            b = Q['carry_bar']
            c = C[:, b]
            sig[:, b] = (ok & (c > dO) & (c > vw[:, b]) & (c > sma) & ((c - pC) / atr > -Q['drop'])
                         & (atr >= Q['cov'] * cost_pts))
    return sig


def build_slots(m, waterfall=WATERFALL, params=None):
    return [(setup, m.idx[s], HOLD[setup], setup_signal(m, setup, m.idx[s], params=params))
            for setup, s in waterfall if s in m.idx]


def charges(symbol, date, entry_price, lot):
    """2 x Zerodha index-option charges (premium estimated as 0.8% of spot) + slippage, INR per round trip."""
    d = pd.Timestamp(date)
    prem = P['idx_prem'] * entry_price * lot
    stt = 0.00017 if d < pd.Timestamp('2016-06-01') else 0.0005 if d < pd.Timestamp('2023-04-01') \
        else 0.000625 if d < pd.Timestamp('2024-10-01') else 0.001
    bse = symbol in BSE
    exch = (0.0005 if bse else 0.00053) if d < pd.Timestamp('2024-10-01') else (0.000325 if bse else 0.0003503)
    gst = 0.15 if d < pd.Timestamp('2017-07-01') else 0.18
    brokerage = 40.0                                          # Rs 20 per executed order
    ex, sebi = exch * 2 * prem, 1e-6 * 2 * prem
    zerodha = brokerage + stt * prem + ex + sebi + gst * (brokerage + ex + sebi) + 0.00003 * prem
    points = 5.0 if d.year < 2021 else 10.0
    slip = points * lot
    if symbol in EXTRA:
        slip = max(slip, EXTRA[symbol]['slip_pct'] * entry_price * lot)
    return 2.0 * zerodha + slip


class Engine:
    """Two-phase event engine used for both historical replay and live operation.

    on_open(d, b): exits due at this open (next-session 09:15 exit; gap through an entry-day stop), then
    the pending entry.  on_close(d, b): the entry-day resting stop, then - if flat - the first live slot
    schedules an entry at the next open.  A missing session of the held index blocks the book until it can
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
            elif p['stop'] is not None and d == p['d0'] and o <= p['stop']:
                self._close(d, b, o, 'stop_gap')
            if self.position is not None:
                return
        if pending is not None and pending[0] == (d, b) and b <= P['last_entry_bar']:
            setup, k, hold, _ = self.slots[pending[1]]
            if m.valid[k, d]:
                entry = m.O[k, d, b] + self.extra
                stop = entry - P['stop'] * m.atr[k, d] if hold == 2 else None
                self.position = dict(k=k, setup=setup, entry=entry, d0=d, b0=b, stop=stop)

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


def metrics(t, sessions):
    if len(t) == 0:
        return dict(trades=0, net_inr=0.0, win_rate=0.0, expectancy=0.0, max_drawdown=0.0,
                    participation_pct=0.0, per_session_inr=0.0)
    curve = np.concatenate(([0.0], t.pnl.cumsum().to_numpy()))
    return dict(trades=int(len(t)), net_inr=float(t.pnl.sum()), win_rate=float((t.pnl > 0).mean() * 100),
                expectancy=float(t.pnl.mean()), max_drawdown=float((np.maximum.accumulate(curve) - curve).max()),
                participation_pct=float(pd.Series(t.d0).nunique() / sessions * 100),
                per_session_inr=float(t.pnl.sum() / sessions))
