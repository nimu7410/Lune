"""Live signal runner for strategy_v3 - tells you what to do now.  No broker integration: you place the orders.

    python3 live_v3.py               # before 09:15, then after every 5-minute candle closes (09:20 .. 15:00)

Builds data_live/ = the supplied history (links) + every later session from Upstox (validated exactly like the
supplied files; bad days dropped, never repaired) + today's completed candles (a gap-free prefix from 09:15),
replays the frozen v3 engine over the last sessions and prints one instruction:
  SELL <index> at the 09:15 open | BUY 1 lot <index> at the next candle open (+ stop level, exit time) | HOLD | NOTHING.
The engine is deterministic and positions never outlive the next 09:15 open, so replaying from a flat book three
sessions back reproduces the live state exactly.  Index stop levels are index points; set the option stop-loss
yourself from the option's delta, or watch the index.  The first run of a day builds and caches the history
(about a minute); later runs that day take a few seconds.  Also lists today's exits, flags a shock session
(no overnight holds) and prints the edge-monitor alert for any index whose overnight edge has faded.
"""
import datetime as dt
import pickle
import time
import urllib.parse
import numpy as np
import pandas as pd
import requests
import strategy as BASE
import strategy_v2 as V2
import strategy_v3 as V3
import forward_test as F

LIVE = V2.ROOT / 'data_live'


def today_candles(key, now):
    """Completed 5-minute candles of the current session (a candle starting at t is complete at t + 5 min)."""
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{urllib.parse.quote(key, safe='')}/minutes/5"
    for k in range(8):
        r = requests.get(url, timeout=60)
        if r.status_code == 429:
            time.sleep(2 + 3 * k)
            continue
        j = r.json()
        if j.get('status') != 'success':
            raise RuntimeError(j)
        df = pd.DataFrame(j['data']['candles'], columns=['ts', 'Open', 'High', 'Low', 'Close', 'Volume', 'oi'])
        df['Datetime'] = pd.to_datetime(df.ts.str[:19])
        df = df[df.Datetime + pd.Timedelta(minutes=5) <= now].sort_values('Datetime')
        return df[df.Datetime.dt.normalize() == now.normalize()][['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
    raise RuntimeError(url)


def prefix_ok(g, day):
    """Exact 09:15.. grid without gaps and sane OHLC (the live loader accepts only such a prefix)."""
    p = g[['Open', 'High', 'Low', 'Close']].to_numpy(float)
    grid = pd.date_range(day + pd.Timedelta(minutes=555), periods=len(g), freq='5min')
    return (0 < len(g) <= 75 and pd.DatetimeIndex(g.Datetime).equals(grid) and np.isfinite(p).all() and (p > 0).all()
            and (p[:, 1] >= p[:, [0, 2, 3]].max(1)).all() and (p[:, 2] <= p[:, [0, 1, 3]].min(1)).all())


def build(now):
    today = now.normalize()
    supplied_end = pd.Timestamp(V2.SPLITS['hidden'][1])
    full = [p for p in (LIVE / BASE.DATA_DIRS['NIFTY']).glob('*_5m.csv') if LIVE.exists()
            and f'{supplied_end:%Y_%m_%d}' < p.stem[:10] < f'{today:%Y_%m_%d}' and len(p.read_text().splitlines()) == 76]
    last = max([supplied_end] + [pd.Timestamp(p.stem[:10].replace('_', '-')) for p in full])
    F.build(last + pd.Timedelta(days=1), today - pd.Timedelta(days=1), LIVE)       # completed sessions only
    for sym, key in F.KEYS.items():
        g = today_candles(key, now)
        if not len(g):
            continue
        if not prefix_ok(g, today):
            raise RuntimeError(f'{sym}: today\'s candles are not a gap-free prefix; wait for the next candle')
        if sym in BASE.DATA_DIRS:
            g.assign(Datetime=g.Datetime.dt.strftime('%Y-%m-%d %H:%M:%S')).to_csv(
                LIVE / BASE.DATA_DIRS[sym] / f'{today:%Y_%m_%d}_5m.csv', index=False)
        else:
            p = LIVE / 'indices_5m' / f'{sym}.parquet'
            old = pd.read_parquet(p)
            pd.concat([old[old.Datetime < today], g[['Datetime', 'Open', 'High', 'Low', 'Close']]]).to_parquet(p, index=False)


def instruction(m, now):
    """Replay the frozen engine over the last sessions up to the last completed candle and describe the state."""
    slots = V3.build_slots(m)
    e = V3.Engine(m, slots)
    d_last = len(m.cal) - 1
    is_today = m.cal[d_last] == now.normalize()
    n_today = int(np.isfinite(m.C[:, d_last]).sum(1).max()) if is_today else 75
    for d in range(max(0, d_last - 3), d_last + 1):
        for b in range(75 if d < d_last else n_today):
            e.on_open(d, b)
            e.on_close(d, b)
    hhmm = lambda d, b: (m.cal[d] + pd.Timedelta(minutes=555 + 5 * b)).strftime('%H:%M')
    lines = [f"today: {t['sym']} ({t['setup']}) exited at {hhmm(t['d1'], t['b1'])}, index {t['exit']:.2f} ({t['reason']})"
             for t in e.trades if is_today and t['d1'] == d_last]
    if is_today and V3.shock_sessions(m)[d_last]:
        lines.append('shock session (NIFTY opened >= 0.75 ATR below its previous close): no overnight holds today')
    exit_at = "SELL at the next session's 09:15 open"
    p = e.position
    if p is not None:
        name = m.names[p['k']]
        if not is_today:
            lines.append(f"SELL {name} ({p['setup']}, bought {m.cal[p['d0']]:%d %b} at index {p['entry']:.2f}) "
                         f"at the next session's 09:15 open")
        elif p['d0'] < d_last:
            lines.append(f"SELL {name} ({p['setup']}) now - it was due at the 09:15 open (missing data for it today)")
        else:
            stop = f", index stop {p['stop']:.2f} today only" if p['stop'] is not None else ', no stop'
            when = 'SELL at the 14:55 candle open today' if p['intraday'] else exit_at
            lines.append(f"HOLD {name} ({p['setup']}) bought at index {p['entry']:.2f}{stop}; {when}")
    elif e.pending is not None:
        (d, b), j = e.pending
        setup, k, hold, _ = slots[j]
        stop = f"; stop = fill - {V3.P['stop'] * m.atr[k, d]:.2f} index points, today only" if hold != V3.CARRY else '; no stop'
        when = 'SELL at the 14:55 candle open today' if hold == V3.INTRADAY else exit_at
        lines.append(f"BUY 1 lot {m.names[k]} ({setup}) at the {hhmm(d, b)} candle open{stop}; {when}")
    else:
        lines.append('NOTHING to do: flat, no signal' + ('' if is_today else ' (no candles yet today)'))
    off = [s for s, on in zip(m.names, V3.edge_ok(m)[:, d_last]) if not on]
    if off:
        lines.append('edge monitor alert - last 120 carry nights averaged <= 0 net for: ' + ', '.join(off)
                     + ' (consider pausing them)')
    return '\n'.join(lines)


def market(today, root=LIVE):
    """History through yesterday (built once per day, cached) + today's completed candles.  Equals
    strategy_v2.Market(root, live=True, through=today) - test_v3.py checks this - in a few seconds."""
    cache = root / f'history_{today:%Y%m%d}.pkl'
    if cache.exists():
        m = pickle.load(open(cache, 'rb'))
    else:
        for old in root.glob('history_*.pkl'):
            old.unlink()
        m = V2.Market(data_root=root, through=today - pd.Timedelta(days=1))
        pickle.dump(m, open(cache, 'wb'))
    rows = {}
    for s in m.names:
        if s in BASE.DATA_DIRS:
            f = root / BASE.DATA_DIRS[s] / f'{today:%Y_%m_%d}_5m.csv'
            rows[s] = pd.read_csv(f, parse_dates=['Datetime']) if f.exists() else None
        else:
            x = pd.read_parquet(root / 'indices_5m' / f'{s}.parquet')
            rows[s] = x[x.Datetime.dt.normalize() == today]
    if all(rows.get(s) is None or not len(rows[s]) for s in BASE.DATA_DIRS):
        return m                                              # no session today (yet)
    nI = len(m.names)
    m.cal = m.cal.append(pd.DatetimeIndex([today]))
    for a in ('O', 'H', 'L', 'C'):
        setattr(m, a, np.concatenate([getattr(m, a), np.full((nI, 1, 75), np.nan)], axis=1))
    for k, s in enumerate(m.names):
        g = rows[s]
        if g is None or not len(g) or not prefix_ok(g.reset_index(drop=True), today):
            continue
        for a, col in (('O', 'Open'), ('H', 'High'), ('L', 'Low'), ('C', 'Close')):
            getattr(m, a)[k, -1, :len(g)] = g[col].to_numpy(float)
    m.valid = np.concatenate([m.valid, np.isfinite(m.O[:, -1:, 0])], axis=1)
    m.available = np.ones((nI, len(m.cal)), bool)
    for s, first in V2.AVAILABLE_FROM.items():
        if s in m.idx:
            m.available[m.idx[s]] = m.cal >= first
    m._daily(root)
    return m


def main():
    now = pd.Timestamp(dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).replace(tzinfo=None))
    build(now)
    print(now.strftime('%Y-%m-%d %H:%M IST'), '|', instruction(market(now.normalize()), now))


if __name__ == '__main__':
    main()
