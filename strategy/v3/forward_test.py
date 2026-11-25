"""Forward test on sessions AFTER the supplied data (never seen by any version).

    python3 forward_test.py [--end YYYY-MM-DD]

Fetches Upstox 5-minute bars for the six indices after the last supplied session, validates them exactly
like the supplied files (75-bar grid, sane OHLC; bad days dropped), and builds data_forward/ (links to the
supplied files + the new days) so the supplied data and the frozen split calendar stay untouched.
Runs v3, v2 and the unchanged v1 with entries only on the new sessions (the last one is settlement-only),
prints their summaries and trades and saves the summaries to results/forward_test.json.
Delete data_forward/ afterwards if you like; it is rebuilt on every run.
"""
import argparse
import json
import os
import time
import urllib.parse
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import strategy as BASE
import strategy_v2 as V
import strategy_v3 as V3

ROOT = Path(__file__).resolve().parent
SRC, DST = ROOT / 'data', ROOT / 'data_forward'
KEYS = {'NIFTY': 'NSE_INDEX|Nifty 50', 'BANKNIFTY': 'NSE_INDEX|Nifty Bank', 'FINNIFTY': 'NSE_INDEX|Nifty Fin Service',
        'MIDCPNIFTY': 'NSE_INDEX|NIFTY MID SELECT', 'SENSEX': 'BSE_INDEX|SENSEX', 'NIFTYNXT50': 'NSE_INDEX|Nifty Next 50'}


def fetch(key, a, b):
    url = f"https://api.upstox.com/v3/historical-candle/{urllib.parse.quote(key, safe='')}/minutes/5/{b}/{a}"
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
        return df.sort_values('Datetime')[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
    raise RuntimeError(url)


def good_days(df):
    """Only exact 09:15..15:25 75-bar days with valid OHLC survive (never repaired)."""
    out = {}
    for day, g in df.groupby(df.Datetime.dt.normalize()):
        p = g[['Open', 'High', 'Low', 'Close']].to_numpy(float)
        grid = pd.date_range(day + pd.Timedelta(minutes=555), periods=75, freq='5min')
        if (pd.DatetimeIndex(g.Datetime).equals(grid) and np.isfinite(p).all() and (p > 0).all()
                and (p[:, 1] >= p[:, [0, 2, 3]].max(1)).all() and (p[:, 2] <= p[:, [0, 1, 3]].min(1)).all()):
            out[day] = g
    return out


def build(start, end, dst=DST):
    """Link the supplied files into dst and add validated Upstox sessions start..end (dst's own extra-index bars
    are kept, so repeated calls only need the new days)."""
    for folder in list(BASE.DATA_DIRS.values()):
        (dst / folder).mkdir(parents=True, exist_ok=True)
        for f in (SRC / folder).glob('*_5m.csv'):
            if not (dst / folder / f.name).exists():
                os.symlink(os.path.relpath(f, dst / folder), dst / folder / f.name)
    for sub in ('daily_ref', 'warmup_extra'):
        if not (dst / sub).exists():
            os.symlink(os.path.relpath(SRC / sub, dst), dst / sub)
    (dst / 'indices_5m').mkdir(exist_ok=True)
    report = {}
    for sym, key in KEYS.items():
        days = {}
        if start <= end:
            df = pd.concat([fetch(key, str(m.date()), str(min(end, m + pd.offsets.MonthEnd(0)).date()))
                            for m in pd.date_range(start.replace(day=1), end, freq='MS')])
            df = df[(df.Datetime >= start) & (df.Datetime < end + pd.Timedelta(days=1))]
            days = good_days(df)
            report[sym] = (len(days), df.Datetime.dt.normalize().nunique() - len(days))
        if sym in BASE.DATA_DIRS:
            for day, g in days.items():
                g.assign(Datetime=g.Datetime.dt.strftime('%Y-%m-%d %H:%M:%S')).to_csv(
                    dst / BASE.DATA_DIRS[sym] / f'{day:%Y_%m_%d}_5m.csv', index=False)
        else:
            p = dst / 'indices_5m' / f'{sym}.parquet'
            old = pd.read_parquet(p if p.exists() else SRC / 'indices_5m' / f'{sym}.parquet')
            new = pd.concat([old] + [g[['Datetime', 'Open', 'High', 'Low', 'Close']] for g in days.values()])
            new.drop_duplicates('Datetime').sort_values('Datetime').to_parquet(p, index=False)
    return report


def summary(pnl, sessions):
    pnl = np.asarray(pnl, float)
    if len(pnl) == 0:
        return dict(trades=0, net_inr=0.0)
    curve = np.r_[0, pnl.cumsum()]
    return dict(trades=len(pnl), net_inr=round(float(pnl.sum())), win_rate=round(float((pnl > 0).mean() * 100), 1),
                expectancy=round(float(pnl.mean())), max_drawdown=round(float((np.maximum.accumulate(curve) - curve).max())))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--end', default=str((pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).date()))
    a = ap.parse_args()
    last_supplied = pd.Timestamp(V.SPLITS['hidden'][1])
    start, end = last_supplied + pd.Timedelta(days=1), pd.Timestamp(a.end)
    print('new sessions per index (kept, dropped):', build(start, end))
    # v2
    m = V.Market(data_root=DST, through=end)
    fwd = m.cal > last_supplied
    ok = fwd.copy()
    ok[np.flatnonzero(fwd)[-1]] = False                      # last new session: settlement only
    t = V.backtest(m, V.build_slots(m), int(np.flatnonzero(fwd)[0]), len(m.cal) - 1, ok)
    t3 = V3.backtest(m, V3.build_slots(m), int(np.flatnonzero(fwd)[0]), len(m.cal) - 1, ok)
    assert t.attrs['execution_complete'] and t3.attrs['execution_complete']
    sessions = int(ok.sum())
    # v1 (unchanged code and costs) on the same sessions
    frames = BASE.load_frames(str(DST), through=end)
    labels = {s: np.where(pd.DatetimeIndex(f.Datetime).normalize().unique() > last_supplied, 'forward', 'past').astype(object)
              for s, f in frames.items()}
    for s, f in frames.items():
        days = pd.DatetimeIndex(f.Datetime).normalize().unique()
        labels[s][days == days.max()] = 'settlement'
    g = BASE.backtest(frames, labels, restrict='forward')
    assert g.attrs['execution_complete']
    g_same = g.rupees - [V.charges(r.sym, r.entry_ts, r.entry, BASE.LOTS[r.sym])
                         - (5.0 if r.entry_ts.year < 2021 else 10.0) * BASE.LOTS[r.sym] for r in g.itertuples()]
    print(f'forward sessions with entries allowed: {sessions} ({m.cal[fwd][0].date()} .. {m.cal[fwd][-2].date()}),'
          f' settlement {m.cal[fwd][-1].date()}')
    res = {'period': [str(m.cal[fwd][0].date()), str(m.cal[fwd][-1].date())], 'entry_sessions': sessions,
           'v3': summary(t3.pnl, sessions), 'v2': summary(t.pnl, sessions), 'v1_own_costs': summary(g.rupees, sessions),
           'v1_same_costs_as_v2': summary(g_same, sessions),
           'v3_net_by_instrument': {k: round(float(v)) for k, v in t3.groupby('sym').pnl.sum().items()},
           'v2_net_by_instrument': {k: round(float(v)) for k, v in t.groupby('sym').pnl.sum().items()}}
    json.dump(res, open(ROOT / 'results' / 'forward_test.json', 'w'), indent=1)
    for k in ('v3', 'v2', 'v1_own_costs', 'v1_same_costs_as_v2'):
        print(f'{k:22s}', res[k])
    for name, x in (('v3', t3), ('v2', t)):
        print(f'\n{name} trades:')
        print(x.assign(date=m.cal[x.d0].date, exit_date=m.cal[x.d1].date, entry=x.entry.round(2))[
            ['date', 'sym', 'setup', 'entry', 'exit_date', 'exit', 'reason', 'pnl']].round(0).to_string(index=False))
    print('\nv1 trades (own costs):')
    print(g[['entry_ts', 'sym', 'style', 'entry', 'exit_ts', 'exit', 'reason', 'rupees']].round(0).to_string(index=False))


if __name__ == '__main__':
    main()
