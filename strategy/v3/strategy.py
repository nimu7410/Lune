"""Corrected baseline: causal spot-index execution reference (5-minute OHLCV).

Broker execution and derivative-price PnL are not certified by this backtest.
Long-only, one position at a time across all instruments, 1 lot only.

Instruments & Lot Sizes:
- MIDCPNIFTY: 120
- SENSEX:     20
- BANKNIFTY:  30
- FINNIFTY:   60
- NIFTY:      65

Charges & Costs:
- 2021–present: 10 index points per completed round trip
- Before 2021:   5 index points per completed round trip
Net PnL (₹) = (Gross Index Points - Charges) * Lot Size

Execution Timings:
- Strictly before 15:00 anyhow (never after 15:00).
- Night trades entered at Bar 67 (14:50 closed bar, filled at 14:55 open).
- Intraday momentum continuation entered between Bars 24 and 60 (11:15 to 14:15).

Waterfall Priority:
[MIDCPNIFTY -> SENSEX -> BANKNIFTY -> FINNIFTY -> NIFTY]
On each entry bar, check Instrument 1 for a signal; if none, check 2; if none, check 3; etc.
"""
import sys
import numpy as np
import pandas as pd

LOTS = {
    "MIDCPNIFTY": 120,
    "SENSEX": 20,
    "BANKNIFTY": 30,
    "FINNIFTY": 60,
    "NIFTY": 65
}

DATA_DIRS = {
    "MIDCPNIFTY": "midcpnifty 5m",
    "SENSEX": "sensex 5m",
    "BANKNIFTY": "banknifty 5m",
    "FINNIFTY": "finnifty 5m",
    "NIFTY": "nifty50_5m_data"
}

# Derivatives did not exist for these indices before these dates.  Keep the
# underlying history for warm-up, but never emit an unavailable-product signal.
AVAILABLE_FROM = {
    "FINNIFTY": np.datetime64("2021-01-11"),
    "MIDCPNIFTY": np.datetime64("2022-01-24"),
}

PRIORITY = ["MIDCPNIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "NIFTY"]

CARRY_BAR = 67  # 14:50 bar, minutes since midnight = 890. Fills at 14:55 open (strictly before 15:00)
SESSION_TOD = (555, 925)  # 09:15 to 15:25 inclusive (75 bars)
MAX_OBSERVED_GAP_DAYS = 3  # Friday->Monday is ~2.7 days; longer gaps are unobserved risk.

SPLIT_RATIO = (0.60, 0.20, 0.20)

P = {
    "reg": 200,      # 200-session daily SMA regime filter
    "cov": 20.0,     # datr >= 20 * cost
    "k": 0.40,       # intraday momentum trigger in daily ATR units
    "stop": 1.0,     # protective stop for momentum leg in daily ATR units
    "drop": 1.0,     # drop gate: skip entry if down > 1.0 datr from prior close
    "gth": 0.0,      # Tune-selected unconditional next-open exit; no opening-price decision
    "gbar": 11,      # panic release bar (10:10 bar close, 10:15 clock)
    "m_lo": 24,      # earliest momentum entry bar (11:15)
    "m_hi": 60       # latest momentum entry bar (14:15)
}


def cost_of(year):
    """Round-trip cost in index points."""
    return 5.0 if year < 2021 else 10.0


def load_frames(data_root='data', live=False, through=None):
    """Read daily CSVs with exact-grid validation and portfolio-wide date drops.

    `through` bounds price access; the split calendar uses filenames only.
    Live mode retains an exact, gap-free prefix of the latest session.
    """
    from pathlib import Path
    inventory = {s:{pd.Timestamp(p.stem[:10].replace('_','-')):p
                   for p in sorted((Path(data_root)/DATA_DIRS[s]).glob('*_5m.csv'))}
                 for s in PRIORITY}
    calendar = sorted(set().union(*(set(files) for files in inventory.values())))
    selected = [d for d in calendar if through is None or d<=pd.Timestamp(through)]
    if not selected:
        return {}
    bad=set(); by_symbol={}
    for sym,files in inventory.items():
        if not files:
            continue
        first,last=min(files),max(files); pieces=[]
        for date in selected:
            if date not in files:
                if first<=date<=last:
                    bad.add(date)
                continue
            try:
                df=pd.read_csv(files[date],usecols=['Datetime','Open','High','Low','Close'])
                df['Datetime']=pd.to_datetime(df['Datetime'])
                expected=pd.date_range(date+pd.Timedelta(minutes=555),periods=75,freq='5min')
                prefix=live and date==selected[-1] and 0<len(df)<=75
                prices=df[['Open','High','Low','Close']].to_numpy(float)
                valid=(pd.DatetimeIndex(df.Datetime).equals(expected[:len(df)] if prefix else expected)
                       and np.isfinite(prices).all() and (prices>0).all()
                       and (prices[:,1]>=prices[:,[0,2,3]].max(axis=1)).all()
                       and (prices[:,2]<=prices[:,[0,1,3]].min(axis=1)).all())
                if not valid:
                    bad.add(date);continue
                df['date']=date
                df['tod']=df.Datetime.dt.hour*60+df.Datetime.dt.minute
                pieces.append(df)
            except (ValueError,KeyError,pd.errors.ParserError):
                bad.add(date)
        if pieces:
            by_symbol[sym]=pd.concat(pieces,ignore_index=True)
    result={}
    for sym,df in by_symbol.items():
        df=df[~df.date.isin(bad)].reset_index(drop=True)
        df.attrs['calendar']=calendar
        df.attrs['dropped_dates']=tuple(sorted(bad))
        result[sym]=df
    return result


def features(df):
    """Causal per-bar features. Every daily aggregate is shifted one session."""
    ts = pd.DatetimeIndex(df["Datetime"])
    o, h, l, c = (df[k].to_numpy(np.float64) for k in ("Open", "High", "Low", "Close"))
    date = ts.normalize()
    day = pd.factorize(date)[0].astype(np.int32)
    bar = ((ts.hour * 60 + ts.minute - 555) // 5).to_numpy(np.int32)
    dates = pd.DatetimeIndex(pd.Series(date).drop_duplicates().to_numpy())

    g = pd.DataFrame({"day": day, "o": o, "h": h, "l": l, "c": c}).groupby("day", sort=False)
    D = g.agg(O=("o", "first"), H=("h", "max"), L=("l", "min"), C=("c", "last"))
    prev = D.shift(1)
    tr = np.maximum(D.H - D.L, np.maximum((D.H - prev.C).abs(), (D.L - prev.C).abs()))

    F = dict(day=day, bar=bar, dates=dates,
             tod=(ts.hour * 60 + ts.minute).to_numpy(np.int32),
             nbars=np.full(len(df), 75, np.int32),
             o=o, h=h, l=l, c=c, ts=ts.to_numpy())
    F["year"] = dates.year.to_numpy(np.int32)[day]
    # Wilder ATR of daily true range, prior completed sessions only
    F["datr"] = tr.ewm(alpha=1.0 / 14.0, adjust=False).mean().shift(1).to_numpy()[day]
    # 200-session SMA of daily closes, prior completed sessions only
    F["sma"] = D.C.rolling(P["reg"]).mean().shift(1).to_numpy()[day]
    F["pC"] = prev.C.to_numpy()[day]
    F["dO"] = g.o.transform("first").to_numpy()
    # Unweighted running mean of typical price (H+L+C)/3 since session open
    F["vwap"] = (pd.Series((h + l + c) / 3.0).groupby(day).cumsum().to_numpy() / (bar + 1))
    return F


def _first_of_day(mask, day):
    """Keep only the first True bar of each session."""
    out = np.zeros(len(mask), bool)
    prev_day = -1
    for i in np.flatnonzero(mask):
        if day[i] != prev_day:
            out[i] = True
            prev_day = day[i]
    return out


def proposals(sym, F, enable_mom=True, enable_carry=True):
    """Generate trade proposals on closed bars."""
    n = len(F["c"])
    c, bar, datr = F["c"], F["bar"], F["datr"]
    sig = dict(dir=np.zeros(n, np.int8), stop=np.zeros(n),
               gth=np.zeros(n), gbar=np.zeros(n, np.int16),
               days=np.ones(n, np.int8), style=np.zeros(n, dtype=object))
    cost = np.where(F["year"] < 2021, 5.0, 10.0)
    available = np.ones(n, dtype=bool)
    if sym in AVAILABLE_FROM:
        available = F["ts"] >= AVAILABLE_FROM[sym]

    with np.errstate(invalid="ignore"):
        up = available & np.isfinite(F["sma"]) & (c > F["sma"]) & np.isfinite(datr) & (datr > 0)
        cov_ok = datr >= P["cov"] * cost
        drop_ok = (c - F["pC"]) / datr > -P["drop"]

        if enable_mom:
            # Entry A - Intraday momentum continuation (11:15 to 14:15, strictly before 15:00)
            a = (up & cov_ok & drop_ok & (bar >= P["m_lo"]) & (bar <= P["m_hi"]) &
                 ((c - F["dO"]) >= P["k"] * datr) & (c > F["vwap"]))
            a = _first_of_day(a, F["day"])
            sig["dir"][a] = 1
            sig["stop"][a] = P["stop"] * datr[a]
            sig["style"][a] = "momentum"

        if enable_carry:
            # Entry B - Quality Night trade strictly at 14:50 (bar 67, fills at 14:55 open, strictly before 15:00)
            # Confirmed positive session (c > dO) and positive intraday momentum (c > vwap)
            b = up & cov_ok & drop_ok & (bar == CARRY_BAR) & (c > F["dO"]) & (c > F["vwap"])
            sig["dir"][b] = 1
            sig["style"][b] = "carry"

        live = sig["dir"] != 0
        sig["gth"][live] = P["gth"]
        sig["gbar"][live] = P["gbar"]
    return sig


def split_labels(frames):
    """Frozen chronological 60/20/20 calendar; no randomized month blocks."""
    calendar=sorted(set().union(*(set(f.attrs.get('calendar',pd.DatetimeIndex(f.Datetime).normalize())) for f in frames.values())))
    a,b=round(.6*len(calendar)),round(.8*len(calendar))
    if not 0<a<b<len(calendar):
        raise ValueError('Insufficient dates for three chronological splits')
    tune_end,validate_end=calendar[a-1],calendar[b-1]
    return {s:np.array(['tune' if d<=tune_end else 'validate' if d<=validate_end else 'hidden'
            for d in pd.DatetimeIndex(f.Datetime).normalize().unique()]) for s,f in frames.items()}


class Execution:
    """Two-phase spot-price execution reference, shared by batch and stream replay.

    Call on_open with OPEN prices only, then on_close after the candle completes.
    `pending` is the next-open entry intent; a real adapter must supply actual fills
    and reconcile broker positions. This class does not route orders.
    """
    def __init__(self, slippage_points=0.0):
        if not np.isfinite(slippage_points) or slippage_points < 0:
            raise ValueError('Slippage must be finite and nonnegative')
        self.slippage = float(slippage_points)
        self.position = None
        self.pending = None
        self.trades = []
        self.timestamp = None
        self.closed = True
        self.quotes = {}

    def _exit(self, price, timestamp, reason):
        p = self.position
        price -= self.slippage
        gross = price - p['entry']
        charge = cost_of(p['year'])
        self.trades.append(dict(sym=p['sym'], dir=1, style=p['style'],
            entry_ts=p['entry_ts'], exit_ts=pd.Timestamp(timestamp),
            entry=p['entry'], exit=price, gross=gross, chg=charge,
            pnl=gross-charge, rupees=(gross-charge)*LOTS[p['sym']],
            reason=reason, lab=p['lab'], year=p['year'], data_gap=p['data_gap']))
        self.position = None

    def on_open(self, timestamp, quotes):
        """quotes[symbol]: o, pC, datr, lab; no candle high/low/close is read."""
        ts = pd.Timestamp(timestamp)
        if not self.closed or (self.timestamp is not None and ts <= self.timestamp):
            raise ValueError('Each increasing open must follow the previous close')
        if (ts.tzinfo is not None or ts.second or ts.microsecond or ts.minute % 5 or
            not 555<=ts.hour*60+ts.minute<=925):
            raise ValueError('Use naive IST five-minute bar-start timestamps')
        for symbol, q in quotes.items():
            if symbol not in LOTS or not np.isfinite(q['o']) or q['o'] <= 0:
                raise ValueError('Invalid opening quote')
        self.timestamp, self.closed, self.quotes = ts, False, quotes
        pending, self.pending = self.pending, None  # Expire unfilled old intents.
        p = self.position
        if p is not None:
            if p['sym'] not in quotes:
                p['data_gap'] = True
                return
            q = quotes[p['sym']]
            elapsed = ts-p['last_seen']
            gap = (p['data_gap'] or q.get('data_gap', False) or
                   elapsed > pd.Timedelta(days=MAX_OBSERVED_GAP_DAYS) or
                   (ts.date()==p['last_seen'].date() and elapsed>pd.Timedelta(minutes=5)))
            p['last_seen'] = ts
            if gap:
                p['data_gap'] = True
                self._exit(q['o'], ts, 'data_gap_recovery')
            elif p['exit_due'] is not None:
                if ts >= p['exit_due']:
                    self._exit(q['o'], ts, p['exit_reason'])
            elif ts.date() > p['entry_ts'].date():
                if p['gth'] > 0:
                    # The opening price is observed first. The resulting order
                    # executes at a LATER open, never back at this same print.
                    panic = (np.isfinite(q['pC']) and np.isfinite(q['datr']) and
                             q['o']-q['pC'] <= -p['gth']*q['datr'])
                    p['exit_due'] = ts.normalize()+pd.Timedelta(minutes=555+5*(p['gbar']+1)) if panic else ts+pd.Timedelta(minutes=5)
                    p['exit_reason'] = 'gap_bounce' if panic else 'carry_delayed'
                else:
                    self._exit(q['o'], ts, 'carry_open')
            elif p['has_stop'] and q['o'] <= p['stop']:
                self._exit(q['o'], ts, 'stop_gap')
            # Never rotate into another symbol using this candle's later prices.
            return
        if pending is None or pending['due'] != ts or pending['sym'] not in quotes:
            return
        if not 555 <= ts.hour*60+ts.minute < 900:
            return
        q = quotes[pending['sym']]
        fill = q['o']+self.slippage
        self.position = dict(sym=pending['sym'], entry=fill, entry_ts=ts,
            stop=fill-pending['stop'], has_stop=pending['stop']>0,
            gth=pending['gth'], gbar=pending['gbar'], exit_due=None,
            exit_reason=None, style=pending['style'], lab=q.get('lab','all'),
            year=ts.year, last_seen=ts, data_gap=False)

    def on_close(self, timestamp, bars, proposals):
        """Process resting stops, then schedule entries from closed-bar signals."""
        ts = pd.Timestamp(timestamp)
        if self.closed or ts != self.timestamp:
            raise ValueError('Close must match the current open')
        if set(bars) != set(self.quotes):
            raise ValueError('Open and close symbol snapshots must match')
        for symbol, bar in bars.items():
            values = np.array([bar[k] for k in ('o','h','l','c')], float)
            if (not np.isfinite(values).all() or (values<=0).any() or
                bar['h'] < max(bar['o'],bar['c'],bar['l']) or
                bar['l'] > min(bar['o'],bar['c'],bar['h']) or
                bar['o'] != self.quotes[symbol]['o']):
                raise ValueError('Invalid completed candle')
        p = self.position
        if p is not None and p['sym'] in bars:
            # Bracket is active on the entry candle, and through its entry day.
            if (p['has_stop'] and ts.date()==p['entry_ts'].date() and
                bars[p['sym']]['l'] <= p['stop']):
                self._exit(p['stop'], ts+pd.Timedelta(minutes=5), 'stop')
        self.closed = True
        if self.position is not None:
            return
        due = ts+pd.Timedelta(minutes=5)
        if due.date()!=ts.date() or due.hour*60+due.minute >= 900:
            return
        for sym in PRIORITY:
            proposal = proposals.get(sym)
            if proposal is None or not proposal['dir']:
                continue
            if sym not in bars or proposal['dir'] != 1:
                raise ValueError('Entry must refer to an observed long signal')
            if (not all(np.isfinite(proposal[k]) for k in ('stop','gth','gbar')) or
                proposal['stop']<0 or proposal['gth']<0 or
                not 0<=proposal['gbar']<=73 or int(proposal['gbar'])!=proposal['gbar']):
                raise ValueError('Invalid stop or exit settings')
            self.pending = dict(proposal, sym=sym, due=due)
            break


def backtest(frames, labels=None, restrict='tune', enable_mom=True,
             enable_carry=True, precomputed_feats=None, slippage_points=0.0):
    """Causal waterfall replay. Open positions at data end remain unresolved."""
    syms = [s for s in PRIORITY if s in frames and len(frames[s])]
    engine = Execution(slippage_points)
    if not syms:
        return _ledger(engine)
    S = precomputed_feats if precomputed_feats is not None else {s:features(frames[s]) for s in syms}
    props = {s:proposals(s,S[s],enable_mom,enable_carry) for s in syms}
    events = np.concatenate([np.column_stack((S[s]['ts'].astype('datetime64[s]').astype(np.int64),
                np.full(len(S[s]['ts']),k),np.arange(len(S[s]['ts'])))) for k,s in enumerate(syms)])
    events = events[np.lexsort((events[:,1],events[:,0]))]
    gaps={}
    for s in syms:
        dropped=np.array(frames[s].attrs.get('dropped_dates',()),dtype='datetime64[D]')
        counts=np.searchsorted(dropped,S[s]['ts'].astype('datetime64[D]'),side='right')
        gaps[s]=np.r_[False,counts[1:]>counts[:-1]]
    k=0
    while k<len(events):
        end=k+1
        while end<len(events) and events[end,0]==events[k,0]:
            end+=1
        timestamp=pd.Timestamp(int(events[k,0]),unit='s')
        quotes, candles, closed_signals = {}, {}, {}
        for _, si, i in events[k:end]:
            s=syms[int(si)];i=int(i);d=S[s]
            lab=labels[s][d['day'][i]] if labels is not None else 'all'
            gap = gaps[s][i]
            quotes[s] = dict(o=d['o'][i],pC=d['pC'][i],datr=d['datr'][i],lab=lab,data_gap=gap)
            candles[s] = {key:d[key][i] for key in ('o','h','l','c')}
            if restrict=='all' or lab==restrict:
                closed_signals[s]={key:props[s][key][i] for key in props[s]}
        engine.on_open(timestamp,quotes)
        engine.on_close(timestamp,candles,closed_signals)
        k=end
    return _ledger(engine)


def _ledger(engine):
    columns=['sym','dir','style','entry_ts','exit_ts','entry','exit','gross','chg','pnl','rupees','reason','lab','year','data_gap']
    result=pd.DataFrame(engine.trades,columns=columns)
    result.attrs['open_position']=engine.position
    result.attrs['pending_entry']=engine.pending
    result.attrs['execution_complete']=engine.position is None
    return result


def compute_metrics(tr):
    if len(tr) == 0:
        return dict(n=0, net_inr=0., net_pts=0., exp=0., win_rate=0., pf=0., max_dd=0.)
    n = len(tr)
    net_inr = tr["rupees"].sum()
    net_pts = tr["pnl"].sum()
    exp = net_inr / n
    win_rate = (tr["rupees"] > 0).mean() * 100
    wins = tr[tr["rupees"] > 0]["rupees"].sum()
    losses = abs(tr[tr["rupees"] < 0]["rupees"].sum())
    pf = wins / losses if losses > 0 else np.nan
    # Include the initial zero-equity point; an initial loss is drawdown.
    cum = tr["rupees"].cumsum().to_numpy()
    curve = np.concatenate(([0.0], cum))
    dd = float((np.maximum.accumulate(curve) - curve).max())
    return dict(n=n, net_inr=net_inr, net_pts=net_pts, exp=exp, win_rate=win_rate, pf=pf, max_dd=dd)


if __name__ == '__main__':
    import argparse
    import json
    import resource
    resource.setrlimit(resource.RLIMIT_AS,(2*1024**3,2*1024**3))
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('data_root',nargs='?',default='data')
    parser.add_argument('--split',choices=['tune','validate','hidden'],default='tune')
    parser.add_argument('--slippage',type=float,default=0.,help='Adverse points per entry and exit fill')
    args=parser.parse_args()
    # scorecard.main limits prices to the requested chronological prefix.
    from scorecard import main
    main(args.data_root,args.split,args.slippage)
