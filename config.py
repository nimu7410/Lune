from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time


@dataclass
class TradeSettings:
    # Scheduling & sizing
    poll_interval: float = 1.0
    # Fallback exchange lot for a legacy row with no index. Per-index lot sizes
    # live in `instruments.py` (NIFTY 65, BANKNIFTY 30, SENSEX 20, ...) because
    # the six trade different contracts on two exchanges; this is the fallback
    # for the legacy single-index case.
    lot_size: int = 65
    # Exchange lots per leg of a new trade: each v3 entry buys this many lots
    # of the call and sells this many of the put (a synthetic long future).
    # v3 was tested on one fixed lot.
    lots: int = 1
    # Expiry cadence and weekday are per index - see INDEX_EXPIRY at the bottom
    # of this file. They differ by exchange and have each moved more than once,
    # so a single global pair could only ever be right for part of the universe.
    option_roll_days_before_expiry: int = 0  # 0=roll on expiry day, 1=roll D-1, etc. (calendar days)
    future_roll_days_before_expiry: int = 0  # 0=roll on expiry day, 1=roll D-1, etc. (calendar days)

    # v3 trades only its carry (B), as the night trade; the momentum setup (A)
    # is retired (`strategy_logic.LIVE_SETUPS`). B is signalled as frozen, on
    # the 14:45/14:50 candle, and filled at `night_entry_at`: after the HH/LL
    # desk squares its day off (`manual_desk.ManualSettings.square_off_at`,
    # 15:10), and only once the desk is flat. It is sold at the next session's
    # 09:15 open, and the desk takes no day trade until it is.
    night_entry_at: dt_time = dt_time(15, 11)

    # Manager-level entry cutoff for any book but the carry. Only the retired A
    # books used it; the night trade (carry) is exempt by design.
    #
    # There is deliberately no `close_all_positions_by`. v3 holds positions
    # into the next session's 09:15 open (strategy/v3/strategy_v3.py): a
    # manager-level flatten at 15:15 would not trim the strategy, it would
    # delete it.
    no_new_trades_after: dt_time = dt_time(14, 30)

    # Risk controls
    #
    # The strategy's own risk parameters are NOT here. The 1.0 daily-ATR stop,
    # the 0.40 ATR momentum trigger, the 11:15-14:15 window, the 200-session
    # regime filter, the 20x cost gate, the shock guard and the waterfall are
    # frozen in `strategy/v3/` because they were validated as one set; a second, looser copy in this file would
    # silently override the strategy that was tested.
    # There is no take-profit and no daily loss limit in this strategy, so
    # neither has a setting here.
    # Entry cooldown / retry budget
    entry_fail_cooldown_sec: float = 15.0
    entry_fail_budget_per_min: int = 3

    # Entry spread guard (LIVE). A leg is opened only on a two-sided book whose
    # bid/ask spread is at most this fraction of the mid, or one tick, whichever
    # is wider. Both legs are checked before the first order, and each order
    # again. Exits and flattens are never refused for a wide spread.
    max_entry_spread_pct: float = 0.03

    # Order wait/timeout
    buy_wait_timeout_sec: float = 60.0
    sell_wait_timeout_sec: float = 60.0
    exchordid_wait_timeout_sec: float = 30.0
    exit_exchordid_wait_timeout_sec: float = 30.0
    poll_interval_sec: float = 0.5  # order state polling

    # Exit retries
    exit_max_retries: int = 3
    exit_retry_backoff_sec: float = 2.0

    # Emergency flatten
    emergency_flatten_retries: int = 3
    emergency_flatten_sleep_sec: float = 1.5

    # Exchange holidays. They shift the option expiry calendar
    # (`getdata.GetSymbol`) and are skipped by the live data build
    # (`strategy_logic.build_live`): add each year's here.
    expiry_holidays: tuple[str, ...] = (
        "15-Jan-2026",
        "26-Jan-2026",
        "03-Mar-2026",
        "26-Mar-2026",
        "31-Mar-2026",
        "03-Apr-2026",
        "14-Apr-2026",
        "01-May-2026",
        "28-May-2026",
        "26-Jun-2026",
        "14-Sep-2026",
        "02-Oct-2026",
        "20-Oct-2026",
        "10-Nov-2026",
        "24-Nov-2026",
        "25-Dec-2026",
        "26-Jan-2027",
    )


def parse_holiday(value) -> date | None:
    """One `expiry_holidays` entry as a date, or None when it does not parse.

    `DD-MMM-YYYY` as listed above; `YYYY-MM-DD` and `DD-MM-YYYY` are accepted
    too. Callers decide what an unparseable entry means: the leg builder
    refuses it, the market-hours runner skips it.
    """
    if isinstance(value, date):
        return value
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            continue
    return None


@dataclass
class DataSettings:
    refresh_interval: int = 60
    boundary_grace_seconds: int = 3
    boundary_retry_interval: float = 5.0
    boundary_retry_attempts: int = 18
    candle_interval_minutes: int = 5 # Chart_timeframe

    # When candles can exist at all. The boundary loop fires every 5 minutes for
    # as long as the process lives, and outside these hours the broker has
    # nothing to return - so every one of those polls was a wasted API call and
    # a pair of warning lines ("no data" / "5m refresh returned no rows"). The
    # close is deliberately past 15:30 so the 15:25 candle, which only finalises
    # at 15:30, is still collected. These candles feed the chart; the strategy
    # reads its own, from v3's live data (`strategy_logic`).
    # The 09:15 candle cannot be fetched until its 09:20 close.
    session_open: dt_time = dt_time(9, 20)
    session_close: dt_time = dt_time(15, 35)


@dataclass
class WebsocketSettings:
    # Attempts made at the quiet log level before a reconnect run is treated as
    # a real outage and escalated. It is NOT a give-up count: the reconnect
    # supervisor keeps trying until the feed is back or the bot is shutting
    # down. A bounded count used to end the loop, which is how a mid-session
    # broker blip could leave the bot with no price feed for the rest of the
    # day and nothing scheduled to notice.
    max_retries: int = 5
    # Seconds before the first retry, and the ceiling the delay doubles toward.
    # The cap has to be long enough that a multi-minute broker outage is waited
    # out rather than hammered: at the old 15s ceiling the loop reissued a
    # doomed handshake every 15 seconds for the whole outage.
    backoff_initial: float = 2.0
    backoff_cap: float = 60.0
    # How long a fresh socket gets to finish its handshake and authenticate
    # before the supervisor counts the attempt as failed and backs off again.
    open_timeout: float = 15.0
    # One reconnect-failure line per this many seconds once an outage is
    # established, so a long broker outage costs a handful of log lines
    # instead of hundreds of identical handshake dumps.
    failure_log_interval: float = 60.0


@dataclass
class ChartSettings:
    # Frontend poll/refresh intervals (milliseconds)
    realtime_poll_ms: int = 1000
    # Display label for chart timeframe (e.g., "5m"). Leave empty to derive from data interval.
    timeframe_label: str = "5m"


@dataclass(frozen=True)
class IndexExpiry:
    """When one index's contracts expire.

    Two facts per leg: how often the exchange lists a new contract
    (`"weekly"` or `"monthly"`) and which weekday it expires on. A monthly
    contract expires on the **last** occurrence of that weekday in the month;
    a weekly one on the next occurrence of it.

    These are exchange facts, not strategy parameters - NSE and BSE have each
    moved their expiry day more than once, and BANKNIFTY's weeklies were
    withdrawn entirely. When one moves, change it here. `instruments.py`
    reads this table and `getdata.GetSymbol` builds the expiry of every
    synthetic long from it. Every symbol built is still
    confirmed against the broker's scrip master before it can be traded, so a
    stale entry refuses trades on that index rather than trading the wrong
    contract.

    Index futures are monthly on every exchange, so `future_cadence` has no
    weekly option; the weekday is kept separate because a future need not
    expire on the same day as the weekly option cycle.
    """

    option_cadence: str          # "weekly" | "monthly"
    option_weekday: str          # Mon..Sun name, e.g. "TUE"
    future_weekday: str          # weekday the monthly future expires on
    future_cadence: str = "monthly"

    def __post_init__(self) -> None:
        for field_name, value in (("option_cadence", self.option_cadence),
                                  ("future_cadence", self.future_cadence)):
            if value not in ("weekly", "monthly"):
                raise ValueError(
                    f"{field_name} must be 'weekly' or 'monthly', got {value!r}"
                )
        if self.future_cadence != "monthly":
            raise ValueError("index futures are monthly on every exchange")


#: Expiry calendar per index. Keyed by the strategy's index name;
#: `instruments.py` asserts the keys match the strategy universe.
INDEX_EXPIRY: dict[str, IndexExpiry] = {
    # Weekly options, expiring Tuesday. Monthly future on the last Tuesday.
    "NIFTY": IndexExpiry(
        option_cadence="weekly",
        option_weekday="TUE",
        future_weekday="TUE",
    ),
    # Weeklies withdrawn: options roll monthly, on the last Tuesday of the
    # month - the same contract the future expires on.
    "BANKNIFTY": IndexExpiry(
        option_cadence="monthly",
        option_weekday="TUE",
        future_weekday="TUE",
    ),
    "FINNIFTY": IndexExpiry(
        option_cadence="monthly",
        option_weekday="TUE",
        future_weekday="TUE",
    ),
    "MIDCPNIFTY": IndexExpiry(
        option_cadence="monthly",
        option_weekday="TUE",
        future_weekday="TUE",
    ),
    "NIFTYNXT50": IndexExpiry(
        option_cadence="monthly",
        option_weekday="TUE",
        future_weekday="TUE",
    ),
    # BSE lists SENSEX weeklies expiring Thursday; the monthly future expires
    # on the last Thursday.
    "SENSEX": IndexExpiry(
        option_cadence="weekly",
        option_weekday="THU",
        future_weekday="THU",
    ),
}


# Instantiate defaults
TRADE_SETTINGS = TradeSettings()
DATA_SETTINGS = DataSettings()
WEBSOCKET_SETTINGS = WebsocketSettings()
CHART_SETTINGS = ChartSettings()


__all__ = [
    "parse_holiday",
    "INDEX_EXPIRY",
    "IndexExpiry",
    "TRADE_SETTINGS",
    "DATA_SETTINGS",
    "WEBSOCKET_SETTINGS",
    "CHART_SETTINGS",
    "TradeSettings",
    "DataSettings",
    "WebsocketSettings",
    "ChartSettings",
]
