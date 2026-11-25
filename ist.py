"""The bot's only clock: Asia/Kolkata, always.

Every wall-clock decision this project makes is an IST decision - the 09:15
session open, the 14:30 manager cutoff, the next-session 09:15 strategy exit, the day a backup file
belongs to, the day boundary a history fetch starts from. Reading any of those
off the host's local time works right up until the machine is re-imaged in a
different region, and then it fails silently and expensively: the strategy's
session geometry shifts and no exception is ever raised.

So nothing in this project calls `datetime.now()`, `date.today()`,
`time.mktime()` or `datetime.now(timezone.utc)` directly. Everything comes
through here, and everything that leaves the process - a database row, a JSON
field, a log line, a filename - is IST.

Two conventions worth knowing before using this module:

* **Aware vs naive.** `now()` and `parse_ist()` return *aware* IST datetimes;
  those are what the database gets, because a `TIMESTAMPTZ` column should be
  handed an unambiguous instant. The strategy layer instead works in IST-naive
  wall clock (it compares bar times against `dt_time(9, 15)` and friends), so
  `now_naive()` / `to_ist_naive()` exist for it. Both describe the same instant;
  only the label differs.

* **The feed's legacy `Z`.** Shoonya reports IST wall-clock strings. The chart
  feed historically re-published those same digits with a `Z` suffix, i.e.
  labelled UTC while meaning IST. `format_feed_time()` now emits an honest
  `+05:30`, but `parse_feed_time()` still reads a trailing `Z` as IST wall clock
  so anything produced by an older build - or held in a browser tab that has not
  reloaded - resolves to the same instant it always did.

IST has never observed daylight saving time, so its offset is a constant
+05:30. That is what makes `IST_OFFSET` safe to use as a plain arithmetic
shift; do not copy that assumption to any other zone.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time as dt_time, timedelta, tzinfo
from typing import Any, Optional
from zoneinfo import ZoneInfo

__all__ = [
    "IST",
    "IST_NAME",
    "IST_OFFSET",
    "IST_OFFSET_SUFFIX",
    "FEED_TIME_FORMAT",
    "LOG_TIME_FORMAT",
    "DATE_KEY_FORMAT",
    "ISTFormatter",
    "combine",
    "date_key",
    "epoch",
    "format_feed_time",
    "format_log",
    "from_epoch",
    "isoformat",
    "midnight",
    "now",
    "now_naive",
    "parse_feed_time",
    "parse_ist",
    "to_ist",
    "to_ist_naive",
    "today",
]


IST_NAME = "Asia/Kolkata"
IST: tzinfo = ZoneInfo(IST_NAME)

# IST is a fixed +05:30 with no DST, historically or scheduled.
IST_OFFSET = timedelta(hours=5, minutes=30)
IST_OFFSET_SUFFIX = "+05:30"

# The wire format for candle and pivot timestamps. Producers must agree on this
# byte for byte: the chart places markers and candles on one axis by string
# identity of the leading digits, and a marker rendered in a different format
# lands on a different scale from the bar it belongs to.
FEED_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S" + IST_OFFSET_SUFFIX

# Human-facing console/log rendering. The trailing literal is a label, not a
# strftime directive - `%Z` would print the host's zone abbreviation.
LOG_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S IST"

# Date stamp for per-day artefacts (log files, backup archives).
DATE_KEY_FORMAT = "%Y%m%d"

# Broker (Shoonya TPSeries) wall-clock format, always IST digits.
_BROKER_TIME_FORMAT = "%d-%m-%Y %H:%M:%S"


# ---------------------------------------------------------------------------
# Current time
# ---------------------------------------------------------------------------
def now() -> datetime:
    """Current instant as an aware IST datetime. Use this for database writes."""
    return datetime.now(IST)


def now_naive() -> datetime:
    """Current IST wall clock, tz-stripped, matching the strategy's bar times."""
    return datetime.now(IST).replace(tzinfo=None)


def today() -> date:
    """Today's date *in IST*, which is not always the host's or UTC's date."""
    return datetime.now(IST).date()


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------
def to_ist(value: datetime) -> datetime:
    """Return `value` as an aware IST datetime.

    A naive input is taken to already be IST wall clock and simply labelled;
    an aware one is genuinely converted. Both are lossless for their input's
    own meaning, which is why every entry point into this module funnels here.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)


def to_ist_naive(value: datetime) -> datetime:
    """Return `value` as IST wall clock with the tzinfo dropped."""
    return to_ist(value).replace(tzinfo=None)


def from_epoch(value: float) -> datetime:
    """Read a POSIX timestamp as an aware IST datetime."""
    return datetime.fromtimestamp(float(value), IST)


def epoch(value: datetime) -> float:
    """POSIX timestamp for `value`, reading a naive input as IST wall clock.

    `datetime.timestamp()` on a naive value assumes the *host's* zone, which is
    exactly the dependency this module exists to remove.
    """
    return to_ist(value).timestamp()


def midnight(value: Optional[datetime] = None) -> datetime:
    """Aware IST midnight starting the IST day that contains `value` (or now)."""
    moment = now() if value is None else to_ist(value)
    return moment.replace(hour=0, minute=0, second=0, microsecond=0)


def combine(day: date, moment: dt_time) -> datetime:
    """An aware IST datetime from a date and a wall-clock time."""
    return datetime.combine(day, moment, tzinfo=IST)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_ist(value: Any) -> Optional[datetime]:
    """Parse a timestamp into aware IST, honouring any offset it carries.

    Accepts `datetime` objects, ISO-8601 strings (with or without an offset,
    with `Z` read as UTC), and the broker's `%d-%m-%Y %H:%M:%S` wall clock.
    Naive input is read as IST. Returns None rather than raising, so a single
    malformed row can never take down a caller in the trading path.

    Use `parse_feed_time` instead for this project's own candle/pivot strings,
    where a trailing `Z` means IST rather than UTC.
    """
    parsed = _parse_to_datetime(value, utc_suffix_is_ist=False)
    return None if parsed is None else to_ist(parsed)


def parse_feed_time(value: Any) -> Optional[datetime]:
    """Parse a candle/pivot timestamp into aware IST.

    Identical to `parse_ist` except that a trailing `Z` is treated as IST wall
    clock, not UTC. That is this feed's own historical convention (see the
    module docstring); honouring the `Z` literally would shift every bar and
    pivot forward by 5h30m. A real `+05:30`/`+00:00` offset is always honoured
    properly, so correctly-labelled input - which is what this build emits - is
    read correctly too.
    """
    parsed = _parse_to_datetime(value, utc_suffix_is_ist=True)
    return None if parsed is None else to_ist(parsed)


def _parse_to_datetime(value: Any, *, utc_suffix_is_ist: bool) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        # Dropping the suffix leaves IST wall clock; "+00:00" makes it real UTC.
        text = text[:-1] + ("" if utc_suffix_is_ist else "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.strptime(text, _BROKER_TIME_FORMAT)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def format_feed_time(value: Any) -> Optional[str]:
    """Render a timestamp in the chart feed's IST wire format."""
    parsed = parse_feed_time(value)
    return None if parsed is None else parsed.strftime(FEED_TIME_FORMAT)


def format_log(value: Optional[datetime] = None) -> str:
    """Render an instant for humans, explicitly labelled IST."""
    moment = now() if value is None else to_ist(value)
    return moment.strftime(LOG_TIME_FORMAT)


def isoformat(value: Optional[datetime] = None) -> str:
    """ISO-8601 with the real `+05:30` offset - what every JSON field emits."""
    moment = now() if value is None else to_ist(value)
    return moment.isoformat()


def date_key(value: Optional[datetime] = None) -> str:
    """`YYYYMMDD` for the IST day of `value` (or today), for per-day filenames."""
    moment = now() if value is None else to_ist(value)
    return moment.strftime(DATE_KEY_FORMAT)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
class ISTFormatter(logging.Formatter):
    """Stamps log records in IST regardless of the host's configured zone.

    `logging` formats `record.created` (a POSIX timestamp) through
    `time.localtime` by default, so on a non-IST host every line in every log
    file would carry a different zone from the timestamps in the database.
    """

    def formatTime(self, record, datefmt=None):  # noqa: N802 - logging API
        instant = from_epoch(record.created)
        if datefmt:
            return instant.strftime(datefmt)
        return instant.strftime("%Y-%m-%d %H:%M:%S,") + f"{int(record.msecs):03d}"
