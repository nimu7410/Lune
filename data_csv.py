# data_csv.py
"""Download NIFTY 50 spot candles from Shoonya into one CSV per day under data/.

    python data_csv.py

Attaches to the live bot's broker session when one exists; see `_login`.
"""

import logging
import os
import time
from datetime import date, time as dt_time, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd

import ist
from shoonya_api import ShoonyaApiPy


DATA_DIR = Path(__file__).resolve().parent / "data"
TIMEFRAME = "15m"
START_DATE = date(2026, 7, 16)
# Shoonya token for Nifty 50 spot
NIFTY_TOKEN = "26000"


def _timeframe_to_minutes(tf: str) -> int:
    if not tf.endswith("m"):
        raise ValueError(f"Unsupported timeframe '{tf}', expected minute-based like '1m'/'15m'")
    try:
        minutes = int(tf[:-1])
    except ValueError as exc:
        raise ValueError(f"Unsupported timeframe '{tf}'") from exc
    if minutes <= 0:
        raise ValueError(f"Timeframe minutes must be positive, got {minutes}")
    return minutes


def _date_range(start: date, end: date) -> Iterable[date]:
    """Inclusive date range generator."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _login(api: ShoonyaApiPy) -> None:
    """
    Attach to the shared session.

    If the live bot already holds a valid session this reuses it and never
    touches a browser — important, because minting a second session could
    invalidate the running bot's token.  A new session is created only when no
    valid one exists.  Set SHOONYA_ALLOW_MINT=0 to forbid even that.
    """
    login_response = api.login_and_search()
    if login_response.get("stat") != "Ok":
        raise RuntimeError(f"Shoonya login failed; cannot download data: {login_response}")


def _fetch_and_save_day(api: ShoonyaApiPy, token: str, day: date) -> None:
    # Use ISO-style naming so files sort chronologically
    filename = DATA_DIR / f"{day.strftime('%Y_%m_%d')}_{TIMEFRAME}.csv"
    if filename.exists():
        logging.info("Skipping %s (already exists)", filename.name)
        return

    # IST day bounds: the epochs sent to the broker must not depend on the
    # host's zone, or a machine outside IST downloads a shifted window and
    # files it under the wrong date.
    start_dt = ist.combine(day, dt_time.min)
    end_dt = start_dt + timedelta(days=1) - timedelta(seconds=1)

    interval_minutes = _timeframe_to_minutes(TIMEFRAME)
    logging.info("Fetching %s %s data...", day.isoformat(), TIMEFRAME)
    ret = api.get_time_price_series(
        exchange="NSE",
        token=token,
        starttime=start_dt.timestamp(),
        endtime=end_dt.timestamp(),
        interval=interval_minutes,
    )
    if not ret or (isinstance(ret, dict) and ret.get("stat") == "Not_Ok"):
        logging.warning("No data returned for %s: %s", day.isoformat(), ret)
        return

    df = pd.DataFrame(ret)
    if df.empty:
        logging.warning("Empty dataframe for %s; skipping", day.isoformat())
        return

    # Normalize column names; handle either "time" or "timestamp"
    if "timestamp" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"timestamp": "time"})

    df = df.rename(
        columns={
            "time": "Datetime",
            "into": "Open",
            "inth": "High",
            "intl": "Low",
            "intc": "Close",
        }
    )
    if "Datetime" not in df:
        logging.warning("Missing 'Datetime' column for %s; raw keys: %s", day, list(df.columns))
        return

    # Explicit format to avoid dayfirst warnings. Left as bare IST wall clock,
    # with no offset: that is the convention the backtest stack under
    # `strategy/` reads these files with, matching the IST-naive bar times the
    # live runtime uses. The digits are IST either way - only the label differs.
    df["Datetime"] = pd.to_datetime(
        df["Datetime"], format="%d-%m-%Y %H:%M:%S", errors="coerce"
    )
    df = df.dropna(subset=["Datetime"])
    df = df.sort_values("Datetime")
    df = df[["Datetime", "Open", "High", "Low", "Close"]]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(filename, index=False)
    logging.info("Saved %s (%d rows)", filename.name, len(df))


def main() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(ist.ISTFormatter("%(asctime)s | %(levelname)s | %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)

    api = ShoonyaApiPy()
    _login(api)

    token = os.getenv("SHOONYA_NIFTY_TOKEN", NIFTY_TOKEN)
    logging.info("Using token %s for Nifty 50", token)

    today = ist.today()
    for day in _date_range(START_DATE, today):
        _fetch_and_save_day(api, token, day)
        time.sleep(0.2)  # small pause to avoid rate limits


if __name__ == "__main__":
    main()
