"""The strategy's data, from Shoonya, as v3's own live runner keeps it.

  MarketDataTests - Shoonya's candles arrive in the columns of v3's session
                    files, identical to the recorded sessions they came from
  BuildTests      - `build_live` fills the live folder as `live_v3.build` did:
                    past sessions validated and kept whole, today's settled
                    candles as a gap-free prefix, the last session read again
                    once a day, holidays skipped, files written atomically
  ReplayTests     - an engine fed by Shoonya decides, candle by candle, exactly
                    what v3 decides on the same session from files
"""

from __future__ import annotations

import dataclasses
import tempfile
import unittest
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

import strategy_logic as S
from tests.order_path_fakes import (
    SPOT_TOKENS,
    FakeShoonya,
    candle_close,
    day_index,
    live_view,
    v3_market,
    write_live_root,
)

COLUMNS = ["Datetime", "Open", "High", "Low", "Close"]


def setUpModule() -> None:
    # `live_v3.market` opens its day cache with a bare `open()`; that is the
    # frozen release's code, and the handle is closed when collected.
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed file.*history_")


def source(at: datetime, **kwargs) -> S.MarketData:
    return S.MarketData(FakeShoonya(lambda: at, **kwargs), SPOT_TOKENS)


def ohlc(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["Datetime"])[COLUMNS]


class MarketDataTests(unittest.TestCase):
    def test_a_session_arrives_as_the_recorded_file_holds_it(self) -> None:
        day = pd.Timestamp("2015-02-10")
        bars = source(datetime(2015, 2, 10, 16, 0)).candles("BANKNIFTY", day, day + timedelta(hours=23))
        self.assertEqual(list(bars.columns), S.CSV_COLUMNS)
        self.assertTrue(bars[COLUMNS].reset_index(drop=True).equals(
            ohlc(S.DATA_DIR / "banknifty 5m" / "2015_02_10_5m.csv")))

    def test_no_candles_is_an_empty_frame_of_the_same_shape(self) -> None:
        day = pd.Timestamp("2015-02-08")                                # a Sunday
        bars = source(datetime(2015, 2, 8, 12, 0)).candles("NIFTY", day, day + timedelta(hours=23))
        self.assertTrue(bars.empty)
        self.assertEqual(list(bars.columns), S.CSV_COLUMNS)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(bars.Datetime))

    def test_only_settled_candles_count(self) -> None:
        now = datetime(2015, 2, 10, 11, 20, 2)
        bars = source(now).candles("NIFTY", pd.Timestamp("2015-02-10"))
        self.assertEqual(bars.Datetime.iloc[-1], pd.Timestamp("2015-02-10 11:20"), "the forming candle")
        self.assertEqual(S.settled(bars, now).Datetime.iloc[-1], pd.Timestamp("2015-02-10 11:10"))
        later = now.replace(second=3)
        self.assertEqual(S.settled(bars, later).Datetime.iloc[-1], pd.Timestamp("2015-02-10 11:15"))


class BuildTests(unittest.TestCase):
    """Sessions after the supplied history, served from the live cache's own files."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.live = Path(tmp.name) / "data_live"

    def build(self, at: datetime, **kwargs) -> tuple[bool, FakeShoonya]:
        data = source(at, skip=kwargs.pop("skip", ()))
        return S.build_live(data, at, self.live, **kwargs), data.api

    def test_missing_sessions_and_todays_candles_are_kept_as_v3_keeps_them(self) -> None:
        changed, api = self.build(datetime(2026, 9, 10, 10, 0, 4))
        self.assertTrue(changed)
        for day in ("2026_09_07", "2026_09_08", "2026_09_09"):
            for folder in S.V2.BASE.DATA_DIRS.values():
                with self.subTest(day=day, folder=folder):
                    self.assertTrue(ohlc(self.live / folder / f"{day}_5m.csv").equals(
                        ohlc(S.LIVE_DIR / folder / f"{day}_5m.csv")))
        today = ohlc(self.live / "nifty50_5m_data" / "2026_09_10_5m.csv")
        self.assertEqual((len(today), today.Datetime.iloc[-1]), (9, pd.Timestamp("2026-09-10 09:55")))
        extra = pd.read_parquet(self.live / "indices_5m" / "NIFTYNXT50.parquet")
        cached = pd.read_parquet(S.LIVE_DIR / "indices_5m" / "NIFTYNXT50.parquet")
        through = pd.Timestamp("2026-09-10")
        self.assertTrue(extra[extra.Datetime < through][COLUMNS].reset_index(drop=True).equals(
            cached[cached.Datetime < through][COLUMNS].reset_index(drop=True)))
        self.assertEqual(int((extra.Datetime.dt.normalize() == through).sum()), 9)
        self.assertFalse(list(self.live.rglob("*.tmp")), "written through temporary files")
        self.assertEqual(sum(1 for p in (self.live / "nifty50_5m_data").iterdir() if p.is_symlink()),
                         len(list((S.DATA_DIR / "nifty50_5m_data").glob("*_5m.csv"))),
                         "the tested history is linked, never copied")

    def test_holidays_and_weekends_are_never_asked_for(self) -> None:
        self.build(datetime(2026, 9, 10, 10, 0, 4))
        _changed, api = self.build(datetime(2026, 9, 15, 9, 0))
        asked = {day for _name, day in api.calls}
        self.assertNotIn(date(2026, 9, 14), asked, "a configured holiday")
        self.assertNotIn(date(2026, 9, 12), asked, "a Saturday")
        self.assertIn(date(2026, 9, 11), asked)

    def test_the_last_session_is_read_again_and_only_a_change_counts(self) -> None:
        self.build(datetime(2026, 9, 10, 9, 0))
        self.assertFalse(self.build(datetime(2026, 9, 10, 9, 1), refetch_last=True)[0],
                         "the broker's session is the one on file")
        path = self.live / "sensex 5m" / "2026_09_09_5m.csv"
        provisional = pd.read_csv(path)
        provisional.loc[74, "Close"] += 5.0                  # a closing candle saved while settling
        provisional.to_csv(path, index=False)
        self.assertFalse(self.build(datetime(2026, 9, 10, 9, 2))[0], "not read again without asking")
        self.assertTrue(self.build(datetime(2026, 9, 10, 9, 3), refetch_last=True)[0])
        self.assertTrue(ohlc(path).equals(ohlc(S.LIVE_DIR / "sensex 5m" / "2026_09_09_5m.csv")))

    def test_a_session_with_a_hole_is_dropped_and_today_refused(self) -> None:
        hole = datetime(2026, 9, 8, 12, 0)
        self.build(datetime(2026, 9, 9, 9, 0), skip=[hole])
        self.assertFalse((self.live / "nifty50_5m_data" / "2026_09_08_5m.csv").exists(),
                         "a bad day is dropped, never repaired")
        self.assertTrue((self.live / "nifty50_5m_data" / "2026_09_07_5m.csv").exists())
        with self.assertRaisesRegex(RuntimeError, "gap-free prefix"):
            self.build(datetime(2026, 9, 10, 12, 30, 4), skip=[datetime(2026, 9, 10, 11, 0)])


class ReplayTests(unittest.TestCase):
    def test_an_engine_fed_by_shoonya_decides_what_v3_decides(self) -> None:
        day = "2015-02-10"
        m = v3_market()
        d = day_index(m, day)
        with tempfile.TemporaryDirectory() as tmp:
            root = write_live_root(Path(tmp), day)
            clock = {"now": candle_close(day, 0)}
            engine = S.StrategyEngine(root=root, history=root, now_fn=lambda: clock["now"],
                                      source=S.MarketData(FakeShoonya(lambda: clock["now"]),
                                                          SPOT_TOKENS))
            for candles in (1, 24, 25, 26, 58, 59, 68, 69, 75):
                now = candle_close(day, candles) + timedelta(seconds=2)    # settled
                clock["now"] = now
                fed = engine.refresh(now)
                files = S.replay(live_view(m, d, candles), now)
                with self.subTest(candles=candles):
                    for field in dataclasses.fields(S.Snapshot):
                        self.assertEqual(getattr(fed, field.name), getattr(files, field.name),
                                         field.name)


if __name__ == "__main__":
    unittest.main()
