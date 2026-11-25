"""Everything this bot reports, stores, or decides on is IST. These lock that in.

The failures these guard against are all silent ones: nothing raises when a
timestamp is read in the wrong zone, the bot simply trades and records the
wrong five and a half hours. So each test pins an exact value rather than an
invariant that a shifted clock would still satisfy.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, time as dt_time, timedelta, timezone
from unittest.mock import patch

import ist
from data_handler import DataHandler, _to_iso
from strategy_logic import FEED_TIME_FORMAT, parse_bar_timestamp
from db_loggers import ForecastLogger, _to_ist


class ISTPrimitiveTests(unittest.TestCase):
    def test_offset_is_the_fixed_indian_offset(self):
        self.assertEqual(ist.IST_OFFSET, timedelta(hours=5, minutes=30))
        self.assertEqual(
            ist.now().utcoffset(), timedelta(hours=5, minutes=30)
        )

    def test_epoch_reads_naive_input_as_ist_not_as_host_local(self):
        # 05:30 IST on the epoch day *is* the epoch. A host-local reading would
        # only agree by coincidence on an IST machine.
        self.assertEqual(ist.epoch(datetime(1970, 1, 1, 5, 30)), 0.0)
        self.assertEqual(ist.epoch(datetime(1970, 1, 1, tzinfo=timezone.utc)), 0.0)
        self.assertEqual(ist.from_epoch(0), datetime(1970, 1, 1, 5, 30, tzinfo=ist.IST))

    def test_aware_input_is_converted_rather_than_relabelled(self):
        utc_noon = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(ist.to_ist_naive(utc_noon), datetime(2026, 8, 11, 17, 30))

    def test_naive_input_is_labelled_rather_than_shifted(self):
        self.assertEqual(
            ist.to_ist(datetime(2026, 8, 11, 9, 15)),
            datetime(2026, 8, 11, 9, 15, tzinfo=ist.IST),
        )

    def test_midnight_is_the_ist_day_not_the_utc_day(self):
        # 20:00 UTC is already the next day in IST; the trading day that
        # instant belongs to must start at the *IST* midnight after it.
        late_utc = datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc)
        self.assertEqual(
            ist.midnight(late_utc), datetime(2026, 8, 14, 0, 0, tzinfo=ist.IST)
        )

    def test_parse_ist_honours_a_real_utc_label(self):
        self.assertEqual(
            ist.parse_ist("2026-08-11T09:55:00Z"),
            datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST),
        )

    def test_parse_feed_time_reads_legacy_z_as_ist_wall_clock(self):
        # The feed's own historical convention: IST digits labelled UTC.
        # Honouring the Z literally would record a 15:25 bar as 20:55.
        self.assertEqual(
            ist.parse_feed_time("2026-08-11T15:25:00Z"),
            datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST),
        )

    def test_parse_feed_time_accepts_every_shape_the_feed_can_emit(self):
        expected = datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST)
        for value in (
            "2026-08-11T15:25:00+05:30",  # current wire format
            "2026-08-11T15:25:00Z",       # legacy wire format
            "2026-08-11T15:25:00",        # bare wall clock
            "11-08-2026 15:25:00",        # raw broker format
            "2026-08-11T09:55:00+00:00",  # a genuinely labelled offset
            datetime(2026, 8, 11, 15, 25),
        ):
            with self.subTest(value=value):
                self.assertEqual(ist.parse_feed_time(value), expected)

    def test_unparseable_input_returns_none_instead_of_raising(self):
        for value in (None, "", "   ", "nonsense", "2026-13-45T99:99:99"):
            with self.subTest(value=value):
                self.assertIsNone(ist.parse_feed_time(value))
                self.assertIsNone(ist.parse_ist(value))

    def test_formatting_is_stable_under_repeated_round_trips(self):
        rendered = ist.format_feed_time("11-08-2026 15:25:00")
        self.assertEqual(rendered, "2026-08-11T15:25:00+05:30")
        self.assertEqual(ist.format_feed_time(rendered), rendered)

    def test_isoformat_carries_the_offset_so_readers_cannot_guess_wrong(self):
        self.assertTrue(
            ist.isoformat(datetime(2026, 8, 11, 15, 25)).endswith("+05:30")
        )

    def test_combine_and_date_key_stay_on_the_ist_calendar(self):
        self.assertEqual(
            ist.combine(date(2026, 8, 11), dt_time(9, 15)),
            datetime(2026, 8, 11, 9, 15, tzinfo=ist.IST),
        )
        late_utc = datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc)
        self.assertEqual(ist.date_key(late_utc), "20260814")


class FeedTimestampContractTests(unittest.TestCase):
    """Candle and marker producers must agree byte for byte on the wire format."""

    def test_wire_format_states_the_offset_explicitly(self):
        self.assertEqual(FEED_TIME_FORMAT, "%Y-%m-%dT%H:%M:%S+05:30")

    def test_live_producer_emits_the_wire_format(self):
        live = _to_iso(datetime(2026, 8, 11, 9, 15, tzinfo=ist.IST))
        self.assertEqual(live, "2026-08-11T09:15:00+05:30")

    def test_strategy_reads_both_wire_formats_as_the_same_bar(self):
        # A page or response still carrying the old `Z` must not become a
        # second, five-and-a-half-hour-later bar in the engine's chronology.
        legacy = parse_bar_timestamp("2026-08-11T15:25:00Z")
        current = parse_bar_timestamp("2026-08-11T15:25:00+05:30")
        self.assertEqual(legacy, current)
        self.assertEqual(legacy, datetime(2026, 8, 11, 15, 25))
        self.assertIsNone(legacy.tzinfo, "the engines compare against bare dt_time")


class _StubApi:
    def __init__(self, rows):
        self.rows = rows
        self.request = None

    def get_time_price_series(self, **kwargs):
        self.request = kwargs
        return self.rows


class DataHandlerISTTests(unittest.TestCase):
    ROWS = [
        {"time": "11-08-2026 09:15:00", "into": "100", "inth": "102", "intl": "99", "intc": "101"},
        {"time": "11-08-2026 15:25:00", "into": "105", "inth": "106", "intl": "104", "intc": "105"},
    ]

    def _handler(self, now_value):
        return DataHandler(
            _StubApi(self.ROWS), "NSE", "26000", now_fn=lambda: now_value
        )

    def test_day_start_follows_the_ist_calendar_day(self):
        # 20:00 UTC on the 13th is 01:30 IST on the 14th. The old UTC-midnight
        # behaviour anchored history to the 13th and fetched the wrong session.
        handler = self._handler(datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(
            handler._today_midnight(),
            datetime(2026, 8, 14, 0, 0, tzinfo=ist.IST),
        )

    def test_history_is_requested_from_ist_midnight(self):
        handler = self._handler(datetime(2026, 8, 11, 9, 30, tzinfo=ist.IST))
        handler._fetch_5m_df(handler._today_midnight())
        self.assertEqual(
            handler.api.request["starttime"],
            datetime(2026, 8, 11, 0, 0, tzinfo=ist.IST).timestamp(),
        )

    def test_broker_wall_clock_is_labelled_ist_never_shifted_into_it(self):
        handler = self._handler(datetime(2026, 8, 11, 15, 30, tzinfo=ist.IST))
        frame = handler._fetch_5m_df(handler._today_midnight())
        first = frame["time"].iloc[0].to_pydatetime()
        self.assertEqual(ist.to_ist(first), datetime(2026, 8, 11, 9, 15, tzinfo=ist.IST))

    def test_published_bars_carry_the_ist_offset(self):
        handler = self._handler(datetime(2026, 8, 11, 15, 30, tzinfo=ist.IST))
        frame = handler._fetch_5m_df(handler._today_midnight())
        bars = handler._format_bars(
            [{"time": row.time.to_pydatetime(), "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}
             for row in frame.itertuples()]
        )
        self.assertEqual(
            [bar["time"] for bar in bars],
            ["2026-08-11T09:15:00+05:30", "2026-08-11T15:25:00+05:30"],
        )

    def test_published_bars_survive_a_round_trip_through_the_strategy_parser(self):
        published = _to_iso(datetime(2026, 8, 11, 9, 15, tzinfo=ist.IST))
        self.assertEqual(parse_bar_timestamp(published), datetime(2026, 8, 11, 9, 15))


class ForecastPivotTimeTests(unittest.TestCase):
    """Regression guard for a silently-dead `_to_ist`.

    Its body had been displaced below the `return` of another function, so it
    returned None for every input: `forecast_logs.pivot_time` was written NULL
    on every row, and the pivot list was "sorted" by a constant key, leaving
    the reported latest pivot as whichever LL happened to be appended last.
    """

    PIVOTS = {
        # Deliberately ordered so that list position and chronology disagree:
        # the newest pivot is an HH, and HH points are emitted before LL ones.
        "hh_points": [{"time": "2026-08-11T15:25:00+05:30", "high": 24800.0}],
        "ll_points": [{"time": "2026-08-11T09:20:00+05:30", "low": 24100.0}],
    }

    def test_to_ist_actually_parses(self):
        self.assertEqual(
            _to_ist("2026-08-11T15:25:00+05:30"),
            datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST),
        )
        self.assertIsNone(_to_ist(None))
        self.assertIsNone(_to_ist(""))

    def test_pivot_time_is_populated_and_in_ist(self):
        logger = ForecastLogger(None, None, pivot_provider=lambda: self.PIVOTS)
        _display, data = logger._fetch()
        self.assertEqual(
            data["pivot_time"], datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST)
        )

    def test_latest_pivot_is_chosen_by_time_not_by_list_position(self):
        logger = ForecastLogger(None, None, pivot_provider=lambda: self.PIVOTS)
        _display, data = logger._fetch()
        self.assertEqual(data["pivot_type"], "HH")
        self.assertEqual(data["pivot_price"], 24800.0)

    def test_legacy_z_pivots_are_read_as_ist(self):
        legacy = {
            "hh_points": [{"time": "2026-08-11T15:25:00Z", "high": 24800.0}],
            "ll_points": [],
        }
        logger = ForecastLogger(None, None, pivot_provider=lambda: legacy)
        _display, data = logger._fetch()
        self.assertEqual(
            data["pivot_time"], datetime(2026, 8, 11, 15, 25, tzinfo=ist.IST)
        )

    def test_unparseable_pivot_times_do_not_break_the_sort(self):
        messy = {
            "hh_points": [{"time": "nonsense", "high": 1.0}],
            "ll_points": [{"time": "2026-08-11T09:20:00+05:30", "low": 24100.0}],
        }
        logger = ForecastLogger(None, None, pivot_provider=lambda: messy)
        _display, data = logger._fetch()
        self.assertEqual(data["pivot_type"], "LL")

    def test_no_pivots_yields_a_null_pivot_time(self):
        logger = ForecastLogger(
            None, None, pivot_provider=lambda: {"hh_points": [], "ll_points": []}
        )
        _display, data = logger._fetch()
        self.assertIsNone(data["pivot_time"])
        self.assertEqual(data["hh_count"], 0)


class DatabaseSessionTimezoneTests(unittest.TestCase):
    def _connector(self):
        from db_connector import TimescaleDBConnector

        return TimescaleDBConnector(password="test-only", auto_provision=False)

    def test_pool_connections_are_pinned_to_ist(self):
        self.assertEqual(self._connector().cfg["options"], "-c timezone=Asia/Kolkata")

    def test_startup_accepts_a_session_already_on_ist(self):
        connector = self._connector()
        with patch.object(
            connector, "execute", return_value=[{"TimeZone": "Asia/Kolkata"}]
        ):
            connector._verify_session_timezone()

    def test_startup_refuses_a_session_on_any_other_timezone(self):
        connector = self._connector()
        for reported in ("UTC", "America/New_York", ""):
            with self.subTest(reported=reported):
                with patch.object(
                    connector, "execute", return_value=[{"TimeZone": reported}]
                ):
                    with self.assertRaisesRegex(RuntimeError, "session timezone"):
                        connector._verify_session_timezone()


if __name__ == "__main__":
    unittest.main()
