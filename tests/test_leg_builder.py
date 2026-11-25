"""`GetSymbol` builds v3's synthetic long future and nothing else.

Every v3 BUY buys the call and sells the put of one strike - the in-the-money
call strike closest to the money - on the nearest expiry with two trading
sessions left. These checks cover the strike, the expiry (against the
exchanges' 2026 calendar), the symbols, that the ATM it publishes comes from
the live tick, that a dead feed is reported only while legs are wanted, and
that any bias that is not v3's is refused out loud rather than traded.
"""

from __future__ import annotations

import logging
import sys
import time
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import config
import getdata
import instruments
from config import TRADE_SETTINGS, parse_holiday
from getdata import GetSymbol, LegSelectionError
from tests.log_capture import capture_logs

#: Every 2026 option expiry each index lists, as NSE and BSE published them
#: (holiday shifts included). Recorded from the exchange calendar v6 was
#: priced on; `config.INDEX_EXPIRY` must keep producing exactly these.
LISTED_2026 = {
    "NIFTY": (
        "2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27", "2026-02-03", "2026-02-10",
        "2026-02-17", "2026-02-24", "2026-03-02", "2026-03-10", "2026-03-17", "2026-03-24",
        "2026-03-30", "2026-04-07", "2026-04-13", "2026-04-21", "2026-04-28", "2026-05-05",
        "2026-05-12", "2026-05-19", "2026-05-26", "2026-06-02", "2026-06-09", "2026-06-16",
        "2026-06-23", "2026-06-30", "2026-07-07", "2026-07-14", "2026-07-21", "2026-07-28",
        "2026-08-04", "2026-08-11", "2026-08-18", "2026-08-25", "2026-09-01", "2026-09-08",
        "2026-09-15", "2026-09-22", "2026-09-29", "2026-10-06", "2026-10-13", "2026-10-19",
        "2026-10-27", "2026-11-03", "2026-11-09", "2026-11-17", "2026-11-23", "2026-12-01",
        "2026-12-08", "2026-12-15", "2026-12-22", "2026-12-29",
    ),
    "BANKNIFTY": (
        "2026-01-27", "2026-02-24", "2026-03-30", "2026-04-28", "2026-05-26", "2026-06-30",
        "2026-07-28", "2026-08-25", "2026-09-29", "2026-10-27", "2026-11-23", "2026-12-29",
    ),
    "SENSEX": (
        "2026-01-01", "2026-01-08", "2026-01-14", "2026-01-22", "2026-01-29", "2026-02-05",
        "2026-02-12", "2026-02-19", "2026-02-26", "2026-03-05", "2026-03-12", "2026-03-19",
        "2026-03-25", "2026-04-02", "2026-04-09", "2026-04-16", "2026-04-23", "2026-04-30",
        "2026-05-07", "2026-05-14", "2026-05-21", "2026-05-27", "2026-06-04", "2026-06-11",
        "2026-06-18", "2026-06-25", "2026-07-02", "2026-07-09", "2026-07-16", "2026-07-23",
        "2026-07-30", "2026-08-06", "2026-08-13", "2026-08-20", "2026-08-27", "2026-09-03",
        "2026-09-10", "2026-09-17", "2026-09-24", "2026-10-01", "2026-10-08", "2026-10-15",
        "2026-10-22", "2026-10-29", "2026-11-05", "2026-11-12", "2026-11-19", "2026-11-26",
        "2026-12-03", "2026-12-10", "2026-12-17", "2026-12-24", "2026-12-31",
    ),
    "FINNIFTY": (
        "2026-01-27", "2026-02-24", "2026-03-30", "2026-04-28", "2026-05-26", "2026-06-30",
        "2026-07-28", "2026-08-25", "2026-09-29", "2026-10-27", "2026-11-23", "2026-12-29",
    ),
    "MIDCPNIFTY": (
        "2026-01-27", "2026-02-24", "2026-03-30", "2026-04-28", "2026-05-26", "2026-06-30",
        "2026-07-28", "2026-08-25", "2026-09-29", "2026-10-27", "2026-11-23", "2026-12-29",
    ),
    "NIFTYNXT50": (
        "2026-01-27", "2026-02-24", "2026-03-30", "2026-04-28", "2026-05-26", "2026-06-30",
        "2026-07-28", "2026-08-25", "2026-09-29", "2026-10-27", "2026-11-23", "2026-12-29",
    ),
}


class _Api:
    """Scrip master that lists whatever it is asked for."""

    def searchscrip(self, exchange, searchtext):
        return {"values": [{"tsym": searchtext, "token": "1"}]}


def _fetcher(index: str, price=lambda: 24_000.0) -> GetSymbol:
    inst = instruments.get(index)
    return GetSymbol(
        _Api(),
        price,
        instrument=inst,
        option_expiry_weekday=inst.option_expiry_weekday,
        future_expiry_weekday=inst.future_expiry_weekday,
        option_roll_days_before_expiry=TRADE_SETTINGS.option_roll_days_before_expiry,
        future_roll_days_before_expiry=TRADE_SETTINGS.future_roll_days_before_expiry,
        expiry_holidays=TRADE_SETTINGS.expiry_holidays,
    )


def _sessions_2026() -> list[date]:
    holidays = {parse_holiday(day) for day in TRADE_SETTINGS.expiry_holidays}
    day, out = date(2026, 1, 1), []
    while day.year == 2026:
        if day.weekday() < 5 and day not in holidays:
            out.append(day)
        day += timedelta(days=1)
    return out


class TheStrikeIsTheItmCallClosestToTheMoney(unittest.TestCase):
    def test_the_strike_is_the_highest_listed_one_below_the_price(self):
        nifty = _fetcher("NIFTY")                              # 50-point strikes
        self.assertEqual(nifty._itm_call_strike(24_037.45), 24_000)
        self.assertEqual(nifty._itm_call_strike(24_049.99), 24_000)
        self.assertEqual(nifty._itm_call_strike(24_050.05), 24_050)
        self.assertEqual(_fetcher("BANKNIFTY")._itm_call_strike(52_061.80), 52_000)
        self.assertEqual(_fetcher("MIDCPNIFTY")._itm_call_strike(12_990.10), 12_975)
        self.assertEqual(_fetcher("NIFTYNXT50")._itm_call_strike(68_150.00), 68_100)

    def test_a_price_on_a_strike_is_at_the_money_so_the_next_one_down_is_taken(self):
        self.assertEqual(_fetcher("NIFTY")._itm_call_strike(24_000.0), 23_950)
        self.assertEqual(_fetcher("SENSEX")._itm_call_strike(75_000.0), 74_900)

    def test_the_legs_buy_the_call_and_sell_the_put_of_that_strike(self):
        with mock.patch("getdata.ist.today", return_value=date(2026, 9, 21)):
            legs = _fetcher("NIFTY")._build_legs(24_037.45, "HH")
        self.assertEqual((legs["buy"], legs["sell"]), ("NIFTY29SEP26C24000", "NIFTY29SEP26P24000"))
        self.assertEqual((legs["bias"], legs["sell_side"], legs["index"], legs["exchange"]),
                         ("HH", "S", "NIFTY", "NFO"))
        self.assertEqual((legs["strike"], legs["expiry"], legs["quantity"]),
                         (24_000, "2026-09-29", instruments.get("NIFTY").quantity))


class TheExpiryHasTwoSessionsLeft(unittest.TestCase):
    def test_every_2026_session_picks_the_nearest_expiry_two_sessions_out(self):
        sessions = _sessions_2026()
        self.assertGreater(len(sessions), 240)
        for name, listed in LISTED_2026.items():
            builder = _fetcher(name)
            expiries = [date.fromisoformat(day) for day in listed]
            for day in sessions:
                ahead = [e for e in expiries
                         if sum(day < s <= e for s in sessions) >= getdata.MIN_SESSIONS_TO_EXPIRY]
                if not ahead:
                    continue                           # its expiry is in 2027
                with self.subTest(index=name, day=str(day)):
                    self.assertEqual(builder._entry_expiry(day), ahead[0])

    def test_an_overnight_hold_never_reaches_expiry_day(self):
        nifty = _fetcher("NIFTY")
        # Friday 25 Sep 2026: Monday 28 is one session before the 29 Sep
        # expiry, so a next-open exit would still be fine - two sessions left.
        self.assertEqual(nifty._entry_expiry(date(2026, 9, 25)), date(2026, 9, 29))
        # Monday 28 Sep: the expiry is the next session, so the next week's.
        self.assertEqual(nifty._entry_expiry(date(2026, 9, 28)), date(2026, 10, 6))
        # Expiry day itself rolls too.
        self.assertEqual(nifty._entry_expiry(date(2026, 9, 29)), date(2026, 10, 6))

    def test_holidays_are_not_sessions(self):
        nifty = _fetcher("NIFTY")
        # Fri 2 Oct 2026 is a holiday: from Thu 1 Oct the sessions to the
        # Tue 6 Oct expiry are Mon 5 and Tue 6.
        self.assertEqual(nifty._sessions_after(date(2026, 10, 1), date(2026, 10, 6)), 2)
        self.assertEqual(nifty._entry_expiry(date(2026, 10, 1)), date(2026, 10, 6))


class SensexTakesTheBfoMastersNames(unittest.TestCase):
    def test_both_legs_resolve_through_the_master(self):
        fetcher = _fetcher("SENSEX", lambda: 75_040.0)
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        contracts = {(date(2026, 10, 1), "C", 75_000): ("SENSEX26O0175000CE", 20),
                     (date(2026, 10, 1), "P", 75_000): ("SENSEX26O0175000PE", 20)}
        with mock.patch("getdata.ist.today", return_value=date(2026, 9, 28)), \
                mock.patch("getdata._load_bfo_contracts", return_value=contracts):
            legs = fetcher.update_symbol()
        self.assertEqual((legs["buy"], legs["sell"]), ("SENSEX26O0175000CE", "SENSEX26O0175000PE"))
        self.assertEqual(legs["exchange"], "BFO")

    def test_a_leg_the_master_lacks_blocks_the_entry(self):
        fetcher = _fetcher("SENSEX", lambda: 75_040.0)
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        contracts = {(date(2026, 10, 1), "C", 75_000): ("SENSEX26O0175000CE", 20)}
        with mock.patch("getdata.ist.today", return_value=date(2026, 9, 28)), \
                mock.patch("getdata._load_bfo_contracts", return_value=contracts), \
                capture_logs(logging.getLogger()):
            self.assertIsNone(fetcher.update_symbol())
        self.assertIn("no SENSEX contract", fetcher.legs_blocked_reason or "")


class TheAtmIsDerivedFromTheLivePrice(unittest.TestCase):
    def test_the_atm_is_the_nearest_listed_strike_to_the_live_price(self):
        nifty = _fetcher("NIFTY")            # 50-point strikes
        self.assertEqual(nifty._atm_strike(24_037.45), 24_050)
        self.assertEqual(nifty._atm_strike(24_012.10), 24_000)
        self.assertEqual(nifty._atm_strike(24_000.00), 24_000)
        bank = _fetcher("BANKNIFTY")         # 100-point strikes
        self.assertEqual(bank._atm_strike(52_061.80), 52_100)
        self.assertEqual(bank._atm_strike(52_049.99), 52_000)

    def test_a_price_exactly_between_two_strikes_rounds_up_not_to_the_even_one(self):
        # `round()` is banker's rounding: it would send 24025 to 24000 and
        # 24075 to 24100, deciding the ATM by the parity of the strike grid.
        nifty = _fetcher("NIFTY")
        self.assertEqual(nifty._atm_strike(24_025.0), 24_050)
        self.assertEqual(nifty._atm_strike(24_075.0), 24_100)

    def test_the_legs_publish_the_atm_they_were_chosen_against(self):
        legs = _fetcher("NIFTY")._build_legs(24_037.45, "HH")
        self.assertEqual(legs["atm_strike"], 24_050)

    def test_the_live_getter_is_what_update_symbol_prices_against(self):
        # The strike has to track the WebSocket, not a value cached at startup.
        price = {"ltp": 24_037.45}
        fetcher = _fetcher("NIFTY", lambda: price["ltp"])
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        legs = fetcher.update_symbol()
        self.assertEqual((legs["atm_strike"], legs["strike"]), (24_050, 24_000))

        price["ltp"] = 24_620.00           # the index moves
        legs = fetcher.update_symbol()
        self.assertEqual((legs["atm_strike"], legs["strike"]), (24_600, 24_600))


class LegsStatusTests(unittest.TestCase):
    """What the dashboard's legs bay reads: current legs, or why there are none."""

    def test_states_follow_what_the_builder_published(self):
        fetcher = _fetcher("NIFTY")
        self.assertEqual("idle", fetcher.legs_status()["state"])     # never asked yet

        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        legs = fetcher.update_symbol()
        status = fetcher.legs_status()
        self.assertEqual(("armed", legs["sell"], legs["buy"]),
                         (status["state"], status["sell"], status["buy"]))

        # Legs left standing once the window shuts are not current ones.
        fetcher.set_pivot_type_getter(lambda: None)
        fetcher.update_symbol()
        self.assertIsNotNone(fetcher.final_symbols)
        self.assertEqual({"state": "idle", "sell": None, "buy": None},
                         {k: fetcher.legs_status()[k] for k in ("state", "sell", "buy")})

        fetcher.legs_blocked_reason = "NFO does not list NIFTY29SEP26P24000"
        self.assertEqual(("blocked", "NFO does not list NIFTY29SEP26P24000"),
                         (fetcher.legs_status()["state"], fetcher.legs_status()["note"]))


class AMissingPriceIsReportedOnlyWhenLegsAreWanted(unittest.TestCase):
    def _gapped(self, bias):
        """A NIFTY builder whose feed has printed no `lp` for a minute."""
        getdata._THROTTLE.reset("no_price:NIFTY")
        asked = []
        fetcher = _fetcher("NIFTY", lambda: asked.append(1))   # None: no current `lp`
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: bias)
        fetcher._no_price_since = time.monotonic() - 60
        return fetcher, asked

    def test_no_price_is_asked_for_while_no_legs_are_wanted(self):
        fetcher, asked = self._gapped(None)
        with capture_logs(logging.getLogger()) as logs:
            fetcher.update_symbol()
        self.assertEqual(asked, [])
        self.assertFalse(logs.at_least(logging.WARNING), logs.text())
        self.assertIn("no intended entry", fetcher.legs_blocked_reason)

    def test_a_missing_price_while_legs_are_wanted_is_still_reported(self):
        fetcher, asked = self._gapped("HH")
        with capture_logs(logging.getLogger()) as logs:
            self.assertIsNone(fetcher.update_symbol())
        self.assertEqual(asked, [1])
        self.assertTrue(logs.has("no live price", logging.WARNING), logs.text())


class OnlyV3sLegsExist(unittest.TestCase):
    def test_a_bearish_bias_has_no_legs(self):
        with self.assertRaises(LegSelectionError) as caught:
            _fetcher("NIFTY")._build_legs(24_000.0, "LL")
        self.assertIn("long-only", str(caught.exception))

    def test_a_bearish_bias_reaches_update_symbol_as_a_named_block(self):
        getdata._THROTTLE.reset("no_legs:NIFTY")
        fetcher = _fetcher("NIFTY")
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "LL")
        with capture_logs(logging.getLogger()) as logs:
            self.assertIsNone(fetcher.update_symbol())
        self.assertIn("long-only", fetcher.legs_blocked_reason or "")
        self.assertTrue(logs.has("long-only", logging.ERROR), logs.text())
        getdata._THROTTLE.reset("no_legs:NIFTY")

    def test_a_bearish_preview_is_empty_and_silent(self):
        with capture_logs(logging.getLogger()) as logs:
            self.assertIsNone(_fetcher("NIFTY").preview_legs("LL"))
        self.assertFalse(logs.at_least(logging.WARNING), logs.text())
        self.assertIsNotNone(_fetcher("NIFTY").preview_legs("HH"))

    def test_config_carries_no_delta_floor_settings(self):
        self.assertFalse(hasattr(config, "GREEK_SETTINGS"))
        self.assertFalse(hasattr(TRADE_SETTINGS, "sell_with_futures"))

    def test_no_instrument_carries_a_hedge_offset(self):
        for index in instruments.NAMES:
            inst = instruments.get(index)
            self.assertFalse(hasattr(inst, "hedge_offset_long"), index)
            self.assertFalse(hasattr(inst, "hedge_offset_short"), index)


if __name__ == "__main__":
    unittest.main(verbosity=2)
