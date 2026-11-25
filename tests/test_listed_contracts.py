"""Expiry, strike and lot follow the exchange's listing, not the config."""

from __future__ import annotations

import logging
import unittest
from datetime import date
from unittest import mock

import getdata
import instruments
from config import TRADE_SETTINGS
from getdata import GetSymbol
from manual_desk import DeskLegs
from tests.log_capture import capture_logs


def _book(root, expiry, strikes, lot, name=None):
    """A master book: a call and a put at each strike, NFO-named unless `name` is given."""
    code = expiry.strftime("%d%b%y").upper()
    return {(expiry, kind, k): ((name or "{root}{code}{kind}{k}").format(root=root, code=code, kind=kind, k=k), lot)
            for k in strikes for kind in "CP"}


def _builder(index, price, cls=GetSymbol):
    fetcher = cls(None, lambda: price, instrument=instruments.get(index),
                  expiry_holidays=TRADE_SETTINGS.expiry_holidays)
    fetcher.tok = "1"
    fetcher.set_pivot_type_getter(lambda: "HH")
    return fetcher


class ListedContractTests(unittest.TestCase):
    def setUp(self):
        saved = {n: instruments.get(n).quantity for n in instruments.NAMES}
        self.addCleanup(lambda: [instruments.set_lot(n, q) for n, q in saved.items()])

    def _nfo(self, books, today):
        return (mock.patch("getdata.ist.today", return_value=today),
                mock.patch("getdata._load_nfo_contracts", return_value=books))

    def test_a_wider_grid_on_the_next_month_takes_the_listed_strike(self):
        # Fri 20 Nov 2026: the 23 Nov expiry is one session out, so the entry
        # is the 29 Dec series - which lists 100s where the config says 50.
        nov, dec = date(2026, 11, 23), date(2026, 12, 29)
        books = {"FINNIFTY": {**_book("FINNIFTY", nov, range(27000, 27500, 50), 60),
                              **_book("FINNIFTY", dec, range(27000, 27500, 100), 60)}}
        fetcher = _builder("FINNIFTY", 27_372.0)
        today, load = self._nfo(books, date(2026, 11, 20))
        with today, load:
            legs = fetcher.update_symbol()
        self.assertEqual((legs["buy"], legs["sell"]), ("FINNIFTY29DEC26C27300", "FINNIFTY29DEC26P27300"))
        self.assertEqual((legs["strike"], legs["atm_strike"], legs["expiry"]), (27300, 27400, "2026-12-29"))

    def test_the_expiry_is_the_listed_one_when_the_calendar_disagrees(self):
        # 26 Jan 2027 is a holiday the exchange has already moved off; with it
        # missing from the config the calendar says the 26th, the listing the 25th.
        books = {"BANKNIFTY": _book("BANKNIFTY", date(2027, 1, 25), range(57000, 59000, 100), 30)}
        fetcher = GetSymbol(None, lambda: 58_040.0, instrument=instruments.get("BANKNIFTY"))
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        today, load = self._nfo(books, date(2027, 1, 4))
        with today, load, capture_logs(logging.getLogger()) as logs:
            legs = fetcher.update_symbol()
        self.assertEqual(legs["buy"], "BANKNIFTY25JAN27C58000")
        self.assertTrue(logs.has("trading the listed one", logging.WARNING), logs.text())

    def test_an_expiry_too_close_is_passed_over_for_the_next_listed(self):
        books = {"NIFTY": {**_book("NIFTY", date(2026, 11, 17), range(25800, 26100, 50), 65),
                           **_book("NIFTY", date(2026, 11, 23), range(25800, 26100, 50), 65)}}
        fetcher = _builder("NIFTY", 25_990.0)
        today, load = self._nfo(books, date(2026, 11, 16))       # one session to the 17th
        with today, load:
            self.assertEqual(fetcher.update_symbol()["buy"], "NIFTY23NOV26C25950")

    def test_a_far_quarterly_is_never_taken_for_a_missing_near_series(self):
        books = {"BANKNIFTY": _book("BANKNIFTY", date(2027, 3, 30), range(57000, 59000, 100), 30)}
        fetcher = _builder("BANKNIFTY", 58_040.0)
        today, load = self._nfo(books, date(2027, 1, 22))
        with today, load, capture_logs(logging.getLogger()):
            self.assertIsNone(fetcher.update_symbol())
        self.assertIn("no BANKNIFTY contract", fetcher.legs_blocked_reason)

    def test_a_revised_lot_is_adopted_from_the_listing(self):
        expiry = date(2027, 1, 28)
        contracts = _book("SENSEX", expiry, range(84000, 85000, 100), 25, name="SENSEX27JAN{k}{kind}E")
        fetcher = _builder("SENSEX", 84_555.0)
        with mock.patch("getdata.ist.today", return_value=date(2027, 1, 20)), \
                mock.patch("getdata._load_bfo_contracts", return_value=contracts), \
                capture_logs(logging.getLogger()) as logs:
            legs = fetcher.update_symbol()
        self.assertEqual((legs["buy"], legs["sell"], legs["quantity"]),
                         ("SENSEX27JAN84500CE", "SENSEX27JAN84500PE", 25))
        self.assertEqual((instruments.get("SENSEX").quantity, instruments.quantity_of("SENSEX", 0)), (25, 25))
        self.assertTrue(logs.has("lists a lot of 25, not 20", logging.WARNING), logs.text())

    def test_the_lot_is_adopted_while_idle(self):
        books = {"NIFTY": _book("NIFTY", date(2026, 11, 23), range(25800, 26100, 50), 75)}
        fetcher = _builder("NIFTY", 25_990.0)
        fetcher.set_pivot_type_getter(lambda: None)
        today, load = self._nfo(books, date(2026, 11, 16))
        with today, load, capture_logs(logging.getLogger()):
            fetcher.update_symbol()
        self.assertEqual(instruments.get("NIFTY").quantity, 75)

    def test_legs_with_two_lots_are_refused(self):
        expiry = date(2026, 11, 23)
        book = _book("NIFTY", expiry, range(25800, 26100, 50), 65)
        book[(expiry, "P", 25950)] = ("NIFTY23NOV26P25950", 75)
        fetcher = _builder("NIFTY", 25_990.0)
        today, load = self._nfo({"NIFTY": book}, date(2026, 11, 16))
        with today, load, capture_logs(logging.getLogger()):
            self.assertIsNone(fetcher.update_symbol())
        self.assertIn("unusable lots", fetcher.legs_blocked_reason)

    def test_a_series_with_no_strike_near_the_money_is_refused(self):
        books = {"NIFTY": _book("NIFTY", date(2026, 11, 23), (24000, 27000), 65)}
        fetcher = _builder("NIFTY", 25_990.0)
        today, load = self._nfo(books, date(2026, 11, 16))
        with today, load, capture_logs(logging.getLogger()):
            self.assertIsNone(fetcher.update_symbol())
        self.assertIn("no call-and-put strike within", fetcher.legs_blocked_reason)

    def test_a_strike_listing_only_one_side_is_skipped(self):
        expiry = date(2026, 11, 23)
        book = _book("NIFTY", expiry, range(25800, 26100, 50), 65)
        del book[(expiry, "P", 25950)]
        fetcher = _builder("NIFTY", 25_990.0)
        today, load = self._nfo({"NIFTY": book}, date(2026, 11, 16))
        with today, load:
            self.assertEqual(fetcher.update_symbol()["strike"], 25900)

    def test_the_desk_short_takes_the_listed_strike_above(self):
        books = {"FINNIFTY": _book("FINNIFTY", date(2026, 12, 29), range(27000, 27500, 100), 60)}
        fetcher = _builder("FINNIFTY", 27_310.0, DeskLegs)
        fetcher.set_pivot_type_getter(lambda: "LL")
        today, load = self._nfo(books, date(2026, 11, 20))
        with today, load:
            legs = fetcher.update_symbol()
        self.assertEqual((legs["buy"], legs["sell"]), ("FINNIFTY29DEC26P27400", "FINNIFTY29DEC26C27400"))

    def test_without_the_nfo_master_the_calendar_and_scrip_search_still_serve(self):
        class Api:
            def searchscrip(self, exchange, searchtext):
                return {"stat": "Ok", "values": [{"tsym": searchtext}]}

        fetcher = _builder("NIFTY", 24_037.45)
        fetcher.api = Api()
        getdata._THROTTLE.reset("no_master:NIFTY")
        with mock.patch("getdata.ist.today", return_value=date(2026, 9, 21)), \
                mock.patch("getdata._load_nfo_contracts", side_effect=OSError("offline")), \
                capture_logs(logging.getLogger()) as logs:
            legs = fetcher.update_symbol()
        self.assertEqual(legs["buy"], "NIFTY29SEP26C24000")
        self.assertTrue(logs.has("configured calendar", logging.WARNING), logs.text())

    def test_the_nfo_master_is_parsed_per_index_and_read_once_a_day(self):
        rows = [
            {"Symbol": "", "TradingSymbol": "", "Instrument": "", "Expiry": "", "OptionType": "", "StrikePrice": "0", "LotSize": ""},
            {"Symbol": "NIFTY", "TradingSymbol": "NIFTY23NOV26C25950", "Instrument": "OPTIDX", "Expiry": "23-NOV-2026", "OptionType": "CE", "StrikePrice": "25950", "LotSize": "65"},
            {"Symbol": "NIFTYNXT50", "TradingSymbol": "NIFTYNXT5023NOV26P70000", "Instrument": "OPTIDX", "Expiry": "23-NOV-2026", "OptionType": "PE", "StrikePrice": "70000", "LotSize": "25"},
            {"Symbol": "NIFTYFPI", "TradingSymbol": "NIFTYFPI23NOV26C1", "Instrument": "OPTIDX", "Expiry": "23-NOV-2026", "OptionType": "CE", "StrikePrice": "1", "LotSize": "1"},
            {"Symbol": "RELIANCE", "TradingSymbol": "RELIANCE23NOV26C1400", "Instrument": "OPTSTK", "Expiry": "23-NOV-2026", "OptionType": "CE", "StrikePrice": "1400", "LotSize": "500"},
        ]
        day = date(2026, 11, 16)
        with mock.patch("getdata._load_master_rows", return_value=rows) as load, \
                mock.patch.dict(getdata._NFO_BOOKS, {"day": None, "books": {}}):
            books = getdata._load_nfo_contracts(day)
            getdata._load_nfo_contracts(day)
            self.assertEqual(load.call_count, 1)
            getdata._load_nfo_contracts(day, True)
            self.assertEqual(load.call_count, 2)
        self.assertEqual(books, {
            "NIFTY": {(date(2026, 11, 23), "C", 25950): ("NIFTY23NOV26C25950", 65)},
            "NIFTYNXT50": {(date(2026, 11, 23), "P", 70000): ("NIFTYNXT5023NOV26P70000", 25)},
        })


if __name__ == "__main__":
    unittest.main()
