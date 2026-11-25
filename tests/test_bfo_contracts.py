"""BFO names come from the broker master, not NSE's symbol formula."""

import io
import time
import unittest
import zipfile
from datetime import date
from unittest.mock import patch

import getdata
from getdata import GetSymbol
import instruments


class BfoContractTests(unittest.TestCase):
    def test_leg_age_starts_when_spot_price_was_read(self):
        fetcher = GetSymbol(None, lambda: 24000, instrument=instruments.get("NIFTY"))
        fetcher.tok = "26000"
        fetcher.set_pivot_type_getter(lambda: "HH")
        clock = [100.0]

        def delayed_lookup(_symbol):
            clock[0] = 200.0
            return True

        legs = {"buy": "NIFTY29SEP26P22750", "sell": "NIFTY29SEP26P24250",
                "bias": "HH", "sell_side": "S"}
        with patch("getdata.time.monotonic", side_effect=lambda: clock[0]), \
             patch.object(fetcher, "_build_legs", return_value=legs), \
             patch.object(fetcher, "_resolve_tradingsymbol", side_effect=delayed_lookup):
            published = fetcher.update_symbol()
        self.assertEqual(published["built_at"], 100.0)

    def test_monthly_and_weekly_names_and_futures_are_read_from_master(self):
        rows = (
            "Exchange,Token,LotSize,Symbol,TradingSymbol,Expiry,Instrument,OptionType,StrikePrice,TickSize,\n"
            "BFO,1,20,BSXOPT,SENSEX26SEP73500PE,24-SEP-2026,OPTIDX,PE,73500,0.05,\n"
            "BFO,2,20,BSXOPT,SENSEX26O0173500PE,01-OCT-2026,OPTIDX,PE,73500,0.05,\n"
            "BFO,3,20,BSXFUT,SENSEX26SEPFUT,24-SEP-2026,FUTIDX,XX,0,0.05,\n"
        )
        content = io.BytesIO()
        with zipfile.ZipFile(content, "w") as archive:
            archive.writestr("BFO_symbols.txt", rows)

        class Response:
            def raise_for_status(self):
                pass

        Response.content = content.getvalue()

        with patch("getdata.requests.get", return_value=Response()):
            contracts = getdata._load_bfo_contracts()
        self.assertEqual(contracts[(date(2026, 9, 24), "P", 73500)],
                         ("SENSEX26SEP73500PE", 20))
        self.assertEqual(contracts[(date(2026, 10, 1), "P", 73500)],
                         ("SENSEX26O0173500PE", 20))
        self.assertEqual(contracts[(date(2026, 9, 24), "F", 0)],
                         ("SENSEX26SEPFUT", 20))

    def test_legs_use_master_symbols_and_fail_closed_on_missing_contract(self):
        fetcher = GetSymbol(None, lambda: 75000, instrument=instruments.get("SENSEX"))
        contracts = {
            (date(2026, 9, 24), "P", 73500): ("SENSEX26SEP73500PE", 20),
            (date(2026, 9, 24), "P", 76200): ("SENSEX26SEP76200PE", 20),
        }
        legs = {"buy": "SENSEX24SEP26P73500", "sell": "SENSEX24SEP26P76200"}
        with patch("getdata.ist.today", return_value=date(2026, 9, 23)), \
             patch("getdata._load_bfo_contracts", return_value=contracts) as load:
            self.assertIsNone(fetcher._resolve_listed_legs(legs))
            self.assertEqual(legs["buy"], "SENSEX26SEP73500PE")
            self.assertEqual(legs["sell"], "SENSEX26SEP76200PE")
            self.assertEqual(load.call_count, 1)
            self.assertIn("no SENSEX contract", fetcher._resolve_listed_legs(
                {"buy": "SENSEX24SEP26P73600", "sell": legs["sell"]}) or "")

    def test_master_failure_has_a_retry_delay(self):
        fetcher = GetSymbol(None, lambda: 75000, instrument=instruments.get("SENSEX"))
        legs = {"buy": "SENSEX24SEP26P73500", "sell": "SENSEX24SEP26P76200"}
        with patch("getdata.ist.today", return_value=date(2026, 9, 23)), \
             patch("getdata._load_bfo_contracts", side_effect=OSError("offline")) as load:
            self.assertIn("unavailable", fetcher._resolve_listed_legs(legs) or "")
            self.assertIn("unavailable", fetcher._resolve_listed_legs(legs) or "")
            self.assertEqual(load.call_count, 1)

    def test_blocked_contract_clears_previously_published_legs(self):
        fetcher = GetSymbol(None, lambda: 75000, instrument=instruments.get("SENSEX"))
        fetcher.tok = "1"
        fetcher.set_pivot_type_getter(lambda: "HH")
        good = {"buy": "SENSEX24SEP26P73500", "sell": "SENSEX24SEP26P76200"}
        missing = {"buy": "SENSEX24SEP26P73600", "sell": "SENSEX24SEP26P76200"}
        contracts = {
            (date(2026, 9, 24), "P", 73500): ("SENSEX26SEP73500PE", 20),
            (date(2026, 9, 24), "P", 76200): ("SENSEX26SEP76200PE", 20),
        }
        with patch("getdata.ist.today", return_value=date(2026, 9, 23)), \
             patch("getdata._load_bfo_contracts", return_value=contracts), \
             patch.object(fetcher, "_build_legs", side_effect=[good, missing]):
            published = fetcher.update_symbol()
            self.assertEqual(published["buy"], "SENSEX26SEP73500PE")
            self.assertLessEqual(time.monotonic() - published["built_at"], 1)
            self.assertIsNone(fetcher.update_symbol())
        self.assertIsNone(fetcher.final_symbols)
        self.assertIn("no SENSEX contract", fetcher.legs_blocked_reason or "")

    def test_broker_lookup_failure_is_not_mistaken_for_an_unlisted_contract(self):
        class Api:
            calls = 0

            def searchscrip(self, **_kwargs):
                self.calls += 1
                raise OSError("broker unavailable")

        api = Api()
        fetcher = GetSymbol(api, lambda: 24000, instrument=instruments.get("NIFTY"))
        with patch("getdata.time.monotonic", return_value=100):
            self.assertIsNone(fetcher._resolve_tradingsymbol("NIFTY29SEP26P22750"))
            self.assertIsNone(fetcher._resolve_tradingsymbol("NIFTY29SEP26P22750"))
        self.assertEqual(api.calls, 1)


if __name__ == "__main__":
    unittest.main()
