"""HH/LL live routing: contract pick, LMT IOC pricing, fill confirmation, halts."""

from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal

from hhll_algo import HHLLPaperAlgo
from hhll_detector import Pivot
from hhll_live import (Contract, Fill, LiveRoutingError, ShoonyaFutureRouter,
                       UncertainFillError, assert_flat, resolve_front_future)
from zerodha_charges import fo_charges

CONTRACT = Contract(tsym="NIFTY26NOV26F", token="35001", exchange="NFO",
                    expiry=date(2026, 11, 26), lot=65, tick=Decimal("0.05"), freeze=1800)


class FakeApi:
    def __init__(self, *, quote=None, statuses=None, orderno="N1"):
        self.quote = quote or {"stat": "Ok", "tsym": CONTRACT.tsym, "sp1": "100", "bp1": "99.9",
                               "uc": "150", "lc": "50"}
        self.statuses = list(statuses or [{"status": "COMPLETE", "fillshares": "65", "avgprc": "100.5"}])
        self.orderno = orderno
        self.placed = []

    def get_quotes(self, exchange, token, timeout=30):
        return self.quote

    def place_order(self, **kw):
        self.placed.append(kw)
        return {"stat": "Ok", "norenordno": self.orderno}

    def single_order_history(self, orderno):
        row = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return [row]


def _router(api, timeout=0.0):
    return ShoonyaFutureRouter(api, CONTRACT, poll_s=0.0, timeout_s=timeout, sleep=lambda _: None)


class PricingTests(unittest.TestCase):
    def test_buy_crosses_by_2pct_rounded_up_and_clamped_to_band(self):
        r = _router(FakeApi())
        # ask 100 x 1.02 = 102 -> band uc 150 does not bind
        self.assertEqual(r.limit_price("B", FakeApi().quote), Decimal("102.00"))
        q = {"stat": "Ok", "tsym": CONTRACT.tsym, "sp1": "100", "uc": "101"}
        self.assertEqual(r.limit_price("B", q), Decimal("101"))

    def test_sell_crosses_by_2pct_rounded_down_and_clamped_to_band(self):
        r = _router(FakeApi())
        q = {"stat": "Ok", "tsym": CONTRACT.tsym, "bp1": "100.03", "lc": "97"}
        self.assertEqual(r.limit_price("S", q), Decimal("98.00"))  # 98.0294 floored to tick
        q3 = {"stat": "Ok", "tsym": CONTRACT.tsym, "bp1": "100.03", "lc": "99"}
        self.assertEqual(r.limit_price("S", q3), Decimal("99"))    # clamped up to lower circuit
        q2 = {"stat": "Ok", "tsym": CONTRACT.tsym, "bp1": "100", "lc": "99.5"}
        self.assertEqual(r.limit_price("S", q2), Decimal("99.5"))

    def test_bad_quote_refused(self):
        r = _router(FakeApi())
        with self.assertRaises(LiveRoutingError):
            r.limit_price("B", {"stat": "Not_Ok"})
        with self.assertRaises(LiveRoutingError):
            r.limit_price("B", {"stat": "Ok", "tsym": "OTHER", "sp1": "1"})


class SendTests(unittest.TestCase):
    def test_order_fields_and_fill(self):
        api = FakeApi()
        fill = _router(api).send("BUY", 65, "HH@1")
        kw = api.placed[0]
        self.assertEqual((kw["buy_or_sell"], kw["exchange"], kw["tradingsymbol"], kw["quantity"]),
                         ("B", "NFO", CONTRACT.tsym, 65))
        self.assertEqual((kw["price_type"], kw["retention"], kw["product_type"]), ("LMT", "IOC", "M"))
        self.assertEqual(fill, Fill(orderno="N1", price=100.5, qty=65))

    def test_rejected_is_no_fill(self):
        api = FakeApi(statuses=[{"status": "REJECTED"}])
        with self.assertRaises(LiveRoutingError) as cm:
            _router(api).send("SELL", 65)
        self.assertNotIsInstance(cm.exception, UncertainFillError)

    def test_partial_or_stuck_is_uncertain(self):
        with self.assertRaises(UncertainFillError):
            _router(FakeApi(statuses=[{"status": "COMPLETE", "fillshares": "30", "avgprc": "100"}])).send("BUY", 65)
        with self.assertRaises(UncertainFillError):
            _router(FakeApi(statuses=[{"status": "OPEN"}])).send("BUY", 65)

    def test_bad_quantity_refused_before_order(self):
        api = FakeApi()
        with self.assertRaises(LiveRoutingError):
            _router(api).send("BUY", 30)  # not a lot multiple
        self.assertEqual(api.placed, [])


class AlgoLiveTests(unittest.TestCase):
    class StubRouter:
        def __init__(self, fills):
            self.fills = list(fills)
            self.calls = []

        def send(self, side, units, reason=""):
            self.calls.append((side, units, reason))
            nxt = self.fills.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return Fill(orderno="X", price=nxt, qty=units)

    def _pivot(self, kind, price, i):
        return Pivot(pivot_type=kind, price=price, extreme_at_ms=i, extreme_time=None,
                     extreme_index=i, confirmed_at_ms=i * 1000, confirmed_time=None,
                     confirmed_price=price, confirmed_index=i, confirmation_lag_ticks=1,
                     confirmation_lag_ms=0, leg_size=0.0, reversal_size=0.0, anchor_price=0.0)

    def test_fill_price_and_charges_come_from_broker_fill(self):
        router = self.StubRouter([100.0, 110.0, 110.0])  # open short, close short, open long
        algo = HHLLPaperAlgo(lot_size=65, mode="LIVE", router=router)
        algo.on_pivot(self._pivot("HH", 100.0, 1))   # short at 100 (fill)
        algo.on_pivot(self._pivot("LL", 90.0, 2))    # close short at 110, open long
        self.assertEqual(router.calls[0], ("SELL", 65, "HH@100.0"))
        self.assertEqual(algo.trades[0].entry_price, 100.0)
        self.assertEqual(algo.trades[0].exit_price, 110.0)
        self.assertEqual(algo.trades[0].gross_pnl, Decimal("-650.00"))
        self.assertEqual(algo.trades[0].entry_charges, fo_charges(instrument="future", side="SELL", units=65, price=100.0).total)

    def test_failed_close_halts_and_does_not_open(self):
        router = self.StubRouter([100.0, LiveRoutingError("rejected")])
        algo = HHLLPaperAlgo(lot_size=65, mode="LIVE", router=router)
        algo.on_pivot(self._pivot("HH", 100.0, 1))
        algo.on_pivot(self._pivot("LL", 90.0, 2))
        self.assertIsNotNone(algo.halted)
        self.assertEqual(len(router.calls), 2)          # no open after failed close
        self.assertEqual(algo.position.direction, "SHORT")  # still short, known
        algo.on_pivot(self._pivot("HH", 120.0, 3))
        self.assertEqual(len(router.calls), 2)          # halted: nothing more sent

    def test_failed_open_leaves_flat_and_halted(self):
        router = self.StubRouter([UncertainFillError("stuck")])
        algo = HHLLPaperAlgo(lot_size=65, mode="LIVE", router=router)
        algo.on_pivot(self._pivot("HH", 100.0, 1))
        self.assertIsNone(algo.position)
        self.assertIn("stuck", algo.halted)


class ContractTests(unittest.TestCase):
    class SearchApi:
        def __init__(self, rows, meta=None):
            self.rows, self.meta = rows, meta or {"stat": "Ok", "ls": "65", "ti": "0.05", "frzqty": "1800"}
            self.asked = None

        def searchscrip(self, exchange, searchtext):
            return {"stat": "Ok", "values": self.rows}

        def get_security_info(self, exchange, token):
            self.asked = token
            return self.meta

    ROWS = [
        {"tsym": "NIFTY26NOV2625000CE", "token": "1"},
        {"tsym": "NIFTY26NOV26F", "token": "35001"},
        {"tsym": "NIFTY26DEC26F", "token": "35002"},
        {"tsym": "NIFTY26OCT26F", "token": "34999"},
        {"tsym": "BANKNIFTY26NOV26F", "token": "9"},
    ]

    def test_nearest_future_still_ahead(self):
        api = self.SearchApi(self.ROWS)
        c = resolve_front_future(api, today=date(2026, 11, 2))
        self.assertEqual((c.tsym, c.token, c.lot, c.freeze), ("NIFTY26NOV26F", "35001", 65, 1755))

    def test_expiry_day_rolls_to_next_month(self):
        c = resolve_front_future(self.SearchApi(self.ROWS), today=date(2026, 11, 26))  # NOV26F expires 26-Nov
        self.assertEqual(c.tsym, "NIFTY26DEC26F")

    def test_assert_flat(self):
        class P:
            def __init__(self, rows): self.rows = rows
            def get_positions(self): return self.rows
        assert_flat(P([]), CONTRACT.tsym)
        assert_flat(P([{"tsym": "NIFTY26NOV26F", "netqty": "0"}]), CONTRACT.tsym)
        with self.assertRaises(LiveRoutingError):
            assert_flat(P([{"tsym": "NIFTY26NOV26F", "netqty": "-65"}]), CONTRACT.tsym)


if __name__ == "__main__":
    unittest.main()
