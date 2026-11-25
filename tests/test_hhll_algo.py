"""HH/LL algo: pivot -> trade flow, flips, and Zerodha charge accounting."""

from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import hhll_algo
from hhll_algo import HHLLPaperAlgo
from zerodha_charges import fo_charges


def _feed(algo: HHLLPaperAlgo, prices: list[float], t0: float = 1_700_000_000.0) -> None:
    for i, p in enumerate(prices):
        algo.on_price(p, t0 + i)


# Reversal threshold = 3 pts (reversal_ticks 60 x tick 0.05).
# Confirmed pivots: HH 100, LL 97, HH 100, LL 80, HH 120, LL 90.
ZIGZAG = [90, 95, 100, 97, 100, 97, 92, 85, 80, 84, 88, 95, 102, 110, 120, 116, 110, 100, 90, 93]


class AlgoFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Path(self.tmp.name) / "ledger.jsonl"
        self.algo = HHLLPaperAlgo(lot_size=65, lots=1, tick_size=0.05, reversal_ticks=60,
                                  min_leg=0, ledger_path=self.ledger)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_pivots_alternate_and_flip_position(self) -> None:
        _feed(self.algo, ZIGZAG)
        types = [(p.pivot_type, p.price) for p in self.algo.pivots]
        self.assertEqual(types, [("HH", 100.0), ("LL", 97.0), ("HH", 100.0),
                                 ("LL", 80.0), ("HH", 120.0), ("LL", 90.0)])
        # First pivot opens; every later pivot flips = close + open, two orders.
        n = len(types)
        self.assertEqual(len(self.algo.executions), 2 * n - 1)
        self.assertEqual(len(self.algo.trades), n - 1)
        self.assertEqual(self.algo.executions[0].side, "SELL")          # HH -> short
        self.assertEqual(self.algo.position.direction, "LONG")          # last pivot LL -> long
        self.assertEqual(self.algo.executions[-1].reason, "LL@90.0")

    def test_gross_and_net_pnl_and_charges(self) -> None:
        _feed(self.algo, ZIGZAG)
        t = self.algo.trades[0]
        self.assertEqual(t.direction, "SHORT")
        self.assertEqual(t.gross_pnl, Decimal(str(round(-65 * (t.exit_price - t.entry_price), 2))))
        self.assertEqual(t.net_pnl, t.gross_pnl - t.entry_charges - t.exit_charges)
        sell = fo_charges(instrument="future", side="SELL", units=65, price=t.entry_price)
        buy = fo_charges(instrument="future", side="BUY", units=65, price=t.exit_price)
        self.assertEqual(t.entry_charges, sell.total)
        self.assertEqual(t.exit_charges, buy.total)

    def test_no_order_without_new_pivot(self) -> None:
        algo = HHLLPaperAlgo(lot_size=65, tick_size=0.05, reversal_ticks=60, min_leg=0)
        _feed(algo, [90, 95, 100, 97, 98, 97.5, 98])  # HH confirms at 97; later ticks add no pivot
        self.assertEqual(len(algo.executions), 1)
        self.assertEqual(algo.position.direction, "SHORT")

    def test_ledger_rows_written(self) -> None:
        _feed(self.algo, ZIGZAG)
        rows = [json.loads(line) for line in self.ledger.read_text().splitlines()]
        self.assertEqual(sum(r["kind"] == "execution" for r in rows), len(self.algo.executions))
        self.assertEqual(sum(r["kind"] == "trade" for r in rows), len(self.algo.trades))

    def test_live_refused_and_status(self) -> None:
        with self.assertRaises(ValueError):
            HHLLPaperAlgo(mode="LIVE")  # LIVE without a router
        with self.assertRaises(ValueError):
            HHLLPaperAlgo(mode="TEST", router=object())
        _feed(self.algo, ZIGZAG)
        st = self.algo.status()
        self.assertEqual(st["position"], "LONG")
        self.assertEqual(st["trades"], 5)
        self.assertEqual(Decimal(st["realised_net"]), self.algo.realised_net())
        self.assertEqual(st["min_leg"], 0.0)

    def test_big_leg_length_and_300_charges_profitability(self) -> None:
        """With min_leg big enough (e.g. 20 pts), micro chop (3 pts) is skipped,
        avoiding charge churn, and only macro legs execute, yielding net profit."""
        algo = HHLLPaperAlgo(lot_size=65, lots=1, tick_size=0.05, reversal_ticks=60, min_leg=20.0)
        _feed(algo, ZIGZAG)
        # 3 pt micro-swings (100 -> 97 -> 100) are skipped!
        # Only qualifying legs (80 -> 120 and 120 -> 90) execute!
        self.assertEqual(len(algo.trades), 2)
        for t in algo.trades:
            self.assertGreater(t.gross_pnl, Decimal(0))
            self.assertGreater(t.net_pnl, Decimal(0))
        # Total net PnL is solidly positive
        self.assertGreater(algo.realised_net(), Decimal("3000.0"))

    def test_one_exit_condition_only_opposite_pivot(self) -> None:
        """Verify: HH -> SHORT, LL -> LONG, and only opposite pivot exits."""
        # 16 ticks x 0.05 = 0.8 pts fast confirmation
        algo = HHLLPaperAlgo(lot_size=65, lots=1, tick_size=0.05, reversal_ticks=16, min_leg=20.0)
        # Rally 23300 to 23350 (leg 50 pts), pull back to 23349.2 (0.8 pts) -> confirms HH
        _feed(algo, [23300.0, 23320.0, 23350.0, 23349.2])
        self.assertEqual(algo.position.direction, "SHORT")
        self.assertEqual(len(algo.trades), 0)

        # Micro dip to 23345 and bounce to 23346 (LL with leg 5 < 20 pts) -> NOT exited!
        _feed(algo, [23345.0, 23346.0])
        self.assertEqual(algo.position.direction, "SHORT")
        self.assertEqual(len(algo.trades), 0)

        # Drop all the way to 23310 (leg 40 pts from 23350) and bounce to 23310.8 (0.8 pts) -> confirms LL
        _feed(algo, [23320.0, 23310.0, 23310.8])
        # Opposite pivot (LL) closes SHORT and flips to LONG!
        self.assertEqual(len(algo.trades), 1)
        self.assertEqual(algo.trades[0].direction, "SHORT")
        self.assertGreater(algo.trades[0].net_pnl, Decimal("1500.0"))
        self.assertEqual(algo.position.direction, "LONG")


if __name__ == "__main__":
    unittest.main()
