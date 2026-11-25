"""The manual desk: its ledger, broker product, tape, candles, routes and orders.

Everything here runs on fakes except `ManualDeskPostgresTests`, which needs
the real PostgreSQL and is opt-in (`LUNE_DB_TESTS=1`): it builds the desk's
schema in a throwaway schema of its own, drives a paper and a live desk
through it end to end - the live one through `main.py`'s own order-update
writer - and drops the schema again. It never touches `botdata` or
`manualdata`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
import threading
import time
import unittest
import uuid
from dataclasses import replace
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
from flask import Flask

import instruments
import ist
import manual_desk
from config import TRADE_SETTINGS
from manual_desk import (
    CandleCache,
    DeskBusy,
    DeskLegs,
    ManualBroker,
    ManualDesk,
    ManualLedger,
    ManualSettings,
    ManualTradeManagerLive,
    ManualTradeManagerTest,
    TickTape,
)
from getdata import GetSymbol, LegSelectionError
from tests.log_capture import capture_logs
from tests.order_path_fakes import FakeBroker, FakeDB, FakeSymbols

#: Open around the clock, so the checks do not depend on when they run.
ALWAYS = ManualSettings(entry_from=dt_time(0, 0), square_off_at=dt_time(23, 59, 58),
                        market_close=dt_time(23, 59, 59), command_ttl_sec=30.0)

FAST = replace(
    TRADE_SETTINGS,
    buy_wait_timeout_sec=3.0, sell_wait_timeout_sec=3.0,
    exchordid_wait_timeout_sec=2.0, exit_exchordid_wait_timeout_sec=2.0,
    poll_interval_sec=0.02, exit_retry_backoff_sec=0.01,
    emergency_flatten_sleep_sec=0.01, entry_fail_cooldown_sec=0.0,
)

LOT = instruments.get("NIFTY").quantity


class DeskSymbols(FakeSymbols):
    """`FakeSymbols` that the desk can ask to build legs, as it asks `DeskLegs`:
    for the `side` of the click, a short being the long's legs turned over."""

    side = "LONG"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.builds = 0
        self.long_legs = (self.buy_leg, self.sell_leg)

    def update_symbol(self):
        self.builds += 1
        short = self.side == "SHORT"
        self.bias = "LL" if short else "HH"
        self.buy_leg, self.sell_leg = self.long_legs[::-1] if short else self.long_legs
        return self.final_symbols


class ProductBroker(FakeBroker):
    """`FakeBroker` with the broker's per-product position book."""

    def __init__(self, db, **kwargs) -> None:
        self.books: dict[str, dict[str, int]] = {}
        self._product = "M"
        self.products: list[str] = []
        super().__init__(db, **kwargs)

    def place_order(self, **kwargs):
        self._product = kwargs["product_type"]
        self.products.append(self._product)
        return super().place_order(**kwargs)

    def _apply_position(self, symbol: str, side: str, qty: int) -> None:
        book = self.books.setdefault(self._product, {})
        book[symbol] = book.get(symbol, 0) + (qty if side == "B" else -qty)

    def get_positions(self):
        return [{"stat": "Ok", "tsym": sym, "netqty": str(qty), "prd": prd}
                for prd, book in self.books.items() for sym, qty in book.items()]


def fake_ledger(db) -> ManualLedger:
    """`FakeDB` behind the desk's ledger. It models `botdata` and no DDL, so the
    retarget is the identity and the schema counts as provisioned."""
    ledger = ManualLedger(db, schema="botdata")
    ledger._schema_ready = True
    return ledger


def live_manager(db, broker, symbols, price=24_350.0, settings=ALWAYS):
    manager = ManualTradeManagerLive(
        api=ManualBroker(broker, "I"),
        db_connector=fake_ledger(db),
        symbol_fetcher=symbols,
        symbol_fetchers={"NIFTY": symbols},
        index_live_price_getters={"NIFTY": lambda: price},
        live_price_getter=lambda: price,
        fresh_price_getter=lambda: price,
        trade_settings=FAST,
        desk_settings=settings,
    )
    manager.ENTRY_POSITION_VERIFY_TIMEOUT_SEC = 0.5
    manager.ENTRY_POSITION_VERIFY_POLL_SEC = 0.02
    manager.RECONCILE_INTERVAL_SEC = 0.0
    return manager


class LedgerTests(unittest.TestCase):
    def test_every_statement_is_retargeted(self) -> None:
        seen = []

        class Raw:
            def execute(self, statement, params=None, *, fetch=False):
                seen.append(statement)

            def get_raw_connection(self):
                outer = self

                class Cursor:
                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        return False

                    def execute(self, statement, params=None):
                        seen.append(statement)

                class Conn:
                    returned = None

                    def cursor(self):
                        return Cursor()

                    def commit(self):
                        seen.append("COMMIT")

                outer.conn = Conn()
                return outer.conn

            def put_raw_connection(self, conn, *, close=False):
                seen.append(("PUT", conn is self.conn, close))

        raw = Raw()
        ledger = ManualLedger(raw)
        ledger.execute("SELECT 1 FROM botdata.trade_logs JOIN botdata.order_logs ON true")
        conn = ledger.get_raw_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE botdata.order_logs SET x=1 -- notbotdata.trade_logs")
        conn.commit()
        ledger.put_raw_connection(conn, close=True)
        self.assertEqual(
            ["SELECT 1 FROM manualdata.trade_logs JOIN manualdata.order_logs ON true",
             "UPDATE manualdata.order_logs SET x=1 -- notbotdata.trade_logs",
             "COMMIT", ("PUT", True, True)], seen)
        with self.assertRaises(TypeError):
            ledger.execute(b"SELECT 1 FROM botdata.trade_logs")


class BrokerTests(unittest.TestCase):
    def test_orders_go_out_as_the_desk_product(self) -> None:
        api = mock.Mock()
        ManualBroker(api, "I").place_order(product_type="M", tradingsymbol="X", quantity=65)
        api.place_order.assert_called_once_with(product_type="I", tradingsymbol="X", quantity=65)

    def test_only_its_own_positions_are_seen(self) -> None:
        api = mock.Mock()
        api.get_positions.return_value = [
            {"stat": "Ok", "tsym": "A", "netqty": "65", "prd": "M"},
            {"stat": "Ok", "tsym": "A", "netqty": "130", "prd": "I"},
            {"stat": "Ok", "tsym": "B", "netqty": "5"},
        ]
        self.assertEqual([{"stat": "Ok", "tsym": "A", "netqty": "130", "prd": "M"}],
                         ManualBroker(api, "I").get_positions())

    def test_an_unreadable_book_stays_unreadable(self) -> None:
        api = mock.Mock()
        api.get_positions.return_value = [{"stat": "Not_Ok", "emsg": "x"}, "garbage"]
        self.assertEqual([{"stat": "Not_Ok", "emsg": "x"}, "garbage"], ManualBroker(api).get_positions())
        api.get_positions.return_value = None
        self.assertIsNone(ManualBroker(api).get_positions())


class TickTapeTests(unittest.TestCase):
    def setUp(self) -> None:
        # Mid-session, so the checks do not depend on when they run.
        noon = datetime.combine(ist.today(), dt_time(12, 0), tzinfo=ist.IST)
        patcher = mock.patch.object(manual_desk.ist, "now", return_value=noon)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_pre_open_ticks_are_dropped(self) -> None:
        tape = TickTape({"NIFTY": "NSE|26000"})
        pre_open = datetime.combine(ist.today(), dt_time(9, 14, 59), tzinfo=ist.IST)
        with mock.patch.object(manual_desk.ist, "now", return_value=pre_open):
            tape.on_tick({"e": "NSE", "tk": "26000", "lp": "1"})   # yesterday's close, re-sent
        tape.on_tick({"e": "NSE", "tk": "26000", "lp": "2"})
        self.assertEqual([2.0], tape.read("NIFTY")[3])

    def test_ticks_are_attributed_and_kept(self) -> None:
        tape = TickTape({"NIFTY": "NSE|26000", "SENSEX": "BSE|1", "OTHER": "BSE|26000"})
        tape.on_tick({"e": "NSE", "tk": "26000", "lp": "24000.5"})
        tape.on_tick({"tk": "1", "lp": "80000"})            # unique token, no exchange
        tape.on_tick({"tk": "26000", "lp": "1"})            # shared token, no exchange: unattributable
        tape.on_tick({"e": "NSE", "tk": "26000", "v": "3"})  # a delta with no price
        tape.on_tick({"e": "NSE", "tk": "26000", "lp": "24001"})
        gen, total, times, prices = tape.read("NIFTY")
        self.assertEqual((2, [24000.5, 24001.0]), (total, prices))
        self.assertEqual([24001.0], tape.read("NIFTY", 1)[3])
        self.assertEqual(80000.0, tape.last("SENSEX"))
        self.assertIsNone(tape.last("OTHER"))

    def test_a_new_day_starts_a_new_tape(self) -> None:
        tape = TickTape({"NIFTY": "NSE|26000"})
        tape.on_tick({"e": "NSE", "tk": "26000", "lp": "1"})
        with mock.patch.object(manual_desk.ist, "today", return_value=ist.today() + timedelta(days=1)):
            tape.on_tick({"e": "NSE", "tk": "26000", "lp": "2"})
        self.assertEqual((1, 1, [2.0]), (tape.generation, tape.size("NIFTY"), tape.read("NIFTY")[3]))


class CandleCacheTests(unittest.TestCase):
    def test_keeps_only_today_and_caches_within_a_bar(self) -> None:
        today, yesterday = ist.today(), ist.today() - timedelta(days=1)
        api = mock.Mock()
        api.get_time_price_series.return_value = [
            {"time": f"{yesterday:%d-%m-%Y} 15:29:00", "into": "1", "inth": "2", "intl": "0.5", "intc": "1.5"},
            {"time": f"{today:%d-%m-%Y} 09:15:00", "into": "10", "inth": "12", "intl": "9", "intc": "11"},
            {"time": f"{today:%d-%m-%Y} 09:16:00", "into": "11", "inth": "13", "intl": "10", "intc": "12"},
        ]
        cache = CandleCache(api, {"NIFTY": ("NSE", "26000")})
        bars = cache.get("NIFTY", 1)
        self.assertEqual([f"{today:%Y-%m-%d}T09:15:00+05:30", f"{today:%Y-%m-%d}T09:16:00+05:30"],
                         [b["time"] for b in bars])
        self.assertEqual(ist.midnight().timestamp(), api.get_time_price_series.call_args.kwargs["starttime"])
        before_bell = mock.Mock()
        before_bell.get_time_price_series.return_value = api.get_time_price_series.return_value[:1]
        self.assertEqual([], CandleCache(before_bell, {"NIFTY": ("NSE", "26000")}).get("NIFTY", 1))
        self.assertEqual(1, api.get_time_price_series.call_count)
        self.assertEqual(1, api.get_time_price_series.call_args.kwargs["interval"])
        cache.get("NIFTY", 1)
        self.assertLessEqual(api.get_time_price_series.call_count, 2)  # 2 only if a minute just turned
        api.get_time_price_series.side_effect = RuntimeError("broker down")
        cache._cache[("NIFTY", 1)] = (-1, 0.0, bars)                   # force a refetch
        with mock.patch.object(manual_desk.LOGGER, "warning") as warned:
            self.assertEqual(bars, cache.get("NIFTY", 1))             # stale beats nothing
        warned.assert_called_once()
        with self.assertRaises(RuntimeError):
            cache.get("NIFTY", 15)


class DeskLegsTests(unittest.TestCase):
    """The desk's builder adds the mirror short; the algo's stays long-only."""

    def builder(self, cls=DeskLegs):
        return cls(None, lambda: 24_350.0, instrument=instruments.get("NIFTY"),
                   expiry_holidays=TRADE_SETTINGS.expiry_holidays)

    def test_a_short_buys_the_itm_put_and_sells_the_call_of_that_strike(self) -> None:
        with mock.patch("getdata.ist.today", return_value=date(2026, 9, 21)):
            short = self.builder()._build_legs(24_037.45, "LL")
            long = self.builder()._build_legs(24_037.45, "HH")
            on_strike = self.builder()._build_legs(24_050.0, "LL")
        self.assertEqual(("NIFTY29SEP26P24050", "NIFTY29SEP26C24050", "LL", "S"),
                         (short["buy"], short["sell"], short["bias"], short["sell_side"]))
        self.assertEqual(("NIFTY29SEP26C24000", "NIFTY29SEP26P24000", "HH"),
                         (long["buy"], long["sell"], long["bias"]))
        self.assertEqual(24_100, on_strike["strike"])  # on a strike is at the money
        self.assertEqual((short["expiry"], short["index"], short["exchange"]),
                         (long["expiry"], "NIFTY", "NFO"))
        with self.assertRaises(LegSelectionError):
            self.builder(GetSymbol)._build_legs(24_037.45, "LL")

    def test_the_desk_asks_for_the_side_of_the_click(self) -> None:
        desk = ManualDesk(api=mock.Mock(), mode="TEST", spot_tokens={"NIFTY": ("NSE", "26000")},
                          spot_keys={"NIFTY": "NSE|26000"}, price_getters={"NIFTY": lambda: 24_000.0},
                          ledger_db=FakeDB(), settings=ALWAYS)
        fetcher = desk.fetchers["NIFTY"]
        self.assertIsInstance(fetcher, DeskLegs)
        self.assertEqual("HH", fetcher._latest_forecast_pivot())
        fetcher.side = "SHORT"
        self.assertEqual("LL", fetcher._latest_forecast_pivot())


class LiveOrderPathTests(unittest.TestCase):
    """The algo's live manager, run by the desk: real SQL shapes, fake broker."""

    def run_cycle(self, manager) -> None:
        asyncio.run(manager._cycle_once())

    def test_buy_then_square_off_in_mis_beside_an_algo_nrml_position(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        broker = ProductBroker(db)
        # The algo holds the same strike in NRML. The desk must neither see it
        # (it would refuse to stack, or size its exit off it) nor touch it.
        broker.books["M"] = {symbols.buy_leg: LOT, symbols.sell_leg: -LOT}
        manager = live_manager(db, symbols=symbols, broker=broker)

        order = manager.decider.submit("BUY", "NIFTY", 2)
        with self.assertRaises(DeskBusy):
            manager.decider.submit("SELL")
        self.run_cycle(manager)
        last = manager.decider.snapshot()["last"]
        self.assertTrue(last["ok"], last)
        self.assertEqual(order["id"], last["id"])
        self.assertEqual(1, symbols.builds)
        self.assertTrue(manager.in_trade)
        self.assertEqual({"I"}, set(broker.products))
        self.assertEqual({symbols.buy_leg: 2 * LOT, symbols.sell_leg: -2 * LOT}, broker.books["I"])
        row = db.trade_logs[0]
        self.assertEqual(("manual", "NIFTY", 2 * LOT, "LIVE"),
                         (row["book"], row["index_name"], row["quantity"], row["execution_mode"]))

        manager.decider.submit("SELL")
        self.run_cycle(manager)
        last = manager.decider.snapshot()["last"]
        self.assertTrue(last["ok"], last)
        self.assertFalse(manager.in_trade)
        self.assertEqual({symbols.buy_leg: 0, symbols.sell_leg: 0}, broker.books["I"])
        self.assertEqual({symbols.buy_leg: LOT, symbols.sell_leg: -LOT}, broker.books["M"])
        self.assertEqual("CLOSED", db.trade_logs[0]["trade_state"])
        self.assertEqual([], db.unsupported)

    def test_short_then_square_off_in_mis_beside_an_algo_nrml_position(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        broker = ProductBroker(db)
        call, put = symbols.long_legs
        broker.books["M"] = {call: LOT, put: -LOT}
        manager = live_manager(db, symbols=symbols, broker=broker)

        manager.decider.submit("SHORT", "NIFTY", 2)
        self.run_cycle(manager)
        last = manager.decider.snapshot()["last"]
        self.assertTrue(last["ok"], last)
        self.assertIn("Shorted 2 lots NIFTY", last["message"])
        self.assertEqual(("SHORT", True), (manager.direction, manager.in_trade))
        # The put bought, the call sold; the algo's NRML book untouched.
        self.assertEqual({put: 2 * LOT, call: -2 * LOT}, broker.books["I"])
        self.assertEqual([(put, "B"), (call, "S")], [(o["symbol"], o["side"]) for o in broker.orders][:2])
        row = db.trade_logs[0]
        self.assertEqual(("SHORT", put, call), (row["direction"], row["buy_symbol"], row["sell_symbol"]))

        manager.decider.submit("SELL")
        self.run_cycle(manager)
        last = manager.decider.snapshot()["last"]
        self.assertTrue(last["ok"], last)
        self.assertFalse(manager.in_trade)
        self.assertEqual({put: 0, call: 0}, broker.books["I"])
        self.assertEqual({call: LOT, put: -LOT}, broker.books["M"])
        self.assertEqual([], db.unsupported)

        # The next long builds long legs again.
        manager.decider.submit("BUY", "NIFTY", 1)
        self.run_cycle(manager)
        self.assertTrue(manager.decider.snapshot()["last"]["ok"])
        self.assertEqual({call: LOT, put: -LOT}, broker.books["I"])

    def test_refusals_are_answered_and_send_nothing(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=False, reason="no live price")
        broker = ProductBroker(db)
        manager = live_manager(db, broker, symbols)
        cases = [
            (lambda: manager.decider.submit("SELL"), "No open manual position"),
            (lambda: manager.decider.submit("BUY", "NIFTY", 1), "no live price"),
        ]
        for submit, expected in cases:
            submit()
            self.run_cycle(manager)
            self.assertIn(expected, manager.decider.snapshot()["last"]["message"])
        closed = live_manager(db, broker, DeskSymbols(ready=True),
                              settings=replace(ALWAYS, entry_from=dt_time(23, 59, 59)))
        closed.decider.submit("BUY", "NIFTY", 1)
        self.run_cycle(closed)
        self.assertIn("Manual entries are taken", closed.decider.snapshot()["last"]["message"])
        self.assertEqual([], broker.orders)

    def test_a_click_the_loop_never_reads_expires_unsent(self) -> None:
        manager = live_manager(FakeDB(), ProductBroker(FakeDB()), DeskSymbols(ready=True),
                               settings=replace(ALWAYS, command_ttl_sec=0.05))
        manager.decider.submit("BUY", "NIFTY", 1)
        time.sleep(0.1)
        last = manager.decider.snapshot()["last"]
        self.assertFalse(last["ok"])
        self.assertIn("Expired", last["message"])
        manager.decider.submit("SELL")  # the slot is free again

    def test_open_position_is_squared_off_at_the_cut(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        broker = ProductBroker(db)
        manager = live_manager(db, broker, symbols)
        manager.decider.submit("BUY", "NIFTY", 1)
        self.run_cycle(manager)
        self.assertTrue(manager.in_trade)
        manager.desk_settings = manager.decider.settings = replace(ALWAYS, square_off_at=dt_time(0, 0))
        self.run_cycle(manager)
        self.assertFalse(manager.in_trade)
        self.assertEqual({symbols.buy_leg: 0, symbols.sell_leg: 0}, broker.books["I"])


class RouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.desk = ManualDesk(
            api=mock.Mock(), mode="TEST", spot_tokens={"NIFTY": ("NSE", "26000")},
            spot_keys={"NIFTY": "NSE|26000"}, price_getters={"NIFTY": lambda: 24_000.0},
            ledger_db=FakeDB(), settings=ALWAYS, fetchers={"NIFTY": DeskSymbols(ready=True)},
        )
        self.desk.ledger = fake_ledger(self.desk.ledger.db)
        self.desk.manager = self.desk._new_manager()
        self.stop = threading.Event()
        self.desk.thread = threading.Thread(target=self.stop.wait, daemon=True)
        self.desk.thread.start()
        app = Flask(__name__)
        self.desk.register_routes(app)
        self.client = app.test_client()

    def tearDown(self) -> None:
        self.stop.set()

    def post(self, body, **kwargs):
        return self.client.post("/manual/order", json=body, **kwargs)

    def test_settings_are_validated_stored_and_change_the_traded_index(self) -> None:
        post = lambda fixed, trade="NIFTY", **k: self.client.post(
            "/manual/settings", json={"fixed": fixed, "trade": trade}, **k).status_code
        self.assertEqual(503, post({"NIFTY": 18}))                           # no store: nothing to write to
        self.desk.auto._store = store = mock.Mock()
        for bad in (0, 0.1, 101, True, "18", None):
            self.assertEqual(400, post({"NIFTY": bad}), bad)
        self.assertEqual(400, post({}))                                      # every instrument, not some
        self.assertEqual(400, post({"NIFTY": 18}, "NOPE"))
        self.assertEqual(403, post({"NIFTY": 18}, headers={"Origin": "http://evil.test"}))
        store.save_state.assert_not_called()
        self.assertEqual(202, post({"NIFTY": 18}))
        store.save_state.assert_has_calls([mock.call({"fixed": {"NIFTY": 18.0}}, "request"),
                                           mock.call({"trade": "NIFTY"}, "desk")])
        self.assertEqual(("NIFTY",), self.desk.auto.order)

    def test_order_validation(self) -> None:
        self.assertEqual(415, self.client.post("/manual/order", data="action=BUY").status_code)
        self.assertEqual(403, self.post({"action": "SELL"}, headers={"Origin": "http://evil.test"}).status_code)
        self.assertEqual(400, self.post({"action": "HOLD", "index": "NIFTY", "lots": 1}).status_code)
        self.assertEqual(400, self.post({"action": "SHORT", "index": "NIFTY", "lots": 0}).status_code)
        self.assertEqual(400, self.post({"action": "BUY", "index": "NIFTY", "lots": 0}).status_code)
        self.assertEqual(400, self.post({"action": "BUY", "index": "NIFTY", "lots": 1.5}).status_code)
        self.assertEqual(400, self.post({"action": "BUY", "index": "NIFTY", "lots": True}).status_code)
        self.assertEqual(400, self.post({"action": "BUY", "index": "DOW", "lots": 1}).status_code)
        accepted = self.post({"action": "BUY", "index": "NIFTY", "lots": 3},
                             headers={"Origin": "http://localhost"})
        self.assertEqual(202, accepted.status_code)
        self.assertEqual(("BUY", "NIFTY", 3), tuple(accepted.json["order"][k] for k in ("action", "index", "lots")))
        self.assertEqual(409, self.post({"action": "SELL"}).status_code)
        self.stop.set()
        self.desk.thread.join()
        self.assertEqual(503, self.post({"action": "SELL"}).status_code)

    def test_a_short_books_the_fall_as_profit(self) -> None:
        now = ist.now()
        rows = [{"id": i, "log_time": now, "exit_time": now, "index_name": "NIFTY", "direction": side,
                 "fill_price": 24_000.0, "exit_price": 23_990.0, "quantity": LOT, "is_in_trade": False,
                 "trade_state": "CLOSED", "buy_symbol": "B", "sell_symbol": "S", "symbol": "S"}
                for i, side in ((2, "SHORT"), (1, "LONG"))]
        self.desk.ledger = mock.Mock(execute=mock.Mock(side_effect=[rows, [], []]))
        state = self.desk.state()
        self.assertEqual([("SHORT", 10.0, 10.0 * LOT), ("LONG", -10.0, -10.0 * LOT)],
                         [(t["direction"], t["points"], t["pnl"]) for t in state["trades"]])
        charges = sum(t["charges"] for t in state["trades"])
        self.assertGreater(charges, 0)
        self.assertEqual((2, round(charges, 2)), (state["closed"], state["charges"]))
        self.assertAlmostEqual(-charges, state["net_pnl"], places=2)
        self.assertAlmostEqual(-charges / LOT, state["net_points"], places=1)
        self.assertNotIn("realised_pnl", state)
        self.assertEqual({"cash": round(200_000.0 - charges, 2), "margin_per_lot": 170_000.0}, state["account"])

    def test_cash_carries_every_net_pnl(self) -> None:
        now = ist.now()

        def row(i, at):
            return {"id": i, "log_time": at, "exit_time": at, "index_name": "NIFTY", "direction": "SHORT",
                    "fill_price": 24_000.0, "exit_price": 23_990.0, "quantity": LOT, "is_in_trade": False,
                    "trade_state": "CLOSED", "buy_symbol": "B", "sell_symbol": "S", "symbol": "S"}

        earlier = [row(1, now - timedelta(days=3))]
        self.desk.ledger = mock.Mock(execute=mock.Mock(side_effect=[[row(2, now)], [], earlier, []]))
        state = self.desk.state()
        before = manual_desk._settle(*self.desk._trade(earlier[0]), earlier[0], [])["net_pnl"]
        self.assertLess(before, 10.0 * LOT)
        self.assertEqual(10.0 * LOT - state["charges"], state["net_pnl"])
        self.assertAlmostEqual(200_000.0 + before + state["net_pnl"], state["account"]["cash"], places=2)
        self.desk.ledger.execute.side_effect = [[row(2, now)], []]   # earlier days are cached
        self.assertEqual(state["account"], self.desk.state()["account"])

    def test_leg_quotes_priced_like_the_index_fall_back_to_the_model(self) -> None:
        now = ist.now()
        row = {"id": 1, "log_time": now, "exit_time": now, "index_name": "SENSEX", "direction": "SHORT",
               "fill_price": 76_667.59, "exit_price": 76_630.49, "quantity": 20, "is_in_trade": False,
               "trade_state": "CLOSED", "buy_symbol": "B", "sell_symbol": "S", "symbol": "S"}

        def fills(entry, exit_):
            return [{"trade_id": 1, "phase": ph, "symbol": sym, "side": side, "qty": 20, "premium": prem}
                    for ph, sym, side, prem in (("entry", "B", "B", entry[0]), ("entry", "S", "S", entry[1]),
                                                ("exit", "B", "S", exit_[0]), ("exit", "S", "B", exit_[1]))]

        bad = fills((76_667.59, 398.85), (76_630.49, 76_630.49))
        good = fills((470.0, 400.0), (450.0, 420.0))
        for given, basis in ((bad, "model"), (good, "quotes")):
            self.desk.ledger = mock.Mock(execute=mock.Mock(side_effect=[[row], given, []]))
            trade = self.desk.state()["trades"][0]
            self.assertEqual(basis, trade["charges_basis"])
            self.assertLess(trade["charges"], 300)   # one SENSEX lot: Rs 4,199 was the index-price quote
            self.assertEqual(round(trade["pnl"] - trade["charges"], 2), trade["net_pnl"])
            self.assertEqual(round(trade["net_pnl"] / 20, 2), trade["net_points"])

    def test_books_tag_day_and_night_and_survive_either_ledger(self) -> None:
        """`/books`: the desk's trades are DAY, v3's are NIGHT, both figured the same way;
        one ledger that cannot be read leaves the other on screen and is never shown flat."""
        now = ist.now()
        yesterday = now - timedelta(days=1)

        def row(i, side, entry, exit_, opened, closed=None, index="NIFTY"):
            return {"id": i, "log_time": opened, "exit_time": closed, "index_name": index, "direction": side,
                    "fill_price": entry, "exit_price": exit_, "quantity": LOT,
                    "is_in_trade": exit_ is None, "trade_state": "OPEN" if exit_ is None else "CLOSED",
                    "buy_symbol": "B", "sell_symbol": "S", "symbol": "S"}

        day_rows = [row(7, "SHORT", 24_010.0, None, now), row(6, "LONG", 24_000.0, 24_020.0, now, now)]
        # Same id as a day trade: two ledgers, two sequences. Opened yesterday, sold at today's open.
        # The retired momentum setup's closed trade is not the night book's (it started fresh).
        night_rows = [{**row(6, "LONG", 23_900.0, 24_000.0, yesterday, now), "book": "v3_carry"},
                      {**row(5, "LONG", 23_000.0, 24_000.0, yesterday, now), "book": "v3_momentum"}]
        # Today's rows for today's read; no earlier days, no leg quotes.
        self.desk.ledger = mock.Mock(execute=mock.Mock(side_effect=lambda *a, **k: (
            day_rows if "is_in_trade = TRUE OR" in a[0] else [])))
        earlier_night = [{**row(3, "LONG", 23_000.0, 23_050.0, yesterday - timedelta(days=1), yesterday), "book": None},
                         {**row(2, "LONG", 22_000.0, 23_000.0, yesterday - timedelta(days=1), yesterday),
                          "book": "v3_momentum_intraday"}]
        night_ledger = lambda *a, **k: earlier_night if "::date < %s" in a[0] else night_rows
        self.desk.night_db = night = mock.Mock(execute=mock.Mock(side_effect=night_ledger))
        self.desk.tape.last = lambda name: 24_000.0
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        archives = mock.patch.object(ManualDesk, "ARCHIVES", Path(folder.name))   # no archive today
        archives.start()
        self.addCleanup(archives.stop)

        data = self.client.get("/books").json
        self.assertEqual("TEST", data["mode"])
        self.assertTrue(all("botdata.trade_logs" in c[0][0] and c[0][1][0] == "TEST"   # this console's mode only
                            for c in night.execute.call_args_list))
        self.assertEqual([("DAY", 7, True), ("DAY", 6, False), ("NIGHT", 6, False)],
                         [(t["book"], t["id"], t["open"]) for t in data["trades"]])
        day, night_book = data["day"], data["night"]
        self.assertNotIn("trades", day)
        self.assertEqual(1, day["closed"])
        self.assertEqual(1, night_book["closed"])
        self.assertIsNone(night_book["position"])
        night_trade = data["trades"][2]
        self.assertEqual("model", night_trade["charges_basis"])
        self.assertGreater(night_trade["charges"], 0)
        self.assertEqual(round(100.0 * LOT - night_trade["charges"], 2), night_trade["net_pnl"])
        self.assertEqual((10.0, 10.0 * LOT), (day["position"]["open_points"], day["position"]["open_pnl"]))
        self.assertEqual(round(day["net_pnl"] + night_book["net_pnl"], 2), data["net_pnl"])
        self.assertEqual(round(day["charges"] + night_book["charges"], 2), data["charges"])
        self.assertEqual((2, 10.0 * LOT), (data["closed"], data["open_pnl"]))
        # The whole account, realised only: capital and every realised net of both books (earlier days too).
        earlier = manual_desk._settle(*self.desk._trade(earlier_night[0]), earlier_night[0], [])["net_pnl"]
        account = data["account"]
        self.assertEqual(round(day["net_pnl"], 2), account["day_realised"])
        self.assertEqual(round(earlier + night_book["net_pnl"], 2), account["night_realised"])
        self.assertEqual(round(200_000.0 + account["day_realised"] + account["night_realised"], 2), account["balance"])
        self.assertNotIn("open_pnl", account)                                # the open position is not in it

        self.desk.tape.last = lambda name: None                              # held, unmarked: never zero
        unmarked = self.client.get("/books").json
        self.assertIsNone(unmarked["open_pnl"])
        self.assertEqual(account["balance"], unmarked["account"]["balance"])  # realised: no mark needed
        self.desk.tape.last = lambda name: 24_000.0

        night.execute.side_effect = RuntimeError("botdata away")
        with mock.patch.object(manual_desk.LOGGER, "exception"):
            data = self.client.get("/books").json
        self.assertIsNone(data["night"])
        self.assertIsNone(data["account"])                                   # never a balance missing a book
        self.assertEqual({"DAY"}, {t["book"] for t in data["trades"]})
        self.assertEqual(data["day"]["net_pnl"], data["net_pnl"])

        self.desk.ledger.execute.side_effect = RuntimeError("manualdata away")
        night.execute.side_effect = night_ledger
        with mock.patch.object(manual_desk.LOGGER, "exception"):
            data = self.client.get("/books").json
        self.assertIsNone(data["day"])
        self.assertEqual({"NIGHT"}, {t["book"] for t in data["trades"]})

        night_rows[1:] = [{**row(5, "LONG", 23_000.0, None, yesterday), "book": "v3_momentum"}]
        held_night = self.client.get("/books").json["night"]["position"]   # a held position always shows
        self.assertEqual((5, True), (held_night["id"], held_night["open"]))

        self.desk.night_db = None                                            # not wired: unknown, not flat
        with mock.patch.object(manual_desk.LOGGER, "exception"):
            data = self.client.get("/books").json
        self.assertEqual((None, None, [], None, None, None), (data["day"], data["night"], data["trades"],
                                                              data["closed"], data["net_pnl"], data["open_pnl"]))

    def test_a_night_trade_archived_by_a_restart_still_shows_today(self) -> None:
        """A flat stop archives and truncates v3's ledger; today's sale at the open stays in view,
        once, and only for this mode and this day."""
        open_ = ist.now().replace(hour=9, minute=15, second=0, microsecond=0)
        header = "id,log_time,direction,fill_price,exit_price,is_in_trade,execution_mode,index_name,quantity,exit_time,book\n"
        line = lambda i, mode="TEST", exit_at=open_, book="v3_carry": (
            f"{i},{(open_ - timedelta(days=3)).isoformat()},LONG,14370.1,14407.7,False,{mode},MIDCPNIFTY,,"
            f"{exit_at.isoformat()},{book}\n")
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / f"trade_logs_{ist.date_key()}.csv").write_text(
                header + line(11) + line(12, "LIVE") + line(13, exit_at=open_ - timedelta(days=1))
                + line(15, book="v3_momentum"), encoding="utf-8")   # the retired setup: not the night book's
            (Path(folder) / f"trade_logs_{ist.date_key()}.r1.csv").write_text(header + line(11), encoding="utf-8")
            (Path(folder) / "trade_logs_20200101.csv").write_text(header + line(14), encoding="utf-8")
            self.desk.night_db = mock.Mock(execute=mock.Mock(return_value=[]))
            with mock.patch.object(ManualDesk, "ARCHIVES", Path(folder)):
                night = self.desk.night_state()
        self.assertEqual([11], [t["id"] for t in night["trades"]])          # once: the .r1 copy is the same trade
        trade = night["trades"][0]
        lot = instruments.get("MIDCPNIFTY").quantity
        self.assertEqual((37.6, 1, lot), (trade["points"], trade["lots"], trade["quantity"]))
        self.assertEqual(round(37.6 * lot - trade["charges"], 2), trade["net_pnl"])
        self.assertEqual(1, night["closed"])

    def test_candle_frames(self) -> None:
        self.assertEqual(400, self.client.get("/manual/candles?tf=3").status_code)
        with mock.patch.object(self.desk.candles, "get", return_value=[]) as get:
            self.assertEqual(200, self.client.get("/manual/candles?index=nifty&tf=15").status_code)
            get.assert_called_once_with("NIFTY", 15)


class ManualConsoleTests(unittest.TestCase):
    """One console for both books: the HH/LL tick chart, today's DAY and NIGHT trades, the account."""

    def test_merged_books_and_layout(self) -> None:
        import json
        from urllib.parse import urlparse

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:  # pragma: no cover - optional dev dependency
            self.skipTest("playwright is not installed")
        from tests.test_ui_layout import ROOT, SIZES, _page_source

        yesterday = (ist.now() - timedelta(days=1)).replace(hour=15, minute=11, second=0, microsecond=0)
        today_open = ist.now().replace(hour=9, minute=15, second=0, microsecond=0)
        night_trade = {"id": 6, "book": "NIGHT", "index": "BANKNIFTY", "direction": "LONG", "entry": 51_000.0,
                       "exit": 51_100.0, "quantity": 35, "lots": 1, "opened_ist": ist.isoformat(yesterday),
                       "closed_ist": ist.isoformat(today_open), "state": "CLOSED", "open": False,
                       "buy": "B", "sell": "S", "points": 100.0, "pnl": 3500.0, "charges": 150.0,
                       "net_pnl": 3350.0, "net_points": 95.71}
        night = {"position": None, "closed": 1, "net_points": 95.71, "net_pnl": 3350.0, "charges": 150.0}
        day_base = {"mode": "TEST", "running": True, "product": "PAPER", "max_lots": 50,
                    "account": {"cash": 200_000.0, "margin_per_lot": 170_000.0},
                    "window": {"entry_from": "09:15", "square_off_at": "15:10"},
                    "position": None, "closed": 0, "net_points": 0, "net_pnl": 0, "charges": 0,
                    "orders": {"queued": None, "working": None, "last": None}, "busy": False,
                    "quarantined": False, "as_of_ist": ist.isoformat()}
        held = {"id": 7, "book": "DAY", "index": "NIFTY", "direction": "LONG", "entry": 24_020.0,
                "quantity": 2 * LOT, "lots": 2, "opened_ist": ist.isoformat(), "closed_ist": None, "state": "OPEN",
                "open": True, "buy": "NIFTY27OCT26C24000", "sell": "NIFTY27OCT26P24000", "mark": 24_030.0,
                "open_points": 10.0, "open_pnl": 10.0 * 2 * LOT}
        closed = {**held, "open": False, "exit": 24_030.0, "points": 10.0, "pnl": 1300.0, "mark": None,
                  "charges": 130.0, "net_pnl": 1170.0, "net_points": 9.0, "closed_ist": ist.isoformat()}

        def books(day, day_trades, night_book=night):
            answered = [b for b in (day, night_book) if b is not None]
            held_now = [b["position"] for b in answered if b["position"]]
            trades = [*day_trades, *([night_trade] if night_book else [])]
            open_now = sum(p["open_pnl"] for p in held_now)
            account = {"start": 200_000.0, "day_realised": 25_000.0 + day["net_pnl"], "night_realised": 3350.0,
                       "balance": 200_000.0 + 25_000.0 + day["net_pnl"] + 3350.0} if day and night_book else None
            return {"mode": "TEST", "day": day, "night": night_book, "trades": trades, "account": account,
                    "closed": sum(b["closed"] for b in answered),
                    "net_pnl": round(sum(b["net_pnl"] for b in answered), 2),
                    "charges": round(sum(b["charges"] for b in answered), 2),
                    "open_pnl": open_now, "as_of_ist": ist.isoformat()}

        state = books(day_base, [])
        errors, requested = [], set()
        start = int(time.time() * 1000) - 600_000
        ticks = {"index": "NIFTY", "reset": True, "t": [start + i * 1000 for i in range(600)],
                 "p": [24_000 + (i % 37) - (i % 11) * 1.5 for i in range(600)]}

        def serve(route):
            url = urlparse(route.request.url)
            requested.add(url.path)
            if url.path == "/":
                return route.fulfill(body=_page_source(), content_type="text/html")
            if url.path in ("/Lune-v6.js", "/tick-chart.js", "/Lune-v5.css", "/books.css") or url.path.startswith("/fonts/"):
                return route.fulfill(path=str(ROOT / "static" / url.path[1:]))
            if url.path == "/books":
                return route.fulfill(json=state)
            if url.path == "/manual/tick_stream":
                return route.fulfill(body=f"data: {json.dumps(ticks)}\n\n", content_type="text/event-stream")
            if url.path == "/manual/candles":
                return route.fulfill(json={"index": "NIFTY", "candles": []})
            if url.path == "/account_balance":
                return route.fulfill(json={"cash": 512_345.6, "collateral": 0, "margin_used": 12_000})
            return route.fulfill(json={})

        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch()
            except Exception as exc:  # pragma: no cover - browser binary absent
                self.skipTest(f"chromium unavailable: {exc}")
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.route("**/*", serve)
            page.goto("http://lune.test/")
            # One console: no mode switch and no order ticket; the algo's sections and the books shown.
            self.assertEqual(0, page.locator("#uiSwitch, #desk, #ticket, #buyButton, #shortButton, #sellButton, #lots").count())
            for section in ("#market", "#overview", "#blotter", "#performance"):
                self.assertTrue(page.locator(section).is_visible(), section)
            # The market chart opens on v3's candles; HH/LL swaps in the tick chart, remembered.
            self.assertTrue(page.locator("#tickView").is_hidden())
            self.assertNotIn("/manual/tick_stream", requested)                # no stream while not shown
            page.locator('#chartModes [data-chart="tick"]').click()
            page.wait_for_function("document.querySelector('#deskCount').textContent === '600 ticks'")
            self.assertTrue(page.locator("#market > .chart-surface").is_hidden())
            self.assertTrue(page.locator("#readout").is_hidden())
            self.assertGreaterEqual(page.evaluate("document.getElementById('deskPlot').clientHeight"), 250)
            self.assertEqual(0, page.locator("#deskFrames").count())        # ticks only: no 1M/5M/15M bars
            self.assertTrue(page.locator("#deskWindows").is_visible())
            self.assertNotIn("/manual/candles", requested)
            page.reload()
            page.wait_for_function("document.querySelector('#deskCount').textContent === '600 ticks'")
            page.locator('#chartModes [data-chart="v3"]').click()
            self.assertTrue(page.locator("#tickView").is_hidden())
            self.assertTrue(page.locator("#market > .chart-surface").is_visible())

            self.assertEqual(("flat", "Flat"), (page.locator("#dayHold").get_attribute("data-side"),
                                                page.locator("#dayHold .hold-side").inner_text()))
            self.assertEqual("flat", page.locator("#nightHold").get_attribute("data-side"))
            # The night trade opened yesterday and sold at today's open: tagged, and dated.
            page.wait_for_function("document.querySelectorAll('#blotterRows tr').length === 1")
            row = page.locator("#blotterRows tr").first
            self.assertEqual("night", row.get_attribute("data-book"))
            cells = row.locator("td").all_inner_texts()
            self.assertEqual(("NIGHT", "BANKNIFTY"), (cells[0], cells[2]))
            self.assertIn("15:11", cells[1]); self.assertTrue(cells[1].endswith("–09:15"), cells[1])
            self.assertNotEqual("15:11", cells[1][:5])                   # yesterday's time carries its date
            page.wait_for_function("document.querySelector('#pnl').textContent === '+₹3,350'")
            self.assertEqual("+₹3,350", page.locator("#blotterNight").inner_text())
            self.assertEqual("₹0", page.locator("#blotterDay").inner_text())
            self.assertEqual("1 closed", page.locator("#closed").text_content())
            # The header: the open trade, and each book's realised today.
            self.assertEqual(("₹0", "+₹3,350", "₹0"),
                             tuple(page.locator(f"#{i}").inner_text() for i in ("todayOpen", "todayNight", "todayDay")))
            # The account, realised only: capital + both books' realised (all days); Shoonya's funds beside it.
            page.wait_for_function("document.querySelector('#acctBalance').textContent === '₹2,28,350'")
            self.assertEqual(0, page.locator("#acctOpen").count())
            # Styled, not just present: the balance is set large and the lines as a ruled list.
            self.assertGreaterEqual(page.evaluate("parseFloat(getComputedStyle(document.getElementById('acctBalance')).fontSize)"), 28)
            self.assertEqual("flex", page.evaluate("getComputedStyle(document.querySelector('.account-lines div')).display"))
            self.assertEqual("15px", page.evaluate("getComputedStyle(document.getElementById('todayDay')).fontSize"))
            self.assertEqual(("₹2,00,000", "+₹25,000", "+₹3,350"),
                             tuple(page.locator(f"#{i}").inner_text() for i in ("acctStart", "acctDay", "acctNight")))
            self.assertEqual("ACCOUNT · PAPER", page.locator("#accountTitle").inner_text())
            page.wait_for_function("document.querySelector('#acctBroker').textContent === '₹5,12,346 / ₹12,000'")

            # The desk opens a day position on its own: the bay, the trades and the account follow.
            state = books({**day_base, "position": held}, [held])
            page.wait_for_function("document.querySelector('#dayHold').dataset.side === 'long'")
            self.assertEqual("LONG NIFTY", page.locator("#dayHold .hold-side").inner_text())
            self.assertEqual(f"+₹{10 * 2 * LOT:,}", page.locator("#dayHold b.hold-open").inner_text())
            self.assertIn("open +₹", page.locator("#pnlMeta").inner_text())
            self.assertEqual(f"+₹{10 * 2 * LOT:,}", page.locator("#todayOpen").inner_text())
            self.assertEqual("₹2,28,350", page.locator("#acctBalance").inner_text())  # unrealised: not in it
            page.wait_for_function("document.querySelectorAll('#blotterRows tr').length === 2")
            self.assertEqual(["day", "night"], page.eval_on_selector_all(
                "#blotterRows tr", "rows => rows.map(r => r.dataset.book)"))
            self.assertEqual("OPEN", page.locator("#blotterRows tr").first.locator("td").nth(6).inner_text())

            # The filter shows one book at a time, and is remembered.
            page.locator('#bookFilter [data-book="NIGHT"]').click()
            self.assertEqual(["night"], page.eval_on_selector_all("#blotterRows tr", "rows => rows.map(r => r.dataset.book)"))
            page.locator('#bookFilter [data-book="DAY"]').click()
            self.assertEqual(["day"], page.eval_on_selector_all("#blotterRows tr", "rows => rows.map(r => r.dataset.book)"))
            self.assertEqual("DAY", page.evaluate("localStorage.getItem('lune.books')"))
            page.locator('#bookFilter [data-book="ALL"]').click()

            for width, height in SIZES:
                page.set_viewport_size({"width": width, "height": height})
                bad = page.evaluate("""() => {
                  const out = [];
                  if (document.documentElement.scrollWidth > innerWidth + 1) out.push('page scrolls sideways');
                  for (const el of document.querySelectorAll('.head *, #overview *, #blotter *, #performance *')) {
                    if (el.closest('.head > .picker, .ledger-scroll')) continue;   // these scroll by design
                    const b = el.getBoundingClientRect();
                    if ((b.width || b.height) && (b.right > innerWidth + 1 || b.left < -1))
                      out.push(`${el.tagName}#${el.id}.${el.className} spans ${Math.round(b.left)}..${Math.round(b.right)}`);
                  }
                  for (const el of document.querySelectorAll('.books *, .account *')) {
                    const box = el.closest('.module, .account').getBoundingClientRect(), b = el.getBoundingClientRect();
                    if (b.width && b.right > box.right + 1) out.push(`${el.tagName}.${el.className} overflows its box`);
                  }
                  return out;
                }""")
                self.assertEqual([], bad, f"{width}x{height}")
            page.set_viewport_size({"width": 1440, "height": 1000})

            # Squared off by the desk: realised moves into the day figures and the account.
            state = books({**day_base, "closed": 1, "net_points": 9.0, "net_pnl": 1170.0, "charges": 130.0}, [closed])
            page.wait_for_function("document.querySelector('#blotterDay').textContent === '+₹1,170'")
            self.assertEqual(("+₹4,520", "−₹280", "+₹4,520"), (page.locator("#blotterNet").inner_text(),
                                                             page.locator("#blotterCharges").inner_text(),
                                                             page.locator("#pnl").inner_text()))
            row = page.locator('#blotterRows tr[data-book="day"]').first.locator("td").all_inner_texts()
            self.assertEqual(["DAY", "LONG", "+9.00", "−₹130", "+₹1,170"], [row[0], row[3], *row[7:]])
            self.assertEqual("₹2,29,520", page.locator("#acctBalance").inner_text())
            self.assertEqual(("₹0", "+₹1,170"), (page.locator("#todayOpen").inner_text(),
                                                page.locator("#todayDay").inner_text()))
            page.screenshot(path="/tmp/lune-merged-desktop.png", full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.screenshot(path="/tmp/lune-merged-mobile.png", full_page=True)
            page.set_viewport_size({"width": 1440, "height": 1000})

            # A book whose ledger cannot be read is unknown, never flat, and the other stays up.
            state = books(None, [])
            page.wait_for_function("document.querySelector('#dayHold').dataset.side === 'unknown'")
            self.assertEqual("Day ledger unavailable · night book only", page.locator("#pnlMeta").inner_text())
            self.assertIn("Day ledger unavailable", page.locator("#blotterStatus").inner_text())
            self.assertEqual(("—", "Unavailable"), (page.locator("#acctBalance").inner_text(),
                                                    page.locator("#accountTag").text_content()))
            self.assertEqual(["night"], page.eval_on_selector_all("#blotterRows tr", "rows => rows.map(r => r.dataset.book)"))
            state = {"mode": "TEST", "day": None, "night": None, "trades": [], "closed": None,
                     "net_pnl": None, "charges": None, "open_pnl": None, "account": None}
            page.wait_for_function("document.querySelector('#nightHold').dataset.side === 'unknown'")
            self.assertEqual(("—", "Ledger unavailable"), (page.locator("#pnl").inner_text(),
                                                           page.locator("#pnlMeta").inner_text()))
            self.assertFalse({"/net_pnl", "/manual/state", "/manual/order"} & requested, requested)
            self.assertEqual([], errors)
            browser.close()


@unittest.skipUnless(os.getenv("LUNE_DB_TESTS") == "1", "set LUNE_DB_TESTS=1 to run against PostgreSQL")
class ManualDeskPostgresTests(unittest.TestCase):
    """The desk's own SQL, end to end, in a throwaway schema."""

    def setUp(self) -> None:
        from db_connector import TimescaleDBConnector

        self.db = TimescaleDBConnector(minconn=1, maxconn=6, auto_provision=False)
        self.db.connect()
        self.schema = f"manual_selftest_{uuid.uuid4().hex[:10]}"

    def tearDown(self) -> None:
        if self.db._pool is None:   # a stopped desk closes the pool it was given
            self.db.connect()
        self.db.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE;')
        self.db.close()

    def desk(self, mode, api, fetcher, price):
        desk = ManualDesk(
            api=api, mode=mode, spot_tokens={"NIFTY": ("NSE", "26000")},
            spot_keys={"NIFTY": "NSE|26000"}, price_getters={"NIFTY": lambda: price[0]},
            ledger_db=self.db, settings=ALWAYS, fetchers={"NIFTY": fetcher},
        )
        desk.ledger = ManualLedger(self.db, self.schema)
        desk.ledger.ensure_schema()
        desk.tape.fresh = lambda name, max_age=5.0: price[0]   # the desk prices fills from its tape's ticks
        app = Flask(__name__)
        desk.register_routes(app)
        return desk, app.test_client()

    def wait_done(self, client, order_id, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = client.get("/manual/state").json
            last = (state.get("orders") or {}).get("last")
            if last and last["id"] == order_id:
                return state, last
            time.sleep(0.05)
        self.fail(f"order {order_id} never finished")

    def trade(self, client, action, **order):
        accepted = client.post("/manual/order", json={"action": action, **order})
        self.assertEqual(202, accepted.status_code, accepted.json)
        return self.wait_done(client, accepted.json["order"]["id"])

    def test_schema_is_a_copy_of_the_algo_ledger_and_idempotent(self) -> None:
        import db_connector

        ledger = ManualLedger(self.db, self.schema)
        ledger.ensure_schema()
        manual_desk.ensure_manual_schema(self.db, self.schema)  # a second start changes nothing
        rows = self.db.execute(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema=%s;",
            (self.schema,), fetch=True)
        have = {(r["table_name"], r["column_name"]) for r in rows}
        want = {(t.name, c.name) for t in db_connector.TABLES if t.name in manual_desk.LEDGER_TABLES
                for c in t.columns}
        want |= {("leg_fills", c) for c in ("trade_id", "phase", "symbol", "side", "qty", "premium", "quoted_at")}
        self.assertEqual(want, have)
        ledger.execute("INSERT INTO botdata.trade_logs (log_time,symbol,direction,fill_price,is_in_trade) "
                       "VALUES (now(),'A','LONG',1,TRUE);")
        with self.assertRaisesRegex(Exception, "immutable"):
            ledger.execute("UPDATE botdata.trade_logs SET symbol='B';")

    def test_paper_desk_end_to_end(self) -> None:
        price = [24_000.0]
        desk, client = self.desk("TEST", mock.Mock(), DeskSymbols(ready=True), price)
        desk.start()
        try:
            state, last = self.trade(client, "BUY", index="NIFTY", lots=2)
            self.assertTrue(last["ok"], last)
            position = state["position"]
            self.assertEqual(("NIFTY", 2, 2 * LOT, 24_000.0, "TEST"),
                             (position["index"], position["lots"], position["quantity"],
                              position["entry"], state["mode"]))
            self.assertTrue(position["buy"] and position["sell"])
            self.assertTrue(client.get("/manual/state").json["busy"] is False)
            state, last = self.trade(client, "BUY", index="NIFTY", lots=1)
            self.assertIn("Already holding", last["message"])
            price[0] = 24_012.5
            state, last = self.trade(client, "SELL")
            self.assertTrue(last["ok"], last)
            self.assertIsNone(state["position"])
            self.assertEqual((1, round(12.5 * 2 * LOT - state["charges"], 2)), (state["closed"], state["net_pnl"]))
            self.assertGreater(state["charges"], 0)
            state, last = self.trade(client, "SHORT", index="NIFTY", lots=1)
            self.assertTrue(last["ok"], last)
            self.assertEqual(("SHORT", 24_012.5), (state["position"]["direction"], state["position"]["entry"]))
            price[0] = 24_002.5
            state, last = self.trade(client, "SELL")
            self.assertTrue(last["ok"], last)
            self.assertEqual((2, round(12.5 * 2 * LOT + 10.0 * LOT - state["charges"], 2)),
                             (state["closed"], state["net_pnl"]))
            self.assertEqual(sum(t["net_points"] for t in state["trades"]), state["net_points"])
            self.assertEqual(round(200_000.0 + state["net_pnl"], 2), state["account"]["cash"])
            algo = self.db.execute("SELECT count(*) AS n FROM botdata.trade_logs WHERE book='manual';", fetch=True)
            self.assertEqual(0, algo[0]["n"])
        finally:
            self.assertTrue(desk.stop(timeout=10))

    def test_live_desk_through_the_production_order_writer(self) -> None:
        import main

        price = [24_350.0]
        ledger_holder = {}

        class PushingBroker(ProductBroker):
            """Pushes Shoonya "om" messages through main.py's writer, as the websocket does."""

            def _push(self, orderno, tsym, status, fill, avg):
                ledger_holder["writer"]({"t": "om", "norenordno": orderno, "tsym": tsym,
                                         "status": status, "fillshares": str(fill),
                                         "avgprc": str(avg), "exchordid": f"X{orderno}"})

        symbols = DeskSymbols(ready=True)
        broker = PushingBroker(FakeDB())
        desk, client = self.desk("LIVE", broker, symbols, price)
        writer = main._make_order_update_logger(desk.ledger)
        ledger_holder["writer"] = writer
        with mock.patch.object(ManualTradeManagerLive, "ENTRY_POSITION_VERIFY_TIMEOUT_SEC", 2.0):
            desk.start(order_writer=writer)
            try:
                desk.manager.RECONCILE_INTERVAL_SEC = 0.0
                state, last = self.trade(client, "BUY", index="NIFTY", lots=1)
                self.assertTrue(last["ok"], last)
                self.assertEqual({"I"}, set(broker.products))
                logs = desk.ledger.execute(
                    "SELECT state1, state2, buy_fill_qty, sell_fill_qty, lifecycle_state FROM botdata.order_logs;",
                    fetch=True)
                self.assertEqual([("COMPLETE", "COMPLETE", LOT, LOT, "OPEN")],
                                 [tuple(r.values()) for r in logs])
                state, last = self.trade(client, "SELL")
                self.assertTrue(last["ok"], last)
                self.assertIsNone(state["position"])
                self.assertEqual({symbols.buy_leg: 0, symbols.sell_leg: 0}, broker.books["I"])
                attempts = desk.ledger.execute(
                    "SELECT phase, status FROM botdata.order_attempts ORDER BY id;", fetch=True)
                self.assertEqual(4, len(attempts))
                self.assertTrue(all(a["status"] == "COMPLETE" for a in attempts), attempts)
            finally:
                self.assertTrue(desk.stop(timeout=10))



class TickTraderFollowsPivotsTests(unittest.TestCase):
    def test_two_pivots_inside_one_look_still_exit(self) -> None:
        """Book went short then long again between two looks: our long's exit pivot has passed."""
        trader = manual_desk.TickTrader(("NIFTY",), lambda name: ([], [], -1))
        book = trader.books["NIFTY"]
        book.pos = SimpleNamespace(side=1, ms=1000)
        self.assertIsNone(trader.desired("LONG", "NIFTY", True))
        self.assertIsNone(trader.desired("LONG", "NIFTY", True))
        book.pos = SimpleNamespace(side=1, ms=int(time.time() * 1000))
        self.assertEqual(trader.desired("LONG", "NIFTY", True), ("SELL", "NIFTY", 1))

    def test_a_reversal_is_squared_off_and_re_entered_in_one_pass(self) -> None:
        trader = manual_desk.TickTrader(("NIFTY",), lambda name: ([], [], -1))
        trader.books["NIFTY"].pos = SimpleNamespace(side=-1, ms=int(time.time() * 1000))
        trader.order = ("NIFTY",)
        manager = SimpleNamespace(desk_settings=ALWAYS, auto=trader, in_trade=True, _index="NIFTY", _entry_day=None)
        decider = manual_desk.ManualDecider(manager)
        done = []

        async def sell(*_):
            done.append("SELL")
            manager.in_trade = False
            return True, "flat"

        async def open_(order, *_):
            done.append(order["action"])
            return True, "in"

        decider._sell, decider._open = sell, open_
        asyncio.run(decider.step(in_trade=True, direction="LONG", price_getter=None, enter=None, exit_=None))
        self.assertEqual(["SELL", "SHORT"], done)

    def test_a_book_changing_side_wakes_the_desk_loop(self) -> None:
        trader = manual_desk.TickTrader(("NIFTY",), lambda name: ([], [], -1))
        book = SimpleNamespace(pos=None, last_ms=None)
        book.on_tick = lambda price, ms: setattr(book, "pos", book.pos if price < 2 else SimpleNamespace(side=1, ms=ms))
        trader.books["NIFTY"] = book
        trader.on_tick("NIFTY", 1.0, 1.0)
        self.assertEqual(0, trader.woke.seq)
        trader.on_tick("NIFTY", 2.0, 2.0)
        self.assertEqual(1, trader.woke.seq)



class NightTradeHandoffTests(unittest.TestCase):
    """The HH/LL desk and the v3 night trade never overlap: the desk opens nothing
    while v3 holds (until v3's 09:15 exit), and v3 waits for the desk to be flat."""

    @staticmethod
    def flat_auto_decider(blocker):
        trader = manual_desk.TickTrader(("NIFTY",), lambda name: ([], [], -1))
        trader.order = ("NIFTY",)
        trader.books["NIFTY"].pos = SimpleNamespace(side=1, ms=int(time.time() * 1000))
        manager = SimpleNamespace(desk_settings=ALWAYS, auto=trader, in_trade=False, _index=None,
                                  _entry_day=None, entry_paused=blocker)
        decider = manual_desk.ManualDecider(manager)
        opened = []

        async def open_(order, *_):
            opened.append((order["action"], order["index"]))
            return True, "in"

        decider._open = open_
        return decider, trader, opened

    def step(self, decider, in_trade=False):
        asyncio.run(decider.step(in_trade=in_trade, direction="LONG" if in_trade else None,
                                 price_getter=None, enter=None, exit_=None))

    def test_the_auto_trader_waits_while_v3_holds_then_resumes(self) -> None:
        v3 = {"busy": "v3 night trade is holding BANKNIFTY"}
        decider, trader, opened = self.flat_auto_decider(lambda: v3["busy"])
        with capture_logs(manual_desk.LOGGER) as captured:
            self.step(decider)
            self.step(decider)
            self.assertEqual([], opened)
            v3["busy"] = None
            trader.books["NIFTY"].pos = SimpleNamespace(side=1, ms=int(time.time() * 1000) + 1)
            self.step(decider)
        self.assertEqual([("BUY", "NIFTY")], opened)
        paused = [m for m in captured.messages(logging.WARNING)
                  if "New entries paused: v3 night trade is holding BANKNIFTY" in m]
        self.assertEqual(1, len(paused), "said once")
        self.assertTrue(captured.has("New entries resumed", logging.WARNING))

    def test_a_held_desk_position_still_exits_while_v3_holds(self) -> None:
        trader = manual_desk.TickTrader(("NIFTY",), lambda name: ([], [], -1))
        trader.books["NIFTY"].pos = SimpleNamespace(side=-1, ms=int(time.time() * 1000))
        trader.order = ("NIFTY",)
        manager = SimpleNamespace(desk_settings=ALWAYS, auto=trader, in_trade=True, _index="NIFTY",
                                  _entry_day=None, entry_paused=lambda: "v3 night trade is holding NIFTY")
        decider = manual_desk.ManualDecider(manager)
        done = []

        async def sell(*_):
            done.append("SELL")
            manager.in_trade = False
            return True, "flat"

        async def open_(order, *_):
            done.append(order["action"])
            return True, "in"

        decider._sell, decider._open = sell, open_
        self.step(decider, in_trade=True)
        self.assertEqual(["SELL"], done, "the exit goes through; the reversal's entry does not")

    def test_a_click_is_refused_while_v3_holds(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        manager = live_manager(db, symbols=symbols, broker=ProductBroker(db))
        manager.entry_blocker = lambda: "v3 night trade is holding NIFTY"
        manager.decider.submit("BUY", "NIFTY", 1)
        asyncio.run(manager._cycle_once())
        last = manager.decider.snapshot()["last"]
        self.assertFalse(last["ok"])
        self.assertIn("Entries paused: v3 night trade is holding NIFTY", last["message"])
        self.assertFalse(manager.in_trade)
        self.assertEqual([], db.trade_logs)
        self.assertEqual(0, symbols.builds, "nothing was even built")

    def test_the_order_path_refuses_too_and_a_broken_check_pauses(self) -> None:
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        manager = live_manager(db, symbols=symbols, broker=ProductBroker(db))
        manager.entry_blocker = lambda: "v3 night trade is entering a position"
        self.assertTrue(manager._past_entry_cutoff({"index": "NIFTY"}))
        manager.entry_blocker = None
        self.assertFalse(manager._past_entry_cutoff({"index": "NIFTY"}))

        def broken():
            raise RuntimeError("v3 gone")

        manager.entry_blocker = broken
        self.assertIn("v3 gone", manager.entry_paused())
        self.assertTrue(manager._past_entry_cutoff({"index": "NIFTY"}))

    def test_the_desk_says_when_it_is_not_flat(self) -> None:
        desk = ManualDesk.__new__(ManualDesk)
        desk.manager = None
        self.assertIsNone(desk.busy_reason(), "not started: holds nothing")
        db, symbols = FakeDB(), DeskSymbols(ready=True)
        desk.manager = live_manager(db, symbols=symbols, broker=ProductBroker(db))
        self.assertIsNone(desk.busy_reason())
        desk.manager.decider.submit("BUY", "NIFTY", 1)
        self.assertEqual("the HH/LL desk has an order in flight", desk.busy_reason())
        asyncio.run(desk.manager._cycle_once())
        self.assertTrue(desk.manager.in_trade)
        self.assertEqual("the HH/LL desk is holding NIFTY", desk.busy_reason())
        desk.manager.decider.submit("SELL")
        asyncio.run(desk.manager._cycle_once())
        self.assertFalse(desk.manager.in_trade)
        self.assertIsNone(desk.busy_reason())
        desk.manager.entering = True
        self.assertEqual("the HH/LL desk is entering a position", desk.busy_reason())
        desk.manager.entering, desk.manager.exiting = False, True
        self.assertEqual("the HH/LL desk is exiting a position", desk.busy_reason())

    def test_the_manager_is_built_with_the_blocker(self) -> None:
        desk = ManualDesk.__new__(ManualDesk)
        desk.entry_blocker = lambda: "x"
        desk.settings, desk.mode, desk.api, desk.ledger = ALWAYS, "TEST", None, fake_ledger(FakeDB())
        desk.names, desk.fetchers = ("NIFTY",), {"NIFTY": DeskSymbols(ready=True)}
        desk.tape = TickTape({"NIFTY": "NSE|26000"})
        desk.auto = None
        self.assertEqual("x", desk._new_manager().entry_paused())


if __name__ == "__main__":
    unittest.main()
