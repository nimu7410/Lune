"""Run: venv/bin/python -m unittest tests.test_pnl_history"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from flask import Flask

import instruments
import ist
from live_server import performance, register_pnl_history
from manual_desk import trade_figures

NIFTY_LOT = instruments.get("NIFTY").quantity
SENSEX_LOT = instruments.get("SENSEX").quantity


def trade(id=1, **changes):
    return {**dict(id=id, log_time="2026-09-01T20:00:00+00:00", symbol="NIFTY26SEP24000CE", index_name="NIFTY",
                   direction="LONG", fill_price="24000.10", exit_price="24010.20",
                   is_in_trade=False, execution_mode="TEST"), **changes}


def net(row, fills=()):
    """What the console's books make of one row: its net ₹ after charges."""
    typed = {**row, "log_time": ist.parse_ist(row["log_time"]), "exit_time": ist.parse_ist(row.get("exit_time")),
             "fill_price": float(row["fill_price"]), "exit_price": float(row["exit_price"]),
             "quantity": int(row["quantity"]) if row.get("quantity") else None}
    return trade_figures(typed, fills)


class PnlHistoryTests(unittest.TestCase):
    def test_net_rupees_by_instrument_filters_duplicates_and_ist_day(self):
        first = trade()
        short = trade(2, direction="SHORT", fill_price="80000.00", exit_price="79950.00",
                      symbol="SENSEX26SEP80000PE", index_name="SENSEX")
        live = trade(3, execution_mode="LIVE")
        opened = trade(4, is_in_trade=True)
        rows = [first, first, short, live, opened]
        data = performance(rows, "2026-09-02", "2026-09-02", mode="TEST")
        a, b = net(first), net(short)
        # Every figure is the books' own: points x quantity, less the round trip's charges.
        self.assertEqual(round(10.1 * NIFTY_LOT - a["charges"], 2), a["net_pnl"])
        self.assertEqual(round(50.0 * SENSEX_LOT - b["charges"], 2), b["net_pnl"])
        self.assertEqual((round(a["net_pnl"] + b["net_pnl"], 2), 2, 2), (data["pnl"], data["trades"], data["wins"]))
        self.assertEqual(round(a["charges"] + b["charges"], 2), data["charges"])
        self.assertEqual("2026-09-02", data["daily"][0]["date"])
        # Grouped by the index traded, never by option tradingsymbol.
        self.assertEqual(["NIFTY", "SENSEX"], [r["instrument"] for r in data["instruments"]])
        self.assertEqual(["NIFTY", "SENSEX"], data["available_instruments"])
        self.assertEqual(a["net_pnl"], performance(rows, "2026-09-02", "2026-09-02", "NIFTY", "TEST")["pnl"])
        self.assertEqual(0, performance(rows, "2026-09-01", "2026-09-01")["trades"])
        # A row from before the multi-index book has no index: it was NIFTY's.
        legacy = performance([trade(index_name=None)], "2026-09-02", "2026-09-02")
        self.assertEqual(["NIFTY"], [r["instrument"] for r in legacy["instruments"]])
        loss = performance([trade(exit_price="24000.10")], "2026-09-02", "2026-09-02")
        self.assertEqual((0, True), (loss["wins"], loss["pnl"] < 0))     # flat before charges: a loss after them

    def test_quantity_and_recorded_leg_quotes_are_used(self):
        two_lots = trade(quantity=str(2 * NIFTY_LOT))
        self.assertEqual(net(two_lots)["net_pnl"],
                         performance([two_lots], "2026-09-02", "2026-09-02")["pnl"])
        fills = [{"phase": ph, "symbol": sym, "side": side, "qty": NIFTY_LOT, "premium": prem}
                 for ph, sym, side, prem in (("entry", "C", "B", 400.0), ("entry", "P", "S", 380.0),
                                             ("exit", "C", "S", 405.0), ("exit", "P", "B", 375.0))]
        day = {**trade(), "_book": "DAY", "_fills": fills}
        quoted = net(trade(), fills)
        self.assertEqual("quotes", quoted["charges_basis"])
        self.assertEqual(quoted["net_pnl"], performance([day], "2026-09-02", "2026-09-02")["pnl"])
        # With all 4 legs quoted, the ₹ is the legs' cash (sells less buys), not the index move:
        # (405 - 400) + (380 - 375) = 10 points, though the index moved 10.1.
        self.assertEqual((10.0, round(10.0 * NIFTY_LOT, 2)), (quoted["points"], quoted["pnl"]))
        self.assertEqual(round(quoted["pnl"] - quoted["charges"], 2), quoted["net_pnl"])
        # A missing leg, or two buys at exit, falls back to the index move.
        junk = [*fills[:3], {**fills[3], "premium": 100.0}]           # a placeholder bid: 275 pts off the index
        for broken in (fills[:3], [*fills[:3], {**fills[3], "side": "S"}], junk):
            self.assertEqual((10.1, "model"), (net(trade(), broken)["points"], net(trade(), broken)["charges_basis"]))

    def test_books_split_day_and_night(self):
        """A row's book is the ledger it came from; the same id in both is two trades."""
        night = trade()
        day = {**trade(), "exit_price": "24000.60", "_book": "DAY"}
        rows = [night, day]
        data = performance(rows, "2026-09-02", "2026-09-02")
        self.assertEqual(2, data["trades"])
        self.assertEqual([("DAY", net(day)["net_pnl"], 1), ("NIGHT", net(night)["net_pnl"], 1)],
                         [(b["book"], b["pnl"], b["trades"]) for b in data["books"]])
        self.assertEqual(net(day)["net_pnl"], performance(rows, "2026-09-02", "2026-09-02", book="DAY")["pnl"])
        self.assertEqual(net(night)["net_pnl"], performance(rows, "2026-09-02", "2026-09-02", book="NIGHT")["pnl"])

    def test_night_book_starts_fresh_with_the_carry(self):
        """The retired momentum setup's trades stay archived but out of the night figures."""
        rows = [trade(1, book="v3_carry"), trade(2, book="v3_momentum"),
                trade(3, book="v3_momentum_intraday"), trade(4, book="manual", _book="DAY")]
        data = performance(rows, "2026-09-02", "2026-09-02")
        self.assertEqual([("DAY", 1), ("NIGHT", 1)], [(b["book"], b["trades"]) for b in data["books"]])
        self.assertEqual(2, data["trades"])

    def test_route_archives_validation_and_unavailable(self):
        fills = [{"trade_id": 1, "phase": ph, "symbol": sym, "side": side, "qty": NIFTY_LOT, "premium": prem}
                 for ph, sym, side, prem in (("entry", "C", "B", 400.0), ("entry", "P", "S", 380.0),
                                             ("exit", "C", "S", 405.0), ("exit", "P", "B", 375.0))]

        class DB:
            queries = []

            def execute(self, sql, params, fetch):
                self.queries.append(sql)
                if "leg_fills" in sql:
                    return fills
                # The v3 row duplicates an archived one; the desk's has the same id in its own ledger.
                return [trade()] if "botdata." in sql else [trade(exit_price="24000.60")]
        app = Flask(__name__)
        app.config["TRADE_MODE"] = "TEST"
        register_pnl_history(app, DB())
        loser = trade(2, exit_price="23990.10")
        with TemporaryDirectory() as directory, patch("live_server.BACKUPS", Path(directory)), \
                patch("live_server.archived_trades", return_value=[trade(), loser]):
            client = app.test_client()
            response = client.get("/pnl_history?start=2026-09-02&end=2026-09-02")
            self.assertEqual(200, response.status_code)
            day_net = net(trade(exit_price="24000.60"), fills)["net_pnl"]     # its own leg quotes
            night_net = round(net(trade())["net_pnl"] + net(loser)["net_pnl"], 2)
            self.assertEqual(3, response.json["trades"])
            self.assertEqual(round(day_net + night_net, 2), response.json["pnl"])
            self.assertEqual([("DAY", day_net, 1), ("NIGHT", night_net, 2)],
                             [(b["book"], b["pnl"], b["trades"]) for b in response.json["books"]])
            DB.queries.clear()
            day = client.get("/pnl_history?start=2026-09-02&end=2026-09-02&book=day").json
            self.assertEqual((day_net, 1, "DAY"), (day["pnl"], day["trades"], day["book"]))
            self.assertIn("manualdata.trade_logs", DB.queries[0])
            self.assertIn("manualdata.leg_fills", DB.queries[1])
            self.assertFalse(any("botdata." in q for q in DB.queries))
            nifty = client.get("/pnl_history?start=2026-09-02&end=2026-09-02&instrument=nifty").json
            self.assertEqual(3, nifty["trades"])
            self.assertEqual(0, client.get("/pnl_history?start=2026-09-02&end=2026-09-02&instrument=SENSEX").json["trades"])
            for query in ("start=bad", "start=2026-10-01&end=2026-09-01", "mode=BAD", "book=EVENING"):
                self.assertEqual(400, client.get("/pnl_history?" + query).status_code)
            with patch.object(DB, "execute", return_value=None):
                with patch.object(app.logger, "exception"):
                    self.assertEqual(503, client.get("/pnl_history").status_code)


if __name__ == "__main__":
    unittest.main()
