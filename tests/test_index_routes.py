"""The console can be pointed at any index v3 evaluates, and never at nothing.

The chart, the last price and the gate verdicts all have to move together: a
BANKNIFTY chart under a NIFTY price is a worse lie than no price at all. And a
query string outlives a deploy, so a name that is no longer served must fall
back to the default rather than 404 the poller into a blank page.
"""

from __future__ import annotations

import unittest

from live_server import create_realtime_pivot_routes, create_trading_ui_route


class _FakeSnapshots:
    """Stands in for `PivotSnapshotService`; answers with its own name."""

    def __init__(self, name: str) -> None:
        self.name = name

    def latest_pivots(self):
        return {"hh_points": [], "ll_points": [{"index": self.name}]}

    def latest_snapshot(self):
        return {"candles": [{"index": self.name}], "pivots": {}, "signal": None}

    def latest_engine(self):
        return {"signal": {"index": self.name}, "last": None}


def _app():
    from flask import Flask

    services = {name: _FakeSnapshots(name) for name in ("NIFTY", "BANKNIFTY", "SENSEX")}
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    create_realtime_pivot_routes(
        app, data_handler=None,
        pivot_service=services["NIFTY"], pivot_services=services,
    )
    create_trading_ui_route(app, indices=list(services), chart_index="NIFTY")
    return app


class IndexRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = _app().test_client()

    def _served(self, query: str) -> str:
        body = self.client.get(f"/realtime_pivot_snapshot{query}").get_json()
        return body["candles"][0]["index"]

    def test_each_index_serves_its_own_feed(self) -> None:
        for name in ("NIFTY", "BANKNIFTY", "SENSEX"):
            self.assertEqual(name, self._served(f"?index={name}"))
            self.assertEqual(
                name,
                self.client.get(f"/realtime_pivots?index={name}")
                    .get_json()["ll_points"][0]["index"],
            )

    def test_case_and_whitespace_are_not_a_different_index(self) -> None:
        self.assertEqual("SENSEX", self._served("?index=sensex"))
        self.assertEqual("SENSEX", self._served("?index= SenSex "))

    def test_absent_or_unknown_name_falls_back_to_the_default(self) -> None:
        """A stale query string must not leave the operator with a blank page."""
        for query in ("", "?index=", "?index=NASDAQ", "?index=NIFTY%20BANK"):
            self.assertEqual("NIFTY", self._served(query))

    def test_page_offers_exactly_the_indices_that_are_served(self) -> None:
        page = self.client.get("/").get_data(as_text=True)
        self.assertIn('data-indices="NIFTY,BANKNIFTY,SENSEX"', page)
        self.assertIn('data-index="NIFTY"', page)
        self.assertIn('data-index="SENSEX"', page)          # a chip per index
        # Two switches - the chart's and the engine bay's - so a chip apiece.
        self.assertEqual(6, page.count("picker-chip"))

    def test_the_engine_bay_is_served_every_index_at_once(self) -> None:
        """It is switched independently of the chart, so it cannot poll per index."""
        body = self.client.get("/entry_gates").get_json()
        self.assertEqual(sorted(body), ["BANKNIFTY", "NIFTY", "SENSEX"])
        for name, engine in body.items():
            self.assertEqual(name, engine["signal"]["index"])


if __name__ == "__main__":
    unittest.main()
