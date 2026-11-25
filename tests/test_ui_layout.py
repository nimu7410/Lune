"""Render wide market values at desktop and phone sizes to catch UI clipping."""

from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import instruments

ROOT = Path(__file__).resolve().parent.parent

# Widest readings the console can be handed: a six-figure index with paise.
WIDE = {
    "priceMain": "299,999",
    "pricePaise": ".95",
    "delta": "-1,234.55 · -12.34%",
    "asOf": "15:29:59 IST · live",
    "todayOpen": "−₹12,34,567",
    "todayNight": "+₹12,34,567",
    "todayDay": "−₹12,34,567",
    "o": "299,970.70", "h": "299,990.25", "l": "299,959.15", "c": "299,984.45",
    "barCount": "375 bars",
}

SIZES = [(1920, 1080), (1600, 900), (1440, 860), (1366, 768), (1280, 720),
         (1180, 760), (1024, 700), (1024, 650), (900, 1200), (768, 1024), (430, 932), (360, 760), (320, 568)]

MEASURE = """() => {
  const bad = [];
  const q = document.getElementById('quote');
  const pad = parseFloat(getComputedStyle(q).paddingRight);
  const edge = q.getBoundingClientRect().right - pad;
  // The figure is a block-level flex box: it stays inside the sheet while its
  // text spills out of it, so the glyph runs themselves are what gets measured.
  for (const run of document.querySelectorAll('.figure span, .band span')) {
    const b = run.getBoundingClientRect();
    if (b.right > edge + 1) bad.push(`#${run.id} ends at ${Math.round(b.right)}, past the sheet at ${Math.round(edge)}`);
  }
  const name = document.getElementById('quoteIndex'), cell = name.parentElement;
  const words = document.createRange(); words.selectNodeContents(name);
  const cellEdge = cell.getBoundingClientRect().right - parseFloat(getComputedStyle(cell).paddingRight);
  if (words.getBoundingClientRect().right > cellEdge + 1)
    bad.push(`index name ${name.textContent} ends at ${Math.round(words.getBoundingClientRect().right)}, past its column at ${Math.round(cellEdge)}`);
  for (const el of document.querySelectorAll('.stage *')) {
    const b = el.getBoundingClientRect();
    if (!b.width && !b.height) continue;
    if (b.right > innerWidth + 1 || b.left < -1)
      bad.push(`${el.tagName}.${el.className || el.id} spans ${Math.round(b.left)}..${Math.round(b.right)} of ${innerWidth}`);
  }
  for (const el of document.querySelectorAll('.operations > .module *')) {
    const box = el.closest('.module').getBoundingClientRect(), b = el.getBoundingClientRect();
    if (b.width && b.right > box.right + 1) bad.push(`${el.tagName}.${el.className || el.id} overflows its bay`);
  }
  if (document.documentElement.scrollWidth > innerWidth + 1)
    bad.push(`page scrolls sideways: ${document.documentElement.scrollWidth} > ${innerWidth}`);
  for (const el of document.querySelectorAll('.quote, .operations > .module')) {
    for (const child of el.children) {
      if (child.getBoundingClientRect().bottom > el.getBoundingClientRect().bottom + 1)
        bad.push(`${el.className} clips ${child.className}`);
    }
  }
  for (const v of document.querySelectorAll('.today-lines dd')) {
    const box = v.closest('.band').getBoundingClientRect(), b = v.getBoundingClientRect();
    if (b.right > box.right + 1) bad.push(`#${v.id} ends at ${Math.round(b.right)}, past its column at ${Math.round(box.right)}`);
  }
  const veil = document.getElementById('veil');
  const readout = document.getElementById('readout');
  // Only a veil on screen can cover anything (the page may be scrolled past the chart).
  if (veil.getBoundingClientRect().height && veil.getBoundingClientRect().bottom > readout.getBoundingClientRect().top + 1)
    bad.push('loading veil covers the candle readout');
  if (document.getElementById('plot').clientHeight < 250)
    bad.push('chart too short to read');
  return bad;
}"""


def _page_source() -> str:
    """The real template, rendered the way the route renders it."""
    import jinja2

    html = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    return jinja2.Template(html).render(
        url_for=lambda _endpoint, filename: filename,
        timeframe="5m",
        poll_ms=1000,
        indices=list(instruments.NAMES),
        chart_index="NIFTY",
    )


class StylesheetTests(unittest.TestCase):
    def test_every_comment_closes(self) -> None:
        """An unclosed /* swallows every rule after it, silently: the page still renders, unstyled."""
        import re
        for sheet in sorted((ROOT / "static").glob("*.css")):
            depth = 0
            for mark in re.findall(r"/\*|\*/", sheet.read_text(encoding="utf-8")):
                depth += 1 if mark == "/*" else -1
                self.assertIn(depth, (0, 1), f"{sheet.name}: nested or stray comment mark")
            self.assertEqual(0, depth, f"{sheet.name}: a comment never closes")


class ConsoleLayoutTests(unittest.TestCase):
    """Rendered geometry, at every width the stylesheet changes shape at."""

    @classmethod
    def setUpClass(cls) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:  # pragma: no cover - optional dev dependency
            raise unittest.SkipTest("playwright is not installed")
        cls._ctx = sync_playwright().start()
        try:
            cls.browser = cls._ctx.chromium.launch()
        except Exception as exc:  # pragma: no cover - browser binary absent
            cls._ctx.stop()
            raise unittest.SkipTest(f"chromium unavailable: {exc}")

        # A throwaway copy so the page can load its own stylesheet over file://.
        cls.dir = Path(__import__("tempfile").mkdtemp(prefix="lune-ui-"))
        shutil.copy(ROOT / "static" / "Lune-v5.css", cls.dir / "Lune-v5.css")
        shutil.copy(ROOT / "static" / "books.css", cls.dir / "books.css")
        shutil.copytree(ROOT / "static" / "fonts", cls.dir / "fonts")
        (cls.dir / "Lune-v6.js").write_text("", encoding="utf-8")  # geometry only, no feed
        (cls.dir / "index.html").write_text(_page_source(), encoding="utf-8")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.browser.close()
        cls._ctx.stop()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_nothing_prints_out_of_bounds(self) -> None:
        failures = []
        for width, height in SIZES:
            page = self.browser.new_page(viewport={"width": width, "height": height})
            page.goto((self.dir / "index.html").as_uri())
            page.evaluate(
                "(vals) => { for (const [id, text] of Object.entries(vals)) "
                "document.getElementById(id).textContent = text; }", WIDE)
            for problem in page.evaluate(MEASURE):
                failures.append(f"{width}x{height}: {problem}")
            page.close()
        self.assertEqual([], failures, "\n" + "\n".join(failures))

    def test_live_script_filters_and_populated_layout(self):
        from live_server import performance
        from tests.test_pnl_history import trade
        rows = [trade(), trade(2, symbol="BANKNIFTY26SEP999999999999999999999CE", index_name="BANKNIFTY",
                               fill_price="52000.00", exit_price="51990.00")]
        nifty_net = performance(rows, "2026-09-02", "2026-09-02", "NIFTY")["pnl"]
        requests, errors = [], []
        fail = False
        flat = {"position": None, "closed": 0, "net_pnl": 0.0, "charges": 0.0}
        books_data = {"mode": "TEST", "day": flat, "night": flat, "trades": [], "closed": 0,
                      "net_pnl": 0.0, "charges": 0.0, "open_pnl": 0.0}

        def serve(route):
            nonlocal fail
            url = urlparse(route.request.url)
            if url.path == "/":
                return route.fulfill(body=_page_source(), content_type="text/html")
            if url.path in ("/Lune-v6.js", "/Lune-v5.css", "/books.css") or url.path.startswith("/fonts/"):
                return route.fulfill(path=str(ROOT / "static" / url.path[1:]))
            data = {}
            if url.path == "/books":
                data = books_data
            elif url.path == "/pnl_history":
                requests.append(parse_qs(url.query))
                if fail:
                    return route.fulfill(status=503, json={"error": "unavailable"})
                query = requests[-1]
                data = performance(rows, query["start"][0], query["end"][0], query.get("instrument", [""])[0],
                                   query["mode"][0], query["book"][0])
            elif url.path == "/realtime_pivot_snapshot":
                data = {"candles": [{"time": f"2026-09-02T09:{15+i*5:02d}:00+05:30", "open": 24000+i*8, "high": 24025+i*8, "low": 23990+i*8, "close": 24015+i*8} for i in range(8)]}
            elif url.path == "/live_price_stream":
                return route.fulfill(body="data: 24071\n\n", content_type="text/event-stream")
            elif url.path == "/broker_ping":
                data = {"available": False}
            elif url.path == "/feed_health":
                data = {"available": True, "connected": True, "reconnecting": False, "failures": 0,
                        "uptime_s": 8040, "reconnects": 1, "ticks": 5000, "in_session": True,
                        "feeds": [{"index": name, "source": source, "tick_age_s": 0.4}
                                  for name, source in zip(instruments.NAMES,
                                                          ("ws", "ws", "rest", "ws", "stale", "ws"))]}
            elif url.path == "/option_legs":
                data = {"ledger": True, "as_of_ist": "2026-09-02T11:32:05+05:30",
                        "held": {"index": "NIFTY", "sell": "NIFTY08SEP26P24500", "buy": "NIFTY08SEP26C24500"},
                        "legs": [
                    {"index": "NIFTY", "exchange": "NFO", "lot": 75, "state": "held",
                     "sell": "NIFTY08SEP26P24500", "buy": "NIFTY08SEP26C24500", "note": None},
                    {"index": "NIFTYNXT50", "exchange": "NFO", "lot": 25, "state": "armed",
                     "sell": "NIFTYNXT5029SEP26P71000", "buy": "NIFTYNXT5029SEP26C71000", "note": None},
                    {"index": "SENSEX", "exchange": "BFO", "lot": 20, "state": "blocked", "sell": None,
                     "buy": None, "note": "BFO master has no SENSEX contract for the 75000 put " * 4},
                    {"index": "MIDCPNIFTY", "exchange": "NFO", "lot": 140, "state": "blocked", "sell": None,
                     "buy": None, "note": "NFO does not list MIDCPNIFTY29SEP26P13000 (built for MIDCPNIFTY)"},
                    {"index": "FINNIFTY", "exchange": "NFO", "lot": 65, "state": "idle",
                     "sell": None, "buy": None, "note": "no legs"},
                    {"index": "BANKNIFTY", "exchange": "NFO", "lot": 35, "state": "offline",
                     "sell": None, "buy": None, "note": "no leg builder"}]}
            elif url.path == "/entry_gates":
                data = {"NIFTY": {"live": {
                    "live": True, "price": 24071, "confirmed_time": "2026-09-02T09:50:00+05:30",
                    "gates": [{"name": name, "ok": False} for name in ("Regime", "ATR vs cost", "Drop gate", "Momentum trigger")],
                    "carry": {"blocked_by": ["Regime", "ATR vs cost"], "passed": 1, "total": 5},
                    "reach": {"leg": "momentum", "open": True, "price": 24_120.5, "points": 49.5, "atr": 0.2},
                }}, **{name: {"signal": {"gates": [{"name": "Regime", "ok": True}], "reach": reach}}
                       for name, reach in (
                    ("BANKNIFTY", {"leg": "momentum", "open": True, "price": 1, "points": 12_345.6, "atr": 9}),
                    ("SENSEX", {"leg": "carry", "open": True, "price": 1, "points": -3, "atr": -0.1}),
                    ("FINNIFTY", {"leg": "carry", "open": False, "price": 1, "points": 5, "atr": 0.1}))}}
            return route.fulfill(json=data)

        page = self.browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.route("**/*", serve)
        page.goto("http://lune.test/")
        page.wait_for_function("!document.querySelector('#pnlApply').disabled")
        page.locator("#pnlStart").fill("2026-09-02")
        page.locator("#pnlEnd").fill("2026-09-02")
        page.locator("#pnlApply").click()
        page.wait_for_function("document.querySelector('#historyTrades').textContent === '2'")
        self.assertEqual(["BANKNIFTY", "NIFTY"],                     # by instrument, not tradingsymbol
                         page.eval_on_selector_all("#instrumentPnl tr td:first-child", "cells => cells.map(c => c.textContent)"))
        self.assertTrue(page.locator("#historyTotal").inner_text().startswith(("+₹", "−₹")))
        self.assertIn("AFTER ₹", page.locator("#historyMeta").inner_text())
        self.assertEqual(["Night · v3, held overnight"],             # archive rows are v3's
                         page.eval_on_selector_all("#bookPnl tr td:first-child", "cells => cells.map(c => c.textContent)"))
        self.assertIn("Day + night", page.locator("#pnlStatus").inner_text())
        curve = page.locator("#historyLine").get_attribute("d")
        self.assertTrue(curve.startswith("M") and "L" in curve)
        self.assertNotIn("NaN", curve)
        page.wait_for_function("document.querySelector('#enginePicker [data-index=NIFTY] .reach').textContent")
        self.assertEqual(
            {"NIFTY": "+50", "BANKNIFTY": "+12.3k", "SENSEX": "✓", "FINNIFTY": "off"},
            {name: page.locator(f'#enginePicker [data-index="{name}"] .reach').inner_text()
             for name in ("NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY")})
        self.assertIn("+49.50 to 24,120.50", page.locator("#biasMeta").inner_text())
        page.wait_for_function("document.querySelectorAll('#legs li').length === 6")
        self.assertEqual("Held · NIFTY", page.locator("#legsTag").text_content())
        self.assertEqual(["held", "armed", "blocked", "blocked", "idle", "offline"],
                         page.eval_on_selector_all("#legs li", "rows => rows.map(r => r.dataset.state)"))
        self.assertIn("NIFTYNXT5029SEP26C71000", page.locator('#legs li[data-state="armed"]').inner_text())
        self.assertIn("Blocked · NFO does not list", page.locator('#legs li[data-state="blocked"]').nth(1).inner_text())
        page.wait_for_function("document.querySelectorAll('#feeds span').length === 6")
        self.assertEqual("error", page.locator("#wsLed").get_attribute("data-health"))
        self.assertIn("4/6 on ticks", page.locator("#wsMeta").inner_text())
        self.assertIn("1 reconnect ", page.locator("#wsMeta").inner_text() + " ")
        # The four bays share one row on a desktop.
        tops = page.eval_on_selector_all(".operations > .module", "ms => ms.map(m => m.getBoundingClientRect().top)")
        self.assertEqual(4, len(tops))
        self.assertEqual(1, len(set(tops)), tops)
        for width, height in SIZES:
            page.set_viewport_size({"width": width, "height": height})
            self.assertEqual([], page.evaluate(MEASURE), f"{width}x{height}")
        page.set_viewport_size({"width": 1440, "height": 1000})
        page.screenshot(path="/tmp/lune-desktop.png", full_page=True)
        page.set_viewport_size({"width": 390, "height": 844})
        page.screenshot(path="/tmp/lune-mobile.png", full_page=True)
        page.locator("#pnlInstrument").select_option("NIFTY")
        page.locator("#pnlApply").click()
        page.wait_for_function("document.querySelector('#historyTrades').textContent === '1'")
        self.assertEqual(f"+₹{round(nifty_net):,}", page.locator("#historyTotal").inner_text())
        fail = True
        page.locator("#pnlApply").click()
        page.wait_for_function("document.querySelector('#pnlStatus').textContent.includes('unavailable')")
        self.assertTrue(page.locator("#pnlResults").is_hidden())
        fail = False
        page.locator("#pnlMode").select_option("LIVE")
        page.locator("#pnlApply").click()
        page.wait_for_function("document.querySelector('#pnlStatus').textContent.includes('No closed trades')")
        page.locator('.head .picker [data-index="BANKNIFTY"]').click()
        self.assertEqual("BANKNIFTY", page.locator("#quoteIndex").inner_text())
        # The longest names must fit their column wherever the console is drawn,
        # and switching back must give NIFTY its full display size again.
        for name in ("MIDCPNIFTY", "NIFTYNXT50", "NIFTY"):
            page.locator(f'.head .picker [data-index="{name}"]').click()
            for width, height in SIZES:
                page.set_viewport_size({"width": width, "height": height})
                page.evaluate("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))")
                self.assertEqual([], page.evaluate(MEASURE), f"{name} {width}x{height}")
        page.set_viewport_size({"width": 1440, "height": 1000})
        self.assertEqual("", page.evaluate("document.getElementById('quoteIndex').style.fontSize"))
        held = {"index": "NIFTY", "direction": "LONG", "entry": 100.0, "lots": 1, "quantity": 75,
                "open": True, "mark": None, "open_points": None, "open_pnl": None}
        books_data = {**books_data, "night": {**flat, "position": held}, "open_pnl": None,
                      "trades": [{**held, "book": "NIGHT", "opened_ist": "2026-09-02T15:11:00+05:30"}]}
        page.reload()
        page.wait_for_function("document.querySelector('#pnlMeta').textContent === 'Realised ₹0 · open unmarked'")
        self.assertEqual("LONG NIFTY", page.locator("#nightHold .hold-side").inner_text())
        self.assertEqual("—", page.locator("#nightHold b.hold-open").inner_text())   # unmarked: never a zero
        self.assertEqual("Flat", page.locator("#dayHold .hold-side").inner_text())
        self.assertEqual([], errors)
        page.close()


    def test_chart_pan_zoom(self) -> None:
        """Wheel and keys zoom, drag pans, the price scale stretches, double-click resets."""
        candles = {"n": 60}
        errors = []

        def serve(route):
            url = urlparse(route.request.url)
            if url.path == "/":
                return route.fulfill(body=_page_source(), content_type="text/html")
            if url.path in ("/Lune-v6.js", "/Lune-v5.css", "/books.css") or url.path.startswith("/fonts/"):
                return route.fulfill(path=str(ROOT / "static" / url.path[1:]))
            if url.path == "/realtime_pivot_snapshot":
                # close = 24000 + bar number, so the readout says which bar is under the pointer
                return route.fulfill(json={"candles": [
                    {"time": f"2026-09-02T{9 + (15 + i) // 60:02d}:{(15 + i) % 60:02d}:00+05:30",
                     "open": 24000 + i, "high": 24004 + i, "low": 23996 + i, "close": 24000 + i}
                    for i in range(candles["n"])]})
            return route.fulfill(json={})

        page = self.browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.route("**/*", serve)
        page.goto("http://lune.test/")
        page.wait_for_function("document.querySelector('#barCount').textContent === '60 bars'")
        box = page.locator("#plot").bounding_box()
        cx, cy = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
        gutter = box["x"] + box["width"] - 30

        def bar(x=cx, y=cy):
            page.mouse.move(x + 2, y); page.mouse.move(x, y)   # a fresh pointermove, even in place
            return round(float(page.locator("#c").inner_text().replace(",", "")) - 24000)

        def price():
            page.mouse.move(cx + 2, cy); page.mouse.move(cx, cy)
            return page.locator("#tipPrice").inner_text()

        def drag(x0, y0, x1, y1):
            page.mouse.move(x0, y0); page.mouse.down()
            page.mouse.move((x0 + x1) / 2, (y0 + y1) / 2); page.mouse.move(x1, y1); page.mouse.up()

        whole = bar()
        self.assertTrue(25 <= whole <= 35, whole)
        page.mouse.wheel(0, -600)                       # zoom in, the newest bar stays at the right edge
        zoomed = bar()
        self.assertGreater(zoomed, whole + 10)
        self.assertEqual(59, bar(box["x"] + box["width"] - 90))
        drag(cx, cy, cx + 300, cy)                      # drag right: older bars
        panned = bar()
        self.assertLess(panned, zoomed)
        candles["n"] = 61                               # a new bar arrives: the panned view holds still
        page.wait_for_function("document.querySelector('#barCount').textContent === '61 bars'")
        self.assertEqual(panned, bar())
        page.mouse.dblclick(cx, cy)                     # double-click: back to every bar, following
        self.assertIn(bar(), (whole, whole + 1))
        page.locator("#plot").focus()
        page.keyboard.press("+"); page.keyboard.press("+")
        self.assertGreater(bar(), whole + 5)
        page.keyboard.press("End")
        self.assertIn(bar(), (whole, whole + 1))

        before = price()
        drag(gutter, cy, gutter, cy + 150)              # the price scale: drag down squeezes
        self.assertNotEqual(before, price())
        page.mouse.dblclick(gutter, cy)
        self.assertEqual(before, price())
        self.assertEqual([], errors)
        page.close()

if __name__ == "__main__":
    unittest.main()
