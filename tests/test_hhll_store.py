"""The HH/LL database (hhll_store): the tick codec always; the SQL end to end, opt-in.

`StorePostgresTests` needs the real PostgreSQL (`LUNE_DB_TESTS=1`): each test builds the
`hhll` tables in a throwaway schema of its own and drops it again. It never touches the
real `hhll` schema, nor any file outside a temporary directory.
"""

from __future__ import annotations

import json
import os
import random
import tarfile
import tempfile
import threading
import time
import unittest
import uuid
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from unittest import mock

import hhll_store
import hhll_tick
import ist
import manual_desk
from hhll_store import decode_ticks, encode_ticks


class CodecTests(unittest.TestCase):
    def test_round_trip_is_exact(self) -> None:
        rnd = random.Random(3)
        x, ms, px = 25000.0, [], []
        t = 1_793_680_546_432
        for _ in range(20_000):
            x = round(x + rnd.choice([-1, 1]) * rnd.choice([0.05, 0.5, 1, 2]), 2)
            t += rnd.choice([0, 1, 250, 1000, -3])                   # equal stamps and a clock step back
            ms.append(t)
            px.append(x)
        seqs = list(range(100, 100 + len(px)))
        blob = encode_ticks(seqs, ms, px)
        self.assertEqual((seqs, ms, px), decode_ticks(blob))
        self.assertLess(len(blob), 24 * len(px) / 8)                # compressed at least 8x
        odd = [0.1 + 0.2, 1 / 3, 1e-9, 7.0]                         # not paise: stored as raw floats
        self.assertEqual(([0, 1, 5, 6], [1, 2, 3, 4], odd), decode_ticks(encode_ticks([0, 1, 5, 6], [1, 2, 3, 4], odd)))
        self.assertEqual(([], [], []), decode_ticks(encode_ticks([], [], [])))

    def test_damage_is_refused(self) -> None:
        blob = bytearray(encode_ticks([0, 1], [5, 6], [1.5, 2.5]))
        blob[len(blob) // 2] ^= 0xFF
        with self.assertRaises(Exception):
            decode_ticks(bytes(blob))
        with self.assertRaises(ValueError):
            encode_ticks([0], [1, 2], [3.0])


@unittest.skipUnless(os.getenv("LUNE_DB_TESTS") == "1", "set LUNE_DB_TESTS=1 to run against PostgreSQL")
class StorePostgresTests(unittest.TestCase):
    def setUp(self) -> None:
        from db_connector import TimescaleDBConnector

        self.schema = f"hhll_selftest_{uuid.uuid4().hex[:10]}"
        self.store = hhll_store.Store(TimescaleDBConnector(minconn=1, maxconn=4, auto_provision=False),
                                      self.schema).connect()
        self.saved = hhll_tick.STORE
        hhll_tick.use_store(self.store)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def tearDown(self) -> None:
        hhll_tick.use_store(self.saved)
        self.store.close(5)
        self.store.db.connect()
        self.store.db.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE;')
        self.store.db.close()

    def count(self, table: str) -> int:
        with self.store.tx() as cur:
            cur.execute(f'SELECT count(*) FROM "{self.schema}".{table}')
            return cur.fetchone()[0]

    # -- ticks and the archive ------------------------------------------------
    def test_ticks_live_then_archived_then_merged(self) -> None:
        s = self.store
        d1, d2 = date(2026, 11, 2), date(2026, 11, 3)
        rows = [(d1, "NIFTY", i, 1_000 + i, 25000 + i * 0.05) for i in range(3000)]
        rows += [(d1, "SENSEX", i, 2_000 + i, 80000.5) for i in range(10)]
        rows += [(d2, "NIFTY", i, 9_000 + i, 25100.0) for i in range(5)]
        s.insert_ticks(rows)
        s.insert_ticks(rows[:50])                                    # a retried batch changes nothing
        self.assertEqual(3015, self.count("ticks"))
        self.assertEqual(3000, s.next_seq(d1, "NIFTY"))
        self.assertEqual(0, s.next_seq(d1, "BANKNIFTY"))
        before = s.ticks(d1, "NIFTY")
        self.assertEqual(list(range(3000)), before[0])
        self.assertEqual([d1, d2], s.days())
        self.assertEqual((1_000, 3_999), s.span(d1))
        self.assertEqual(([10, 11], [1_010, 1_011], [25000.5, 25000.55]), s.ticks(d1, "NIFTY", 10, 12))
        self.assertEqual([("NIFTY", 3, 9_003, 25100.0), ("NIFTY", 4, 9_004, 25100.0)],
                         s.ticks_after(d2, {"NIFTY": 2, "SENSEX": -1}))

        done = s.archive_closed_days(d2)                             # d1 is over; d2 is today
        self.assertEqual({("2026-11-02", "NIFTY", 3000), ("2026-11-02", "SENSEX", 10)},
                         {(x["day"], x["index"], x["ticks"]) for x in done})
        self.assertEqual(5, self.count("ticks"))                     # only today's rows stay
        self.assertEqual(before, s.ticks(d1, "NIFTY"))               # read back exactly, from the archive
        self.assertEqual({"NIFTY": before[1:], "SENSEX": ([2_000 + i for i in range(10)], [80000.5] * 10)},
                         s.day_ticks(d1))
        self.assertEqual([d1, d2], s.days())
        self.assertEqual((1_000, 3_999), s.span(d1))
        self.assertEqual(3000, s.next_seq(d1, "NIFTY"))              # seqs continue past the archive
        self.assertEqual([], s.archive_closed_days(d2))              # nothing left to do

        s.insert_ticks([(d1, "NIFTY", 3000, 5_000, 25200.0)])        # a straggler of a finished day
        self.assertEqual(before[0] + [3000], s.ticks(d1, "NIFTY")[0])
        s.archive_closed_days(d2)
        self.assertEqual((before[0] + [3000], before[1] + [5_000], before[2] + [25200.0]), s.ticks(d1, "NIFTY"))
        self.assertEqual(5, self.count("ticks"))
        with s.tx() as cur:
            cur.execute(f'SELECT ticks, first_seq, last_seq, codec, encode(sha256(data), \'hex\') = sha256 '
                        f'FROM "{self.schema}".tick_archive WHERE index_name = \'NIFTY\'')
            self.assertEqual((3001, 0, 3000, hhll_store.CODEC, True), cur.fetchone())

    def test_a_damaged_archive_keeps_the_ticks(self) -> None:
        s = self.store
        d1 = date(2026, 11, 2)
        s.insert_ticks([(d1, "NIFTY", i, i, 100.0) for i in range(5)])
        s.archive_closed_days(date(2026, 11, 3))
        with s.tx() as cur:
            cur.execute(f'UPDATE "{self.schema}".tick_archive SET sha256 = \'0\'')
        s.insert_ticks([(d1, "NIFTY", 5, 5, 100.0)])
        with self.assertRaises(RuntimeError):
            s.archive_closed_days(date(2026, 11, 3))
        self.assertEqual(1, self.count("ticks"))                     # the straggler is still there

    # -- pivots, trades, reviews, state, ledger -----------------------------------
    def test_pivots_trades_reviews_state(self) -> None:
        s = self.store
        d = date(2026, 11, 3)
        b = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params())
        t0 = int(datetime(2026, 11, 3, 10, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
        for i in range(3600):
            k = i % 1200
            b.on_tick(25000 + (k if k < 600 else 1200 - k) * 0.1, t0 + i * 1000)
        rows = [hhll_store.pivot_row("NIFTY", d, b.chain, p) for p in b.pivots]
        s.insert_pivots(rows)
        s.insert_pivots(rows[::-1])
        s.insert_pivots(rows)                                        # the desk and the engine both write
        got = s.pivots(d, "NIFTY", b.chain, 500)
        self.assertEqual([(p.kind, p.price, p.ms, p.conf_price, p.conf_ms, p.leg) for p in b.pivots], got)
        self.assertEqual(got[-2:], s.pivots(d, "NIFTY", b.chain, 2))
        self.assertEqual([], s.pivots(d, "NIFTY", b.chain + 1, 500))

        t = dict(b.trades[0], chop=float("nan"))                    # JSONB has no NaN: stored as null
        self.assertTrue(s.add_trade(t))
        self.assertFalse(s.add_trade(t))
        self.assertIsNone(s.trades()[0]["chop"])
        self.assertEqual(b.trades[0]["charges"], s.trades()[0]["charges"])
        s.add_review({"kind": "review", "at": "2026-11-03T10:00:00+05:30", "x": float("inf")})
        s.add_review({"kind": "derive", "rows": [1, 2]})
        self.assertEqual(["review", "derive"], [r["kind"] for r in s.reviews()])

        self.assertEqual(({}, 0), (s.load_state(), s.state_version()))
        self.assertEqual(1, s.save_state({"a": 1}))
        self.assertEqual(2, s.save_state({"a": 2}))
        self.assertEqual(({"a": 2}, 2), (s.load_state(), s.state_version()))
        s.add_ledger([{"kind": "execution", "price": 1.5}])
        self.assertEqual([{"kind": "execution", "price": 1.5}], s.ledger())

    # -- the writer ----------------------------------------------------------------
    def test_writer_retries_and_reports_what_is_written(self) -> None:
        s = self.store
        d = date(2026, 11, 3)
        seen = []
        s.writer.on_ticks.append(lambda day, name, seq: seen.append((day, name, seq)))
        real = s.insert_ticks
        fails = [2]

        def flaky(rows, cur=None):
            if fails[0]:
                fails[0] -= 1
                raise hhll_store.OperationalError("db away")
            return real(rows, cur)

        with mock.patch.object(s, "insert_ticks", flaky), mock.patch.object(hhll_store.LOG, "error"):
            for i in range(5):
                s.writer.put_tick((d, "NIFTY", i, i, 100.0 + i))
            s.writer.put_ledger({"kind": "trade"})
            self.assertTrue(s.writer.flush(15))
        self.assertEqual([0, 1, 2, 3, 4], s.ticks(d, "NIFTY")[0])
        self.assertEqual((d, "NIFTY", 4), seen[-1])
        self.assertEqual(1, len(s.ledger()))

    # -- the desk tape -------------------------------------------------------------
    def test_tape_keeps_only_the_tail_and_reads_the_rest_back(self) -> None:
        s = self.store
        noon = datetime.combine(ist.today(), dt_time(12, 0), tzinfo=ist.IST)
        today = ist.today()
        s.insert_ticks([(today, "NIFTY", i, 1_000 + i, 24000.0) for i in range(7)])   # an earlier run today
        with mock.patch.object(manual_desk.ist, "now", return_value=noon):
            tape = manual_desk.TickTape({"NIFTY": "NSE|26000"}, keep=10)
            seen = []
            tape.observers.append(lambda *a: seen.append(a))
            tape.persist_to(s)
            self.assertEqual(7, tape.size("NIFTY"))
            for i in range(60):
                tape.on_tick({"e": "NSE", "tk": "26000", "lp": str(24001 + i)})
            self.assertTrue(s.writer.flush(10))
            tape.on_tick({"e": "NSE", "tk": "26000", "lp": "25000"})                  # trims after the commit
            self.assertEqual(68, tape.size("NIFTY"))
            self.assertLessEqual(len(tape._ms["NIFTY"]), 21)                          # only the tail in memory
            self.assertEqual([s_ for *_, s_ in seen], list(range(7, 68)))             # seqs continue
            gen, total, ms, px = tape.read("NIFTY")
            self.assertEqual((68, [24000.0] * 7 + [24001.0 + i for i in range(60)] + [25000.0]), (total, px))
            self.assertEqual(px[30:], tape.read("NIFTY", 30)[3])
            hm, hp, last = tape.history("NIFTY")
            self.assertEqual((ms, px, 67), (hm, hp, last))
            self.assertEqual(25000.0, tape.last("NIFTY"))
            self.assertTrue(s.writer.flush(10))
            self.assertEqual(list(range(68)), s.ticks(today, "NIFTY")[0])

    def test_desk_end_to_end(self) -> None:
        from flask import Flask
        from tests.order_path_fakes import FakeDB

        s = self.store
        noon = datetime.combine(ist.today(), dt_time(12, 0), tzinfo=ist.IST)
        with mock.patch.object(manual_desk.ist, "now", return_value=noon), \
                mock.patch.object(hhll_tick, "migrate_files", lambda: {}):
            desk = manual_desk.ManualDesk(api=mock.Mock(), mode="TEST", spot_tokens={"NIFTY": ("NSE", "26000")},
                                          spot_keys={"NIFTY": "NSE|26000"}, price_getters={"NIFTY": lambda: None},
                                          ledger_db=FakeDB(), tick_store=s)
            desk.ledger.ensure_schema = lambda: None              # the ledger is not under test here
            desk.connect()
            self.assertIs(hhll_tick.STORE, s)
            x = 24000.0
            for i in range(3000):                                    # 30-pt swings: a few pivots at R ~ 15
                k = i % 600
                x = 24000 + (k if k < 300 else 600 - k) * 0.1
                desk.tape.on_tick({"e": "NSE", "tk": "26000", "lp": f"{x:.2f}"})
            self.assertTrue(s.writer.flush(10))
            self.assertEqual(list(range(3000)), s.ticks(ist.today(), "NIFTY")[0])
            book = desk.auto.books["NIFTY"]
            self.assertLessEqual(len(book.pivots), 1)                 # the book keeps only its latest
            app = Flask(__name__)
            desk.register_routes(app)
            client = app.test_client()
            pts = client.get("/manual/pivots?index=NIFTY").json
            stored = s.pivots(ist.today(), "NIFTY", book.chain, 500)
            self.assertEqual(len(stored), len(pts["hh_points"]) + len(pts["ll_points"]))
            res = client.get("/manual/tick_stream?index=NIFTY", buffered=False)
            event = next(l for l in res.response if b"data:" in l)    # an SSE event: id: and data: lines
            first = json.loads(next(x for x in event.split(b"\n") if x.startswith(b"data:"))[5:])
            res.close()
            self.assertTrue(first["reset"])
            self.assertEqual(3000, len(first["t"]))
            self.assertTrue(desk.stop())
            with self.assertNoLogs(level="ERROR"):  # the pool is gone: refused, not logged
                self.assertEqual(503, client.get("/manual/pivots?index=NIFTY").status_code)
                self.assertEqual(503, client.get("/manual/state").status_code)
        self.assertIsNone(desk.tape.store)

    # -- the move of the old files -------------------------------------------------
    def test_migrate_files_imports_once_and_backs_up(self) -> None:
        s = self.store
        root = Path(self.tmp.name)
        data = root / "hhll_tick_data"
        (data / "ticks" / "2026-11-03").mkdir(parents=True)
        (data / "ticks" / "2026-11-03" / "NIFTY.csv").write_text("1000,25000.05\n1001,25000.1\n1002,250")
        (data / "ticks" / "2026-11-03" / "SENSEX.csv").write_text("1000,80000\n")
        (data / "params.json").write_text(json.dumps({"active": {"thr_cost": 10}, "active_day": "2026-11-03"}))
        trade = {"index": "NIFTY", "entry_ms": 5, "exit_ms": 9, "day": "2026-11-03", "net_rs": -1.5, "x": 1}
        (data / "trades.jsonl").write_text(json.dumps(trade) + "\n" + json.dumps(trade) + "\n")
        (data / "reviews.jsonl").write_text(json.dumps({"kind": "review", "at": "2026-11-03T10:00:00+05:30"}) + "\n")
        (data / "report.md").write_text("report")
        (root / "logs").mkdir()
        ledger = root / "logs" / "hhll_ledger.jsonl"
        ledger.write_text(json.dumps({"kind": "execution", "at": 1.5}) + "\n")
        s.insert_ticks([(date(2026, 11, 3), "SENSEX", 0, 7, 1.0)])     # already has SENSEX: not imported
        rep = s.migrate_files(data, ledger, root / "backups")
        self.assertEqual((2, 1, 1, 1, 1, True), (rep["ticks"], rep["tick_files_skipped"], rep["trades"],
                                                  rep["reviews"], rep["ledger"], rep["state"]))
        self.assertEqual(([0, 1], [1000, 1001], [25000.05, 25000.1]), s.ticks(date(2026, 11, 3), "NIFTY"))
        self.assertEqual([7], s.ticks(date(2026, 11, 3), "SENSEX")[1])
        self.assertEqual({"active": {"thr_cost": 10}, "active_day": "2026-11-03"}, s.load_state())
        self.assertEqual(1, len(s.trades()))
        self.assertFalse((data / "ticks").exists() or (data / "params.json").exists() or ledger.exists())
        self.assertTrue((data / "report.md").exists())
        with tarfile.open(rep["backup"]) as tar:
            self.assertEqual({"hhll_tick_data/ticks/2026-11-03/NIFTY.csv", "hhll_tick_data/ticks/2026-11-03/SENSEX.csv",
                              "hhll_tick_data/params.json", "hhll_tick_data/trades.jsonl",
                              "hhll_tick_data/reviews.jsonl", "logs/hhll_ledger.jsonl"}, set(tar.getnames()))
        # the same files again (an old writer brought them back): nothing is stored twice
        (data / "reviews.jsonl").write_text(json.dumps({"kind": "review", "at": "2026-11-03T10:00:00+05:30"}) + "\n")
        (data / "trades.jsonl").write_text(json.dumps(trade) + "\n")
        rep = s.migrate_files(data, ledger, root / "backups")
        self.assertEqual((0, 0), (rep["trades"], rep["reviews"]))
        self.assertEqual((1, 1), (len(s.reviews()), len(s.trades())))
        self.assertEqual({}, s.migrate_files(data, ledger, root / "backups"))

    def test_committed_ticks_wake_the_reader(self) -> None:
        s = self.store
        self.assertTrue(s.wait_ticks(0.05))                 # opens the listening connection: read once
        t0 = time.monotonic()
        self.assertFalse(s.wait_ticks(0.3))                 # a working listener heard nothing
        self.assertGreaterEqual(time.monotonic() - t0, 0.25)   # nothing written: the fallback timeout
        threading.Timer(0.1, s.writer.put_tick, [(date(2026, 11, 3), "NIFTY", 0, 1, 25000.0)]).start()
        t0 = time.monotonic()
        self.assertTrue(s.wait_ticks(5.0))
        self.assertLess(time.monotonic() - t0, 1.0)
        self.assertTrue(s.writer.flush(5))
        self.assertEqual(1, len(s.ticks_after(date(2026, 11, 3), {"NIFTY": -1})))

    # -- the engine, live ------------------------------------------------------------
    def test_live_follows_the_stored_ticks(self) -> None:
        s = self.store
        day = date(2026, 11, 3)
        t0 = int(datetime(2026, 11, 3, 10, 0, tzinfo=hhll_tick.IST).timestamp() * 1000)
        ms = [t0 + i * 1000 for i in range(3600)]
        px = [25000 + ((i % 1200) if i % 1200 < 600 else 1200 - i % 1200) * 0.1 for i in range(3600)]
        s.insert_ticks([(day, "NIFTY", i, m, p) for i, (m, p) in enumerate(zip(ms[:1000], px[:1000]))])
        with mock.patch.object(hhll_tick, "today_ist", lambda: day), \
                mock.patch.object(hhll_tick, "session_over", lambda d: False), \
                mock.patch.object(hhll_tick, "migrate_files", lambda: {}), \
                mock.patch.object(hhll_tick, "POLL_S", 0.01):
            live = hhll_tick.Live()
            live.state["ledger_start_ms"] = 0
            self.assertEqual(999, live.seq["NIFTY"])
            threading.Thread(target=live.follow, daemon=True).start()
            s.insert_ticks([(day, "NIFTY", i, m, p) for i, (m, p) in enumerate(zip(ms, px)) if i >= 1000])
            end = time.monotonic() + 20
            while live.seq["NIFTY"] < 3599 and time.monotonic() < end:
                time.sleep(0.05)
            self.assertEqual(3599, live.seq["NIFTY"])
            time.sleep(0.3)
        ref = hhll_tick.Book(hhll_tick.SPECS["NIFTY"], hhll_tick.Params())
        for m, p in zip(ms, px):
            ref.on_tick(p, m)
        self.assertTrue(s.writer.flush(10))
        self.assertEqual([(p.kind, p.ms) for p in ref.pivots],
                         [(k, m) for k, _, m, *_ in s.pivots(day, "NIFTY", ref.chain, 10_000)])
        after = [t["entry_ms"] for t in ref.trades if t["exit_ms"] > ms[999]]   # closed once live, not in the warm-up
        self.assertTrue(after)
        self.assertEqual(after, [t["entry_ms"] for t in s.trades()])
        self.assertEqual(len(after), live.reviews.qsize())


if __name__ == "__main__":
    unittest.main()
