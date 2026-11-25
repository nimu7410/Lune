"""PostgreSQL storage for the HH/LL tick system: schema `hhll` in the Lune database.

Everything the HH/LL system keeps lives here, not in process memory or in files:

  ticks         every spot tick of the session in progress: (day, index, seq) -> (epoch ms, price).
                `seq` is the tick's place in that index's session (0, 1, 2, ...). Only
                unfinished days stay here: `archive_closed_days` compresses each finished
                (day, index) into `tick_archive` and deletes its rows, so this table holds
                one day at most.
  tick_archive  one row per finished (day, index): the day's seqs, times and prices,
                delta-encoded and xz-compressed (`encode_ticks`), with the blob's SHA-256.
                Read back transparently by `ticks` / `day_ticks`.
  pivots        every confirmed HH/LL of every live book, keyed by the chain it belongs to
                (index, day, R) and the ordinal of the tick that confirmed it.
  trades        every closed paper trade of the hhll_tick engine.
  reviews       every review, re-test and threshold derivation of the engine.
  state         the engine's settings (what hhll_tick_data/params.json used to be).
  algo_ledger   hhll_algo's pivots, executions and trades.

A process keeps only short-lived things in memory: the rows still waiting for `Writer`,
the last ticks a chart stream needs, and each book's working state.
"""

from __future__ import annotations

import hashlib
import json
import logging
import lzma
import math
import select
import shutil
import struct
import sys
import tarfile
import threading
import time
from array import array
from collections import deque
from contextlib import contextmanager
from datetime import date, datetime, timezone
from itertools import accumulate
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import psycopg2
from psycopg2 import InterfaceError, OperationalError, extensions, sql
from psycopg2.extras import Json, execute_values

LOG = logging.getLogger("hhll_store")
SCHEMA = "hhll"
#: The store's writer commits what has queued at most this often: ~5 commits a second, each a
#: batch, instead of one commit (and WAL flush) per tick. Nothing reads these rows faster: the
#: desk trades on its own tape, and hhll_tick's paper fills are at the ticks' own prices.
WRITE_EVERY_S = 0.2

_DDL = """
CREATE SCHEMA IF NOT EXISTS {s};
CREATE TABLE IF NOT EXISTS {s}.ticks (
    day         DATE             NOT NULL,
    index_name  TEXT             NOT NULL,
    seq         INTEGER          NOT NULL CHECK (seq >= 0),
    ms          BIGINT           NOT NULL,
    price       DOUBLE PRECISION NOT NULL CHECK (price > 0),
    PRIMARY KEY (day, index_name, seq)
);
CREATE TABLE IF NOT EXISTS {s}.tick_archive (
    day          DATE        NOT NULL,
    index_name   TEXT        NOT NULL,
    ticks        INTEGER     NOT NULL,
    first_seq    INTEGER     NOT NULL,
    last_seq     INTEGER     NOT NULL,
    first_ms     BIGINT      NOT NULL,
    last_ms      BIGINT      NOT NULL,
    codec        TEXT        NOT NULL,
    raw_bytes    INTEGER     NOT NULL,
    sha256       TEXT        NOT NULL,
    data         BYTEA       NOT NULL,
    archived_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (day, index_name)
);
CREATE TABLE IF NOT EXISTS {s}.pivots (
    index_name  TEXT             NOT NULL,
    day         DATE             NOT NULL,
    r           DOUBLE PRECISION NOT NULL,
    conf_idx    INTEGER          NOT NULL,
    kind        TEXT             NOT NULL CHECK (kind IN ('HH', 'LL')),
    price       DOUBLE PRECISION NOT NULL,
    ms          BIGINT           NOT NULL,
    idx         INTEGER          NOT NULL,
    conf_price  DOUBLE PRECISION NOT NULL,
    conf_ms     BIGINT           NOT NULL,
    leg         DOUBLE PRECISION,
    PRIMARY KEY (index_name, day, r, conf_idx)
);
CREATE TABLE IF NOT EXISTS {s}.trades (
    index_name  TEXT             NOT NULL,
    entry_ms    BIGINT           NOT NULL,
    day         DATE             NOT NULL,
    exit_ms     BIGINT           NOT NULL,
    net_rs      DOUBLE PRECISION NOT NULL,
    row         JSONB            NOT NULL,
    logged_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (index_name, entry_ms)
);
CREATE INDEX IF NOT EXISTS trades_exit_idx ON {s}.trades (exit_ms, index_name);
CREATE TABLE IF NOT EXISTS {s}.reviews (
    id    BIGSERIAL   PRIMARY KEY,
    at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind  TEXT        NOT NULL,
    row   JSONB       NOT NULL
);
CREATE TABLE IF NOT EXISTS {s}.state (
    name        TEXT        PRIMARY KEY,
    value       JSONB       NOT NULL,
    version     BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {s}.algo_ledger (
    id    BIGSERIAL   PRIMARY KEY,
    at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind  TEXT        NOT NULL,
    row   JSONB       NOT NULL
);
"""


# ── tick codec ───────────────────────────────────────────────────────────────────
CODEC = "hht1-delta-xz"
_MAGIC = b"HHT1"
_HEAD = struct.Struct("<4sBxxxQ")       # magic, price mode, count
_CENTS, _FLOAT = 1, 0                   # prices as exact 1/100 steps, or raw float64


def _le(a: array) -> bytes:
    if sys.byteorder != "little":
        a = array(a.typecode, a)
        a.byteswap()
    return a.tobytes()


def _from_le(typecode: str, raw: bytes) -> array:
    a = array(typecode)
    a.frombytes(raw)
    if sys.byteorder != "little":
        a.byteswap()
    return a


def _deltas(xs: list) -> array:
    return array("q", (b - a for a, b in zip([0] + xs[:-1], xs)))


def encode_ticks(seqs: list, ms: list, px: list) -> bytes:
    """Lossless: seqs, epoch ms and prices, each delta-encoded as int64, then xz (CRC64).

    Prices go as integer paise when every price is exactly a 1/100 step (index ticks are),
    otherwise as raw float64, so decoding always gives back the very same floats."""
    n = len(seqs)
    if not n == len(ms) == len(px):
        raise ValueError("seqs, ms and prices differ in length")
    exact = all(math.isfinite(p) and abs(p) < 1e15 for p in px)
    cents = [round(p * 100) for p in px] if exact else []
    exact = exact and all(c / 100 == p for c, p in zip(cents, px))
    body = [_le(_deltas(list(seqs))), _le(_deltas(list(ms)))]
    body.append(_le(_deltas(cents)) if exact else _le(array("d", px)))
    raw = _HEAD.pack(_MAGIC, _CENTS if exact else _FLOAT, n) + b"".join(body)
    return lzma.compress(raw, format=lzma.FORMAT_XZ, check=lzma.CHECK_CRC64, preset=9)


def decode_ticks(blob: bytes) -> tuple[list, list, list]:
    raw = lzma.decompress(blob, format=lzma.FORMAT_XZ)
    magic, mode, n = _HEAD.unpack_from(raw)
    if magic != _MAGIC or mode not in (_CENTS, _FLOAT) or len(raw) != _HEAD.size + 24 * n:
        raise ValueError("not an hhll tick archive, or a damaged one")
    o, w = _HEAD.size, 8 * n
    seqs = list(accumulate(_from_le("q", raw[o:o + w])))
    ms = list(accumulate(_from_le("q", raw[o + w:o + 2 * w])))
    tail = raw[o + 2 * w:]
    px = [c / 100 for c in accumulate(_from_le("q", tail))] if mode == _CENTS else _from_le("d", tail).tolist()
    return seqs, ms, px


def _raw_size(blob: bytes) -> int:
    return len(lzma.decompress(blob, format=lzma.FORMAT_XZ))


# ── json ─────────────────────────────────────────────────────────────────────────
def _clean(o: Any) -> Any:
    """JSON-safe: JSONB has no NaN/Infinity, and dates/Decimals go as text."""
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if o is None or isinstance(o, (str, int, bool)):
        return o
    return str(o)


def _json(o: Any) -> Json:
    return Json(_clean(o))


# ── the store ────────────────────────────────────────────────────────────────────
class Store:
    """The `hhll` schema, over a `db_connector.TimescaleDBConnector` pool of its own.

    Thread-safe. At most `db.maxconn` threads use the pool at once; the rest wait their
    turn instead of meeting "pool exhausted". `writer` takes the rows a hot path must not
    wait on (ticks, pivots, the algo's ledger)."""

    def __init__(self, db, schema: str = SCHEMA) -> None:
        self.db = db
        self.schema = schema
        self._s = sql.Identifier(schema)
        self._slots = threading.BoundedSemaphore(max(1, int(getattr(db, "maxconn", 4))))
        self._ready = False
        self.writer = Writer(self, interval=WRITE_EVERY_S)
        self._listen = None                       # wait_ticks' own connection

    # -- plumbing ---------------------------------------------------------------
    def connect(self) -> "Store":
        """Open the pool, create the schema, start the writer. Idempotent."""
        connect = getattr(self.db, "connect", None)
        if callable(connect):
            connect()
        if not self._ready:
            self.ensure_schema()
            self._ready = True
        self.writer.start()
        return self

    def close(self, timeout: float = 30.0) -> bool:
        """Write everything still queued, then close the pool. False if rows were left unwritten."""
        drained = self.writer.stop(timeout)
        if self._listen is not None:
            self._listen.close()
            self._listen = None
        close = getattr(self.db, "close", None)
        if callable(close):
            close()
        self._ready = False
        return drained

    @property
    def tick_channel(self) -> str:
        return f"{self.schema}_ticks"

    def wait_ticks(self, timeout: float) -> bool:
        """Return when a writer (in any process) commits new ticks, or after `timeout`.

        One caller thread. Listens on a connection of its own, outside the pool; while that
        connection cannot be had this is a plain sleep, so the caller's poll still works.

        False only when a working listener heard nothing: no tick was committed meanwhile.
        True otherwise: notified, or the listener has just (re)connected (anything committed
        before it listened went unheard), or it could not be had."""
        try:
            if self._listen is None or self._listen.closed:
                conn = psycopg2.connect(**self.db.cfg)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.tick_channel)))
                self._listen = conn
                return True
            conn = self._listen
            if select.select([conn], [], [], timeout)[0]:
                conn.poll()
            heard = bool(conn.notifies)
            del conn.notifies[:]
            return heard
        except Exception as exc:  # noqa: BLE001 - fall back to polling; reconnect on the next call
            LOG.debug("tick listener: %s", exc)
            conn, self._listen = self._listen, None
            try:
                if conn is not None:
                    conn.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(timeout)
            return True

    def q(self, text: str) -> sql.Composed:
        return sql.SQL(text).format(s=self._s)

    @contextmanager
    def tx(self):
        """One transaction on a pooled connection, with a plain (tuple) cursor."""
        with self._slots:
            conn = self.db.get_raw_connection()
            broken = False
            try:
                with conn.cursor(cursor_factory=extensions.cursor) as cur:
                    yield cur
                conn.commit()
            except BaseException as exc:
                broken = isinstance(exc, (OperationalError, InterfaceError)) or bool(conn.closed)
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001 - the connection is gone; it is discarded below
                    broken = True
                raise
            finally:
                self.db.put_raw_connection(conn, close=broken or bool(conn.closed))

    def ensure_schema(self) -> None:
        with self.tx() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('hhll-schema'), hashtext(%s))", (self.schema,))
            cur.execute(self.q(_DDL))

    # -- ticks ------------------------------------------------------------------
    def next_seq(self, day: date, name: str) -> int:
        """The seq the next tick of (day, name) takes: one past every stored one."""
        with self.tx() as cur:
            cur.execute(self.q(
                "SELECT GREATEST((SELECT max(seq) FROM {s}.ticks WHERE day = %s AND index_name = %s),"
                " (SELECT last_seq FROM {s}.tick_archive WHERE day = %s AND index_name = %s))"),
                (day, name, day, name))
            last = cur.fetchone()[0]
        return 0 if last is None else int(last) + 1

    def insert_ticks(self, rows: list, cur=None) -> int:
        """rows: (day, index, seq, ms, price). A row already stored is left alone (retries)."""
        if not rows:
            return 0
        if cur is None:
            with self.tx() as c:
                return self.insert_ticks(rows, c)
        execute_values(cur, self.q("INSERT INTO {s}.ticks (day, index_name, seq, ms, price) VALUES %s "
                                   "ON CONFLICT DO NOTHING"), rows, page_size=5000)
        return len(rows)

    def ticks(self, day: date, name: str, start: int = 0, stop: Optional[int] = None) -> tuple[list, list, list]:
        """(seqs, ms, prices) of (day, name) with start <= seq < stop, archive and live rows merged."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT data FROM {s}.tick_archive WHERE day = %s AND index_name = %s"), (day, name))
            arc = cur.fetchone()
            cur.execute(self.q("SELECT seq, ms, price FROM {s}.ticks WHERE day = %s AND index_name = %s "
                               "AND seq >= %s AND (%s::int IS NULL OR seq < %s) ORDER BY seq"),
                        (day, name, start, stop, stop))
            live = cur.fetchall()
        return _merge(decode_ticks(bytes(arc[0])) if arc else ([], [], []), live, start, stop)

    def day_ticks(self, day: date) -> dict[str, tuple[list, list]]:
        """Every index's (ms, prices) of `day`."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT index_name, data FROM {s}.tick_archive WHERE day = %s"), (day,))
            arcs = {n: bytes(d) for n, d in cur.fetchall()}
            cur.execute(self.q("SELECT index_name, seq, ms, price FROM {s}.ticks WHERE day = %s "
                               "ORDER BY index_name, seq"), (day,))
            live: dict = {}
            for n, s, m, p in cur.fetchall():
                live.setdefault(n, []).append((s, m, p))
        out = {}
        for n in sorted(set(arcs) | set(live)):
            _, ms, px = _merge(decode_ticks(arcs[n]) if n in arcs else ([], [], []), live.get(n, []), 0, None)
            if ms:
                out[n] = (ms, px)
        return out

    def ticks_after(self, day: date, after: dict[str, int], limit: int = 50_000) -> list[tuple]:
        """Live rows of `day` newer than `after[index]` (a seq), as (index, seq, ms, price), in order."""
        names = list(after)
        with self.tx() as cur:
            cur.execute(self.q(
                # LATERAL: one primary key range scan per index. A plain join merges over the whole day on every poll.
                "SELECT t.index_name, t.seq, t.ms, t.price "
                "FROM unnest(%s::text[], %s::int[]) AS a(name, after) "
                "CROSS JOIN LATERAL (SELECT x.index_name, x.seq, x.ms, x.price FROM {s}.ticks x "
                "  WHERE x.day = %s AND x.index_name = a.name AND x.seq > a.after "
                "  ORDER BY x.seq LIMIT %s) t "
                "ORDER BY t.index_name, t.seq LIMIT %s"),
                (names, [int(after[n]) for n in names], day, limit, limit))
            return cur.fetchall()

    def days(self) -> list[date]:
        """Every day with ticks, live or archived."""
        with self.tx() as cur:
            cur.execute(self.q(
                "WITH RECURSIVE d AS ("
                "  (SELECT day FROM {s}.ticks ORDER BY day LIMIT 1)"
                "  UNION ALL"
                "  SELECT (SELECT t.day FROM {s}.ticks t WHERE t.day > d.day ORDER BY t.day LIMIT 1)"
                "  FROM d WHERE d.day IS NOT NULL)"
                "SELECT day FROM d WHERE day IS NOT NULL "
                "UNION SELECT day FROM {s}.tick_archive ORDER BY 1"))
            return [r[0] for r in cur.fetchall()]

    def span(self, day: date) -> tuple[int, int]:
        """First and last tick of `day` over every index (epoch ms), (0, 0) when none."""
        with self.tx() as cur:
            cur.execute(self.q(
                "SELECT min(lo), max(hi) FROM ("
                "  SELECT min(ms) lo, max(ms) hi FROM {s}.ticks WHERE day = %s"
                "  UNION ALL SELECT min(first_ms), max(last_ms) FROM {s}.tick_archive WHERE day = %s) x"),
                (day, day))
            lo, hi = cur.fetchone()
        return int(lo or 0), int(hi or 0)

    def archive_closed_days(self, today: date) -> list[dict]:
        """Compress every (day, index) before `today` into `tick_archive` and delete its rows.

        One transaction per (day, index): the archive row is written, read back and checked
        (SHA-256 of what the database holds, and a full decode equal to the rows) before the
        rows are deleted, so a failure anywhere leaves the ticks where they were."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT DISTINCT day, index_name FROM {s}.ticks WHERE day < %s ORDER BY 1, 2"),
                        (today,))
            todo = cur.fetchall()
        done = [d for d in (self._archive(day, name) for day, name in todo) if d]
        if done:
            self._vacuum("ticks")
            LOG.info("archived %s", ", ".join(f"{d['day']} {d['index']} {d['ticks']} ticks "
                                              f"{d['raw_bytes']}B->{d['bytes']}B" for d in done))
        return done

    def _archive(self, day: date, name: str) -> Optional[dict]:
        with self.tx() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('hhll-archive'), hashtext(%s))", (self.schema,))
            cur.execute(self.q("SELECT seq, ms, price FROM {s}.ticks WHERE day = %s AND index_name = %s "
                               "ORDER BY seq FOR UPDATE"), (day, name))
            rows = cur.fetchall()
            cur.execute(self.q("SELECT data, sha256 FROM {s}.tick_archive WHERE day = %s AND index_name = %s "
                               "FOR UPDATE"), (day, name))
            old = cur.fetchone()
            seqs, ms, px = [], [], []
            if old:
                blob = bytes(old[0])
                if hashlib.sha256(blob).hexdigest() != old[1]:
                    raise RuntimeError(f"tick archive {day} {name} fails its checksum; nothing changed")
                seqs, ms, px = decode_ticks(blob)
            if not rows:
                return None
            if seqs and rows[0][0] <= seqs[-1]:
                raise RuntimeError(f"ticks of {day} {name} overlap its archive (seq {rows[0][0]} <= {seqs[-1]}); "
                                   "nothing changed")
            seqs += [r[0] for r in rows]
            ms += [int(r[1]) for r in rows]
            px += [float(r[2]) for r in rows]
            blob = encode_ticks(seqs, ms, px)
            if decode_ticks(blob) != (seqs, ms, px):
                raise RuntimeError(f"tick archive {day} {name} does not decode to its ticks; nothing changed")
            digest = hashlib.sha256(blob).hexdigest()
            raw = _raw_size(blob)
            cur.execute(self.q(
                "INSERT INTO {s}.tick_archive (day, index_name, ticks, first_seq, last_seq, first_ms, last_ms,"
                " codec, raw_bytes, sha256, data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (day, index_name) DO UPDATE SET ticks = EXCLUDED.ticks, first_seq = EXCLUDED.first_seq,"
                " last_seq = EXCLUDED.last_seq, first_ms = EXCLUDED.first_ms, last_ms = EXCLUDED.last_ms,"
                " codec = EXCLUDED.codec, raw_bytes = EXCLUDED.raw_bytes, sha256 = EXCLUDED.sha256,"
                " data = EXCLUDED.data, archived_at = now()"),
                (day, name, len(seqs), seqs[0], seqs[-1], min(ms), max(ms), CODEC, raw, digest, blob))
            cur.execute(self.q("SELECT encode(sha256(data), 'hex'), ticks FROM {s}.tick_archive "
                               "WHERE day = %s AND index_name = %s"), (day, name))
            stored = cur.fetchone()
            if stored != (digest, len(seqs)):
                raise RuntimeError(f"tick archive {day} {name} read back wrong; nothing changed")
            cur.execute(self.q("DELETE FROM {s}.ticks WHERE day = %s AND index_name = %s AND seq <= %s"),
                        (day, name, rows[-1][0]))
            if cur.rowcount != len(rows):
                raise RuntimeError(f"deleting {day} {name}: {cur.rowcount} rows, archived {len(rows)}; nothing changed")
        return {"day": day.isoformat(), "index": name, "ticks": len(seqs), "raw_bytes": raw, "bytes": len(blob)}

    def _vacuum(self, table: str) -> None:
        """Give the archived rows' space back (VACUUM cannot run in a transaction)."""
        with self._slots:
            conn = self.db.get_raw_connection()
            broken = False
            try:
                conn.commit()
                old = conn.autocommit
                conn.autocommit = True
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql.SQL("VACUUM (ANALYZE) {}.{}").format(self._s, sql.Identifier(table)))
                finally:
                    conn.autocommit = old
            except Exception as exc:  # noqa: BLE001 - autovacuum does it later anyway
                broken = isinstance(exc, (OperationalError, InterfaceError)) or bool(conn.closed)
                LOG.warning("VACUUM %s.%s skipped: %s", self.schema, table, exc)
            finally:
                self.db.put_raw_connection(conn, close=broken or bool(conn.closed))

    # -- pivots -----------------------------------------------------------------
    def insert_pivots(self, rows: list, cur=None) -> None:
        """rows: (index, day, r, conf_idx, kind, price, ms, idx, conf_price, conf_ms, leg)."""
        if not rows:
            return
        if cur is None:
            with self.tx() as c:
                return self.insert_pivots(rows, c)
        # The desk and the engine insert the same rows at once: one key order, or they deadlock.
        rows = sorted(rows, key=lambda row: row[:4])
        execute_values(cur, self.q(
            "INSERT INTO {s}.pivots (index_name, day, r, conf_idx, kind, price, ms, idx, conf_price, conf_ms, leg) "
            "VALUES %s ON CONFLICT DO NOTHING"), rows, page_size=2000)

    def pivots(self, day: date, name: str, r: float, limit: int = 500) -> list[tuple]:
        """The latest `limit` pivots of the chain (day, name, R), oldest first:
        (kind, price, ms, conf_price, conf_ms, leg)."""
        with self.tx() as cur:
            cur.execute(self.q(
                "SELECT kind, price, ms, conf_price, conf_ms, leg FROM ("
                "  SELECT * FROM {s}.pivots WHERE index_name = %s AND day = %s AND r = %s"
                "  ORDER BY conf_idx DESC LIMIT %s) x ORDER BY conf_idx"),
                (name, day, pivot_r(r), limit))
            return cur.fetchall()

    # -- trades, reviews, state -------------------------------------------------
    def add_trade(self, t: dict) -> bool:
        """Store one closed trade; False when it is already stored."""
        with self.tx() as cur:
            cur.execute(self.q("INSERT INTO {s}.trades (index_name, entry_ms, day, exit_ms, net_rs, row) "
                               "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"),
                        (t["index"], int(t["entry_ms"]), date.fromisoformat(t["day"]), int(t["exit_ms"]),
                         float(t["net_rs"]), _json(t)))
            return cur.rowcount == 1

    def trades(self) -> list[dict]:
        with self.tx() as cur:
            cur.execute(self.q("SELECT row FROM {s}.trades ORDER BY exit_ms, index_name, entry_ms"))
            return [r[0] for r in cur.fetchall()]

    def add_review(self, row: dict) -> None:
        with self.tx() as cur:
            cur.execute(self.q("INSERT INTO {s}.reviews (at, kind, row) VALUES (%s, %s, %s)"),
                        (_at(row.get("at")), str(row.get("kind") or "review"), _json(row)))

    def reviews(self) -> list[dict]:
        with self.tx() as cur:
            cur.execute(self.q("SELECT row FROM {s}.reviews ORDER BY id"))
            return [r[0] for r in cur.fetchall()]

    def recent_reviews(self, limit: int) -> tuple[list[dict], int]:
        """The newest `limit` reviews, oldest first, and how many there are in all."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT row FROM (SELECT id, row FROM {s}.reviews ORDER BY id DESC LIMIT %s) x "
                               "ORDER BY id"), (int(limit),))
            rows = [r[0] for r in cur.fetchall()]
            cur.execute(self.q("SELECT count(*) FROM {s}.reviews"))
            return rows, int(cur.fetchone()[0])

    def latest_review(self, kind: str) -> Optional[dict]:
        """The newest review of `kind`, or None."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT row FROM {s}.reviews WHERE kind = %s ORDER BY id DESC LIMIT 1"), (kind,))
            r = cur.fetchone()
        return r[0] if r else None

    def load_state(self, name: str = "engine") -> dict:
        with self.tx() as cur:
            cur.execute(self.q("SELECT value FROM {s}.state WHERE name = %s"), (name,))
            r = cur.fetchone()
        return r[0] if r else {}

    def save_state(self, value: dict, name: str = "engine") -> int:
        with self.tx() as cur:
            cur.execute(self.q("INSERT INTO {s}.state (name, value, version) VALUES (%s, %s, 1) "
                               "ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value,"
                               " version = {s}.state.version + 1, updated_at = now() RETURNING version"),
                        (name, _json(value)))
            return int(cur.fetchone()[0])

    def state_version(self, name: str = "engine") -> int:
        """Moves on every save: a cheap way to notice new settings."""
        with self.tx() as cur:
            cur.execute(self.q("SELECT version FROM {s}.state WHERE name = %s"), (name,))
            r = cur.fetchone()
        return int(r[0]) if r else 0

    def add_ledger(self, rows: list, cur=None) -> None:
        """rows: dicts with a `kind` (hhll_algo's pivots, executions, trades)."""
        if not rows:
            return
        if cur is None:
            with self.tx() as c:
                return self.add_ledger(rows, c)
        execute_values(cur, self.q("INSERT INTO {s}.algo_ledger (kind, row) VALUES %s"),
                       [(str(r.get("kind") or "row"), _json(r)) for r in rows], page_size=1000)

    def ledger(self) -> list[dict]:
        with self.tx() as cur:
            cur.execute(self.q("SELECT row FROM {s}.algo_ledger ORDER BY id"))
            return [r[0] for r in cur.fetchall()]

    # -- one-time move of the old files -----------------------------------------
    def migrate_files(self, data_dir: Path, ledger_file: Optional[Path], backup_dir: Path) -> dict:
        """Import what hhll_tick / hhll_algo used to keep in files, then pack those files into
        one .tar.xz in `backup_dir` and remove them. Idempotent and safe to repeat: a (day,
        index) the database already has ticks for, a state already stored and non-empty
        review/ledger tables are never imported twice; trades are keyed. All imports are one
        transaction; a file is removed only once it is in a verified backup and unchanged."""
        data_dir = Path(data_dir)
        files: list[Path] = []
        tick_files = [f for f in sorted((data_dir / "ticks").glob("*/*.csv")) if _iso_day(f.parent.name)] \
            if (data_dir / "ticks").is_dir() else []
        named = [data_dir / "params.json", data_dir / "trades.jsonl", data_dir / "reviews.jsonl"]
        if ledger_file is not None:
            named.append(Path(ledger_file))
        files = [f for f in tick_files + named if f.is_file()]
        if not files:
            return {}
        seen = {f: _stat(f) for f in files}
        report: dict = {"ticks": 0, "tick_files_skipped": 0, "trades": 0, "reviews": 0, "ledger": 0, "state": False}
        with self.tx() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('hhll-migrate'), hashtext(%s))", (self.schema,))
            for f in tick_files:
                day, name = _iso_day(f.parent.name), f.stem
                cur.execute(self.q("SELECT EXISTS (SELECT 1 FROM {s}.ticks WHERE day = %s AND index_name = %s)"
                                   " OR EXISTS (SELECT 1 FROM {s}.tick_archive WHERE day = %s AND index_name = %s)"),
                            (day, name, day, name))
                if cur.fetchone()[0]:
                    report["tick_files_skipped"] += 1
                    continue
                rows = [(day, name, i, m, p) for i, (m, p) in enumerate(_read_csv_ticks(f))]
                self.insert_ticks(rows, cur)
                report["ticks"] += len(rows)
            p = data_dir / "params.json"
            if p.is_file():
                cur.execute(self.q("SELECT 1 FROM {s}.state WHERE name = 'engine'"))
                if cur.fetchone() is None:
                    st = json.loads(p.read_text() or "{}")
                    if st:
                        cur.execute(self.q("INSERT INTO {s}.state (name, value, version) VALUES ('engine', %s, 1)"),
                                    (_json(st),))
                        report["state"] = True
            for t in _read_jsonl(data_dir / "trades.jsonl"):
                cur.execute(self.q("INSERT INTO {s}.trades (index_name, entry_ms, day, exit_ms, net_rs, row) "
                                   "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"),
                            (t["index"], int(t["entry_ms"]), date.fromisoformat(t["day"]), int(t["exit_ms"]),
                             float(t["net_rs"]), _json(t)))
                report["trades"] += cur.rowcount
            # No natural key: a row already stored with the same content is not stored again.
            for table, rows in (("reviews", _read_jsonl(data_dir / "reviews.jsonl")),
                                ("algo_ledger", _read_jsonl(Path(ledger_file)) if ledger_file is not None else [])):
                if not rows:
                    continue
                execute_values(cur, sql.SQL(
                    "INSERT INTO {s}.{t} (at, kind, row) SELECT v.at, v.kind, v.row "
                    "FROM (VALUES %s) AS v(at, kind, row) "
                    "WHERE NOT EXISTS (SELECT 1 FROM {s}.{t} x WHERE x.row = v.row)").format(
                        s=self._s, t=sql.Identifier(table)),
                    [(_at(r.get("at")), str(r.get("kind") or "review"), _json(r)) for r in rows],
                    template="(%s::timestamptz, %s::text, %s::jsonb)", page_size=len(rows))
                report["reviews" if table == "reviews" else "ledger"] = cur.rowcount
        report["backup"] = str(_pack_and_remove(files, seen, Path(backup_dir), data_dir.parent))
        LOG.warning("moved the HH/LL files into the database: %s", json.dumps(report))
        return report


def pivot_r(r: float) -> float:
    """A chain as the pivots table keys it (`hhll_tick.Book.chain`, to 1/10000 of a point)."""
    return round(float(r), 4)


def pivot_row(name: str, day: date, r: float, pv) -> tuple:
    """One `hhll_tick.Pivot` as a pivots row."""
    return (name, day, pivot_r(r), int(pv.conf_idx), pv.kind, float(pv.price), int(pv.ms), int(pv.idx),
            float(pv.conf_price), int(pv.conf_ms), None if pv.leg is None else float(pv.leg))


def _merge(arc: tuple, live: list, start: int, stop: Optional[int]) -> tuple[list, list, list]:
    seqs, ms, px = [], [], []
    for s, m, p in zip(*arc):
        if s >= start and (stop is None or s < stop):
            seqs.append(s)
            ms.append(m)
            px.append(p)
    last = seqs[-1] if seqs else None
    for s, m, p in live:
        if last is not None and s <= last:
            continue        # never both live and archived; an archive wins if it ever were
        seqs.append(int(s))
        ms.append(int(m))
        px.append(float(p))
    return seqs, ms, px


def _at(v: Any) -> datetime:
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _iso_day(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _read_csv_ticks(path: Path) -> list[tuple[int, float]]:
    out = []
    with path.open() as fh:
        for line in fh:
            a, _, b = line.partition(",")
            if not b.endswith("\n"):        # a line still being written
                continue
            try:
                m, p = int(a), float(b)
            except ValueError:
                continue
            if 0 < p < float("inf"):
                out.append((m, p))
    return out


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def _stat(f: Path) -> tuple[int, int]:
    s = f.stat()
    return s.st_size, s.st_mtime_ns


def _pack_and_remove(files: list[Path], seen: dict, backup_dir: Path, root: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    out = backup_dir / f"hhll_files_{datetime.now():%Y%m%d_%H%M%S}.tar.xz"
    arcname = {f: str(f.resolve().relative_to(root.resolve())) if f.resolve().is_relative_to(root.resolve())
               else f.name for f in files}
    tmp = out.with_suffix(".tmp")
    with tarfile.open(tmp, "w:xz") as tar:
        for f in files:
            tar.add(f, arcname=arcname[f])
    with tarfile.open(tmp, "r:xz") as tar:                       # verify before anything is removed
        sizes = {m.name: m.size for m in tar.getmembers()}
        for f in files:
            src = tar.extractfile(arcname[f])
            if sizes.get(arcname[f]) != seen[f][0] or src is None or src.read() != f.read_bytes():
                raise RuntimeError(f"backup of {f} does not match it; nothing removed")
    tmp.replace(out)
    for f in files:
        if _stat(f) != seen[f]:
            LOG.critical("%s changed while it was moved into the database (an old writer still running?); "
                         "left in place", f)
            continue
        f.unlink()
    for d in {f.parent for f in files}:
        if d.name and d.parent.name == "ticks":
            try:
                d.rmdir()
            except OSError:
                pass
    ticks = next((f.parent.parent for f in files if f.parent.parent.name == "ticks"), None)
    if ticks is not None:
        try:
            ticks.rmdir()
        except OSError:
            pass
    return out


# ── background writer ────────────────────────────────────────────────────────────
class Writer:
    """Writes queued ticks, pivots and ledger rows in batches on a thread of its own, so the
    websocket thread never waits on the database.

    Nothing is dropped: a failed batch is retried (every insert is idempotent) until it is
    written, while new rows queue behind it. `on_ticks(day, index, last_seq)` is called after
    each committed batch with the highest seq it wrote per (day, index)."""

    def __init__(self, store: Store, interval: float = 0.0) -> None:
        self.store = store
        self.interval = interval
        self.on_ticks: list[Callable[[date, str, int], None]] = []
        self._cond = threading.Condition()
        self._q: deque = deque()
        self._thread: Optional[threading.Thread] = None
        self._stop = False
        self._deadline = 0.0
        self._failing_since: Optional[float] = None

    def put_tick(self, row: tuple) -> None:
        self._put(("tick", row))

    def put_pivot(self, row: tuple) -> None:
        self._put(("pivot", row))

    def put_ledger(self, row: dict) -> None:
        self._put(("ledger", row))

    def _put(self, item: tuple) -> None:
        with self._cond:
            self._q.append(item)
            if len(self._q) == 1:
                self._cond.notify()

    def pending(self) -> int:
        with self._cond:
            return len(self._q)

    def start(self) -> None:
        with self._cond:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop = False
            self._thread = threading.Thread(target=self._run, name="hhll-writer", daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 30.0) -> bool:
        """Write what is queued (retrying up to `timeout`), then stop. True when nothing is left."""
        with self._cond:
            self._stop = True
            self._deadline = time.monotonic() + timeout
            self._cond.notify()
            t = self._thread
        if t is not None:
            t.join(timeout + 5)
        left = self.pending()
        if left:
            LOG.critical("hhll writer stopped with %d rows unwritten", left)
        return left == 0

    def flush(self, timeout: float = 10.0) -> bool:
        """Wait until everything queued so far is written (tests, shutdown)."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with self._cond:
                if not self._q:           # rows leave the queue only once they are committed
                    return True
            time.sleep(0.01)
        return False

    def _run(self) -> None:
        backoff = 1.0
        while True:
            with self._cond:
                while not self._q and not self._stop:
                    self._cond.wait(1.0)
                if not self._q and self._stop:
                    return
                if self._stop and time.monotonic() > self._deadline:
                    return
                batch = list(self._q)
            try:
                self._write(batch)
            except Exception as exc:  # noqa: BLE001 - kept and retried; nothing is dropped
                now = time.monotonic()
                if self._failing_since is None:
                    self._failing_since = now
                    LOG.error("hhll writer: %s; %d rows kept, retrying", exc, len(batch))
                elif now - self._failing_since > 60 and backoff >= 30:
                    LOG.critical("hhll writer failing for %.0fs: %s; %d rows queued", now - self._failing_since,
                                 exc, self.pending())
                    self._failing_since = now
                with self._cond:
                    if self._stop and time.monotonic() > self._deadline:
                        return
                    self._cond.wait(backoff)
                backoff = min(30.0, backoff * 2)
                continue
            backoff = 1.0
            if self._failing_since is not None:
                LOG.warning("hhll writer recovered")
                self._failing_since = None
            with self._cond:
                for _ in range(len(batch)):
                    self._q.popleft()
            if self.interval and not self._stop:   # 0: write on arrival; rows queued meanwhile make the next batch
                time.sleep(self.interval)

    def _write(self, batch: list) -> None:
        ticks = [r for k, r in batch if k == "tick"]
        pivots = [r for k, r in batch if k == "pivot"]
        ledger = [r for k, r in batch if k == "ledger"]
        with self.store.tx() as cur:
            self.store.insert_ticks(ticks, cur)
            self.store.insert_pivots(pivots, cur)
            self.store.add_ledger(ledger, cur)
            if ticks:                               # delivered at commit: wakes `wait_ticks` in the reader
                cur.execute("SELECT pg_notify(%s, '')", (self.store.tick_channel,))
        last: dict = {}
        for day, name, seq, _, _ in ticks:
            if seq > last.get((day, name), -1):
                last[(day, name)] = seq
        for (day, name), seq in last.items():
            for cb in self.on_ticks:
                try:
                    cb(day, name, seq)
                except Exception:  # noqa: BLE001 - a listener must not stop the writer
                    LOG.exception("hhll writer listener failed")


def connect(schema: str = SCHEMA, maxconn: int = 4) -> Store:
    """A connected store on a pool of its own (the Lune database, credentials from .env)."""
    from db_connector import TimescaleDBConnector
    return Store(TimescaleDBConnector(minconn=1, maxconn=maxconn, auto_provision=False), schema).connect()
