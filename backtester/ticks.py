"""Lune's tick data, all six indices, as local verified files.

`sync()` copies every FINISHED session from Lune's database (`hhll.tick_archive`, the
compressed (day, index) blobs Lune itself writes) into data/ticks/<day>/<INDEX>.hht, byte
for byte, and checks each blob's SHA-256 against the database and a full decode against its
row count before keeping it. The session in progress (`hhll.ticks`) is never copied: an
unfinished day would be a different day once it finishes.

`load(day, index)` reads a file back, checks it against the manifest (SHA-256, count, first
and last ms) and that times never go backwards, and returns (epoch ms, prices) as arrays.
"""

from __future__ import annotations

import hashlib
import json
import os
from array import array
from dataclasses import asdict
from datetime import date
from functools import lru_cache
from pathlib import Path

import engine  # noqa: F401  puts Lune on sys.path
from hhll_store import decode_ticks  # Lune's own codec

BASE = Path(__file__).resolve().parent
DATA = BASE / "data" / "ticks"
MANIFEST = DATA / "manifest.json"
LIVE_PARAMS = BASE / "data" / "live_params.json"


def _manifest() -> dict:
    try:
        return json.loads(MANIFEST.read_text())
    except FileNotFoundError:
        return {}


def _write(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _path(day: str, name: str) -> Path:
    return DATA / day / f"{name}.hht"


def _check(ms, px) -> None:
    if any(b < a for a, b in zip(ms, ms[1:])):
        raise ValueError("tick times go backwards")
    if any(not p > 0 for p in px):
        raise ValueError("a price is not positive")


def sync() -> dict:
    """Copy every finished (day, index) not yet here; refresh Lune's live settings. Returns counts."""
    import hhll_store
    import hhll_tick
    st = hhll_store.connect(maxconn=1)
    try:
        man = _manifest()
        with st.tx() as cur:
            cur.execute(st.q("SELECT day, index_name, ticks, first_ms, last_ms, sha256 FROM {s}.tick_archive "
                             "ORDER BY day, index_name"))
            rows = cur.fetchall()
        added = 0
        for day, name, n, lo, hi, sha in rows:
            key = f"{day.isoformat()}/{name}"
            f = _path(day.isoformat(), name)
            if man.get(key, {}).get("sha256") == sha and f.exists():
                continue
            with st.tx() as cur:
                cur.execute(st.q("SELECT data FROM {s}.tick_archive WHERE day = %s AND index_name = %s"), (day, name))
                blob = bytes(cur.fetchone()[0])
            if hashlib.sha256(blob).hexdigest() != sha:
                raise ValueError(f"{key}: archive blob does not match its SHA-256 in the database")
            _, ms, px = decode_ticks(blob)
            if len(ms) != n or ms[0] != lo or ms[-1] != hi:
                raise ValueError(f"{key}: decoded ticks do not match the archive row")
            _check(ms, px)
            f.parent.mkdir(parents=True, exist_ok=True)
            _write(f, blob)
            man[key] = {"day": day.isoformat(), "index": name, "ticks": n, "first_ms": lo, "last_ms": hi, "sha256": sha}
            added += 1
        DATA.mkdir(parents=True, exist_ok=True)
        _write(MANIFEST, json.dumps(man, indent=1, sort_keys=True).encode())
        hhll_tick.use_store(st)
        p, _ = hhll_tick.current_params(hhll_tick.today_ist())
        _write(LIVE_PARAMS, json.dumps(asdict(p), indent=1).encode())
    finally:
        hhll_tick.use_store(None)
        st.close()
    load.cache_clear()
    return {"added": added, "files": len(man)}


def inventory() -> list[dict]:
    """Every local (day, index), oldest first."""
    return sorted(_manifest().values(), key=lambda r: (r["day"], r["index"]))


def days() -> list[date]:
    return sorted({date.fromisoformat(r["day"]) for r in inventory()})


def count(name: str, day: date) -> int:
    return _manifest()[f"{day.isoformat()}/{name}"]["ticks"]


def names_on(day: date) -> list[str]:
    d = day.isoformat()
    return [r["index"] for r in inventory() if r["day"] == d]


@lru_cache(maxsize=64)   # ponytail: ~16 B/tick, 64 index-days ~ 50-100 MB per process
def load(day: date, name: str) -> tuple[array, array]:
    """(epoch ms, prices) of one index's finished session, verified against the manifest."""
    key = f"{day.isoformat()}/{name}"
    row = _manifest().get(key)
    if row is None:
        raise KeyError(f"no ticks for {key}")
    blob = _path(day.isoformat(), name).read_bytes()
    if hashlib.sha256(blob).hexdigest() != row["sha256"]:
        raise ValueError(f"{key}: file does not match its SHA-256, run sync again")
    _, ms, px = decode_ticks(blob)
    if len(ms) != row["ticks"] or ms[0] != row["first_ms"] or ms[-1] != row["last_ms"]:
        raise ValueError(f"{key}: decoded ticks do not match the manifest")
    _check(ms, px)
    return array("q", ms), array("d", px)


def live_params() -> dict:
    """Lune's settings in force when `sync` last ran (hhll.state), or {}."""
    try:
        return json.loads(LIVE_PARAMS.read_text())
    except FileNotFoundError:
        return {}


if __name__ == "__main__":
    print(sync())
