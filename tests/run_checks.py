#!/usr/bin/env python
"""One command that checks the bot is safe to start.

    python tests\\run_checks.py            # everything
    python tests\\run_checks.py --quick    # no database needed
    python tests\\run_checks.py --no-order-path   # skip the slow replay

Runs four things and prints one verdict:

  1. the full unit-test suite (v3 live parity against its own backtest, the
     synthetic-long legs, the order path, timezones, the carry,
     backup/truncate, and the odds and ends in test_misc);
  2. the frozen strategy - the SHA-256 of every file of `strategy/v3` the live
     path runs and of the data it was validated on (`backups/data`), against
     v3's own release checksums (`strategy_v3_manifest.json`, from
     `strategy/v3/freeze_v3.json`), so a changed byte means the bot is no
     longer the strategy that was tested - and the warm-up data v3 needs,
     complete for every session since;
  3. the order path, end to end - the real managers and the real decider driven
     by v3's recorded sessions against fake brokers, proving that a met entry
     condition actually reaches an order and that every refusal says so at a
     level `main.py`'s WARNING handler keeps. This is `check_order_paths.py`
     run in-process; a silent skip and a genuine no-signal day are identical in
     the log and identical in `trade_logs`, so nothing else catches it;
  4. the live environment - database reachable, schema provisioned, the
     `book` column an open position is resumed from, and a writable
     backups directory.

Exit code is 0 only if every check passes, so it can gate a restart.

Nothing here touches the production database (except read-only in 4), the
broker, or the running bot, so it is safe to run while the bot is trading.

Deliberately does not import `main`: that reconfigures logging onto the
production bot log, and a check run must not write there.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import sys
import unittest
from pathlib import Path

from psycopg2 import connect as pg_connect
from psycopg2.extras import RealDictCursor

TESTS_DIR = Path(__file__).resolve().parent
BASE_DIR = TESTS_DIR.parent

# Running this file directly puts `tests/` on sys.path, not the project root,
# so the production modules the checks import would not be found. Put the root
# first, and keep it first: the checks must exercise the real modules.
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

GREEN = "PASS"
RED = "FAIL"
GREY = "SKIP"


class Report:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []

    def add(self, section: str, name: str, ok: bool | None, detail: str = "") -> None:
        status = GREY if ok is None else (GREEN if ok else RED)
        self.rows.append((section, f"{status:<4} {name}", detail))

    @property
    def failed(self) -> int:
        return sum(1 for _s, n, _d in self.rows if n.startswith(RED))

    def show(self) -> None:
        section = None
        for sec, name, detail in self.rows:
            if sec != section:
                section = sec
                print(f"\n{sec}")
                print("-" * 66)
            print(f"  {name}{('  ' + detail) if detail else ''}")


# --------------------------------------------------------------------------- #
#  1. unit tests
# --------------------------------------------------------------------------- #
def run_unit_tests(report: Report, verbose: bool) -> None:
    loader = unittest.TestLoader()
    # Start in `tests/`, but resolve imports from the project root so the test
    # modules can `import db_connector` and friends by their bare names.
    suite = loader.discover(str(TESTS_DIR), pattern="test_*.py",
                            top_level_dir=str(BASE_DIR))

    stream = sys.stderr if verbose else io.StringIO()
    runner = unittest.TextTestRunner(stream=stream, verbosity=2 if verbose else 0)

    # Most of this suite exercises failure paths on purpose, so it emits a wall
    # of ERROR/CRITICAL lines that are the tests passing, not the bot breaking.
    # Silencing them is what makes the verdict below readable; a genuine failure
    # still surfaces through the result object.
    logging.disable(logging.CRITICAL)
    try:
        result = runner.run(suite)
    finally:
        logging.disable(logging.NOTSET)

    total = result.testsRun
    bad = len(result.failures) + len(result.errors)
    report.add("1. UNIT TESTS", f"{total - bad}/{total} passed", bad == 0)

    for case, trace in result.failures + result.errors:
        last = [line for line in trace.strip().splitlines() if line.strip()][-1]
        report.add("1. UNIT TESTS", str(case), False, last[:90])


# --------------------------------------------------------------------------- #
#  2. the frozen strategy and its data
# --------------------------------------------------------------------------- #
def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


#: The SHA-256 of every file the live path runs or reads, by path from the
#: project root - v3's code in `strategy/v3` (as v3's own freeze manifest,
#: `strategy/v3/freeze_v3.json`, recorded it at release) and the history it
#: was tested on in `backups/data`. `backups/data_live` is a cache and is not
#: in it.
MANIFEST = TESTS_DIR / "strategy_v3_manifest.json"
MANIFEST_SHA256 = "4c5d58aa27714a6eedcd8c90bcda372f32c7ae5fbe6ca005462b1fbe738924f7"
#: What `strategy/v3` holds besides the code in the manifest: its release
#: record, documentation and pinned requirements, and a link to its data.
V3_EXTRAS = {"README.md", "freeze_v3.json", "requirements.txt", "data"}


def check_strategy(report: Report) -> None:
    """The live path still runs the unchanged frozen v3, on the data it was tested on."""
    section = "2. FROZEN STRATEGY"
    try:
        import strategy_logic
        manifest = json.loads(MANIFEST.read_text())
    except Exception as exc:  # noqa: BLE001
        report.add(section, "import strategy modules", False, repr(exc))
        return
    report.add(section, f"{MANIFEST.name} unchanged", _sha256(MANIFEST) == MANIFEST_SHA256)

    # Code file by file; the supplied data in bulk.
    data = []
    for name, expected in sorted(manifest.items()):
        if name.startswith("backups/data/"):
            data.append(name)
            continue
        path = BASE_DIR / name
        digest = _sha256(path) if path.exists() else "missing"
        report.add(section, f"{name} SHA-256 unchanged", digest == expected,
                   "" if digest == expected else f"got {digest[:16]}...")
    bad = [name for name in data
           if not (BASE_DIR / name).exists() or _sha256(BASE_DIR / name) != manifest[name]]
    report.add(section, f"{len(data) - len(bad)}/{len(data)} files of the tested history match",
               not bad, "" if not bad else f"first: {bad[0]}")

    # Nothing in the strategy folder beyond its frozen code, its release
    # record and its link to the data it was tested on.
    v3 = BASE_DIR / "strategy" / "v3"
    stray = sorted(path.name for path in v3.iterdir()
                   if f"strategy/v3/{path.name}" not in manifest
                   and path.name not in V3_EXTRAS | {"__pycache__"})
    report.add(section, "strategy/v3 holds only the frozen release", not stray,
               "" if not stray else f"unexpected {stray}")
    link, target = v3 / "data", BASE_DIR / "backups" / "data"
    linked = link.is_symlink() and link.resolve() == target.resolve()
    report.add(section, "strategy/v3/data links to backups/data", linked,
               "" if linked else f"{link} -> {os.readlink(link) if link.is_symlink() else 'not a link'}")
    try:
        frozen = json.loads((v3 / "freeze_v3.json").read_text())["sha256"]
        agree = all(frozen[name.rsplit("/", 1)[1]] == digest for name, digest in manifest.items()
                    if name.startswith("strategy/v3/"))
    except Exception:  # noqa: BLE001 - a missing or torn record fails the check
        agree = False
    report.add(section, "manifest agrees with v3's own freeze_v3.json", agree)

    V2, V3 = strategy_logic.V2, strategy_logic.V3
    expected_params = {"k": 0.40, "stop": 1.0, "m_lo": 24, "m_hi": 60, "carry_bar": 67,
                       "last_entry_bar": 68, "drop": 1.0, "cov": 20.0, "idx_prem": 0.008}
    for name, want in expected_params.items():
        got = V2.P.get(name)
        report.add(section, f"P[{name!r}] == {want}", got == want,
                   "" if got == want else f"got {got}")
    report.add(section, f"shock guard at {V3.SHOCK_ATR} ATR, carry on candles {V3.B_CANDLES}",
               V3.SHOCK_ATR == 0.75 and V3.B_CANDLES == (66, 67))

    for name, want in (
        ("SESSION_OPEN_TIME", "09:15:00"),
        ("SESSION_LAST_BAR_TIME", "15:25:00"),
        ("LEGS_OPEN_TIME", "14:45:00"),
        ("NIGHT_ENTRY_TIME", "15:11:00"),
        ("LEGS_CLOSE_TIME", "15:14:00"),
        ("CARRY_EXIT_TIME", "09:15:00"),
        ("LIVE_SETUPS", "('B',)"),
    ):
        got = str(getattr(strategy_logic, name))
        report.add(section, f"{name} == {want}", got == want,
                   "" if got == want else f"got {got}")

    # Nothing may re-implement the rules alongside the frozen release.
    stray = sorted(path.name for path in (BASE_DIR / "strategy").iterdir()
                   if path.name not in {"v3", "__pycache__"})
    report.add(section, "strategy/ holds only the frozen v3",
               not stray, "" if not stray else f"unexpected {stray}")

    # Every index in the waterfall must be tradeable, and vice versa. A mismatch
    # is an index winning slots the order path cannot fill, or an index being
    # ordered that the strategy never selected.
    try:
        import instruments
    except Exception as exc:  # noqa: BLE001
        report.add(section, "instrument registry imports", False, repr(exc))
        return
    selected = {name for _setup, name in V2.WATERFALL}
    tradeable = set(instruments.INSTRUMENTS)
    report.add(section, "every strategy index is tradeable", selected == tradeable,
               "" if selected == tradeable else f"strategy={sorted(selected)} tradeable={sorted(tradeable)}")
    for name in instruments.NAMES:
        inst = instruments.get(name)
        sized = inst.quantity == V2.LOTS[name] and inst.strike_step > 0
        report.add(
            section,
            f"{name}: {inst.spot_exchange} spot -> {inst.fo_exchange} orders, "
            f"{inst.quantity} units, {inst.strike_step}-pt strikes",
            sized, "" if sized else f"v3 trades {V2.LOTS[name]} units",
        )
        # The expiry calendar decides which contract is built. It is the field
        # most likely to be stale - both exchanges have moved it - so it is
        # printed on every start rather than discovered at 14:50.
        report.add(
            section,
            f"{name}: {inst.expiry.option_cadence} options on "
            f"{inst.expiry.option_weekday}, monthly future on "
            f"{inst.expiry.future_weekday}",
            inst.expiry.option_cadence in ("weekly", "monthly"),
            "",
        )
    report.add(section, f"chart index is {instruments.CHART_INDEX}",
               instruments.CHART_INDEX in instruments.INSTRUMENTS, "")
    check_warmup_data(report, strategy_logic)


def check_warmup_data(report: Report, strategy_logic) -> None:
    """Every session since the supplied data ends, for every index v3 trades.

    `strategy_logic.build_live` fills `backups/data_live/` from Shoonya and
    drops a session that fails validation rather than repairing it - and one
    index short on a date drops that date for all of them, ATR and SMA
    included. So a hole here is reported, not left for the warm-up to absorb.
    """
    section = "2. FROZEN STRATEGY"
    import pandas as pd

    live = strategy_logic.LIVE_DIR
    folders = strategy_logic.V2.BASE.DATA_DIRS
    end = strategy_logic.V2.SPLITS["hidden"][1].replace("-", "_")
    if not live.exists():
        report.add(section, "v3 live data present", None,
                   f"{live} is built from Shoonya on the first start")
        return
    days = {name: {p.stem[:10] for p in (live / folder).glob("*_5m.csv")
                   if p.stem[:10] > end and len(p.read_text().splitlines()) == 76}
            for name, folder in folders.items()}
    parquet = live / "indices_5m" / "NIFTYNXT50.parquet"
    try:
        bars = pd.read_parquet(parquet).Datetime
        counts = bars[bars > pd.Timestamp(end.replace("_", "-")) + pd.Timedelta(days=1)].dt.normalize().value_counts()
        days["NIFTYNXT50"] = {f"{day:%Y_%m_%d}" for day, n in counts.items() if n == 75}
    except Exception as exc:  # noqa: BLE001
        report.add(section, "v3 live data for NIFTYNXT50 readable", False, repr(exc))
        return
    sessions = set().union(*days.values())
    if not sessions:
        report.add(section, "v3 live data present", None, "no session after the supplied data yet")
        return
    gaps = {name: sorted(sessions - have) for name, have in days.items() if sessions - have}
    report.add(section, f"v3 warm-up data complete: {len(sessions)} session(s) after the "
                        f"supplied data, through {max(sessions).replace('_', '-')}",
               not gaps, "" if not gaps else "; ".join(f"{n} lacks {g[:3]}" for n, g in gaps.items()))


# --------------------------------------------------------------------------- #
#  3. the order path, end to end
# --------------------------------------------------------------------------- #
def check_order_path(report: Report, verbose: bool) -> None:
    section = "3. ORDER PATH"
    try:
        from tests import check_order_paths
    except Exception as exc:  # noqa: BLE001
        report.add(section, "import the order-path checks", False, repr(exc))
        return

    # The replay drives failure paths deliberately, so the managers emit a wall
    # of ERROR/CRITICAL lines that are the checks passing. Two things have to be
    # muted. The global disable covers most of it; the root handlers cover the
    # windows where `LogCapture` lifts that disable to prove a refusal was
    # announced. A bare `logging.warning` anywhere in the imported modules has
    # already given root a `BASIC_FORMAT` StreamHandler at level NOTSET, and a
    # propagated record reaches it whatever root's own level is - so the
    # handlers are swapped out rather than levelled up, and restored after.
    root_handlers = logging.root.handlers[:]
    logging.root.handlers = [logging.NullHandler()]
    logging.disable(logging.CRITICAL)
    try:
        result = check_order_paths.run_all(verbose=verbose)
    except Exception as exc:  # noqa: BLE001
        report.add(section, "order-path checks ran", False, f"{type(exc).__name__}: {exc}")
        return
    finally:
        logging.disable(logging.NOTSET)
        logging.root.handlers = root_handlers

    failures = result.failures
    report.add(section, f"{result.passed} order-path check(s) passed", not failures)
    for name, detail in failures:
        report.add(section, name, False, detail[:90])


# --------------------------------------------------------------------------- #
#  4. live environment
# --------------------------------------------------------------------------- #
def check_environment(report: Report) -> None:
    section = "4. ENVIRONMENT"

    try:
        import broker_secrets
        import shoonya_login

        state = broker_secrets.STATE_DIR
        report.add(section, f"state directory: {state}", None,
                   "exists" if state.exists() else "created on first login")
        report.add(section, "state is outside the source tree",
                   not state.resolve().is_relative_to(BASE_DIR.resolve()))
        report.add(section, "one definition of the state directory",
                   shoonya_login.STATE_DIR is broker_secrets.STATE_DIR)
        if os.name != "nt" and state.exists():
            report.add(section, "state directory is private", state.stat().st_mode & 0o077 == 0)
            cache = shoonya_login.SESSION_CACHE_FILE
            if cache.exists():
                report.add(section, "broker session cache is private", cache.stat().st_mode & 0o077 == 0)

        # The condition this whole layout exists to prevent: runtime state, and
        # a plaintext broker token in particular, back in the source directory.
        strays = sorted(
            p.name for p in BASE_DIR.glob(".shoonya_*")
        ) + sorted(p.name for p in BASE_DIR.glob("access_token.txt"))
        report.add(section, "no runtime state in the source directory",
                   not strays,
                   "" if not strays else f"found {strays}")
    except Exception as exc:  # noqa: BLE001
        report.add(section, "state directory layout", False, repr(exc))

    backups = BASE_DIR / "backups"
    try:
        backups.mkdir(parents=True, exist_ok=True)
        probe = backups / ".writecheck"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        report.add(section, "backups/ writable", True)
    except Exception as exc:  # noqa: BLE001
        report.add(section, "backups/ writable", False, repr(exc))

    try:
        from database_backup import tables_to_truncate
        flat = tables_to_truncate(open_trade=False, unresolved_orders=False)
        held = tables_to_truncate(open_trade=True, unresolved_orders=False)
        report.add(section, "flat shutdown clears trade tables",
                   "trade_logs" in flat)
        report.add(section, "open hold retains trade tables",
                   "trade_logs" not in held)
    except Exception as exc:  # noqa: BLE001
        report.add(section, "truncation policy", False, repr(exc))

    try:
        # The connector reads the private .env file when no environment override is set.
        from db_connector import TimescaleDBConnector
    except Exception as exc:  # noqa: BLE001
        report.add(section, "database imports", False, repr(exc))
        return

    connector = None
    try:
        cfg = TimescaleDBConnector(auto_provision=False).cfg.copy()
        cfg["options"] += " -c default_transaction_read_only=on"
        connector = pg_connect(**cfg, cursor_factory=RealDictCursor)

        def read(query, params=None):
            with connector.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

        settings = read(
            "SELECT current_setting('transaction_read_only') AS readonly, "
            "current_setting('TimeZone') AS timezone;"
        )[0]
        report.add(section, "database reachable", True)
        report.add(section, "check connection is read only", settings["readonly"] == "on")
        report.add(section, "database session uses IST", settings["timezone"] == "Asia/Kolkata")
        role = read(
            "SELECT rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=current_user;"
        )[0]
        report.add(
            section, "database role has limited privileges",
            not any(role[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole")),
        )
    except Exception as exc:  # noqa: BLE001
        report.add(section, "database reachable", False, str(exc).strip()[:90])
        if connector is not None:
            connector.close()
        return

    try:
        columns = read(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='botdata' AND table_name='trade_logs';"""
        ) or []
        names = {row["column_name"] for row in columns}
        report.add(section, "botdata.trade_logs exists", bool(names))
        report.add(section, "trade_logs.book column present", "book" in names,
                   "" if "book" in names else "run ensure_schema()")
        report.add(section, "trade_logs.exit_time column present", "exit_time" in names,
                   "" if "exit_time" in names else "run ensure_schema()")

        open_rows = read(
            "SELECT count(*) AS n FROM botdata.trade_logs WHERE is_in_trade = TRUE;"
        )[0]["n"]
        # Not a failure: an open carry overnight is the normal state. It is
        # reported because it changes what a restart is expected to do.
        report.add(section, f"open positions in trade_logs: {open_rows}", None,
                   "a hold is waiting to be sold at 09:15" if open_rows else "flat")
    except Exception as exc:  # noqa: BLE001
        report.add(section, "schema inspection", False, repr(exc))
    finally:
        try:
            connector.close()
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quick", action="store_true",
                        help="skip the database checks; no database needed")
    parser.add_argument("--no-order-path", action="store_true",
                        help="skip the end-to-end order-path replay (the slow part)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="stream individual test names")
    args = parser.parse_args()

    report = Report()
    run_unit_tests(report, args.verbose)
    check_strategy(report)
    if args.no_order_path:
        report.add("3. ORDER PATH", "skipped (--no-order-path)", None)
    else:
        check_order_path(report, args.verbose)
    if not args.quick:
        check_environment(report)
    else:
        report.add("4. ENVIRONMENT", "skipped (--quick)", None)

    report.show()

    failed = report.failed
    print("\n" + "=" * 66)
    if failed:
        print(f"  {failed} CHECK(S) FAILED - do not restart the bot until resolved")
    else:
        print("  ALL CHECKS PASSED - safe to start")
    print("=" * 66)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
