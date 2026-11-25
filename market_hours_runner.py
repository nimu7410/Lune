# market_hours_runner.py
"""
Market-hours supervisor with truly graceful shutdown.

Windows:
  • Child is spawned with CREATE_NEW_PROCESS_GROUP | CREATE_NEW_CONSOLE
  • On stop/EOD it attaches to the child's console and sends a real CTRL+C
    (GenerateConsoleCtrlEvent(CTRL_C_EVENT, child_pid)) so Python runs your
    SIGINT handler and atexit backup.

POSIX:
  • Child is spawned in a new session and we send SIGTERM to the whole group.
"""

from __future__ import annotations

import os
import sys
import time
import signal
import subprocess
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path

import psycopg2

import ist
from config import TRADE_SETTINGS, parse_holiday
from db_connector import DB_PASSWORD

# ────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────
TZ = ist.IST
# The child is started *before* the opening bell, not at it. Every v3 overnight
# position is closed at the next session's 09:15 open, and that exit is the
# most time-critical order the bot places: the edge is the overnight gap itself
# (strategy/v3/strategy_v3.py), not a drift that can be leaned on. Starting at 09:15
# meant the broker login, the websocket handshake and the strategy warm-up all
# happened *after* the moment the position was supposed to be sold.
#
# The lead time costs nothing else: the strategy reads only the 09:15..15:25
# session it is defined over, and v3's earliest entry is unchanged at 11:20. It
# also gives a flaky broker login several minutes of retries before they start
# costing money.
START = dt_time(9, 0)       # 9:00 IST - 15 minutes of headroom before the open
END   = dt_time(15, 35)     # allow the 15:25 bar to settle before export
WEEKDAYS = {0, 1, 2, 3, 4}  # Mon..Fri


def _parse_holidays(values) -> set[date]:
    """Holiday entries as dates, skipping unparseable ones.

    This supervisor's whole job is to stay alive and spawn the child, so its
    import surface is kept to `ist`/`config`/`psycopg2`, and the parser it
    shares with `getdata` lives in `config`.

    A malformed entry is skipped rather than fatal: a typo in the calendar must
    not stop the bot from trading on the ~250 days that *are* sessions. The
    startup banner prints the parsed count so a silently dropped date is
    visible.
    """
    out: set[date] = set()
    for v in values:
        text = str(v).strip()
        if not text:
            continue
        day = parse_holiday(v)
        if day is None:
            print(f"[runner] ⚠ unparseable holiday {text!r} ignored", flush=True)
        else:
            out.add(day)
    return out


# NSE trading holidays. Sourced from `TRADE_SETTINGS.expiry_holidays` so the
# calendar lives in exactly one place: `getdata` already uses that same tuple to
# shift option/future expiry off a closed day, and a second list here would
# eventually disagree with it. On a holiday the market is shut, so both meanings
# — "no session" and "expiry shifts" — are the same set of dates.
HOLIDAYS = _parse_holidays(TRADE_SETTINGS.expiry_holidays)

MAIN_FILE = Path(__file__).with_name("main.py")

CHECK_EVERY_SEC       = 3        # scheduler tick while markets open
GRACE_WAIT_SEC        = 180      # allow time for DB backup to finish
RESTART_BACKOFF_SEC   = 5        # initial delay when child crashes during session
RESTART_BACKOFF_MAX_SEC = 300    # avoid hot loops on permanent startup failures
STABLE_CHILD_SEC      = 300      # reset backoff after a genuinely stable run
SLEEP_CHUNK_SEC       = 1        # long waits are sliced into small chunks

IS_WIN = os.name == "nt"

if IS_WIN:
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_NEW_CONSOLE       = 0x00000010

# ────────────────────────────────────────────────────────────────────────────
# Time helpers
# ────────────────────────────────────────────────────────────────────────────
now_ist = ist.now

def today_bounds() -> tuple[datetime, datetime]:
    today = now_ist().date()
    return ist.combine(today, START), ist.combine(today, END)

def is_trading_day(dt: datetime) -> bool:
    return dt.weekday() in WEEKDAYS and dt.date() not in HOLIDAYS

def within_session(dt: datetime) -> bool:
    if not is_trading_day(dt):
        return False
    s, e = today_bounds()
    return s <= dt < e

def seconds_until(dt: datetime) -> float:
    return max(0.0, (dt - now_ist()).total_seconds())

def next_session_start_after(dt: datetime) -> datetime:
    # Bounds come from `dt`'s own date, not `today_bounds()`. Production only
    # ever calls this with `now_ist()`, where the two are identical - but with
    # `today_bounds()` any `dt` in the past satisfied `dt < s` and the function
    # returned *today's* 09:00 instead of the session after `dt`, which made it
    # untestable against a fixed calendar.
    s = ist.combine(dt.date(), START)
    if is_trading_day(dt) and dt < s:
        return s
    d = dt + timedelta(days=1)
    # Bounded rather than `while True`: a corrupted calendar that somehow marked
    # every day a holiday would otherwise spin here forever, pegging a core and
    # never reaching the scheduler loop. 30 days clears the longest real run of
    # closures by a wide margin; past that, fall through to the plain weekday
    # rule so the bot still trades instead of hanging.
    for _ in range(30):
        if is_trading_day(d):
            return ist.combine(d.date(), START)
        d += timedelta(days=1)
    print(
        f"[runner] ⚠ no trading day within 30 days of {dt:%d %b %Y}; "
        "ignoring holidays for this hop",
        flush=True,
    )
    d = dt + timedelta(days=1)
    while d.weekday() not in WEEKDAYS:
        d += timedelta(days=1)
    return ist.combine(d.date(), START)

def _sleep_sliced(total_sec: float) -> None:
    end = time.time() + total_sec
    while True:
        remain = end - time.time()
        if remain <= 0:
            return
        time.sleep(min(SLEEP_CHUNK_SEC, remain))

# ────────────────────────────────────────────────────────────────────────────
# PostgreSQL readiness gate
#
# Priority #1: nothing downstream (the trading child, the broker login it
# performs on startup) may run until Postgres is confirmed reachable. This is
# a *functional* check — a real connection + query — not a Windows service
# status check. "postgresql-x64-*" reporting RUNNING only means the service
# process has launched; it does not mean recovery/replay has finished or that
# the server is accepting connections yet. Trusting SCM status here would be
# exactly the kind of loophole that lets the child spawn (and burn a broker
# login attempt) against a database that isn't actually up.
#
# Same env vars / defaults as db_connector.TimescaleDBConnector so the two
# never disagree about where the database lives.
# ────────────────────────────────────────────────────────────────────────────
PG_HOST             = os.getenv("TSDB_HOST", "127.0.0.1")
PG_PORT             = int(os.getenv("TSDB_PORT", 5432))
PG_USER             = os.getenv("TSDB_USER", "lune_bot")
# Imported rather than repeated: the runner and child read the same private
# credential, including when the service environment has no TSDB_PASSWORD.
PG_PASSWORD         = os.getenv("TSDB_PASSWORD") or DB_PASSWORD
PG_MAINTENANCE_DB   = os.getenv("TSDB_MAINTENANCE_DB", "postgres")
PG_CONNECT_TIMEOUT  = int(os.getenv("TSDB_CONNECT_TIMEOUT", 5))

PG_WAIT_INITIAL_SEC   = 2     # first retry delay
PG_WAIT_MAX_SEC       = 30    # backoff cap — never wait longer than this between tries
PG_WAIT_LOG_EVERY_SEC = 30    # don't spam the log while waiting

def _postgres_ready() -> tuple[bool, str]:
    """One real connection attempt against the maintenance DB. (ok, detail)."""
    if not PG_PASSWORD:
        return False, "no database password (set TSDB_PASSWORD in .env or environment)"
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_MAINTENANCE_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=PG_CONNECT_TIMEOUT,
        )
    except Exception as exc:
        return False, str(exc)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
    finally:
        conn.close()

def wait_for_postgres() -> None:
    """Block — indefinitely, if need be — until PostgreSQL is actually
    accepting queries. Called at service startup and again immediately before
    every spawn of the trading child, so a DB that isn't up yet at boot, or
    that drops mid-session and hasn't recovered by the time we'd restart the
    child, both block the same way instead of racing ahead.
    """
    delay = PG_WAIT_INITIAL_SEC
    last_log = 0.0
    attempt = 0
    while True:
        attempt += 1
        ok, detail = _postgres_ready()
        if ok:
            print(
                f"[runner] ✅ PostgreSQL is up ({PG_HOST}:{PG_PORT}/{PG_MAINTENANCE_DB}) "
                f"after {attempt} attempt(s).",
                flush=True,
            )
            return
        now = time.time()
        if attempt == 1 or now - last_log >= PG_WAIT_LOG_EVERY_SEC:
            print(
                f"[runner] ⏳ waiting for PostgreSQL at {PG_HOST}:{PG_PORT} "
                f"(attempt {attempt}): {detail}",
                flush=True,
            )
            last_log = now
        _sleep_sliced(delay)
        delay = min(delay * 2, PG_WAIT_MAX_SEC)

# ────────────────────────────────────────────────────────────────────────────
# Windows: real Ctrl+C (SIGINT) to the child's process group
# ────────────────────────────────────────────────────────────────────────────
if IS_WIN:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32

    # Valid, safe signatures (no non-existent wintypes symbols)
    kernel32.AttachConsole.argtypes = [wintypes.DWORD]
    kernel32.AttachConsole.restype  = wintypes.BOOL

    kernel32.FreeConsole.argtypes = []
    kernel32.FreeConsole.restype  = wintypes.BOOL

    # Do NOT set argtypes for SetConsoleCtrlHandler; passing None is common/OK.
    # kernel32.SetConsoleCtrlHandler.restype may be set, but it's optional:
    kernel32.SetConsoleCtrlHandler.restype = wintypes.BOOL

    kernel32.GenerateConsoleCtrlEvent.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.GenerateConsoleCtrlEvent.restype  = wintypes.BOOL

    def _send_real_ctrl_c(child_pid: int) -> bool:
        """
        Attach to the child's console and send a real Ctrl+C to its process group.
        Returns True if we believe it was delivered.
        """
        delivered = False
        try:
            kernel32.FreeConsole()
        except Exception:
            pass

        try:
            if kernel32.AttachConsole(child_pid):
                try:
                    # Make *this* process ignore Ctrl+C so we don't exit ourselves.
                    kernel32.SetConsoleCtrlHandler(None, True)
                    ok = kernel32.GenerateConsoleCtrlEvent(signal.CTRL_C_EVENT, child_pid)
                    delivered = bool(ok)
                finally:
                    # Give the child a moment to run its signal handler
                    time.sleep(0.5)
                    kernel32.FreeConsole()
                    kernel32.SetConsoleCtrlHandler(None, False)
        except Exception:
            delivered = False

        return delivered

# ────────────────────────────────────────────────────────────────────────────
# Child process controls
# ────────────────────────────────────────────────────────────────────────────
_child: subprocess.Popen | None = None

def start_child() -> None:
    global _child
    if _child and _child.poll() is None:
        return

    py = sys.executable
    if not MAIN_FILE.exists():
        print(f"[runner] ERROR: {MAIN_FILE} not found.", flush=True)
        sys.exit(1)

    kwargs: dict = {}
    if IS_WIN:
        kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP | CREATE_NEW_CONSOLE
        stdout = None
        stderr = None
    else:
        kwargs["preexec_fn"] = os.setsid  # type: ignore[attr-defined]
        stdout = sys.stdout
        stderr = sys.stderr

    print(f"[runner] ▶ starting {MAIN_FILE.name}", flush=True)
    _child = subprocess.Popen(
        [py, "-u", str(MAIN_FILE)],
        stdout=stdout,
        stderr=stderr,
        **kwargs,
    )

def _grace_signal() -> None:
    if not _child or _child.poll() is not None:
        return
    try:
        if IS_WIN:
            sent = _send_real_ctrl_c(_child.pid)
            if not sent:
                # Fallback: CTRL_BREAK (group) → then terminate as last resort
                try:
                    os.kill(_child.pid, signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                except Exception:
                    _child.terminate()
        else:
            os.killpg(_child.pid, signal.SIGTERM)
    except Exception as exc:
        print(f"[runner] warn: graceful signal failed: {exc}", flush=True)

def _force_kill() -> None:
    if not _child or _child.poll() is not None:
        return
    try:
        if IS_WIN:
            _child.kill()
        else:
            os.killpg(_child.pid, signal.SIGKILL)
    except Exception as exc:
        print(f"[runner] warn: force kill failed: {exc}", flush=True)

def stop_child_gracefully() -> None:
    global _child
    if not _child:
        return
    if _child.poll() is not None:
        _child = None
        return

    print("[runner] ⏹️  stopping child gracefully…", flush=True)
    _grace_signal()

    deadline = time.time() + GRACE_WAIT_SEC
    while time.time() < deadline:
        if _child.poll() is not None:
            _child = None
            print("[runner] ✅ child exited cleanly", flush=True)
            return
        time.sleep(0.5)

    print("[runner] ⚠️  child didn’t exit in time; forcing kill", flush=True)
    _force_kill()
    _child = None

def on_service_signal(signum, _frame):
    print(f"[runner] got signal {signum}; shutting down…", flush=True)
    stop_child_gracefully()
    sys.exit(0)

# ────────────────────────────────────────────────────────────────────────────
# Main scheduler loop
# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    signal.signal(signal.SIGINT, on_service_signal)
    try:
        signal.signal(signal.SIGTERM, on_service_signal)
    except Exception:
        pass

    print("[runner] market-hours scheduler started (IST).", flush=True)

    upcoming = sorted(h for h in HOLIDAYS if h >= now_ist().date())
    nxt_hol = f"next {upcoming[0]:%a %d %b %Y}" if upcoming else "none upcoming"
    print(
        f"[runner] 📅 {len(HOLIDAYS)} trading holidays loaded ({nxt_hol}).",
        flush=True,
    )
    if now_ist().year not in {day.year for day in HOLIDAYS}:
        print("[runner] ERROR: no holiday calendar for this year; new entries are blocked.", flush=True)

    # Priority #1: confirm the database before anything else — before the
    # market-hours loop even begins evaluating whether to start the child.
    wait_for_postgres()

    restart_delay = RESTART_BACKOFF_SEC
    child_started_at = 0.0
    while True:
        now = now_ist()

        if within_session(now):
            if not _child or _child.poll() is not None:
                wait_for_postgres()
                start_child()
                child_started_at = time.monotonic()

            _, end_dt = today_bounds()
            while now_ist() < end_dt:
                if _child and _child.poll() is not None:
                    uptime = time.monotonic() - child_started_at
                    if uptime >= STABLE_CHILD_SEC:
                        restart_delay = RESTART_BACKOFF_SEC
                    print(
                        f"[runner] child exited unexpectedly after {uptime:.1f}s; "
                        f"restarting in {restart_delay}s...",
                        flush=True,
                    )
                    _sleep_sliced(restart_delay)
                    restart_delay = min(restart_delay * 2, RESTART_BACKOFF_MAX_SEC)
                    wait_for_postgres()
                    start_child()
                    child_started_at = time.monotonic()
                _sleep_sliced(CHECK_EVERY_SEC)

            stop_child_gracefully()

            nxt = next_session_start_after(now_ist())
            wait = seconds_until(nxt)
            print(
                f"[runner] 💤 sleeping overnight until {nxt.strftime('%a %d %b %Y %H:%M')} IST "
                f"({int(wait)}s)",
                flush=True,
            )
            _sleep_sliced(wait)
            continue

        if is_trading_day(now):
            start_dt, _ = today_bounds()
            if now < start_dt:
                wait = seconds_until(start_dt)
                print(
                    f"[runner] ⏳ waiting {int(wait)}s until market open {start_dt.strftime('%H:%M')} IST",
                    flush=True,
                )
                _sleep_sliced(wait)
                continue

        nxt = next_session_start_after(now)
        wait = seconds_until(nxt)
        print(
            f"[runner] 💤 sleeping until next session {nxt.strftime('%a %d %b %Y %H:%M')} IST "
            f"({int(wait)}s)",
            flush=True,
        )
        _sleep_sliced(wait)

if __name__ == "__main__":
    main()
