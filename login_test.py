"""
Shoonya login diagnostic.

Thin consumer of :mod:`shoonya_login` — it exercises exactly the code the live
bot uses, so a pass here means the bot will log in.  (The previous version kept
its own private copy of the login flow, which drifted from production and
therefore proved nothing.)

Usage
-----
    python login_test.py                  # use the cache if it is valid (fast path)
    python login_test.py --force          # force a real browser login
    python login_test.py --status         # report cache state only, no network
    python login_test.py --check-secrets  # resolve credentials only, no broker login

Under systemd the credentials exist only inside a service, so run it with them
loaded:  ./secret.sh run login_test.py --check-secrets
    python login_test.py --logout         # delete the cached session

Nothing here ever prints a token.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

import broker_secrets
import ist
from shoonya_login import (
    COOLDOWN_FILE,
    LEGACY_TOKEN_FILE,
    SESSION_CACHE_FILE,
    SESSION_LOCK_FILE,
    PermanentLoginError,
    ProbeResult,
    ShoonyaLoginError,
    get_session_manager,
    load_credentials,
    probe_token_resilient,
)


def _mask(value: str, keep: int = 3) -> str:
    if not value:
        return "<empty>"
    return value[:keep] + "*" * max(0, len(value) - keep)


def _check_secrets() -> int:
    """Actually resolve credentials, without logging in to the broker."""
    print("Credential source")
    print(f"  mode          : {broker_secrets.source_mode()}")
    print(f"  file          : {broker_secrets.CREDENTIAL_FILE}")

    started = time.time()
    try:
        resolved = broker_secrets.resolve_secrets()
    except broker_secrets.SecretsError as exc:
        print(f"\nSECRETS_FAILED (permanent — fix the configuration): {exc}", file=sys.stderr)
        return 2

    print(f"  resolved from : {resolved.description} in {time.time() - started:.2f}s")
    print(f"  keys          : {', '.join(sorted(resolved.values))}")
    print(f"  digest        : {resolved.digest}")

    # Only now build the credential object: this is what the bot will use.
    try:
        creds = load_credentials()
    except ShoonyaLoginError as exc:
        print(f"\nSECRETS_FAILED: resolved, but not a usable credential set: {exc}", file=sys.stderr)
        return 2

    print("\nCredentials")
    print(f"  user id       : {_mask(creds.userid)}")
    print(f"  client id     : {_mask(creds.client_id)}")
    print(f"  oauth secret  : loaded ({len(creds.oauth_secret)} chars)")
    print(f"  totp secret   : loaded ({len(creds.totp_secret)} chars)")
    print(f"  fingerprint   : {creds.fingerprint}")
    print("\nSECRETS_OK")
    return 0


def _report_status() -> int:
    """Offline only — reports configuration and cache state, never resolves."""
    print("Credential source (configured; use --check-secrets to actually resolve)")
    print(f"  mode          : {broker_secrets.source_mode()}")
    file_state = "present" if broker_secrets.CREDENTIAL_FILE.exists() else "MISSING"
    print(f"  file          : {broker_secrets.CREDENTIAL_FILE} ({file_state})")

    print("\nSession cache")
    print(f"  cache file    : {SESSION_CACHE_FILE}")
    if not SESSION_CACHE_FILE.exists():
        print("  state         : absent (next login will open a browser)")
        return 0

    try:
        data = json.loads(SESSION_CACHE_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"  state         : unreadable ({exc}) — will be re-created")
        return 0

    # `issued_at_utc` is what builds before the IST switchover wrote; a cache
    # file left by one of those still reads correctly here.
    issued = data.get("issued_at_ist") or data.get("issued_at_utc") or "unknown"
    age = ""
    if data.get("issued_at_epoch"):
        age = f" ({(time.time() - float(data['issued_at_epoch'])) / 3600:.1f}h old)"
    print(f"  uid           : {_mask(str(data.get('uid') or ''))}")
    print(f"  generation    : {data.get('generation')}")
    print(f"  issued at     : {issued}{age}")
    print(f"  token         : present ({len(data.get('access_token') or '')} chars, not shown)")

    if COOLDOWN_FILE.exists():
        try:
            cooldown = json.loads(COOLDOWN_FILE.read_text(encoding="utf-8"))
            remaining = float(cooldown.get("until_epoch", 0)) - time.time()
            if remaining > 0:
                kind = "permanent" if cooldown.get("permanent") else "transient"
                print(f"\n  COOLDOWN active ({kind}), {int(remaining)}s left: {cooldown.get('reason')}")
        except (OSError, ValueError):
            pass
    return 0


def _logout() -> int:
    removed = []
    for path in (SESSION_CACHE_FILE, LEGACY_TOKEN_FILE, COOLDOWN_FILE, SESSION_LOCK_FILE):
        try:
            path.unlink()
            removed.append(path.name)
        except OSError:
            pass
    print("Removed:", ", ".join(removed) if removed else "nothing")
    return 0


def _run_login(force: bool) -> int:
    started = time.time()
    try:
        # Inside the handler: building the manager resolves credentials, so it
        # can raise exactly the errors reported below.
        manager = get_session_manager()
        token = manager.get_access_token(force_refresh=force)
    except PermanentLoginError as exc:
        print(f"LOGIN_FAILED (permanent — fix the credentials, do not retry): {exc}", file=sys.stderr)
        return 2
    except ShoonyaLoginError as exc:
        print(f"LOGIN_FAILED (transient — safe to retry later): {exc}", file=sys.stderr)
        return 1

    elapsed = time.time() - started
    print(f"Session obtained in {elapsed:.2f}s (generation {manager.generation}).")
    print(f"  path          : {'browser login' if elapsed > 3 else 'cached token (fast path)'}")
    print(f"  token         : {len(token)} chars (not shown)")

    probe_started = time.time()
    result = probe_token_resilient(manager.userid, token)
    print(f"  validation    : {result.value} in {time.time() - probe_started:.2f}s")

    if result is not ProbeResult.VALID:
        print("LOGIN_FAILED: token was issued but the broker will not accept it.", file=sys.stderr)
        return 1

    print(f"  checked at    : {ist.isoformat()}")
    print("LOGIN_SUCCEEDED")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Shoonya login diagnostic")
    parser.add_argument("--force", action="store_true", help="force a fresh browser login")
    parser.add_argument("--status", action="store_true", help="show cache state only, no network")
    parser.add_argument(
        "--check-secrets",
        action="store_true",
        help="resolve credentials and report the source, no broker login",
    )
    parser.add_argument("--logout", action="store_true", help="delete the cached session")
    parser.add_argument("--verbose", action="store_true", help="show INFO-level login logs")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.logout:
        return _logout()
    if args.status:
        return _report_status()
    if args.check_secrets:
        return _check_secrets()
    return _run_login(args.force)


if __name__ == "__main__":
    sys.exit(main())
