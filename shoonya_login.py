"""
Shoonya session manager — single owner of authentication for the whole bot.

Design goals (in priority order):

1. **Fast.**  A valid cached token is reused after exactly one cheap probe, so a
   restart during market hours costs ~0.3s instead of a ~15s browser login.
2. **Safe.**  A new session may invalidate the previous one broker-side, which
   would silently kill a running live bot.  Minting is therefore serialised
   across processes, guarded by double-checked validation, and only permitted
   for a process that declares itself the session *owner*.
3. **Fail closed.**  A broker outage (timeout / 5xx / garbage) must never be
   mistaken for "token expired".  Only an explicit auth rejection authorises a
   fresh login.
4. **No lockout.**  Bad credentials are never retried; only an explicitly
   expired TOTP earns a second submission, in the next 30s window.

Public entry point is :func:`get_session_manager`.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

import pyotp
import requests
from dotenv import load_dotenv

import broker_secrets
import ist

#: Still loaded: the non-secret tunables below (and the ``env`` credential
#: source in :mod:`broker_secrets`) read from the environment.  Credentials
#: themselves come from an encrypted systemd credential — see
#: :func:`load_credentials`.
load_dotenv()

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Paths & tunables
# ─────────────────────────────────────────────────────────────────────────────

#: Imported, not recomputed.  Both modules used to derive this from the same
#: expression, which is two chances to disagree about where the session cache
#: lives - and a disagreement there means a second browser login rather than a
#: cache hit.  See :func:`broker_secrets._default_state_dir` for the location and
#: the ``LUNE_STATE_DIR`` override.
STATE_DIR = broker_secrets.STATE_DIR

SESSION_CACHE_FILE = STATE_DIR / ".shoonya_session.json"
SESSION_LOCK_FILE = STATE_DIR / ".shoonya_session.lock"
COOLDOWN_FILE = STATE_DIR / ".shoonya_login_cooldown.json"
#: Retired.  A bare access token in plaintext, written only so a build that
#: predates the JSON session cache could still read one.  It is no longer
#: written; :func:`_adopt_legacy_token` consumes any file left over from that
#: era and deletes it, so this constant disappears once the fleet has rolled.
LEGACY_TOKEN_FILE = STATE_DIR / "access_token.txt"
#: Deliberately *not* under STATE_DIR: a login that failed is diagnosed from
#: the log directory, next to the bot log covering the same minutes.
DIAGNOSTICS_DIR = Path(__file__).resolve().parent / "logs"

CACHE_SCHEMA_VERSION = 2

AUTHORIZE_URL = "https://trade.shoonya.com/OAuthlogin/authorize/oauth"
API_BASE_URL = "https://api.shoonya.com/NorenWClientAPI"
#: One keep-alive pool for every REST call. A bare ``requests.post`` opens a
#: fresh TCP+TLS connection each time, which costs ~40 ms per call on top of a
#: ~3 ms round trip. urllib3 pools are thread-safe.
HTTP = requests.Session()

#: Skip the network probe if the token was proven good this recently.
REVALIDATE_AFTER_SEC = float(os.getenv("SHOONYA_REVALIDATE_AFTER_SEC", "120"))
#: Wait budget for another process that is currently minting a token.
LOCK_WAIT_TIMEOUT_SEC = float(os.getenv("SHOONYA_LOCK_WAIT_SEC", "150"))
#: Cooldown after transient failures exhaust, so N processes do not stampede.
TRANSIENT_COOLDOWN_SEC = 60.0
#: Cooldown after a permanent failure (bad password etc.).  Voided
#: automatically when the credentials themselves change.
PERMANENT_COOLDOWN_SEC = 900.0

PROBE_TIMEOUT_SEC = 10.0
EXCHANGE_TIMEOUT_SEC = 30.0
BROWSER_NAV_TIMEOUT_MS = 45_000
BROWSER_TOTAL_TIMEOUT_SEC = 120.0
#: Guard against filling an OTP that rolls over mid-submit.
MIN_OTP_SECONDS_LEFT = 5


# ─────────────────────────────────────────────────────────────────────────────
#  Errors
# ─────────────────────────────────────────────────────────────────────────────


class ShoonyaLoginError(RuntimeError):
    """Base class for every authentication failure raised by this module."""


class PermanentLoginError(ShoonyaLoginError):
    """Credentials/config are wrong.  Retrying would risk an account lock."""


class TransientLoginError(ShoonyaLoginError):
    """Network, page or broker hiccup.  Retrying later is reasonable."""


class SessionUnavailableError(ShoonyaLoginError):
    """No usable cached session and this process is not allowed to mint one."""


class OrderNotReplayedError(ShoonyaLoginError):
    """
    A *mutating* call (place/cancel/modify) failed against an expired session.

    The session has been refreshed but the request was deliberately **not**
    replayed: the caller must reconcile order book / positions before deciding
    whether to submit again.
    """


# Substrings that mean "this session is dead" rather than "this request was bad".
_AUTH_EXPIRY_MARKERS = (
    "session expired",
    "invalid session",
    "session key",
    "invalid token",
    "token expired",
    "unauthorized",
    "unauthorised",
    "not authorized",
    "please login",
    "login again",
    "invalid access token",
)

# Substrings that mean "stop, a human must fix this".
_PERMANENT_MARKERS = (
    "invalid user",
    "invalid password",
    "wrong password",
    "incorrect password",
    "invalid credential",
    "user blocked",
    "account blocked",
    "account locked",
    "user is blocked",
    "blocked user",
    "not registered",
    "invalid client",
    "invalid checksum",
    "client not found",
    "unauthorized client",
    "access denied",
    "consent denied",
)

# Substrings that specifically mean "the OTP was stale" — the one case where a
# second credential submission is justified.
_OTP_MARKERS = ("otp", "one time password", "2fa", "totp", "authenticator")


def _matches(text: str, markers: tuple[str, ...]) -> bool:
    lowered = (text or "").lower()
    return any(marker in lowered for marker in markers)


def is_auth_expiry_text(text: str) -> bool:
    """True when broker text explicitly denotes a dead session."""
    return _matches(text, _AUTH_EXPIRY_MARKERS)


# ─────────────────────────────────────────────────────────────────────────────
#  Probe result
# ─────────────────────────────────────────────────────────────────────────────


class ProbeResult(Enum):
    VALID = "valid"
    #: Broker explicitly rejected the token — the only state that permits mint.
    INVALID_AUTH = "invalid_auth"
    #: Timeout / 5xx / malformed.  Says nothing about the token.
    UNKNOWN = "unknown"


# ─────────────────────────────────────────────────────────────────────────────
#  Credentials
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ShoonyaCredentials:
    #: Stays in ``repr``: it is the account identifier, not a secret — it is
    #: already written in plaintext to the session cache and sent in every API
    #: payload, and it is what makes a traceback identify the right account.
    userid: str
    #: ``repr=False`` on every actual secret: a bare ``repr(creds)`` in a log
    #: line, a traceback frame or a debugger would otherwise print the
    #: password, the TOTP seed and the OAuth secret in plaintext.
    password: str = field(repr=False)
    totp_secret: str = field(repr=False)
    client_id: str = field(repr=False)
    oauth_secret: str = field(repr=False)
    #: Human-readable provenance ("encrypted credential (...)", "environment /
    #: .env").  Diagnostics only — deliberately excluded from
    #: :attr:`fingerprint`, so moving a credential between sources does not
    #: look like a credential *change* and needlessly void a live cooldown.
    source: str = "unknown"

    @property
    def fingerprint(self) -> str:
        """
        Changes whenever any credential changes — voids stale cooldowns.

        Covers exactly the five values a login actually submits, ``totp_secret``
        included: rotating only the TOTP seed used to leave the fingerprint
        unchanged, so a permanent cooldown written against the *old* seed went
        on blocking logins that the new seed would have satisfied.  Unused keys
        (``SHOONYA_API_SECRET``, ``SHOONYA_IMEI``) stay out, so churn on them
        cannot unlock a bad-password cooldown.
        """
        raw = "|".join(
            (self.userid, self.password, self.totp_secret, self.client_id, self.oauth_secret)
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @property
    def client_id_hash(self) -> str:
        return hashlib.sha256(self.client_id.encode("utf-8")).hexdigest()[:16]


def load_credentials() -> ShoonyaCredentials:
    """
    Resolve credentials, with actionable error messages.

    Values come from the encrypted systemd credential that ``lune.service``
    decrypts at start; :mod:`broker_secrets` owns reading it and reports which
    source actually answered.  Only the error *taxonomy* is translated here: a
    missing or malformed credential is a permanent failure a human must fix.
    """
    try:
        resolved = broker_secrets.resolve_secrets()
    except broker_secrets.SecretsPermanentError as exc:
        raise PermanentLoginError(str(exc)) from exc
    except broker_secrets.SecretsError as exc:  # defensive: unclassified
        raise TransientLoginError(str(exc)) from exc

    values = resolved.values

    def _need(name: str, *, fallback_name: str | None = None) -> str:
        value = values.get(name)
        if not value and fallback_name:
            value = values.get(fallback_name)
        if not value:
            hint = f" (or {fallback_name})" if fallback_name else ""
            raise PermanentLoginError(
                f"Missing {name}{hint} in {resolved.description} — Shoonya login "
                f"cannot start. Keys present: {', '.join(sorted(values)) or 'none'}."
            )
        return value.strip()

    oauth_secret = values.get("SHOONYA_OAUTH_SECRET")
    if not oauth_secret:
        raise PermanentLoginError(
            f"Missing SHOONYA_OAUTH_SECRET in {resolved.description}. Add it with "
            "./secret.sh set SHOONYA_OAUTH_SECRET."
        )

    totp_secret = _need("SHOONYA_TOKEN").replace(" ", "")
    try:
        # Fail here with a clear message rather than deep inside the browser run.
        base64.b32decode(totp_secret, casefold=True)
    except Exception as exc:  # noqa: BLE001 - surfaced as config error
        raise PermanentLoginError(
            f"SHOONYA_TOKEN is not a valid base32 TOTP secret: {exc}"
        ) from exc

    creds = ShoonyaCredentials(
        userid=_need("SHOONYA_USERID"),
        password=_need("SHOONYA_PASSWORD"),
        totp_secret=totp_secret,
        client_id=_need("SHOONYA_OAUTH_CLIENT_ID", fallback_name="SHOONYA_VENDOR_CODE"),
        oauth_secret=oauth_secret.strip(),
        source=resolved.description,
    )
    log.info(
        "Shoonya credentials resolved from %s (fingerprint %s).",
        resolved.description,
        creds.fingerprint,
    )
    return creds


# ─────────────────────────────────────────────────────────────────────────────
#  Cached session record
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class CachedSession:
    access_token: str
    uid: str
    client_id_hash: str
    issued_at: float
    validated_at: float = 0.0
    generation: int = 1
    expires_at: Optional[str] = None

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": CACHE_SCHEMA_VERSION,
            "uid": self.uid,
            "client_id_hash": self.client_id_hash,
            "access_token": self.access_token,
            "issued_at_ist": ist.isoformat(ist.from_epoch(self.issued_at)),
            "issued_at_epoch": self.issued_at,
            "validated_at_epoch": self.validated_at,
            "generation": self.generation,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Optional["CachedSession"]:
        if not isinstance(data, dict):
            return None
        if data.get("schema_version") != CACHE_SCHEMA_VERSION:
            return None
        token = (data.get("access_token") or "").strip()
        uid = (data.get("uid") or "").strip()
        if not token or not uid:
            return None
        return cls(
            access_token=token,
            uid=uid,
            client_id_hash=(data.get("client_id_hash") or "").strip(),
            issued_at=float(data.get("issued_at_epoch") or 0.0),
            validated_at=float(data.get("validated_at_epoch") or 0.0),
            generation=int(data.get("generation") or 1),
            expires_at=data.get("expires_at"),
        )


def _ensure_state_dir() -> None:
    """Create the state directory, locked down, before writing into it.

    Delegates to :mod:`broker_secrets` so the permissions are applied in exactly
    one place no matter which module happens to write first.
    """
    try:
        broker_secrets.state_dir()
    except OSError as exc:
        log.warning("Could not create the state directory %s: %s", STATE_DIR, exc)


def _remove_legacy_token_file() -> None:
    """Delete the retired plaintext token file if one is still lying around."""
    try:
        LEGACY_TOKEN_FILE.unlink()
        log.info("Removed the retired plaintext token file %s.", LEGACY_TOKEN_FILE)
    except OSError:
        pass


def prune_stale_state(max_temp_age_sec: float = 3600.0) -> int:
    """Clear debris out of the state directory.  Returns the count removed.

    Two kinds accumulate and nothing else ever cleans them up:

    * per-failure cooldown files from a build that named them
      ``.shoonya_login_cooldown.<kind>_<date>.json``.  The current code writes a
      single ``.shoonya_login_cooldown.json``, so every one of those older files
      is dead weight that will never be read or replaced.
    * ``.<pid>.tmp`` files from an atomic write whose process died between
      creating the temp file and renaming it.

    Old enough is required for the temp files, because a *live* write in another
    process is allowed to have one open right now.
    """
    removed = 0
    now = time.time()
    try:
        entries = list(STATE_DIR.iterdir())
    except OSError:
        return 0

    for entry in entries:
        try:
            if not entry.is_file():
                continue
            stale_cooldown = (
                entry.name.startswith(COOLDOWN_FILE.stem + ".")
                and entry.name.endswith(".json")
                and entry.name != COOLDOWN_FILE.name
            )
            stale_temp = (
                entry.name.endswith(".tmp")
                and (now - entry.stat().st_mtime) > max_temp_age_sec
            )
            if stale_cooldown or stale_temp:
                entry.unlink()
                removed += 1
                log.info("Pruned stale state file %s.", entry.name)
        except OSError:
            continue
    return removed


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write via temp file + os.replace (atomic on Windows and POSIX)."""
    _ensure_state_dir()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        if os.name != "nt":
            os.fchmod(handle.fileno(), 0o600)
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        # Missing, truncated or hand-edited garbage — treated as "no cache".
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Cross-process lock
# ─────────────────────────────────────────────────────────────────────────────


class _InterProcessLock:
    """
    Advisory lock held for the whole mint-and-publish critical section.

    Deliberately a *separate* file from the cache: the cache is replaced
    atomically, which would swap the inode out from under an open handle.
    """

    def __init__(self, path: Path, timeout: float):
        self.path = path
        self.timeout = timeout
        self._handle = None

    def __enter__(self) -> "_InterProcessLock":
        # Earliest writer into the state directory, so it is also the earliest
        # point that needs the directory to exist with the right permissions.
        _ensure_state_dir()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, "a+b")
        deadline = time.monotonic() + self.timeout
        delay = 0.15
        while True:
            if self._try_lock():
                return self
            if time.monotonic() >= deadline:
                self._close()
                raise TransientLoginError(
                    f"Timed out after {self.timeout:.0f}s waiting for another process "
                    "to finish its Shoonya login."
                )
            time.sleep(delay)
            delay = min(delay * 1.5, 1.0)

    def _try_lock(self) -> bool:
        assert self._handle is not None
        try:
            self._handle.seek(0)
            if os.name == "nt":
                import msvcrt

                # LK_NBLCK, not LK_LOCK: LK_LOCK gives up after ~10s, but a
                # browser login legitimately takes far longer than that.
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def __exit__(self, *exc_info) -> None:
        if self._handle is not None:
            try:
                self._handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self._close()

    def _close(self) -> None:
        try:
            if self._handle is not None:
                self._handle.close()
        except OSError:
            pass
        self._handle = None


# ─────────────────────────────────────────────────────────────────────────────
#  Cooldown (shared across processes)
# ─────────────────────────────────────────────────────────────────────────────


def _read_cooldown(fingerprint: str) -> Optional[str]:
    """Return a human-readable reason when logins are currently barred."""
    data = _read_json(COOLDOWN_FILE)
    if not data:
        return None
    # Credentials changed since the failure -> the failure is obsolete.
    if data.get("fingerprint") != fingerprint:
        return None
    until = float(data.get("until_epoch") or 0.0)
    if time.time() >= until:
        return None
    remaining = int(until - time.time())
    kind = "permanent" if data.get("permanent") else "transient"
    return (
        f"Shoonya login is in {kind} cooldown for another {remaining}s: "
        f"{data.get('reason') or 'unspecified failure'}"
    )


def _write_cooldown(fingerprint: str, reason: str, *, permanent: bool) -> None:
    duration = PERMANENT_COOLDOWN_SEC if permanent else TRANSIENT_COOLDOWN_SEC
    _atomic_write_json(
        COOLDOWN_FILE,
        {
            "fingerprint": fingerprint,
            "permanent": permanent,
            "reason": reason[:500],
            "until_epoch": time.time() + duration,
            "written_at_ist": ist.isoformat(),
        },
    )


def _clear_cooldown() -> None:
    try:
        COOLDOWN_FILE.unlink()
    except OSError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  TOTP
# ─────────────────────────────────────────────────────────────────────────────


def _generate_totp(secret: str, *, avoid: Optional[str] = None) -> str:
    """
    Generate a TOTP as late as possible.

    ``avoid`` forces a *different* code than the one supplied, which is what a
    retry needs: replaying an already-rejected OTP just burns an attempt.
    """
    totp = pyotp.TOTP(secret)
    deadline = time.monotonic() + 40
    while True:
        remaining = totp.interval - (int(time.time()) % totp.interval)
        code = totp.now()
        stale = remaining < MIN_OTP_SECONDS_LEFT
        repeated = avoid is not None and code == avoid
        if not stale and not repeated:
            return code
        if time.monotonic() > deadline:
            # Never spin forever; a slightly-too-fresh code beats no login.
            return totp.now()
        time.sleep(min(remaining + 0.3, 2.0))


# ─────────────────────────────────────────────────────────────────────────────
#  Browser authorisation (Playwright, in-process)
# ─────────────────────────────────────────────────────────────────────────────

_BLOCKED_RESOURCES = {"image", "font", "media"}


def _dump_diagnostics(page, tag: str) -> Optional[str]:
    """Persist page HTML + screenshot so a broker page change is diagnosable."""
    try:
        DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
        stamp = ist.now().strftime("%Y%m%d_%H%M%S")
        base = DIAGNOSTICS_DIR / f"login_failure_{stamp}_{tag}"
        try:
            html = page.content()
            base.with_suffix(".html").write_text(html, encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - diagnostics must never mask the error
            pass
        try:
            page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
        except Exception:  # noqa: BLE001
            pass
        return str(base)
    except Exception:  # noqa: BLE001
        return None


def _visible_inputs(page) -> list:
    """Visible, enabled, non-hidden inputs in DOM order."""
    candidates = page.locator("input:visible")
    result = []
    for index in range(candidates.count()):
        node = candidates.nth(index)
        try:
            if (node.get_attribute("type") or "text").lower() in {
                "hidden",
                "submit",
                "button",
                "checkbox",
                "radio",
            }:
                continue
            result.append(node)
        except Exception:  # noqa: BLE001 - DOM churn between count() and read
            continue
    return result


def _classify_fields(page) -> tuple[Optional[Any], Optional[Any], Optional[Any]]:
    """
    Identify (userid, password, otp) semantically, falling back to position.

    Positional selection was the old behaviour and silently types the password
    into the wrong box if the broker adds a field; semantics first is the point.
    """
    inputs = _visible_inputs(page)
    if not inputs:
        return None, None, None

    password = None
    others: list = []
    for node in inputs:
        try:
            attrs = " ".join(
                (node.get_attribute(name) or "")
                for name in ("type", "name", "id", "placeholder", "aria-label", "autocomplete")
            ).lower()
        except Exception:  # noqa: BLE001
            attrs = ""
        if password is None and ("password" in attrs or "pwd" in attrs):
            password = node
        else:
            others.append((node, attrs))

    userid = None
    otp = None
    for node, attrs in others:
        # "pan" is deliberately absent: some brokers label the *userid* field
        # with PAN, and misclassifying it would type the OTP into the username.
        if otp is None and any(marker in attrs for marker in ("otp", "totp", "2fa", "authenticator")):
            otp = node
        elif userid is None and any(marker in attrs for marker in ("uid", "user", "client", "login", "id")):
            userid = node

    remaining = [node for node, _ in others if node is not userid and node is not otp]
    if userid is None and remaining:
        userid = remaining.pop(0)
    if otp is None and remaining:
        otp = remaining.pop(0)

    # Last resort: original positional behaviour.
    if password is None and len(inputs) >= 2:
        password = inputs[1]
    if userid is None and inputs:
        userid = inputs[0]
    if otp is None and len(inputs) >= 3:
        otp = inputs[2]

    return userid, password, otp


def _click_submit(page) -> bool:
    for pattern in ("LOGIN", "SIGN IN", "SUBMIT", "CONTINUE", "PROCEED"):
        try:
            button = page.locator("button:visible", has_text=pattern).first
            if button.count() > 0:
                button.click(timeout=5_000)
                return True
        except Exception:  # noqa: BLE001
            continue
    try:
        page.locator("button[type=submit]:visible, input[type=submit]:visible").first.click(
            timeout=5_000
        )
        return True
    except Exception:  # noqa: BLE001
        return False


def _click_consent(page) -> bool:
    try:
        buttons = page.locator("button:visible")
        for index in range(min(buttons.count(), 12)):
            button = buttons.nth(index)
            text = (button.inner_text() or "").strip().lower()
            if text in {"authorize", "authorise", "allow", "approve", "proceed", "continue", "yes", "submit"}:
                button.click(timeout=3_000)
                return True
    except Exception:  # noqa: BLE001
        pass
    return False


def _page_error_text(page) -> str:
    """Scrape any visible error/toast text the login page is showing."""
    selectors = (
        "[class*=error]:visible",
        "[class*=Error]:visible",
        "[class*=alert]:visible",
        "[class*=toast]:visible",
        "[role=alert]:visible",
        "[class*=invalid]:visible",
    )
    parts: list[str] = []
    for selector in selectors:
        try:
            nodes = page.locator(selector)
            for index in range(min(nodes.count(), 5)):
                text = (nodes.nth(index).inner_text() or "").strip()
                if text and text not in parts:
                    parts.append(text)
        except Exception:  # noqa: BLE001
            continue
    return " | ".join(parts)[:400]


def _authorize_in_browser(
    creds: ShoonyaCredentials,
    *,
    avoid_otp: Optional[str] = None,
    otp_box: Optional[list] = None,
) -> tuple[str, str]:
    """
    Drive the OAuth page and return ``(auth_code, otp_used)``.

    ``otp_box``, if given, receives each OTP as soon as it is generated — the
    caller needs to know which code was burned even when the attempt *fails*,
    so a retry does not resubmit the same rejected code.

    Runs Playwright's *sync* API, so it must never execute on a thread that owns
    a running asyncio loop — :func:`_run_off_loop` guarantees that.
    """

    def _record(code: str) -> str:
        if otp_box is not None:
            otp_box.append(code)
        return code

    from playwright.sync_api import TimeoutError as PWTimeout  # noqa: N814
    from playwright.sync_api import sync_playwright

    login_url = f"{AUTHORIZE_URL}?client_id={requests.utils.quote(creds.client_id)}"
    headless = os.getenv("SHOONYA_LOGIN_HEADLESS", "1") != "0"

    with sync_playwright() as pw:
        browser = None
        context = None
        page = None
        try:
            launch_errors = []
            launch_attempts = []

            # The named Edge channel can resolve differently for LocalSystem,
            # which is the account used by the Windows service. Prefer an
            # explicitly configured browser, then machine-wide Edge paths.
            configured_browser = os.getenv("SHOONYA_BROWSER_PATH", "").strip()
            if configured_browser:
                launch_attempts.append(
                    (f"configured browser {configured_browser}", {"executable_path": configured_browser})
                )
            if os.name == "nt":
                for edge_path in (
                    Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"))
                    / "Microsoft" / "Edge" / "Application" / "msedge.exe",
                    Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
                    / "Microsoft" / "Edge" / "Application" / "msedge.exe",
                ):
                    if edge_path.is_file() and str(edge_path) != configured_browser:
                        launch_attempts.append(
                            (f"Microsoft Edge at {edge_path}", {"executable_path": str(edge_path)})
                        )
            launch_attempts.extend((
                ("Playwright Microsoft Edge channel", {"channel": "msedge"}),
                ("Playwright bundled Chromium", {}),
            ))

            for description, options in launch_attempts:
                try:
                    browser = pw.chromium.launch(headless=headless, **options)
                    break
                except Exception as exc:  # noqa: BLE001
                    launch_errors.append(f"{description}: {exc}")

            if browser is None and not configured_browser:
                # Playwright pins an exact Chromium build; a pip upgrade (or a
                # `playwright install` run from a different env) leaves the
                # pinned build missing. Self-heal once instead of failing the
                # whole trading day.
                log.warning(
                    "No Playwright browser available; running 'playwright install chromium' once."
                )
                try:
                    proc = subprocess.run(
                        [sys.executable, "-m", "playwright", "install", "chromium"],
                        capture_output=True, text=True, timeout=900,
                    )
                    if proc.returncode != 0:
                        launch_errors.append(
                            f"playwright install chromium failed: {(proc.stderr or proc.stdout or '').strip()[-500:]}"
                        )
                    else:
                        try:
                            browser = pw.chromium.launch(headless=headless)
                        except Exception as exc:  # noqa: BLE001
                            launch_errors.append(f"Retry after playwright install: {exc}")
                except Exception as exc:  # noqa: BLE001
                    launch_errors.append(f"playwright install chromium failed: {exc}")

            if browser is None:
                raise PermanentLoginError(
                    "No usable browser for Shoonya login. Set SHOONYA_BROWSER_PATH to a "
                    "working Chromium/Edge executable or run: python -m playwright install "
                    f"chromium. Launch attempts: {' | '.join(launch_errors)}"
                )

            context = browser.new_context(viewport={"width": 1365, "height": 768})
            context.set_default_timeout(BROWSER_NAV_TIMEOUT_MS)
            page = context.new_page()

            # Images/fonts/media are pure latency on a login form.
            def _route(route):
                try:
                    if route.request.resource_type in _BLOCKED_RESOURCES:
                        return route.abort()
                    return route.continue_()
                except Exception:  # noqa: BLE001
                    return None

            page.route("**/*", _route)

            try:
                page.goto(login_url, wait_until="domcontentloaded", timeout=BROWSER_NAV_TIMEOUT_MS)
                page.wait_for_selector("input:visible", timeout=BROWSER_NAV_TIMEOUT_MS)
            except PWTimeout as exc:
                _dump_diagnostics(page, "nav_timeout")
                raise TransientLoginError(
                    f"Shoonya login page did not load in time: {exc}"
                ) from exc

            userid_field, password_field, otp_field = _classify_fields(page)
            if userid_field is None or password_field is None:
                path = _dump_diagnostics(page, "no_fields")
                raise TransientLoginError(
                    "Could not locate the Shoonya userid/password fields — the login page "
                    f"layout may have changed. Diagnostics: {path}"
                )

            userid_field.fill(creds.userid)
            password_field.fill(creds.password)

            # Generate the OTP at the last possible moment: it is only valid for
            # a 30s window and everything above can be slow.
            otp_used = _record(_generate_totp(creds.totp_secret, avoid=avoid_otp))

            if otp_field is not None:
                otp_field.fill(otp_used)
                submitted = _click_submit(page)
            else:
                # Two-step layout: credentials first, OTP on the next screen.
                submitted = _click_submit(page)
                try:
                    page.wait_for_selector("input:visible", timeout=20_000)
                    _, _, late_otp = _classify_fields(page)
                    if late_otp is None:
                        raise TransientLoginError("OTP field never appeared after credential submit.")
                    otp_used = _record(_generate_totp(creds.totp_secret, avoid=avoid_otp))
                    late_otp.fill(otp_used)
                    submitted = _click_submit(page)
                except PWTimeout as exc:
                    _dump_diagnostics(page, "no_otp_step")
                    raise TransientLoginError(f"OTP step never rendered: {exc}") from exc

            if not submitted:
                path = _dump_diagnostics(page, "no_submit")
                raise TransientLoginError(f"Could not find the LOGIN button. Diagnostics: {path}")

            # Wait for the redirect that carries ?code=...
            deadline = time.monotonic() + BROWSER_TOTAL_TIMEOUT_SEC
            code = None
            while time.monotonic() < deadline:
                current = page.url or ""
                if "code=" in current:
                    query = urllib.parse.urlparse(current).query
                    values = urllib.parse.parse_qs(query).get("code")
                    if values and values[0]:
                        code = values[0]
                        break

                error_text = _page_error_text(page)
                if error_text:
                    if _matches(error_text, _PERMANENT_MARKERS):
                        _dump_diagnostics(page, "bad_credentials")
                        raise PermanentLoginError(
                            f"Shoonya rejected the credentials: {error_text}"
                        )
                    if _matches(error_text, _OTP_MARKERS):
                        _dump_diagnostics(page, "otp_rejected")
                        raise TransientLoginError(f"OTP rejected: {error_text}")

                _click_consent(page)
                page.wait_for_timeout(200)

            if not code:
                path = _dump_diagnostics(page, "no_code")
                error_text = _page_error_text(page)
                raise TransientLoginError(
                    "Shoonya never redirected with an authorization code "
                    f"(page said: {error_text or 'nothing'}). Diagnostics: {path}"
                )

            return code, otp_used
        finally:
            # The old Node implementation only closed on the success path and
            # leaked an Edge process on every failure.
            for closer in (context, browser):
                try:
                    if closer is not None:
                        closer.close()
                except Exception:  # noqa: BLE001
                    pass


_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="shoonya-login")


def _run_off_loop(fn: Callable[[], Any]) -> Any:
    """
    Execute ``fn`` on a worker thread.

    main.py calls login from inside an async bootstrap; Playwright's sync API
    refuses to run on a thread with a live asyncio loop, so we always hop off.
    """
    return _EXECUTOR.submit(fn).result()


# ─────────────────────────────────────────────────────────────────────────────
#  Token exchange & validation
# ─────────────────────────────────────────────────────────────────────────────


def _exchange_code(creds: ShoonyaCredentials, auth_code: str) -> dict[str, Any]:
    checksum = hashlib.sha256(
        f"{creds.client_id}{creds.oauth_secret}{auth_code}".encode("utf-8")
    ).hexdigest()
    payload = json.dumps({"code": auth_code, "checksum": checksum}, separators=(",", ":"))
    try:
        response = requests.post(
            f"{API_BASE_URL}/GenAcsTok",
            data="jData=" + payload,
            timeout=EXCHANGE_TIMEOUT_SEC,
        )
    except requests.RequestException as exc:
        raise TransientLoginError(f"GenAcsTok request failed: {exc}") from exc

    try:
        body = response.json()
    except ValueError:
        text = (response.text or "").strip()
        raise TransientLoginError(
            f"GenAcsTok returned non-JSON (HTTP {response.status_code}): {text[:200]}"
        )

    if not isinstance(body, dict):
        raise TransientLoginError(f"GenAcsTok returned unexpected payload: {type(body).__name__}")

    token = body.get("access_token") or body.get("susertoken")
    if body.get("stat") == "Ok" and token:
        return body

    emsg = str(body.get("emsg") or body.get("stat") or "unknown error")
    if _matches(emsg, _PERMANENT_MARKERS):
        raise PermanentLoginError(f"GenAcsTok rejected the request: {emsg}")
    raise TransientLoginError(f"GenAcsTok did not return a token: {emsg}")


def probe_token(userid: str, token: str, *, timeout: float = PROBE_TIMEOUT_SEC) -> ProbeResult:
    """
    Cheapest authoritative "is this session alive?" check.

    UserDetails needs no exchange or instrument, so a failure means the session
    is bad rather than the arguments.  Uses the exact Bearer format the rest of
    the app uses, so a pass here really does predict the app's calls.
    """
    try:
        response = HTTP.post(
            f"{API_BASE_URL}/UserDetails",
            data="jData=" + json.dumps({"uid": userid}, separators=(",", ":")),
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        log.debug("Session probe network error: %s", exc)
        return ProbeResult.UNKNOWN

    if response.status_code in (401, 403):
        return ProbeResult.INVALID_AUTH
    if response.status_code >= 500:
        return ProbeResult.UNKNOWN

    try:
        body = response.json()
    except ValueError:
        return ProbeResult.UNKNOWN

    if isinstance(body, dict):
        if body.get("stat") == "Ok":
            return ProbeResult.VALID
        emsg = str(body.get("emsg") or "")
        if is_auth_expiry_text(emsg):
            return ProbeResult.INVALID_AUTH
        # An unfamiliar Not_Ok is *not* proof of expiry — do not mint on it.
        log.debug("Session probe inconclusive: %s", emsg[:200])
        return ProbeResult.UNKNOWN
    return ProbeResult.UNKNOWN


def probe_token_resilient(userid: str, token: str, *, attempts: int = 3) -> ProbeResult:
    """
    Probe, retrying only the inconclusive case.

    A single dropped packet must not be read as "session dead" (which would
    trigger a needless re-login), nor as "session alive".
    """
    result = ProbeResult.UNKNOWN
    for index in range(attempts):
        result = probe_token(userid, token)
        if result is not ProbeResult.UNKNOWN:
            return result
        if index < attempts - 1:
            time.sleep(0.4 * (index + 1))
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  Session manager
# ─────────────────────────────────────────────────────────────────────────────


class ShoonyaSessionManager:
    """
    Owns the cached session for one set of credentials.

    ``allow_mint`` decides whether *this process* may create a new broker
    session.  Auxiliary processes (historical downloaders, dashboards) should
    pass ``allow_mint=False`` so they can never rotate the live bot's session
    during market hours.
    """

    def __init__(self, creds: Optional[ShoonyaCredentials] = None, *, allow_mint: bool = True):
        self.creds = creds or load_credentials()
        self.allow_mint = allow_mint
        self._lock = threading.RLock()
        self._session: Optional[CachedSession] = None
        #: Tokens the broker has explicitly rejected.  Without this, the
        #: double-check before minting would happily re-adopt the very token we
        #: just proved dead (and a flaky probe could then loop forever).
        self._rejected_tokens: set[str] = set()

    def _mark_rejected(self, token: Optional[str]) -> None:
        if not token:
            return
        self._rejected_tokens.add(token)
        if len(self._rejected_tokens) > 10:
            self._rejected_tokens = set(list(self._rejected_tokens)[-10:])

    # ── public API ───────────────────────────────────────────────────────

    @property
    def userid(self) -> str:
        return self.creds.userid

    @property
    def token(self) -> Optional[str]:
        return self._session.access_token if self._session else None

    @property
    def generation(self) -> int:
        return self._session.generation if self._session else 0

    def get_access_token(self, *, force_refresh: bool = False, allow_mint: Optional[bool] = None) -> str:
        """
        Return a validated access token, minting a new session only if needed.

        Fast path (valid cache, recently probed) does zero network I/O.
        """
        may_mint = self.allow_mint if allow_mint is None else allow_mint
        with self._lock:
            if not force_refresh:
                token = self._try_existing()
                if token:
                    return token

            if not may_mint:
                # Last chance: another process may have just published one.
                token = self._adopt_from_disk(probe=True)
                if token:
                    return token
                raise SessionUnavailableError(
                    "No valid cached Shoonya session and this process is not permitted to "
                    "create one (allow_mint=False). Start the main bot first."
                )

            return self._mint_locked(force_refresh=force_refresh)

    def invalidate(self, token: Optional[str] = None) -> None:
        """
        Mark the in-memory session dead after an explicit broker auth rejection.

        Only clears if the caller's token is the one we hold, so a slow thread
        cannot discard a newer session that another thread just installed.
        """
        with self._lock:
            if self._session is None:
                return
            if token is None or token == self._session.access_token:
                log.warning("Shoonya session marked invalid (generation %d).", self._session.generation)
                self._mark_rejected(self._session.access_token)
                self._session = None

    def refresh_after_expiry(self) -> str:
        """Adopt a newer cached session if one exists, else mint a fresh one."""
        with self._lock:
            token = self._adopt_from_disk(probe=True)
            if token:
                log.info("Adopted a newer Shoonya session published by another process.")
                return token
            return self.get_access_token(force_refresh=True)

    # ── internals ────────────────────────────────────────────────────────

    def _try_existing(self) -> Optional[str]:
        """In-memory session first, then the on-disk cache."""
        session = self._session
        if session:
            if time.time() - session.validated_at < REVALIDATE_AFTER_SEC:
                return session.access_token
            result = probe_token_resilient(self.creds.userid, session.access_token)
            if result is ProbeResult.VALID:
                session.validated_at = time.time()
                return session.access_token
            if result is ProbeResult.UNKNOWN:
                # Broker is unhealthy; keep using what we have rather than
                # minting a new session (which could kill a live one).
                log.warning("Shoonya session probe inconclusive — reusing current token.")
                return session.access_token
            log.warning("Shoonya session rejected by broker; will re-authenticate.")
            self._mark_rejected(session.access_token)
            self._session = None

        return self._adopt_from_disk(probe=True)

    def _adopt_from_disk(self, *, probe: bool) -> Optional[str]:
        data = _read_json(SESSION_CACHE_FILE)
        cached = CachedSession.from_json(data) if data else None
        if cached is None:
            cached = self._adopt_legacy_file()
        if cached is None:
            return None

        if cached.uid != self.creds.userid or (
            cached.client_id_hash and cached.client_id_hash != self.creds.client_id_hash
        ):
            log.info("Ignoring cached Shoonya session: it belongs to different credentials.")
            return None

        if cached.access_token in self._rejected_tokens:
            # Someone (possibly us) already proved this token is dead.  Only a
            # genuinely *different* token published by another process counts.
            log.debug("Ignoring cached Shoonya session: token already rejected.")
            return None

        if not probe or time.time() - cached.validated_at < REVALIDATE_AFTER_SEC:
            self._session = cached
            return cached.access_token

        result = probe_token_resilient(self.creds.userid, cached.access_token)
        if result is ProbeResult.VALID:
            cached.validated_at = time.time()
            self._session = cached
            self._publish(cached, update_disk=True)
            return cached.access_token
        if result is ProbeResult.UNKNOWN:
            raise TransientLoginError(
                "Could not verify the cached Shoonya session (broker unreachable or "
                "returned an unexpected response). Refusing to create a new session "
                "while the broker's state is unknown."
            )
        self._mark_rejected(cached.access_token)
        return None

    def _adopt_legacy_file(self) -> Optional[CachedSession]:
        """Consume a bare access_token.txt written by a pre-cache build, once.

        The file is deleted whether or not its contents were usable: it is a
        broker token sitting in plaintext, nothing writes it any more, and
        leaving it behind means it is still there - and still valid-looking -
        long after the session it came from expired.
        """
        try:
            raw = LEGACY_TOKEN_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        finally:
            _remove_legacy_token_file()
        if not raw or len(raw) < 16:
            return None
        log.info("Adopting legacy access_token.txt into the new session cache.")
        return CachedSession(
            access_token=raw,
            uid=self.creds.userid,
            client_id_hash=self.creds.client_id_hash,
            issued_at=0.0,
            validated_at=0.0,
            generation=1,
        )

    def _mint_locked(self, *, force_refresh: bool) -> str:
        """Serialise minting across processes, re-checking under the lock."""
        blocked = _read_cooldown(self.creds.fingerprint)
        if blocked:
            raise PermanentLoginError(blocked) if "permanent" in blocked else TransientLoginError(blocked)

        with _InterProcessLock(SESSION_LOCK_FILE, LOCK_WAIT_TIMEOUT_SEC):
            if force_refresh:
                # Retire the token the caller is rejecting, so the double-check
                # below cannot hand the same one back.
                if self._session is not None:
                    # We know exactly which token is bad: ours.  A *different*
                    # token on disk is a peer's newer session — worth adopting
                    # rather than minting a second one.
                    self._mark_rejected(self._session.access_token)
                else:
                    # No in-memory session, so "force" can only mean "replace
                    # whatever is cached" (e.g. `login_test.py --force`).
                    on_disk = CachedSession.from_json(_read_json(SESSION_CACHE_FILE) or {})
                    if on_disk is None:
                        on_disk = self._adopt_legacy_file()
                    if on_disk is not None:
                        self._mark_rejected(on_disk.access_token)

            # Double-checked: whoever held the lock before us very likely just
            # published a fresh token, in which case we must not mint another.
            token = self._adopt_from_disk(probe=True)
            if token:
                log.info("Reusing the Shoonya session another process just published.")
                return token

            # Last chance to notice a rotation before we spend a real broker
            # submission.  This process may have been holding a cached copy of
            # the secret for up to SHOONYA_SECRET_TTL_SEC; minting is rare and
            # expensive, so one extra read is cheap insurance against burning
            # an attempt on a password that was rotated an hour ago.
            #
            # Deliberately *before* the cooldown re-check: the refresh can
            # change the fingerprint, and the cooldown that matters is the one
            # recorded against the credentials we are about to submit — not the
            # ones we happened to start with.
            self._refresh_credentials_before_mint()

            blocked = _read_cooldown(self.creds.fingerprint)
            if blocked:
                raise PermanentLoginError(blocked) if "permanent" in blocked else TransientLoginError(blocked)

            return self._perform_login()

    def _perform_login(self) -> str:
        """
        Full browser login with a deliberately small retry budget.

        At most two credential submissions, and the second only for an
        explicitly rejected OTP — anything else risks locking the account.
        """
        last_error: Optional[Exception] = None
        # Populated by the browser step as soon as an OTP is generated, so a
        # failed attempt still tells us which code was burned.
        otp_box: list[str] = []
        max_attempts = 2

        for attempt in range(1, max_attempts + 1):
            started = time.time()
            try:
                log.info("Starting Shoonya browser login (attempt %d/%d).", attempt, max_attempts)
                previous_otp = otp_box[-1] if otp_box else None
                code, _otp_used = _run_off_loop(
                    lambda: _authorize_in_browser(
                        self.creds, avoid_otp=previous_otp, otp_box=otp_box
                    )
                )
                token_response = _exchange_code(self.creds, code)
                token = token_response.get("access_token") or token_response.get("susertoken")

                # Keep the generation monotonic across invalidate/re-mint cycles
                # (invalidate() clears _session, so fall back to what is on disk).
                previous = self._session
                if previous is None:
                    previous = CachedSession.from_json(_read_json(SESSION_CACHE_FILE) or {})

                session = CachedSession(
                    access_token=str(token).strip(),
                    uid=self.creds.userid,
                    client_id_hash=self.creds.client_id_hash,
                    issued_at=time.time(),
                    validated_at=time.time(),
                    generation=(previous.generation + 1) if previous else 1,
                    expires_at=token_response.get("expires_at") or token_response.get("exp"),
                )
                # Publish immediately: a crash between mint and publish would
                # orphan a live broker session.
                self._publish(session, update_disk=True)
                _clear_cooldown()
                log.info(
                    "Shoonya login succeeded in %.1fs (generation %d).",
                    time.time() - started,
                    session.generation,
                )
                return session.access_token

            except PermanentLoginError as exc:
                _write_cooldown(self.creds.fingerprint, str(exc), permanent=True)
                log.error("Shoonya login failed permanently: %s", exc)
                self._reload_credentials_after_rejection()
                raise

            except TransientLoginError as exc:
                last_error = exc
                retryable_otp = _matches(str(exc), _OTP_MARKERS)
                if attempt >= max_attempts or not retryable_otp:
                    break
                backoff = 1.0 + random.random()
                log.warning(
                    "Shoonya login attempt %d failed (%s). Retrying in %.1fs with a fresh OTP.",
                    attempt,
                    exc,
                    backoff,
                )
                time.sleep(backoff)

            except Exception as exc:  # noqa: BLE001 - unexpected: treat as transient
                last_error = TransientLoginError(f"Unexpected login failure: {exc}")
                break

        reason = str(last_error or "unknown failure")
        _write_cooldown(self.creds.fingerprint, reason, permanent=False)
        raise TransientLoginError(f"Shoonya login failed: {reason}")

    def _refresh_credentials_before_mint(self) -> None:
        """
        Re-read the secret immediately before a browser login.

        Best effort by design: we already hold a credential set that resolved
        successfully, so a failed re-read must not turn a recoverable "mint a
        new session" into a hard failure.  If the values
        did change, adopting them here also re-points ``client_id_hash``, which
        is what the on-disk session cache is keyed by.
        """
        try:
            fresh = load_credentials()
        except ShoonyaLoginError as exc:
            log.warning(
                "Could not re-read credentials before minting (%s) — proceeding with "
                "the set already in hand.",
                exc,
            )
            return
        if fresh.fingerprint != self.creds.fingerprint:
            log.warning(
                "Credentials changed in %s since this process started (fingerprint %s -> "
                "%s); minting with the current values.",
                fresh.source,
                self.creds.fingerprint,
                fresh.fingerprint,
            )
            self.creds = fresh

    def _reload_credentials_after_rejection(self) -> None:
        """
        Re-read the secret after the broker rejected our credentials.

        A rejection is the only evidence we ever get that the secret may have
        been changed while this process was holding an older copy.

        This deliberately does **not** retry the login.  The permanent cooldown
        written a moment ago is keyed to the *old* fingerprint, so:

        * credentials really did change  -> the new fingerprint no longer
          matches the cooldown record (:func:`_read_cooldown` line ~409), the
          cooldown stops applying, and the next attempt proceeds with the new
          values;
        * nothing changed               -> same fingerprint, cooldown stands,
          and we never submit a rejected password a second time.

        That is the whole anti-lockout argument: at most one further attempt,
        and only when the credential itself is different.  Re-reading is best
        effort — if the re-read fails we keep what we have.
        """
        try:
            fresh = load_credentials()
        except ShoonyaLoginError as exc:
            log.debug("Could not re-read credentials after the rejection: %s", exc)
            return

        if fresh.fingerprint == self.creds.fingerprint:
            log.info("Credentials are unchanged in %s — the cooldown stands.", fresh.source)
            return

        log.warning(
            "Credentials changed in %s after the broker rejected the previous set "
            "(fingerprint %s -> %s). The permanent cooldown no longer applies; the "
            "next attempt will use the rotated credentials.",
            fresh.source,
            self.creds.fingerprint,
            fresh.fingerprint,
        )
        self.creds = fresh

    def _publish(self, session: CachedSession, *, update_disk: bool) -> None:
        self._session = session
        if not update_disk:
            return
        try:
            _atomic_write_json(SESSION_CACHE_FILE, session.to_json())
        except OSError as exc:
            # A token we cannot cache is still usable right now.
            log.warning("Could not persist the Shoonya session cache: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
#  Module-level accessor
# ─────────────────────────────────────────────────────────────────────────────

_MANAGER: Optional[ShoonyaSessionManager] = None
_MANAGER_LOCK = threading.Lock()


def get_session_manager(*, allow_mint: Optional[bool] = None) -> ShoonyaSessionManager:
    """
    Return the process-wide session manager.

    ``allow_mint`` defaults to the SHOONYA_ALLOW_MINT env var (default on), so
    an auxiliary process can be demoted to reuse-only without a code change.
    """
    global _MANAGER
    with _MANAGER_LOCK:
        if _MANAGER is None:
            # Once per process, on the way to the first login: nothing else
            # ever removes this debris, and a process that is about to use the
            # state directory is the one with a reason to tidy it.
            prune_stale_state()
            env_default = os.getenv("SHOONYA_ALLOW_MINT", "1") != "0"
            _MANAGER = ShoonyaSessionManager(
                allow_mint=env_default if allow_mint is None else allow_mint
            )
        elif allow_mint is not None:
            _MANAGER.allow_mint = allow_mint
        return _MANAGER


def redact(text: Any, token: Optional[str] = None) -> str:
    """Scrub a token out of anything about to be logged."""
    rendered = text if isinstance(text, str) else repr(text)
    secrets = [token] if token else []
    if _MANAGER is not None and _MANAGER.token:
        secrets.append(_MANAGER.token)
    for secret in secrets:
        if secret and len(secret) >= 8:
            rendered = rendered.replace(secret, f"<redacted:{secret[:4]}…>")
    return rendered


def install_token_redaction_filter() -> None:
    """
    Stop NorenRestApiPy from printing the full session token at DEBUG level.

    ``NorenApi.set_session`` logs ``f'{userid} session set to : {token}'``, so
    turning on DEBUG anywhere would otherwise write the token into the logs.
    """

    class _RedactFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                message = record.getMessage()
            except Exception:  # noqa: BLE001
                return True
            if _MANAGER is not None and _MANAGER.token and _MANAGER.token in message:
                record.msg = redact(message)
                record.args = ()
            return True

    for name in ("NorenRestApiPy.NorenApi", "NorenRestApiPy"):
        logging.getLogger(name).addFilter(_RedactFilter())


__all__ = [
    "ShoonyaSessionManager",
    "ShoonyaCredentials",
    "ShoonyaLoginError",
    "PermanentLoginError",
    "TransientLoginError",
    "SessionUnavailableError",
    "OrderNotReplayedError",
    "ProbeResult",
    "get_session_manager",
    "load_credentials",
    "probe_token",
    "probe_token_resilient",
    "is_auth_expiry_text",
    "install_token_redaction_filter",
    "redact",
    "SESSION_CACHE_FILE",
    "SESSION_LOCK_FILE",
    "COOLDOWN_FILE",
    "prune_stale_state",
    "LEGACY_TOKEN_FILE",
]
