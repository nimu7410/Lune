"""
Credential sourcing for the Shoonya login — a systemd encrypted credential.

Why this module exists
----------------------
The broker credentials live in ``secrets/shoonya.cred``: a flat JSON object
keyed by the ``SHOONYA_*`` names, encrypted with ``systemd-creds`` against this
host's key (``/var/lib/systemd/credential.secret``, readable by root only).
The file is useless anywhere else and unreadable to anyone but root here.

``lune.service`` loads it with ``LoadCredentialEncrypted=shoonya:...``: systemd
decrypts it at start into a private, in-memory credentials directory that only
the service can read, and points ``$CREDENTIALS_DIRECTORY`` at it.  Plaintext
never touches disk and never enters the environment.  To change a value, or to
run a one-off command such as ``login_test.py`` with the credentials loaded,
use ``./secret.sh``.

Design goals, in priority order (they mirror :mod:`shoonya_login`):

1. **One source, whole.**  A credential set is taken from exactly one source.
   Never merge credential values with environment values key by key: a new
   password against an old client id is a credential set that exists nowhere,
   and spends broker login attempts proving it wrong.
2. **Fail with the right severity.**  Everything that can go wrong here — the
   service not loading the credential, a malformed or incomplete file — needs
   a human, so it is reported as permanent.
3. **Never log a secret.**  Values are logged only as key names and counts.

Configuration (non-secret, environment variables)
-------------------------------------------------
=============================== =========================================
``SHOONYA_SECRETS_SOURCE``       ``systemd`` (default) | ``env``
=============================== =========================================

* ``systemd`` (default) — the encrypted credential only.  ``.env`` is never
  consulted for credentials.
* ``env`` — environment/``.env`` only, in plaintext.  For development on a
  machine without systemd; never for the live bot.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

log = logging.getLogger(__name__)


def _ensure_dotenv() -> None:
    """
    Make ``.env`` visible to this module on its own, before anything reads it.

    :mod:`shoonya_login` also calls ``load_dotenv()``, but it does so *after*
    importing this module — so any setting that lives only in ``.env``
    (``LUNE_STATE_DIR``, ``SHOONYA_SECRETS_SOURCE``) would be invisible here
    while being honoured there, and the two modules would quietly disagree
    about where state lives.  ``load_dotenv`` never overrides an
    already-exported variable, so a real environment variable still wins, and
    calling it twice is harmless.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


_ensure_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
#  Errors
# ─────────────────────────────────────────────────────────────────────────────
#
#  Deliberately *not* subclasses of shoonya_login's errors: that module imports
#  this one, so the dependency has to point one way.  shoonya_login translates
#  these into its own taxonomy at the boundary.


class SecretsError(RuntimeError):
    """Base class for every credential-sourcing failure."""


class SecretsPermanentError(SecretsError):
    """Configuration is wrong.  Retrying changes nothing."""


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

#: The credential id in ``LoadCredentialEncrypted=shoonya:...`` — also the name
#: embedded in the encrypted file, which systemd checks on decryption.
CREDENTIAL_NAME = "shoonya"

#: The encrypted file.  Only systemd reads it; this module reads the decrypted
#: copy systemd hands the service.  Named here for diagnostics and secret.sh.
CREDENTIAL_FILE = Path(__file__).resolve().parent / "secrets" / f"{CREDENTIAL_NAME}.cred"

#: Keys the credential is expected to carry.  Absence of an optional key is
#: fine; absence of a required one is diagnosed by the caller, which knows
#: which is which.  Listed here so we can log "what did we get" without
#: touching values.
KNOWN_KEYS = (
    "SHOONYA_USERID",
    "SHOONYA_PASSWORD",
    "SHOONYA_TOKEN",
    "SHOONYA_VENDOR_CODE",
    "SHOONYA_OAUTH_CLIENT_ID",
    "SHOONYA_OAUTH_SECRET",
    "SHOONYA_API_SECRET",
    "SHOONYA_IMEI",
)

#: What makes a credential set *complete*.  Checked before a set is accepted,
#: so "one source, whole" has a definition: a source that cannot answer all of
#: these does not get used at all, rather than getting used for the keys it
#: happens to have.
REQUIRED_KEYS = (
    "SHOONYA_USERID",
    "SHOONYA_PASSWORD",
    "SHOONYA_TOKEN",
    "SHOONYA_OAUTH_SECRET",
)

#: The client id has two acceptable spellings; at least one must be present.
CLIENT_ID_KEYS = ("SHOONYA_OAUTH_CLIENT_ID", "SHOONYA_VENDOR_CODE")


def _missing_keys(values: Mapping[str, str]) -> list[str]:
    """Return the names a credential set would need to be usable."""
    missing = [key for key in REQUIRED_KEYS if not values.get(key)]
    if not any(values.get(key) for key in CLIENT_ID_KEYS):
        missing.append(" or ".join(CLIENT_ID_KEYS))
    return missing

def _default_state_dir() -> Path:
    """Where the bot's runtime state lives when ``LUNE_STATE_DIR`` is unset.

    Not the source directory.  Everything kept here is *runtime state* - a live
    broker token, a cross-process lock - and mixing that into a source tree
    means it shows up in listings, in editors, in any archive of the project,
    and in version control the moment one is initialised.  It also has to
    survive the source directory being replaced.

    ``%ProgramData%\\neobot`` is the one place both identities that run this bot
    can reach: the Windows service runs as LocalSystem, interactive runs are as
    Administrator, and a per-user location would give them separate state and a
    second browser login every time the operator ran anything by hand.  The
    same reasoning already put the Playwright browser cache under ProgramData
    (see ``Lune.xml``).

    Off Windows, or with no ``%ProgramData%``, use the user's state directory
    so a project archive cannot accidentally include the broker session.
    """
    program_data = os.getenv("ProgramData")
    if os.name == "nt" and program_data:
        return Path(program_data) / "neobot"
    return Path(os.getenv("XDG_STATE_HOME") or Path.home() / ".local" / "state") / "lune"


def _harden_state_dir(path: Path) -> None:
    """Restrict a freshly created state directory to SYSTEM + Administrators.

    ProgramData grants Users read access by default, and this directory holds a
    live broker access token.  Only ever called on the branch that *created*
    the directory, so an operator who deliberately widens the ACL later is not
    overruled on the next start.  Best effort: a failure here is logged and the
    bot continues, because refusing to trade over a directory ACL would be a
    worse outcome than the exposure it prevents.
    """
    if os.name != "nt":
        path.chmod(0o700)
        return
    try:
        subprocess.run(
            ["icacls", str(path), "/inheritance:r",
             "/grant:r", "*S-1-5-18:(OI)(CI)F",      # SYSTEM
             "/grant:r", "*S-1-5-32-544:(OI)(CI)F"],  # Administrators
            check=True, capture_output=True, timeout=30,
        )
        log.info("Locked down state directory %s to SYSTEM + Administrators.", path)
    except Exception as exc:  # noqa: BLE001 - never fatal, see docstring
        log.warning(
            "Could not restrict permissions on %s (%s); it may be readable by "
            "other local users and it holds a broker token.", path, exc,
        )


def state_dir() -> Path:
    """The state directory, created (and locked down) on first use."""
    if not STATE_DIR.exists():
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        _harden_state_dir(STATE_DIR)
    elif os.name != "nt":
        _harden_state_dir(STATE_DIR)
    return STATE_DIR


#: The one definition of where runtime state lives.  :mod:`shoonya_login`
#: imports this rather than recomputing it, so the two cannot drift and
#: disagree about where the session cache is.  Resolved after
#: :func:`_ensure_dotenv`, so a ``LUNE_STATE_DIR`` that lives only in the
#: environment file is still honoured.
STATE_DIR = Path(os.getenv("LUNE_STATE_DIR") or _default_state_dir())


def source_mode() -> str:
    """Configured sourcing mode: ``systemd`` | ``env``."""
    mode = (os.getenv("SHOONYA_SECRETS_SOURCE") or "systemd").strip().lower()
    if mode not in ("systemd", "env"):
        log.warning(
            "Unknown SHOONYA_SECRETS_SOURCE=%r; using the encrypted systemd credential.", mode
        )
        return "systemd"
    return mode


# ─────────────────────────────────────────────────────────────────────────────
#  Resolved bundle
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedSecrets:
    """One complete credential set, plus how we got it."""

    #: Immutable: callers must not be able to mutate a shared snapshot.
    values: Mapping[str, str]
    #: ``systemd`` | ``env`` — the single source that answered.
    source: str

    @property
    def description(self) -> str:
        """Human-readable provenance, safe to print in diagnostics."""
        return {
            "systemd": f"encrypted credential (secrets/{CREDENTIAL_FILE.name})",
            "env": "environment / .env",
        }.get(self.source, self.source)

    @property
    def digest(self) -> str:
        """
        Short digest of the values, for spotting drift between sources.

        Truncated SHA-256 over high-entropy inputs: it identifies a credential
        *set* without revealing anything about it.
        """
        blob = json.dumps({k: self.values[k] for k in sorted(self.values)}, sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


# ─────────────────────────────────────────────────────────────────────────────
#  Sources
# ─────────────────────────────────────────────────────────────────────────────


def _clean(parsed: Mapping[object, object]) -> dict[str, str]:
    """String values, stripped; empty and ``null`` entries dropped."""
    values: dict[str, str] = {}
    for key, value in parsed.items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            values[str(key).strip()] = text
    return values


def _resolve_from_systemd() -> ResolvedSecrets:
    """The decrypted credential systemd handed this service."""
    directory = os.getenv("CREDENTIALS_DIRECTORY")
    if not directory:
        raise SecretsPermanentError(
            f"No decrypted credentials: this process was not started by systemd with "
            f"the '{CREDENTIAL_NAME}' credential. The bot gets them from lune.service; "
            "for a one-off command use ./secret.sh run <script.py> [args]."
        )

    path = Path(directory) / CREDENTIAL_NAME
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SecretsPermanentError(
            f"Could not read the '{CREDENTIAL_NAME}' credential from {directory} ({exc}). "
            f"The unit needs LoadCredentialEncrypted={CREDENTIAL_NAME}:{CREDENTIAL_FILE}."
        ) from exc

    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise SecretsPermanentError(
            f"The '{CREDENTIAL_NAME}' credential is not valid JSON ({exc}). Re-create it "
            "with ./secret.sh set <KEY>."
        ) from exc
    if not isinstance(parsed, dict):
        raise SecretsPermanentError(
            f"The '{CREDENTIAL_NAME}' credential must be a JSON object, got "
            f"{type(parsed).__name__}."
        )

    values = _clean(parsed)
    missing = _missing_keys(values)
    if missing:
        raise SecretsPermanentError(
            f"The '{CREDENTIAL_NAME}' credential is missing required key(s): "
            f"{', '.join(missing)}. Present: {', '.join(sorted(values)) or 'none'}. "
            "Add them with ./secret.sh set <KEY>."
        )

    unexpected = sorted(set(values) - set(KNOWN_KEYS))
    if unexpected:
        log.debug("The credential carries extra keys this bot ignores: %s", unexpected)

    log.info(
        "Loaded %d credential keys from the encrypted '%s' credential: %s",
        len(values), CREDENTIAL_NAME, ", ".join(sorted(values)),
    )
    return ResolvedSecrets(values=MappingProxyType(values), source="systemd")


def _resolve_from_env() -> ResolvedSecrets:
    """The environment as a *whole* credential set, or nothing."""
    values = _clean({key: os.getenv(key) for key in KNOWN_KEYS})
    missing = _missing_keys(values)
    if missing:
        raise SecretsPermanentError(
            "The environment/.env cannot supply a complete credential set; missing: "
            f"{', '.join(missing)}."
        )
    log.info("Credential source: environment/.env (%d keys).", len(values))
    return ResolvedSecrets(values=MappingProxyType(values), source="env")


def resolve_secrets() -> ResolvedSecrets:
    """
    Return the credential set from the configured source.

    Uncached: both sources are local and cheap, and systemd's decrypted copy is
    fixed for the life of the service — a changed credential takes effect on
    ``systemctl restart lune``.
    """
    if source_mode() == "env":
        return _resolve_from_env()
    return _resolve_from_systemd()


__all__ = [
    "SecretsError",
    "SecretsPermanentError",
    "ResolvedSecrets",
    "resolve_secrets",
    "source_mode",
    "CREDENTIAL_NAME",
    "CREDENTIAL_FILE",
    "STATE_DIR",
    "KNOWN_KEYS",
]
