#!/usr/bin/env bash
#
# setup.sh - one-shot Lune bootstrap for Linux (and macOS, with PostgreSQL
# already installed). The Windows counterpart is install_requirements.bat, which
# this script deliberately does not replace or modify.
#
# It is idempotent: run it as many times as you like. Nothing in the Python
# sources is read for anything but the values they already define, and no project file is modified. The only files
# this script creates are: venv/, .env.example, .env, and
# the PostgreSQL role/database/schema Lune expects.
#
#   ./setup.sh                  full setup
#   ./setup.sh --no-postgres    skip everything database-server related
#   ./setup.sh --no-browser     skip the Chromium download (broker login needs it)
#   ./setup.sh --no-tailscale   skip the Tailscale install
#   ./setup.sh --recreate-venv  delete and rebuild venv/ from scratch
#
# Tailscale is installed from its signed apt repository and left running
# unattended (tailscaled enabled at boot); joining a tailnet still needs either
# `sudo tailscale up` or a TS_AUTHKEY you supply.
#
# Tested target: Ubuntu/Debian (apt). Elsewhere, install Python 3.10+ and a
# running PostgreSQL yourself first - the script picks up from there.

set -Eeuo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

VENV_DIR="$HERE/venv"
DO_POSTGRES=1
DO_BROWSER=1
DO_TAILSCALE=1
RECREATE_VENV=0
WARNINGS=()

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
if [ -t 1 ]; then
    C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'
else
    C_RESET=""; C_BOLD=""; C_DIM=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""
fi

STEP_NO=0
STEP_TOTAL=10
step() { STEP_NO=$((STEP_NO + 1)); printf '\n%s[%d/%d] %s%s\n' "$C_BOLD$C_BLUE" "$STEP_NO" "$STEP_TOTAL" "$1" "$C_RESET"; }
info() { printf '      %s\n' "$1"; }
dim()  { printf '      %s%s%s\n' "$C_DIM" "$1" "$C_RESET"; }
ok()   { printf '      %s+%s %s\n' "$C_GREEN" "$C_RESET" "$1"; }
warn() { printf '      %s!%s %s\n' "$C_YELLOW" "$C_RESET" "$1"; WARNINGS+=("$1"); }
die()  { printf '\n%sSetup failed:%s %s\n' "$C_RED$C_BOLD" "$C_RESET" "$1" >&2; exit 1; }

on_error() {
    local line=$1 cmd=$2
    printf '\n%sSetup failed%s at line %s: %s\n' "$C_RED$C_BOLD" "$C_RESET" "$line" "$cmd" >&2
    printf 'Nothing was rolled back; fix the cause above and re-run ./setup.sh - it is idempotent.\n' >&2
    exit 1
}
trap 'on_error "$LINENO" "$BASH_COMMAND"' ERR

usage() { sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
    case "$1" in
        --no-postgres)   DO_POSTGRES=0 ;;
        --no-browser)    DO_BROWSER=0 ;;
        --no-tailscale)  DO_TAILSCALE=0 ;;
        --recreate-venv) RECREATE_VENV=1 ;;
        -h|--help)       usage; exit 0 ;;
        *) printf 'Unknown option: %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

# ---------------------------------------------------------------------------
# Privilege helpers. Package installs and PostgreSQL administration need root;
# everything else deliberately runs as the invoking user so the venv and the
# Playwright browser cache stay owned by whoever will run the bot.
# ---------------------------------------------------------------------------
IS_ROOT=0; [ "$(id -u)" -eq 0 ] && IS_ROOT=1
HAVE_SUDO=0; command -v sudo >/dev/null 2>&1 && HAVE_SUDO=1
have_root() { [ "$IS_ROOT" -eq 1 ] || [ "$HAVE_SUDO" -eq 1 ]; }
as_root() {
    if [ "$IS_ROOT" -eq 1 ]; then "$@"; else sudo "$@"; fi
}
# Run a command as the postgres OS user (peer authentication on the local socket).
as_postgres() {
    if [ "$IS_ROOT" -eq 1 ]; then
        if command -v runuser >/dev/null 2>&1; then runuser -u postgres -- "$@"
        else su -s /bin/sh postgres -c "$(printf '%q ' "$@")"; fi
    else
        sudo -u postgres "$@"
    fi
}

# ---------------------------------------------------------------------------
# [1/10] Preflight
# ---------------------------------------------------------------------------
step "Checking the host"

UNAME="$(uname -s)"
case "$UNAME" in
    Linux)  OS=linux ;;
    Darwin) OS=macos ;;
    *)      die "Unsupported platform '$UNAME'. Use install_requirements.bat on Windows." ;;
esac

PKG=""
if command -v apt-get >/dev/null 2>&1; then PKG=apt
elif command -v dnf >/dev/null 2>&1; then PKG=dnf
elif command -v brew >/dev/null 2>&1; then PKG=brew
fi

DISTRO="$UNAME"
[ -r /etc/os-release ] && DISTRO="$(. /etc/os-release && echo "$PRETTY_NAME")"
ok "$DISTRO ($OS)"
if [ -n "$PKG" ]; then dim "package manager: $PKG"; else warn "No supported package manager found; system packages must already be installed."; fi
if have_root; then dim "root access: $([ "$IS_ROOT" -eq 1 ] && echo "running as root" || echo "via sudo")"
else warn "No root access - system packages and PostgreSQL setup will be skipped."; fi

APT_UPDATED=0
apt_install() {
    [ "$APT_UPDATED" -eq 0 ] && { info "Refreshing the apt package index..."; as_root env DEBIAN_FRONTEND=noninteractive apt-get update -qq; APT_UPDATED=1; }
    info "Installing: $*"
    as_root env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$@"
}

# ---------------------------------------------------------------------------
# [2/10] Python interpreter
# ---------------------------------------------------------------------------
step "Selecting a Python interpreter"

PY=""
for candidate in python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1 \
       && "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
        PY="$(command -v "$candidate")"
        break
    fi
done
[ -n "$PY" ] || die "No Python 3.10+ interpreter found. Install python3 (apt: 'sudo apt-get install python3') and re-run."

PY_VER="$("$PY" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')"
PY_MM="$("$PY" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
ok "$PY (Python $PY_VER)"

# venv + ensurepip live in a separate Debian package.
if ! "$PY" -c 'import venv, ensurepip' >/dev/null 2>&1; then
    if [ "$PKG" = apt ] && have_root; then
        apt_install "python${PY_MM}-venv" || apt_install python3-venv
    else
        die "Python's venv/ensurepip module is missing. On Debian/Ubuntu: sudo apt-get install python${PY_MM}-venv"
    fi
    "$PY" -c 'import venv, ensurepip' >/dev/null 2>&1 || die "venv is still unavailable after installing python${PY_MM}-venv."
fi
ok "venv and ensurepip available"

# ---------------------------------------------------------------------------
# [3/10] Virtual environment and dependencies
# ---------------------------------------------------------------------------
step "Building the virtual environment and installing dependencies"

VPY="$VENV_DIR/bin/python"
if [ "$RECREATE_VENV" -eq 1 ] && [ -d "$VENV_DIR" ]; then
    info "Removing the existing virtual environment (--recreate-venv)..."
    rm -rf "$VENV_DIR"
fi
if [ ! -x "$VPY" ]; then
    [ -d "$VENV_DIR" ] && { warn "venv/ exists but has no usable interpreter; rebuilding it."; rm -rf "$VENV_DIR"; }
    info "Creating $VENV_DIR ..."
    "$PY" -m venv "$VENV_DIR"
    ok "virtual environment created"
else
    ok "reusing the existing virtual environment"
fi

info "Upgrading pip, setuptools and wheel..."
"$VPY" -m pip install --quiet --upgrade pip setuptools wheel

info "Installing Lune dependencies from requirements.txt (this takes a few minutes)..."
"$VPY" -m pip install --quiet -r "$HERE/requirements.txt"
ok "dependencies installed"

# pip check is advisory here rather than fatal: a transitive pin reported by a
# third-party broker package must not block the database half of the setup.
if PIP_CHECK_OUT="$("$VPY" -m pip check 2>&1)"; then
    ok "dependency tree consistent (pip check)"
else
    warn "pip check reported: $PIP_CHECK_OUT"
fi

# ---------------------------------------------------------------------------
# [4/10] Chromium runtime for the broker login
# ---------------------------------------------------------------------------
step "Installing the Chromium runtime used for broker login"

if [ "$DO_BROWSER" -eq 0 ]; then
    warn "Skipped (--no-browser). Broker login will fail until you run: venv/bin/python -m playwright install chromium"
else
    if [ "$OS" = linux ] && have_root; then
        info "Installing Chromium's system libraries..."
        if ! as_root env "PATH=$PATH" "$VPY" -m playwright install-deps chromium >/dev/null 2>&1; then
            warn "playwright install-deps failed; if the browser will not launch, run it manually: sudo venv/bin/python -m playwright install-deps chromium"
        fi
    elif [ "$OS" = linux ]; then
        warn "No root access, so Chromium's system libraries were not installed."
    fi
    info "Downloading the Chromium build..."
    "$VPY" -m playwright install chromium
    ok "Chromium runtime ready"
fi

# ---------------------------------------------------------------------------
# [5/10] Tailscale
#
# Installed from Tailscale's own apt repository rather than by piping their
# install.sh into a shell: the repo is signed, apt can upgrade it with
# everything else, and re-running this script is a no-op instead of a re-fetch.
# ---------------------------------------------------------------------------
step "Installing Tailscale"

if [ "$DO_TAILSCALE" -eq 0 ]; then
    warn "Skipped (--no-tailscale)."
else
    if command -v tailscale >/dev/null 2>&1; then
        ok "already installed ($(tailscale version 2>/dev/null | head -1))"
    elif [ "$PKG" = apt ] && have_root; then
        TS_ID="$(. /etc/os-release && echo "${ID:-}")"
        TS_CODENAME="$(. /etc/os-release && echo "${VERSION_CODENAME:-}")"
        if [ -z "$TS_CODENAME" ]; then
            warn "No VERSION_CODENAME in /etc/os-release; install Tailscale manually from https://tailscale.com/download"
        else
            info "Adding the Tailscale apt repository for ${TS_ID} ${TS_CODENAME}..."
            as_root curl -fsSL "https://pkgs.tailscale.com/stable/${TS_ID}/${TS_CODENAME}.noarmor.gpg" \
                -o /usr/share/keyrings/tailscale-archive-keyring.gpg
            as_root curl -fsSL "https://pkgs.tailscale.com/stable/${TS_ID}/${TS_CODENAME}.tailscale-keyring.list" \
                -o /etc/apt/sources.list.d/tailscale.list
            APT_UPDATED=0  # a new repository was added; the index must be refreshed
            apt_install tailscale
            ok "installed ($(tailscale version 2>/dev/null | head -1))"
        fi
    elif [ "$PKG" = brew ]; then
        brew install tailscale
    else
        warn "Cannot install Tailscale automatically here; see https://tailscale.com/download"
    fi

    if command -v tailscaled >/dev/null 2>&1 && have_root \
       && [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
        as_root systemctl enable --now tailscaled >/dev/null 2>&1 \
            || warn "Could not enable tailscaled; start it with: sudo systemctl enable --now tailscaled"
    fi

    if command -v tailscale >/dev/null 2>&1; then
        # Joining a tailnet is an account-level action, so it happens only with
        # an auth key the operator supplied - never silently.
        if [ -z "$(tailscale ip -4 2>/dev/null | head -1)" ] && [ -n "${TS_AUTHKEY:-}" ] && have_root; then
            info "Joining the tailnet with the supplied TS_AUTHKEY..."
            as_root tailscale up --authkey "$TS_AUTHKEY" || warn "tailscale up failed; run it manually."
        fi
        TS_IP="$(tailscale ip -4 2>/dev/null | head -1 || true)"
        if [ -n "$TS_IP" ]; then
            # tailscale status --json is pretty-printed ("key": "value"), so it
            # is parsed as JSON rather than grepped for a colon-quote pattern.
            TS_NAME="$(tailscale status --json 2>/dev/null | "$VPY" -c \
                'import json, sys; print((json.load(sys.stdin).get("Self") or {}).get("DNSName", "").rstrip("."))' \
                2>/dev/null || true)"
            if [ -n "$TS_NAME" ]; then
                ok "connected to the tailnet ($TS_IP, $TS_NAME)"
            else
                ok "connected to the tailnet ($TS_IP)"
            fi

            # Unattended operation. There is no `tailscale up --unattended` on
            # Linux - that flag belongs to the Windows/macOS GUI clients, which
            # otherwise run inside a logged-in user session. Here tailscaled is
            # a system daemon, so "unattended" means exactly one thing: it is
            # enabled at boot and needs nobody to log in.
            if systemctl is-enabled tailscaled >/dev/null 2>&1; then
                ok "unattended: tailscaled starts at boot, no user login needed"
            else
                warn "tailscaled is not enabled at boot - this host leaves the tailnet on reboot. Fix: sudo systemctl enable tailscaled"
            fi

            # An expiring node key is the one thing that quietly ends unattended
            # operation months later: the host drops off the tailnet and stays
            # off until a human re-authenticates it. Worth knowing the date now.
            if have_root; then
                TS_EXPIRY="$(as_root tailscale debug netmap 2>/dev/null | "$VPY" -c \
                    'import json, sys; print((json.load(sys.stdin).get("SelfNode") or {}).get("KeyExpiry") or "")' \
                    2>/dev/null || true)"
                if [ -n "$TS_EXPIRY" ]; then
                    warn "Tailscale node key expires $TS_EXPIRY - after that this host is off the tailnet until re-authenticated by hand. Turn on 'Disable key expiry' for it at https://login.tailscale.com/admin/machines"
                else
                    ok "no node-key expiry recorded for this host"
                fi
            fi
        else
            warn "Tailscale is installed but not authenticated. Join with: sudo tailscale up  (or re-run with TS_AUTHKEY=tskey-... ./setup.sh)"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# [6/10] Environment file
#
# Written before the database steps so that this script and the bot resolve the
# same TSDB_* values. A database password must be supplied explicitly.
# ---------------------------------------------------------------------------
step "Preparing the environment file"

if [ ! -f "$HERE/.env.example" ]; then
    cat > "$HERE/.env.example" <<'ENV_EXAMPLE'
# Lune environment. Copy to .env and uncomment only what you need to override.
# Uncomment and set TSDB_PASSWORD before provisioning PostgreSQL.

# --- PostgreSQL -------------------------------------------------------------
# TSDB_HOST=127.0.0.1
# TSDB_PORT=5432
# TSDB_NAME=Lune
# TSDB_USER=lune_bot
# TSDB_PASSWORD=            # required for PostgreSQL
# TSDB_MAINTENANCE_DB=postgres
# TSDB_CONNECT_TIMEOUT=5
# TSDB_AUTO_PROVISION=true  # additive schema creation on connect

# --- Broker credentials -----------------------------------------------------
# NOT here. They live encrypted in secrets/shoonya.cred and lune.service
# decrypts them at start. Set or change a value with:  ./secret.sh set password
# Only for development without systemd: SHOONYA_SECRETS_SOURCE=env plus the
# SHOONYA_* values (USERID, PASSWORD, TOKEN, OAUTH_SECRET, OAUTH_CLIENT_ID or
# VENDOR_CODE) in plaintext here.
# SHOONYA_SECRETS_SOURCE=systemd

# --- Login / runtime --------------------------------------------------------
# SHOONYA_LOGIN_HEADLESS=1  # 0 to watch the login browser
# SHOONYA_BROWSER_PATH=     # explicit Chromium/Edge executable
# LUNE_STATE_DIR=           # default: ~/.local/state/lune on Linux
ENV_EXAMPLE
    ok ".env.example created"
else
    ok ".env.example already present (left untouched)"
fi

if [ ! -f "$HERE/.env" ]; then
    cp "$HERE/.env.example" "$HERE/.env"
    chmod 600 "$HERE/.env"
    ok ".env created from the template; set TSDB_PASSWORD before provisioning PostgreSQL"
else
    chmod 600 "$HERE/.env"
    ok ".env already present (permissions restricted)"
fi

# ---------------------------------------------------------------------------
# Resolve the database settings exactly the way db_connector.py does: explicit
# environment first, then the private .env file.
# ---------------------------------------------------------------------------
eval "$("$VPY" - <<'PYCFG'
import os, shlex

from dotenv import load_dotenv

load_dotenv(".env")  # does not override an already-exported variable, same as the bot

cfg = {
    "PG_HOST": os.getenv("TSDB_HOST", "127.0.0.1"),
    "PG_PORT": os.getenv("TSDB_PORT", "5432"),
    "PG_NAME": os.getenv("TSDB_NAME", "Lune"),
    "PG_USER": os.getenv("TSDB_USER", "lune_bot"),
    "PG_MAINT": os.getenv("TSDB_MAINTENANCE_DB", "postgres"),
    "PG_PASSWORD": os.getenv("TSDB_PASSWORD") or "",
}
for key, value in cfg.items():
    print("%s=%s" % (key, shlex.quote(str(value))))
PYCFG
)"

[ -n "$PG_PASSWORD" ] || die "No database password: set TSDB_PASSWORD in .env or environment."

IS_LOCAL_PG=0
case "$PG_HOST" in
    127.0.0.1|localhost|::1|"") IS_LOCAL_PG=1 ;;
esac
dim "database target: $PG_USER@$PG_HOST:$PG_PORT/$PG_NAME"

# ---------------------------------------------------------------------------
# [7/10] PostgreSQL server
# ---------------------------------------------------------------------------
step "Providing a running PostgreSQL server"

pg_accepting() { pg_isready -h "$PG_HOST" -p "$PG_PORT" -q >/dev/null 2>&1; }

if [ "$DO_POSTGRES" -eq 0 ]; then
    warn "Skipped (--no-postgres)."
elif [ "$IS_LOCAL_PG" -eq 0 ]; then
    ok "$PG_HOST is remote; not installing a local server"
else
    # postgresql-client alone puts psql on PATH without a server, so on Debian
    # the server directory is checked too.
    NEED_PG_INSTALL=0
    command -v psql >/dev/null 2>&1 || NEED_PG_INSTALL=1
    if [ "$OS" = linux ] && ! ls -d /usr/lib/postgresql/*/bin >/dev/null 2>&1; then NEED_PG_INSTALL=1; fi
    if [ "$NEED_PG_INSTALL" -eq 1 ]; then
        if [ "$PKG" = apt ] && have_root; then
            apt_install postgresql postgresql-contrib
        elif [ "$PKG" = brew ]; then
            info "Installing PostgreSQL via Homebrew..."
            brew install postgresql@16 || brew install postgresql
        elif ! command -v psql >/dev/null 2>&1; then
            die "PostgreSQL is not installed and cannot be installed automatically here. Install it, start it, then re-run ./setup.sh"
        fi
    fi
    command -v psql >/dev/null 2>&1 || die "psql is still not on PATH after installation."
    ok "PostgreSQL installed ($(psql --version | awk '{print $3}'))"

    if ! pg_accepting; then
        info "Starting the PostgreSQL service..."
        if [ "$OS" = macos ] && [ "$PKG" = brew ]; then
            brew services start postgresql@16 >/dev/null 2>&1 || brew services start postgresql >/dev/null 2>&1 || true
        elif have_root && [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
            as_root systemctl enable --now postgresql >/dev/null 2>&1 || as_root systemctl start postgresql >/dev/null 2>&1 || true
        elif have_root; then
            # No systemd (container, WSL): drive the Debian cluster wrapper directly.
            as_root service postgresql start >/dev/null 2>&1 || true
            if ! pg_accepting && command -v pg_lsclusters >/dev/null 2>&1; then
                cluster_line="$(pg_lsclusters --no-header 2>/dev/null | head -1 || true)"
                if [ -n "$cluster_line" ]; then
                    as_root pg_ctlcluster "$(echo "$cluster_line" | awk '{print $1}')" "$(echo "$cluster_line" | awk '{print $2}')" start >/dev/null 2>&1 || true
                fi
            fi
        fi
        for _ in $(seq 1 60); do pg_accepting && break; sleep 1; done
    fi
    pg_accepting || die "PostgreSQL is installed but not accepting connections on $PG_HOST:$PG_PORT. Start it and re-run ./setup.sh"
    ok "PostgreSQL is accepting connections on $PG_HOST:$PG_PORT"

    if have_root && [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
        as_root systemctl enable postgresql >/dev/null 2>&1 || true
    fi
fi

# ---------------------------------------------------------------------------
# [8/10] Role and database
# ---------------------------------------------------------------------------
step "Creating the Lune role and database"

# Superuser access over the local socket (peer auth) is what lets us set the
# password that the bot's TCP connections will need.
ADMIN_MODE=""
if [ "$DO_POSTGRES" -eq 1 ] && [ "$IS_LOCAL_PG" -eq 1 ] && have_root \
   && as_postgres psql -p "$PG_PORT" -d "$PG_MAINT" -tAc 'SELECT 1' >/dev/null 2>&1; then
    ADMIN_MODE=peer
elif PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_MAINT" -tAc 'SELECT 1' >/dev/null 2>&1; then
    ADMIN_MODE=tcp
fi

admin_psql() {
    if [ "$ADMIN_MODE" = peer ]; then
        as_postgres psql -p "$PG_PORT" -d "$PG_MAINT" -v ON_ERROR_STOP=1 "$@"
    else
        PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_MAINT" -v ON_ERROR_STOP=1 "$@"
    fi
}

if [ -z "$ADMIN_MODE" ]; then
    warn "Could not reach PostgreSQL as an administrator; skipping role/database creation."
else
    dim "administering via ${ADMIN_MODE} connection"

    # psql interpolates :vars only in input it reads itself, never in a -c
    # string, so every statement carrying a name or a password is fed on stdin.
    # :"var" quotes an identifier, :'var' quotes a literal - which is what keeps
    # a capitalised database name and a password containing '*' correct.
    if [ "$ADMIN_MODE" = peer ]; then
        role_exists="$(admin_psql -tA -v role="$PG_USER" <<'SQL'
SELECT 1 FROM pg_roles WHERE rolname = :'role';
SQL
)"
        if [ -z "$role_exists" ]; then
            admin_psql -q -v role="$PG_USER" -v pw="$PG_PASSWORD" <<'SQL'
CREATE ROLE :"role" WITH LOGIN PASSWORD :'pw';
SQL
            ok "role '$PG_USER' created"
        else
            admin_psql -q -v role="$PG_USER" -v pw="$PG_PASSWORD" <<'SQL'
ALTER ROLE :"role" WITH LOGIN PASSWORD :'pw';
SQL
            ok "role '$PG_USER' password set to the one Lune uses"
        fi
    fi

    db_exists="$(admin_psql -tA -v db="$PG_NAME" <<'SQL'
SELECT 1 FROM pg_database WHERE datname = :'db';
SQL
)"
    if [ -z "$db_exists" ]; then
        admin_psql -q -v db="$PG_NAME" -v role="$PG_USER" <<'SQL'
CREATE DATABASE :"db" OWNER :"role";
SQL
        ok "database \"$PG_NAME\" created (owner: $PG_USER)"
    else
        ok "database \"$PG_NAME\" already exists"
    fi

    # Presentation timezone for psql and any tool that is not the bot's pool.
    # The bot pins IST per connection and refuses to start otherwise; this makes
    # direct psql sessions agree with it.
    if ! admin_psql -q -v db="$PG_NAME" <<'SQL' >/dev/null 2>&1
ALTER DATABASE :"db" SET timezone TO 'Asia/Kolkata';
SQL
    then
        warn "Could not set the database-level IST default (needs ownership); the bot is unaffected."
    fi
fi

# The bot connects over TCP with a password, never over the peer socket. Prove
# that path works, and fix pg_hba.conf only if it is what is blocking it.
if [ "$DO_POSTGRES" -eq 1 ] && [ "$IS_LOCAL_PG" -eq 1 ]; then
    if ! PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_MAINT" -tAc 'SELECT 1' >/dev/null 2>&1; then
        warn "Password authentication over TCP failed; inspecting pg_hba.conf"
        if [ "$ADMIN_MODE" = peer ]; then
            HBA="$(as_postgres psql -p "$PG_PORT" -d "$PG_MAINT" -tAc 'SHOW hba_file')"
            if [ -n "$HBA" ] && ! as_root grep -Eq '^[[:space:]]*host[[:space:]]+all[[:space:]]+all[[:space:]]+127\.0\.0\.1/32[[:space:]]+(scram-sha-256|md5)' "$HBA"; then
                as_root cp -a "$HBA" "$HBA.lune.bak.$(date +%Y%m%d%H%M%S)"
                printf '\n# Added by Lune setup.sh - the bot connects over TCP with a password.\nhost    all             all             127.0.0.1/32            scram-sha-256\nhost    all             all             ::1/128                 scram-sha-256\n' \
                    | as_root tee -a "$HBA" >/dev/null
                as_postgres psql -p "$PG_PORT" -d "$PG_MAINT" -tAc 'SELECT pg_reload_conf()' >/dev/null
                info "Appended local password rules to $HBA (original backed up) and reloaded."
            fi
        fi
        if PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_MAINT" -tAc 'SELECT 1' >/dev/null 2>&1; then
            ok "password authentication over TCP now works"
        else
            die "The bot's connection path ($PG_USER@$PG_HOST:$PG_PORT, password) still fails. Check pg_hba.conf and the role password."
        fi
    else
        ok "password authentication over TCP works"
    fi
fi

# ---------------------------------------------------------------------------
# [9/10] Application schema
#
# Provisioned by the project's own code (db_connector.ensure_schema), so the
# schema this script creates can never drift from the one the bot expects.
# ---------------------------------------------------------------------------
step "Provisioning the botdata schema"

if "$VPY" - <<'PYSCHEMA'
import logging
import sys

logging.basicConfig(level=logging.WARNING, format="      %(levelname)s %(message)s")

from dotenv import load_dotenv

load_dotenv(".env")

from db_connector import TimescaleDBConnector

connector = TimescaleDBConnector(minconn=1, maxconn=2, attempts=3)
connector.connect()
try:
    report = connector.ensure_schema()
    if report is None:
        print("      schema check completed")
    elif report.changed:
        for label, values in (
            ("tables", report.tables_created),
            ("columns", report.columns_added),
            ("constraints", report.constraints_created),
            ("indexes", report.indexes_created),
            ("functions", report.functions_created),
            ("triggers", report.triggers_created),
        ):
            if values:
                print("      + %s: %s" % (label, ", ".join(values)))
        if report.schema_created:
            print("      + schema botdata created")
    else:
        print("      schema already up to date")
finally:
    connector.close()
sys.exit(0)
PYSCHEMA
then
    ok "botdata schema ready"
else
    die "Schema provisioning failed. The database is reachable but the botdata schema could not be applied (see the error above)."
fi

# ---------------------------------------------------------------------------
# [10/10] Verification
# ---------------------------------------------------------------------------
step "Verifying the installation"

# The import sweep separates two very different failures: a dependency that did
# not install (fatal - setup did not do its job) and a project module that is
# simply not present in this directory (reported, but not this script's to fix).
if VERIFY_OUT="$("$VPY" - <<'PYVERIFY' 2>&1
import importlib
import re
import sys
from zoneinfo import ZoneInfo

# Third-party packages the project imports lazily, so importing the modules
# below would not prove they are installed.
lazy = [
    "playwright.sync_api", "pyotp", "psutil", "pandas",
    "flask", "websocket", "requests", "psycopg2", "dotenv", "NorenRestApiPy",
    "pyarrow", "numba",
]
# Project modules. Entry points (main.py, market_hours_runner.py, login_test.py)
# are excluded: importing them configures logging and creates logs/ as a side
# effect, which a verification run has no business doing.
project = [
    "config", "ist", "log_throttle", "db_connector", "db_loggers",
    "database_backup", "data_handler", "data_csv", "getdata",
    "instruments", "strategy_logic",
    "live_server", "live_websocket",
    "shoonya_api", "broker_secrets", "shoonya_login", "trade_manager",
]

# Distribution names pip was asked to install - anything else that turns up
# missing is a piece of the project, not a dependency.
required = set()
with open("requirements.txt", encoding="utf-8") as handle:
    for line in handle:
        line = line.split("#", 1)[0].strip()
        if line:
            required.add(re.split(r"[<>=!~\[; ]", line, 1)[0].strip().lower().replace("-", "_"))

broken, absent = [], {}
for name in lazy + project:
    try:
        importlib.import_module(name)
    except ModuleNotFoundError as exc:
        missing = (exc.name or name).split(".")[0]
        if missing.lower().replace("-", "_") in required or name in lazy:
            broken.append("%s (missing module %r)" % (name, missing))
        else:
            absent.setdefault(missing, []).append(name)
    except Exception as exc:  # noqa: BLE001
        broken.append("%s (%s: %s)" % (name, type(exc).__name__, exc))

ZoneInfo("Asia/Kolkata")  # tzdata must resolve IST

for item in broken:
    print("fatal:%s" % item)
for missing, importers in sorted(absent.items()):
    print("warn:%r is not present in this directory, so %s cannot be imported."
          % (missing, ", ".join(sorted(importers))))
print("ok:%d of %d modules imported cleanly; Asia/Kolkata resolves"
      % (len(lazy) + len(project) - len(broken) - sum(len(v) for v in absent.values()),
         len(lazy) + len(project)))
sys.exit(1 if broken else 0)
PYVERIFY
)"; then
    :
else
    printf '%s\n' "$VERIFY_OUT" | sed 's/^fatal:/      x /'
    die "Import verification failed - a dependency is missing or broken."
fi
while IFS= read -r line; do
    case "$line" in
        warn:*) warn "${line#warn:}" ;;
        ok:*)   ok "${line#ok:}" ;;
        "")     ;;
        *)      info "$line" ;;
    esac
done <<< "$VERIFY_OUT"

if [ "$DO_BROWSER" -eq 1 ]; then
    if "$VPY" -c "
import pathlib, sys
from playwright.sync_api import sync_playwright
with sync_playwright() as pw:
    sys.exit(0 if pathlib.Path(pw.chromium.executable_path).exists() else 1)
" >/dev/null 2>&1; then
        ok "Chromium executable present"
    else
        warn "Chromium is not installed where Playwright expects it; run: venv/bin/python -m playwright install chromium"
    fi
fi

if [ ! -f "$HERE/templates/index.html" ]; then
    warn "templates/index.html is missing, so the operator UI at / will 500 (the JSON routes still work). This file is not in the project directory."
fi

# Credential readiness. The broker credentials live encrypted in
# secrets/shoonya.cred (see broker_secrets.py) and lune.service decrypts them at
# start, so both the file and the unit's LoadCredentialEncrypted= must exist.
CRED_OUT="$("$VPY" - <<'PYCRED' 2>&1 || true
import subprocess

import broker_secrets

if broker_secrets.source_mode() == "env":
    try:
        broker_secrets._resolve_from_env()
    except broker_secrets.SecretsError as exc:
        print("warn:SHOONYA_SECRETS_SOURCE=env, but %s" % exc)
    else:
        print("ok:broker credentials present in .env (plaintext; development only)")
else:
    if broker_secrets.CREDENTIAL_FILE.exists():
        print("ok:encrypted broker credentials present (%s)" % broker_secrets.CREDENTIAL_FILE)
        print("info:Confirm they resolve: ./secret.sh run login_test.py --check-secrets")
    else:
        print("warn:no broker credentials yet. Add them (stored encrypted) one at a time: "
              "./secret.sh set userid, then password, token, oauth_secret, vendor_code")
    unit = subprocess.run(["systemctl", "cat", "lune.service"], capture_output=True, text=True)
    wanted = "LoadCredentialEncrypted=%s:%s" % (broker_secrets.CREDENTIAL_NAME, broker_secrets.CREDENTIAL_FILE)
    if unit.returncode == 0 and wanted not in unit.stdout:
        print("warn:lune.service does not load the credentials, so the bot cannot log in. "
              "Add under [Service]: %s" % wanted)
PYCRED
)"
while IFS= read -r line; do
    case "$line" in
        warn:*) warn "${line#warn:}" ;;
        ok:*)   ok "${line#ok:}" ;;
        info:*) info "${line#info:}" ;;
        "")     ;;
        *)      info "$line" ;;
    esac
done <<< "$CRED_OUT"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
printf '\n%s================================================================%s\n' "$C_BOLD" "$C_RESET"
printf '%s Lune setup complete.%s\n' "$C_BOLD$C_GREEN" "$C_RESET"
printf '%s================================================================%s\n\n' "$C_BOLD" "$C_RESET"
printf '  Python        %s (%s)\n' "$PY_VER" "$VENV_DIR"
printf '  Database      %s@%s:%s/%s (schema: botdata)\n' "$PG_USER" "$PG_HOST" "$PG_PORT" "$PG_NAME"
printf '  Environment   %s\n\n' "$HERE/.env"

if [ "${#WARNINGS[@]}" -gt 0 ]; then
    printf '%sWarnings (%d):%s\n' "$C_YELLOW$C_BOLD" "${#WARNINGS[@]}" "$C_RESET"
    for w in "${WARNINGS[@]}"; do printf '  ! %s\n' "$w"; done
    printf '\n'
fi

# If the systemd unit is installed it, not the operator, owns the bot: running
# main.py by hand alongside it means two processes logging in to the same broker
# account. Say so rather than printing a command that would cause that.
if systemctl list-unit-files lune.service >/dev/null 2>&1; then
    cat <<SUMMARY
Next steps:
  1. Add broker credentials (stored encrypted), one at a time:
                             ./secret.sh set userid   (then password, token,
                             oauth_secret, vendor_code)
  2. Check the login path:   ./secret.sh run login_test.py --check-secrets
  3. lune.service is installed ($(systemctl is-active lune.service), $(systemctl is-enabled lune.service)) and
     runs market_hours_runner.py for you:
                             sudo systemctl status lune.service
                             journalctl -u lune.service -f

  Do NOT also run main.py by hand while the service is up - two processes would
  log in to the same broker account. Stop it first: sudo systemctl stop lune.service

  Activate the environment for interactive work with:  source venv/bin/activate
SUMMARY
else
    cat <<SUMMARY
Next steps:
  1. Add broker credentials (stored encrypted), one at a time:
                             ./secret.sh set userid   (then password, token,
                             oauth_secret, vendor_code)
  2. Check the login path:   venv/bin/python login_test.py --status
  3. Run the bot (TEST mode as set in main.py) with the credentials loaded:
                             ./secret.sh run main.py
     or, market-hours supervised:
                             ./secret.sh run market_hours_runner.py

  Activate the environment for interactive work with:  source venv/bin/activate
SUMMARY
fi
