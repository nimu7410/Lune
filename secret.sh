#!/usr/bin/env bash
# Lune broker credentials, kept encrypted in secrets/shoonya.cred.
#
#   ./secret.sh set password       change one value (asks for it twice, hidden)
#   ./secret.sh run login_test.py --check-secrets
#                                  run a project script with the credentials loaded
#
# Keys: userid password token oauth_secret oauth_client_id vendor_code api_secret imei
# (or the full SHOONYA_* name). A change takes effect on: sudo systemctl restart lune
#
# The file is encrypted with this host's systemd key (root-only), so it can only
# be decrypted on this machine, by root. lune.service decrypts it at start into
# a private in-memory directory; the plaintext never touches disk.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAME=shoonya
FILE="$HERE/secrets/$NAME.cred"
KEYS="USERID PASSWORD TOKEN OAUTH_SECRET OAUTH_CLIENT_ID VENDOR_CODE API_SECRET IMEI"

# systemd-creds warns on every call that the host key is not on encrypted media;
# that is expected on this VM and says nothing about the operation.
creds() { sudo systemd-creds "$@" 2> >(grep -v 'not located on encrypted media' >&2); }

usage() { sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2; }

case "${1:-}" in
set)
    [ $# -eq 2 ] || usage
    key="$(printf '%s' "$2" | tr '[:lower:]-' '[:upper:]_')"
    key="${key#SHOONYA_}"
    case " $KEYS " in *" $key "*) key="SHOONYA_$key" ;; *) echo "Unknown key: $2" >&2; usage ;; esac

    sudo true  # ask for the sudo password (if any) before the value prompts
    IFS= read -rsp "New value for $key: " value; echo
    IFS= read -rsp "Repeat it: " again; echo
    [ -n "$value" ] || { echo "Empty value; nothing changed." >&2; exit 1; }
    [ "$value" = "$again" ] || { echo "The two entries differ; nothing changed." >&2; exit 1; }

    mkdir -p "$HERE/secrets"
    chmod 700 "$HERE/secrets"
    tmp="$FILE.new"
    trap 'sudo rm -f "$tmp"' EXIT
    {
        if sudo test -f "$FILE"; then creds decrypt --name="$NAME" "$FILE" -; else printf '{}'; fi
        printf '\n%s\n%s' "$key" "$value"
    } | python3 -c '
import json, sys
blob, key, value = sys.stdin.read().rsplit("\n", 2)
data = json.loads(blob)
data[key] = value
missing = [k for k in ("SHOONYA_USERID", "SHOONYA_PASSWORD", "SHOONYA_TOKEN", "SHOONYA_OAUTH_SECRET") if not data.get(k)]
if not (data.get("SHOONYA_OAUTH_CLIENT_ID") or data.get("SHOONYA_VENDOR_CODE")):
    missing.append("SHOONYA_OAUTH_CLIENT_ID or SHOONYA_VENDOR_CODE")
if missing:
    print("Still missing before the bot can log in: " + ", ".join(missing), file=sys.stderr)
sys.stdout.write(json.dumps(dict(sorted(data.items())), separators=(",", ":")))
' | creds encrypt --name="$NAME" - "$tmp"
    creds decrypt --name="$NAME" "$tmp" - >/dev/null
    sudo chmod 600 "$tmp"
    sudo mv -f "$tmp" "$FILE"
    echo "Saved $key. Apply it with: sudo systemctl restart lune"
    ;;
run)
    shift
    [ $# -ge 1 ] || usage
    sudo test -f "$FILE" || { echo "No $FILE yet; create it with ./secret.sh set <key>." >&2; exit 1; }
    exec sudo systemd-run --quiet --pipe --wait --collect \
        --uid="$(id -u)" --gid="$(id -g)" \
        -p WorkingDirectory="$HERE" \
        -p LoadCredentialEncrypted="$NAME:$FILE" \
        -E PYTHONUNBUFFERED=1 \
        -- "$HERE/venv/bin/python" "$@"
    ;;
*)
    usage
    ;;
esac
