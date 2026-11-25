#!/usr/bin/env bash
# Starts the tick backtester (systemd user service lune-backtester) and opens the browser.
# ./run.sh stop | restart | log
cd "$(dirname "$0")"
URL="http://127.0.0.1:8060"
case "$1" in
  stop) systemctl --user stop lune-backtester.service && echo "Stopped"; exit 0 ;;
  restart) systemctl --user restart lune-backtester.service ;;
  log) exec journalctl --user -u lune-backtester.service -f ;;
  *) systemctl --user start lune-backtester.service ;;
esac
for _ in $(seq 80); do curl -s -o /dev/null "$URL/api/meta" && break; sleep 0.25; done
curl -s -o /dev/null "$URL/api/meta" || { echo "Failed to start: ./run.sh log"; exit 1; }
xdg-open "$URL" >/dev/null 2>&1 &
echo "Tick backtester running at $URL (tailnet: http://free-a1.tail57c420.ts.net:8060)"
