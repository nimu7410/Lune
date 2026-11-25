# log_throttle.py
"""Rate-limited logging for conditions evaluated on a per-second poll.

Why this exists
---------------
A condition that blocks trading is checked once per poll - once a second, in
this bot. Logging it at its real severity every time would drown the log, and
the usual reaction to that pressure is to demote it to DEBUG. That is exactly
how a trade-blocking condition becomes invisible: `main.py` configures logging
at INFO, so a DEBUG line is not written at all, and a bot that silently refuses
every entry looks identical to a strategy that chose not to trade.

This keeps such conditions at the severity they deserve while emitting at most
one line per interval per key, and reports how many occurrences were folded
into that line so the operator can see it is persistent rather than a blip.
"""

from __future__ import annotations

import threading
import time

DEFAULT_INTERVAL_SEC = 60.0


class LogThrottle:
    """One emit per `interval` per key, counting what was suppressed between."""

    def __init__(self, interval_sec: float = DEFAULT_INTERVAL_SEC) -> None:
        self.interval = float(interval_sec)
        self._lock = threading.Lock()
        self._last: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def should_log(self, key: str) -> tuple[bool, int]:
        """Return (emit_now, occurrences_suppressed_since_the_last_emit)."""
        now = time.monotonic()
        with self._lock:
            last = self._last.get(key)
            if last is None or (now - last) >= self.interval:
                self._last[key] = now
                return True, self._suppressed.pop(key, 0)
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return False, 0

    def reset(self, key: str) -> None:
        """Forget `key`, so the next occurrence logs immediately.

        Called when the condition clears, so a problem that returns after a
        quiet spell is reported at once instead of waiting out the interval.
        """
        with self._lock:
            self._last.pop(key, None)
            self._suppressed.pop(key, None)


def suffix(folded: int) -> str:
    """Human tail for a throttled line, empty when nothing was suppressed."""
    return "" if folded <= 0 else f" ({folded} further occurrences suppressed)"
