"""`caplog`, for the unittest suite.

`unittest.TestCase.assertLogs` is checked *after* `logging.disable`, and
`run_checks.py` silences the whole suite with `logging.disable(logging.CRITICAL)`
so its verdict stays readable. Under that, `assertLogs` fails even when the line
was emitted, and `assertNoLogs` passes even when it was - so a test written with
either is red or vacuously green depending on how it was launched.

Several of these tests exist specifically to prove a refusal is announced loudly
enough for `main.py`'s WARNING handler to keep it. That property has to be testable
from the same command that gates a restart, so the capture lifts the global
disable for its own window and puts it back afterwards. It cannot hide a record:
while attached it is the only handler the logger has.
"""

from __future__ import annotations

import logging


class CapturedLogs(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def at_least(self, level: int) -> list[logging.LogRecord]:
        return [r for r in self.records if r.levelno >= level]

    def messages(self, level: int = logging.DEBUG) -> list[str]:
        return [r.getMessage() for r in self.at_least(level)]

    def text(self, level: int = logging.DEBUG) -> str:
        return "\n".join(self.messages(level))

    def has(self, needle: str, level: int = logging.INFO) -> bool:
        return any(needle.lower() in m.lower() for m in self.messages(level))


class capture_logs:
    """Context manager: collect everything `loggers` emit, then restore them."""

    def __init__(self, *loggers: logging.Logger) -> None:
        self.loggers = loggers
        self.handler = CapturedLogs()
        self._previous: list[tuple[logging.Logger, int, bool, bool, list]] = []
        self._disabled = logging.NOTSET

    def __enter__(self) -> CapturedLogs:
        self._disabled = logging.root.manager.disable
        logging.disable(logging.NOTSET)
        for logger in self.loggers:
            self._previous.append(
                (logger, logger.level, logger.disabled, logger.propagate,
                 logger.handlers[:])
            )
            # The capture is the *only* sink while attached. These tests drive
            # refusal paths on purpose, so the records are the assertion rather
            # than a problem - and lifting the suite-wide `logging.disable`
            # above would otherwise let every one of them out to whatever
            # handler root happens to have picked up, burying `run_checks.py`'s
            # verdict in the output it exists to summarise.
            logger.handlers = [self.handler]
            logger.setLevel(logging.DEBUG)
            logger.disabled = False
            logger.propagate = False
        return self.handler

    def __exit__(self, *exc) -> None:
        for logger, level, disabled, propagate, handlers in self._previous:
            logger.handlers = handlers
            logger.setLevel(level)
            logger.disabled = disabled
            logger.propagate = propagate
        self._previous.clear()
        logging.disable(self._disabled)
