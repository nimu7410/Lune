"""Lossless, validated CSV backups coupled safely to table truncation."""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Iterable

from psycopg2 import sql

# Tables carrying no trade-resume state. order_logs/order_attempts already hold
# everything a restart needs, and a resumed process gets fresh rows in both tables
# again within moments of reconnecting, so these are truncated on every clean
# exit regardless of what is open.
SESSION_TABLES = ("forecast_logs", "ws_orderupdates")

# Tables that are the *only* record a restart has of exposure at the broker.
RESUME_TABLES = ("trade_logs", "order_logs", "order_attempts")


def has_unresolved_orders(db) -> bool:
    """Keep the resume ledger whenever an order may still affect exposure."""
    return bool(db.execute(
        """SELECT 1 FROM botdata.order_logs AS o
           WHERE o.lifecycle_state IN
               ('ENTRY_PENDING','EXIT_PENDING','VERIFYING_FLAT','QUARANTINED')
              OR (o.lifecycle_state='OPEN' AND NOT EXISTS (
                  SELECT 1 FROM botdata.trade_logs AS t
                  WHERE t.is_in_trade=TRUE
                    AND t.buy_orderno=o.buy_orderno
                    AND t.sell_orderno=o.sell_orderno
              ))
           LIMIT 1;""",
        fetch=True,
    ) or db.execute(
        """SELECT 1 FROM botdata.order_attempts
           WHERE status IN ('INTENT','UNKNOWN','ACKNOWLEDGED','OPEN','PENDING','TRIGGER_PENDING')
           LIMIT 1;""",
        fetch=True,
    ))


def tables_to_truncate(*, open_trade: bool, unresolved_orders: bool) -> list[str]:
    """Which tables a shutdown may clear.

    Two conditions retain the resume tables, and both mean the same thing: the
    broker may be holding something this process is about to stop watching.

    * `open_trade` - an `is_in_trade` row. v3 holds its positions overnight,
      so this is the *normal* case rather than the exceptional one: the position
      is held through the shutdown by design, and the resume tables are only
      archived on the nights the rule declines to trade. That is intended - the
      position is real, and the row is what recovers it in the morning.

    * `unresolved_orders` - a non-terminal order log, an unmatched OPEN order
      log, or an unresolved broker attempt. A flatten attempt can have no
      order-log row, yet its client reference is needed after restart.

    Only the tables actually being truncated are backed up: archiving a table
    that survives would append the same rows again on the next shutdown and
    duplicate them in the CSV.
    """
    if open_trade or unresolved_orders:
        return list(SESSION_TABLES)
    return [*SESSION_TABLES, *RESUME_TABLES]


def _validate_archive(
    path: Path, expected_header: list[str], expected_rows: int
) -> None:
    """Fully read and parse an archive before its source can be truncated."""
    with open(path, "r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if header != expected_header:
            raise RuntimeError(
                f"backup header mismatch for {path.name}: {header!r} != {expected_header!r}"
            )
        actual_rows = sum(1 for _ in reader)
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"backup row-count mismatch for {path.name}: "
            f"expected {expected_rows}, validated {actual_rows}"
        )


def _existing_archive_info(
    path: Path
) -> tuple[list[str] | None, int, int | None]:
    if not path.exists():
        return None, 0, None
    with open(path, "r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if not header or "id" not in header:
            return header, sum(1 for _ in reader), None
        id_index = header.index("id")
        row_count = 0
        maximum_id = None
        for row in reader:
            row_count += 1
            row_id = int(row[id_index])
            maximum_id = row_id if maximum_id is None else max(maximum_id, row_id)
        return header, row_count, maximum_id


def _resolve_archive_path(
    backup_dir: Path, table: str, date_str: str, columns: list[str]
) -> tuple[Path, list[str] | None, int, int | None]:
    """Pick the archive this run may append to, rolling on a schema change.

    Same-day runs share one file, which only works while the header matches.
    Additive schema provisioning can add a column between two runs on the same
    day (a restart is all it takes), and the earlier file then has a genuinely
    different, un-appendable shape.

    Failing there would be the worst outcome available: `_archive_table` runs
    inside the shutdown path, so the exception would abort the whole backup,
    skip the truncate, and leave the day unarchived - all to avoid writing a
    second file. So roll to a sibling instead. Both files are kept, both stay
    individually valid, and no row is lost.
    """
    candidate = backup_dir / f"{table}_{date_str}.csv"
    revision = 1
    while True:
        header, rows, maximum_id = _existing_archive_info(candidate)
        if header is None or header == columns:
            return candidate, header, rows, maximum_id
        revision += 1
        candidate = backup_dir / f"{table}_{date_str}.r{revision}.csv"


def _archive_table(conn, table: str, backup_dir: Path, date_str: str) -> tuple[int, Path | None]:
    """Stream one locked table to an atomically published, appendable CSV."""
    temp_path = backup_dir / f".{table}_{date_str}.{uuid.uuid4().hex}.tmp"
    exported_rows = 0
    try:
        # The column list has to come from a client-side cursor. A *named*
        # (server-side) cursor leaves `description` as None until its first row
        # is fetched, so reading it straight after `execute()` raised
        # "'NoneType' object is not iterable" and aborted the whole backup -
        # every table, every shutdown - before a single row was written.
        # Describing first, on its own cursor, also keeps the streaming cursor
        # untouched: `SELECT *` expands to the same column order in both.
        with conn.cursor() as describe:
            describe.execute(
                sql.SQL("SELECT * FROM botdata.{} LIMIT 0").format(sql.Identifier(table))
            )
            columns = [description.name for description in describe.description]

        final_path, existing_header, existing_rows, existing_max_id = (
            _resolve_archive_path(backup_dir, table, date_str, columns)
        )
        if final_path.name != f"{table}_{date_str}.csv":
            logging.warning(
                "%s columns changed since an earlier archive today; writing %s "
                "instead of appending to a file with a different header.",
                table,
                final_path.name,
            )

        with conn.cursor(name=f"backup_{table}_{uuid.uuid4().hex}") as cursor:
            cursor.itersize = 10_000
            cursor.execute(
                sql.SQL("SELECT * FROM botdata.{} ORDER BY id").format(sql.Identifier(table))
            )
            def write_rows(writer) -> int:
                written = 0
                if existing_header is None:
                    writer.writerow(columns)
                for row in cursor:
                    row_id = row["id"] if isinstance(row, Mapping) else row[columns.index("id")]
                    # A prior run may have published this archive and then
                    # rolled the DB transaction back because a later table
                    # failed. Skip those already durable IDs on retry.
                    if existing_max_id is not None and int(row_id) <= existing_max_id:
                        continue
                    writer.writerow(
                        [row[column] for column in columns]
                        if isinstance(row, Mapping)
                        else list(row)
                    )
                    written += 1
                return written

            # Copy the published archive, then append this run's rows.
            fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
            if final_path.exists():
                shutil.copyfile(final_path, temp_path)
            with open(temp_path, "ab") as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
                exported_rows = write_rows(csv.writer(text))
                text.flush()
                text.detach()
                raw.flush()
                os.fsync(raw.fileno())

        if exported_rows == 0:
            temp_path.unlink(missing_ok=True)
            return 0, None
        _validate_archive(temp_path, columns, existing_rows + exported_rows)
        os.replace(temp_path, final_path)
        return exported_rows, final_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def backup_and_truncate_tables(
    connector, tables: Iterable[str], backup_dir: Path, date_str: str
) -> list[tuple[str, int, Path | None]]:
    """Back up all tables and truncate only after every archive validates."""
    table_names = tuple(tables)
    if not table_names:
        return []
    backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name != "nt":
        backup_dir.chmod(0o700)
    conn = connector.get_raw_connection()
    reports: list[tuple[str, int, Path | None]] = []
    try:
        with conn.cursor() as cursor:
            identifiers = sql.SQL(", ").join(
                sql.Identifier("botdata", table) for table in table_names
            )
            cursor.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(identifiers))
        for table in table_names:
            row_count, path = _archive_table(conn, table, backup_dir, date_str)
            reports.append((table, row_count, path))
        with conn.cursor() as cursor:
            identifiers = sql.SQL(", ").join(
                sql.Identifier("botdata", table) for table in table_names
            )
            cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(identifiers))
        conn.commit()
        return reports
    except Exception:
        conn.rollback()
        raise
    finally:
        connector.put_raw_connection(conn)
