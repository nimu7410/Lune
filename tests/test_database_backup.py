import csv
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import database_backup


class ResumePolicyTests(unittest.TestCase):
    def test_price_history_is_retired_without_removing_other_tables(self):
        from db_connector import DROPPED_TABLES, INDEXES, TABLES

        self.assertIn("websocket", DROPPED_TABLES)
        self.assertEqual(
            {table.name for table in TABLES},
            {"forecast_logs", "ws_orderupdates", "trade_logs", "order_logs", "order_attempts"},
        )
        self.assertNotIn("websocket", {index.table for index in INDEXES})
        for open_trade in (False, True):
            for unresolved_orders in (False, True):
                tables = database_backup.tables_to_truncate(
                    open_trade=open_trade, unresolved_orders=unresolved_orders,
                )
                expected = ["forecast_logs", "ws_orderupdates"]
                if not open_trade and not unresolved_orders:
                    expected += ["trade_logs", "order_logs", "order_attempts"]
                self.assertEqual(tables, expected)

    def test_unresolved_attempt_without_order_log_keeps_resume_tables(self):
        db = Mock()
        db.execute.side_effect = [[], [{"?column?": 1}]]
        unresolved = database_backup.has_unresolved_orders(db)

        self.assertTrue(unresolved)
        self.assertEqual(
            database_backup.tables_to_truncate(open_trade=False, unresolved_orders=unresolved),
            list(database_backup.SESSION_TABLES),
        )
        queries = [call.args[0] for call in db.execute.call_args_list]
        self.assertIn("lifecycle_state='OPEN' AND NOT EXISTS", queries[0])
        self.assertIn("FROM botdata.order_attempts", queries[1])
        for state in ("INTENT", "UNKNOWN", "ACKNOWLEDGED", "OPEN", "PENDING", "TRIGGER_PENDING"):
            self.assertIn(state, queries[1])


class _Description:
    def __init__(self, name):
        self.name = name


class _Cursor:
    """Models psycopg2's two cursor kinds, including how they differ.

    A *named* (server-side) cursor leaves `description` as None until its first
    row is fetched; a client-side one populates it on `execute()`. The original
    fake set it eagerly for both, which is why a real backup that read
    `description` straight off a named cursor failed in production against
    every table on every shutdown while these tests stayed green.
    """

    def __init__(self, rows=(), columns=("id", "payload"), named=False):
        self.rows = list(rows)
        self.columns = [_Description(name) for name in columns]
        self.named = named
        self.description = None if named else list(self.columns)
        self.itersize = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _statement):
        self.description = None if self.named else list(self.columns)
        return None

    def __iter__(self):
        # Fetching is what makes a named cursor describe itself.
        self.description = list(self.columns)
        return iter(self.rows)


class _Connection:
    def __init__(self, rows=(), columns=("id", "payload")):
        self.rows = rows
        self.columns = columns
        self.committed = False
        self.rolled_back = False

    def cursor(self, name=None):
        return _Cursor(self.rows if name else (), self.columns, named=name is not None)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _Connector:
    def __init__(self, conn):
        self.conn = conn
        self.returned = False

    def get_raw_connection(self):
        return self.conn

    def put_raw_connection(self, conn):
        self.returned = conn is self.conn


class ArchiveFormatTests(unittest.TestCase):
    """All remaining tables stay plain CSV."""

    def test_tables_are_plain_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for table in ("forecast_logs", "trade_logs", "order_logs",
                          "order_attempts", "ws_orderupdates"):
                connection = _Connection([{"id": 1, "payload": "alpha"}])
                count, path = database_backup._archive_table(
                    connection, table, root, "20260816"
                )
                self.assertEqual(count, 1)
                self.assertEqual(path.name, f"{table}_20260816.csv")
                if os.name != "nt":
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                # Plain text: readable without decompressing.
                with open(path, "r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.reader(handle))
                self.assertEqual(rows, [["id", "payload"], ["1", "alpha"]])

    def test_plain_csv_appends_within_the_same_day(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = _Connection([{"id": 1, "payload": "alpha"}])
            database_backup._archive_table(first, "forecast_logs", root, "20260816")
            second = _Connection([{"id": 1, "payload": "alpha"}, {"id": 2, "payload": "β,quoted"}])
            count, path = database_backup._archive_table(
                second, "forecast_logs", root, "20260816"
            )
            # id 1 is already durable, so only the new row is appended.
            self.assertEqual(count, 1)
            with open(path, "r", encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))
            self.assertEqual(rows, [["id", "payload"], ["1", "alpha"], ["2", "β,quoted"]])

    def test_plain_csv_rolls_to_a_sibling_when_columns_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_backup._archive_table(
                _Connection([{"id": 1, "payload": "alpha"}]), "forecast_logs", root, "20260816"
            )
            widened = _Connection(
                [{"id": 2, "payload": "beta", "extra": "x"}],
                columns=("id", "payload", "extra"),
            )
            _count, path = database_backup._archive_table(
                widened, "forecast_logs", root, "20260816"
            )
            self.assertEqual(path.name, "forecast_logs_20260816.r2.csv")


class CSVBackupTests(unittest.TestCase):
    def test_named_cursor_without_a_description_still_archives(self):
        """Regression: this failure silently disabled backup+truncate.

        `cursor.description` is None on a server-side cursor until the first
        fetch. Reading it there raised "'NoneType' object is not iterable",
        `main._backup_and_clear` swallowed it, and the bot ran for days with
        nothing archived and nothing truncated.
        """
        with tempfile.TemporaryDirectory() as directory:
            connection = _Connection([{"id": 1, "payload": "alpha"}])
            # The precondition that broke it, asserted rather than assumed.
            self.assertIsNone(connection.cursor(name="streaming").description)
            self.assertIsNotNone(connection.cursor().description)

            count, path = database_backup._archive_table(
                connection, "ws_orderupdates", Path(directory), "20260814"
            )

            self.assertEqual(count, 1)
            with open(path, "r", encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))
            self.assertEqual(rows[0], ["id", "payload"])
            self.assertEqual(rows[1], ["1", "alpha"])

    def test_archive_preserves_header_rows_columns_and_appends(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = _Connection([{"id": 1, "payload": "alpha"}, {"id": 2, "payload": "β,quoted"}])
            count, path = database_backup._archive_table(first, "ws_orderupdates", root, "20260814")
            self.assertEqual(count, 2)

            second = _Connection([{"id": 3, "payload": "line\nbreak"}])
            count, _ = database_backup._archive_table(second, "ws_orderupdates", root, "20260814")
            self.assertEqual(count, 1)

            with open(path, "r", encoding="utf-8", newline="") as stream:
                rows = list(csv.reader(stream))
            self.assertEqual(rows, [
                ["id", "payload"],
                ["1", "alpha"],
                ["2", "β,quoted"],
                ["3", "line\nbreak"],
            ])
            self.assertFalse(list(root.glob(".*.tmp")))

    def test_backup_failure_rolls_back_and_never_commits_truncate(self):
        conn = _Connection()
        connector = _Connector(conn)
        with tempfile.TemporaryDirectory() as directory, patch(
            "database_backup._archive_table", side_effect=OSError("disk full")
        ):
            with self.assertRaisesRegex(OSError, "disk full"):
                database_backup.backup_and_truncate_tables(
                    connector, ["ws_orderupdates"], Path(directory), "20260814"
                )
        self.assertTrue(conn.rolled_back)
        self.assertFalse(conn.committed)
        self.assertTrue(connector.returned)

    def test_retry_skips_rows_already_published_before_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = _Connection([{"id": 1, "payload": "one"}, {"id": 2, "payload": "two"}])
            database_backup._archive_table(original, "ws_orderupdates", root, "20260814")
            retry = _Connection([
                {"id": 1, "payload": "one"},
                {"id": 2, "payload": "two"},
                {"id": 3, "payload": "three"},
            ])
            count, path = database_backup._archive_table(
                retry, "ws_orderupdates", root, "20260814"
            )
            self.assertEqual(count, 1)
            with open(path, "r", encoding="utf-8", newline="") as stream:
                rows = list(csv.reader(stream))
            self.assertEqual([row[0] for row in rows[1:]], ["1", "2", "3"])

    def test_added_column_rolls_to_a_sibling_instead_of_failing_shutdown(self):
        """Additive provisioning must not be able to abort the backup.

        `_archive_table` runs in the shutdown path: raising here would skip the
        truncate and leave the day unarchived, which is far worse than writing
        a second file.
        """
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before = _Connection([{"id": 1, "payload": "one"}])
            count, first_path = database_backup._archive_table(
                before, "ws_orderupdates", root, "20260814"
            )
            self.assertEqual(count, 1)
            self.assertEqual(first_path.name, "ws_orderupdates_20260814.csv")

            # A restart provisions a new column; the same day archives again.
            after = _Connection(
                [{"id": 2, "payload": "two", "extra": "2026-08-14 10:54:48+05:30"}],
                columns=("id", "payload", "extra"),
            )
            count, second_path = database_backup._archive_table(
                after, "ws_orderupdates", root, "20260814"
            )
            self.assertEqual(count, 1)
            self.assertEqual(second_path.name, "ws_orderupdates_20260814.r2.csv")

            # Both archives survive and stay individually readable.
            with open(first_path, "r", encoding="utf-8", newline="") as stream:
                self.assertEqual(
                    list(csv.reader(stream)), [["id", "payload"], ["1", "one"]]
                )
            with open(second_path, "r", encoding="utf-8", newline="") as stream:
                self.assertEqual(
                    list(csv.reader(stream)),
                    [
                        ["id", "payload", "extra"],
                        ["2", "two", "2026-08-14 10:54:48+05:30"],
                    ],
                )
            self.assertFalse(list(root.glob(".*.tmp")))

    def test_unchanged_columns_still_append_to_the_same_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_backup._archive_table(
                _Connection([{"id": 1, "payload": "one"}]), "ws_orderupdates", root, "20260814"
            )
            _count, path = database_backup._archive_table(
                _Connection([{"id": 2, "payload": "two"}]), "ws_orderupdates", root, "20260814"
            )
            self.assertEqual(path.name, "ws_orderupdates_20260814.csv")
            self.assertEqual(len(list(root.glob("ws_orderupdates_*.csv"))), 1)

    def test_success_commits_only_after_all_archives_validate(self):
        conn = _Connection()
        connector = _Connector(conn)
        with tempfile.TemporaryDirectory() as directory, patch(
            "database_backup._archive_table",
            side_effect=[(10, Path("one.csv")), (20, Path("two.csv"))],
        ) as archive:
            reports = database_backup.backup_and_truncate_tables(
                connector, ["ws_orderupdates", "forecast_logs"], Path(directory), "20260814"
            )
        self.assertEqual(archive.call_count, 2)
        self.assertEqual([report[1] for report in reports], [10, 20])
        self.assertTrue(conn.committed)
        self.assertFalse(conn.rolled_back)
        self.assertTrue(connector.returned)


if __name__ == "__main__":
    unittest.main()
