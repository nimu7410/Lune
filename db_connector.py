from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Final

from dotenv import dotenv_values, load_dotenv
from psycopg2 import (
    OperationalError,
    connect as pg_connect,
    sql,
    pool,
)
from psycopg2.extras import RealDictCursor

DB_TIMEZONE = "Asia/Kolkata"

# Kept in the private, gitignored .env file. The environment can override it
# for a different deployment; source files never contain the credential.
_ENV_FILE = Path(__file__).with_name(".env")
load_dotenv(_ENV_FILE)
DB_PASSWORD = dotenv_values(_ENV_FILE).get("TSDB_PASSWORD")


def _read_bool_env(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} must be one of: true/false, yes/no, on/off, or 1/0"
    )


class DatabaseNotFoundError(RuntimeError):
    """Raised when the required application database has not been provisioned."""


class TimescaleDBConnector:
    """Thread-safe pool for a pre-created DB with additive schema provisioning."""

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        auto_provision: bool | None = None,
        maintenance_db: str | None = None,
        connect_timeout: int | None = None,
        minconn: int = 1,
        maxconn: int = 5,
        attempts: int = 5,
        backoff: float = 2.0,
    ) -> None:
        # Explicit argument wins (tests, alternate instances), then the
        # environment, then the private .env file.
        resolved_password = (
            password
            if password is not None
            else os.getenv("TSDB_PASSWORD") or DB_PASSWORD
        )
        if not resolved_password:
            raise ValueError(
                "Database password is required: pass password=, set TSDB_PASSWORD, "
                "or set TSDB_PASSWORD in the private .env file"
            )
        self.cfg: Dict[str, Any] = {
            "host": host or os.getenv("TSDB_HOST", "127.0.0.1"),
            "port": port or int(os.getenv("TSDB_PORT", 5432)),
            "dbname": dbname or os.getenv("TSDB_NAME", "Lune"),
            "user": user or os.getenv("TSDB_USER", "lune_bot"),
            "password": resolved_password,
            "connect_timeout": (
                connect_timeout
                if connect_timeout is not None
                else int(os.getenv("TSDB_CONNECT_TIMEOUT", 5))
            ),
            # PostgreSQL stores TIMESTAMPTZ values as absolute instants.  The
            # session timezone controls how every such value is returned and
            # how timezone-less timestamp input is interpreted.  Pin it here
            # so application queries and CSV backups consistently use IST,
            # regardless of the database server or Windows host timezone.
            "options": f"-c timezone={DB_TIMEZONE}",
        }
        self.maintenance_db = maintenance_db or os.getenv(
            "TSDB_MAINTENANCE_DB", "postgres"
        )
        self.auto_provision = (
            _read_bool_env("TSDB_AUTO_PROVISION", default=True)
            if auto_provision is None
            else auto_provision
        )

        self.minconn, self.maxconn = minconn, maxconn
        self.attempts, self.backoff = attempts, backoff
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1")
        if self.minconn < 1 or self.maxconn < self.minconn:
            raise ValueError("connection pool limits must satisfy 1 <= minconn <= maxconn")
        if self.cfg["connect_timeout"] < 1:
            raise ValueError("connect_timeout must be at least 1 second")

        self._pool: pool.ThreadedConnectionPool | None = None
        self._schema_lock = threading.Lock()
        self._schema_ready = False
        self._schema_report = None

    def connect(self) -> None:
        if self._pool is not None:
            if self.auto_provision:
                self.ensure_schema()
            return

        for attempt in range(1, self.attempts + 1):
            try:
                self._pool = self._open_pool()
            except OperationalError as exc:
                if attempt == 1 and self._database_exists() is False:
                    raise DatabaseNotFoundError(
                        f"Required PostgreSQL database {self.cfg['dbname']!r} "
                        "does not exist. Automatic database creation is disabled."
                    ) from exc

                logging.warning(
                    "DB unreachable (attempt %s/%s): %s",
                    attempt,
                    self.attempts,
                    exc,
                )
                if attempt == self.attempts:
                    logging.error("Could not connect to PostgreSQL after retries")
                    raise
                time.sleep(self.backoff * attempt)
            else:
                break

        try:
            self._verify_session_timezone()
            self._ensure_database_timezone_default()
            if self.auto_provision:
                self.ensure_schema()
        except Exception:
            self.close()
            raise

        logging.info(
            "PostgreSQL pool ready for %s@%s:%s/%s",
            self.cfg["user"],
            self.cfg["host"],
            self.cfg["port"],
            self.cfg["dbname"],
        )

    def _open_pool(self) -> pool.ThreadedConnectionPool:
        return pool.ThreadedConnectionPool(
            self.minconn,
            self.maxconn,
            **self.cfg,
            cursor_factory=RealDictCursor,
        )

    def _verify_session_timezone(self) -> None:
        """Refuse to run unless pooled connections really are on IST.

        The ``-c timezone=...`` connection option is the guarantee that every
        ``TIMESTAMPTZ`` this process reads back is rendered in IST, and that a
        timezone-less literal in any query is interpreted as IST.  A silent
        failure there would not raise anywhere - it would just quietly shift
        every timestamp the bot writes and reads by 5h30m - so it is checked
        once, explicitly, before any application query runs.
        """
        rows = self.execute("SHOW TimeZone;", fetch=True) or []
        actual = str(rows[0].get("TimeZone") if rows else "").strip()
        if actual.lower() != DB_TIMEZONE.lower():
            raise RuntimeError(
                f"Database session timezone is {actual!r}, expected {DB_TIMEZONE!r}. "
                "Every timestamp this bot writes and reads would be shifted; "
                "refusing to start."
            )

    def _ensure_database_timezone_default(self) -> None:
        """Persist IST for psql and every future connection to this database.

        Pool connections already carry ``-c timezone=Asia/Kolkata``, and
        :meth:`_verify_session_timezone` has already confirmed it took effect -
        that is what makes *this bot* correct.  The database-level default is
        for everything else: direct psql, reporting tools, and any future code
        that does not go through this connector.  ``TIMESTAMPTZ`` remains the
        storage type; this controls its presentation and the interpretation of
        any timezone-less input.

        ``ALTER DATABASE`` needs ownership, so this is best-effort and logs
        loudly on failure rather than raising: a permissions gap here must not
        keep a trading bot down when its own connections are already correct.
        """
        admin_cfg = dict(self.cfg)
        admin_cfg.pop("options", None)
        try:
            conn = pg_connect(**admin_cfg, cursor_factory=RealDictCursor)
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "Could not connect to set the database-level IST default: %s. "
                "This process is unaffected (its pool is pinned to %s); direct "
                "psql sessions may render timestamps in another timezone.",
                exc,
                DB_TIMEZONE,
            )
            return
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(
                        bool_or(lower(item) = lower(%s)), false
                    ) AS configured
                    FROM pg_database AS d
                    LEFT JOIN pg_db_role_setting AS s
                      ON s.setdatabase = d.oid AND s.setrole = 0
                    LEFT JOIN LATERAL unnest(s.setconfig) AS item ON true
                    WHERE d.datname = current_database()
                    """,
                    (f"TimeZone={DB_TIMEZONE}",),
                )
                configured = bool(cur.fetchone()["configured"])
                if not configured:
                    cur.execute(
                        sql.SQL("ALTER DATABASE {} SET timezone TO {}").format(
                            sql.Identifier(self.cfg["dbname"]),
                            sql.Literal(DB_TIMEZONE),
                        )
                    )
                    logging.info(
                        "Set database-level default timezone to %s for %s",
                        DB_TIMEZONE,
                        self.cfg["dbname"],
                    )
        except Exception as exc:  # noqa: BLE001 - see the docstring
            logging.warning(
                "Could not set the database-level default timezone to %s: %s. "
                "This process is unaffected (its pool is pinned to it); direct "
                "psql sessions may render timestamps in another timezone. "
                "Fix with: ALTER DATABASE %s SET timezone TO '%s';",
                DB_TIMEZONE,
                exc,
                self.cfg["dbname"],
                DB_TIMEZONE,
            )
        finally:
            conn.close()

    def _database_exists(self) -> bool | None:
        """Return target DB presence, or None if the maintenance DB is unreachable."""

        admin_cfg = dict(self.cfg)
        admin_cfg["dbname"] = self.maintenance_db
        try:
            admin = pg_connect(**admin_cfg, cursor_factory=RealDictCursor)
        except OperationalError as exc:
            logging.debug(
                "Maintenance database unavailable; cannot check %s: %s",
                self.cfg["dbname"],
                exc,
            )
            return None

        try:
            with admin.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_database WHERE datname = %s
                    ) AS present
                    """,
                    (self.cfg["dbname"],),
                )
                return bool(cur.fetchone()["present"])
        finally:
            admin.close()

    def ensure_schema(self):
        """Check and add missing schema objects once per connected process."""

        if self._schema_ready:
            return self._schema_report
        if not self._pool:
            raise RuntimeError("connect() must be called first")

        with self._schema_lock:
            if self._schema_ready:
                return self._schema_report

            connection_pool = self._pool
            conn = connection_pool.getconn()
            try:
                report = ensure_botdata_schema(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                connection_pool.putconn(conn)

            self._schema_report = report
            self._schema_ready = True
            if report.changed:
                logging.info("Applied additive database provisioning: %s", report)
            else:
                logging.debug("Database schema is already current")
            return report

    def close(self) -> None:
        connection_pool = self._pool
        self._pool = None
        self._schema_ready = False
        self._schema_report = None
        if connection_pool:
            try:
                connection_pool.closeall()
                logging.info("DB pool closed")
            except Exception as exc:  # noqa: BLE001
                logging.debug("DB pool close ignored: %s", exc)

    def execute(
        self, statement: str, params: tuple | list | None = None, *, fetch: bool = False
    ):
        with self._cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if fetch else None

    async def execute_async(
        self, statement: str, params: tuple | list | None = None, *, fetch: bool = False
    ):
        return await asyncio.to_thread(self.execute, statement, params, fetch=fetch)

    def get_raw_connection(self):
        if not self._pool:
            raise RuntimeError("connect() must be called first")
        return self._pool.getconn()

    def put_raw_connection(self, conn, *, close: bool = False) -> None:
        if not self._pool:
            try:
                conn.close()
            except Exception:
                pass
            return
        self._pool.putconn(conn, close=close)

    async def get_raw_connection_async(self):
        return await asyncio.to_thread(self.get_raw_connection)

    async def put_raw_connection_async(self, conn, *, close: bool = False) -> None:
        await asyncio.to_thread(self.put_raw_connection, conn, close=close)

    @contextmanager
    def _cursor(self):
        if not self._pool:
            raise RuntimeError("connect() must be called first")
        connection_pool = self._pool
        conn = connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                yield cur
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            connection_pool.putconn(conn)


# =========================================================================== #
#  Schema provisioning (was db_schema.py)
#
#  Additive only: missing objects are created, existing ones are validated and
#  never altered, so a restart can never rewrite data the bot already holds.
# =========================================================================== #
SCHEMA_NAME: Final = "botdata"


class SchemaCompatibilityError(RuntimeError):
    """Raised when an existing object cannot be changed safely and additively."""


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    create_definition: str
    add_definition: str
    postgres_type: str


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]


@dataclass(frozen=True)
class IndexSpec:
    name: str
    table: str
    # Trusted constant, interpolated as raw SQL. Never build this from input.
    key: str
    unique: bool = False


@dataclass(frozen=True)
class SchemaReport:
    schema_created: bool
    tables_created: tuple[str, ...]
    columns_added: tuple[str, ...]
    constraints_created: tuple[str, ...]
    indexes_created: tuple[str, ...]
    functions_created: tuple[str, ...]
    triggers_created: tuple[str, ...]

    @property
    def changed(self) -> bool:
        return any(
            (
                self.schema_created,
                self.tables_created,
                self.columns_added,
                self.constraints_created,
                self.indexes_created,
                self.functions_created,
                self.triggers_created,
            )
        )


def _column(
    name: str,
    create_definition: str,
    postgres_type: str,
    *,
    add_definition: str | None = None,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        create_definition=create_definition,
        add_definition=add_definition or create_definition,
        postgres_type=postgres_type,
    )


TABLES: Final = (
    TableSpec(
        "forecast_logs",
        (
            _column("id", "SERIAL PRIMARY KEY", "int4", add_definition="SERIAL"),
            _column("log_time", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("hh_count", "INTEGER", "int4"),
            _column("ll_count", "INTEGER", "int4"),
            _column("pivot_type", "TEXT", "text"),
            _column("pivot_price", "DOUBLE PRECISION", "float8"),
            _column("pivot_time", "TIMESTAMPTZ", "timestamptz"),
            _column("buy_leg", "TEXT", "text"),
            _column("sell_leg", "TEXT", "text"),
        ),
    ),
    TableSpec(
        # The complete, unfiltered Shoonya order-update ("om") record - one
        # row per message, no gating. `order_logs`/`order_attempts` only ever
        # persist a narrow, orderno-matched projection (status/fill/avg price
        # onto a known leg) and silently drop anything that doesn't match a
        # known leg; this table is the full record regardless of match. The
        # typed columns mirror exactly the fields `_on_order_update` (main.py)
        # already parses; `raw` is the message exactly as received.
        "ws_orderupdates",
        (
            _column("id", "SERIAL PRIMARY KEY", "int4", add_definition="SERIAL"),
            _column("log_time", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("msg_type", "TEXT", "text"),
            _column("tsym", "TEXT", "text"),
            _column("orderno", "TEXT", "text"),
            _column("exchordid", "TEXT", "text"),
            _column("status", "TEXT", "text"),
            _column("fill_qty", "INTEGER", "int4"),
            _column("avg_price", "DOUBLE PRECISION", "float8"),
            _column("raw", "JSONB NOT NULL", "jsonb", add_definition="JSONB"),
        ),
    ),
    TableSpec(
        "trade_logs",
        (
            _column("id", "SERIAL PRIMARY KEY", "int4", add_definition="SERIAL"),
            _column("log_time", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("symbol", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("direction", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("fill_price", "DOUBLE PRECISION NOT NULL", "float8", add_definition="DOUBLE PRECISION"),
            # The strategy has no take-profit; it has a stop. This column holds
            # the frozen stop level for the open trade (NULL when there is none).
            _column("stop_price", "DOUBLE PRECISION", "float8"),
            _column("exit_price", "DOUBLE PRECISION", "float8"),
            _column("exit_time", "TIMESTAMPTZ", "timestamptz"),
            _column("is_in_trade", "BOOLEAN NOT NULL", "bool", add_definition="BOOLEAN"),
            _column("win", "INTEGER", "int4"),
            _column("buy_orderno", "TEXT", "text"),
            _column("sell_orderno", "TEXT", "text"),
            _column("buy_symbol", "TEXT", "text"),
            _column("sell_symbol", "TEXT", "text"),
            _column("sell_entry_side", "TEXT", "text"),
            _column("trade_state", "TEXT", "text"),
            _column("execution_id", "TEXT", "text"),
            _column("execution_mode", "TEXT", "text"),
            # Which v3 hold opened this position (`strategy_logic` books): the
            # momentum entry (A), A on a shock session (sold at 14:55), or the
            # carry (B). Only A carries a protective stop, and only on its
            # entry session. A position survives a process shutdown, so on
            # resume the manager must know which hold it is; NULL reads as the
            # carry, the one with no stop, so a resumed position is never given
            # a stop level this process did not set.
            _column("book", "TEXT", "text"),
            # Which of v3's six indices received the slot. The portfolio
            # holds one position between them, and the index decides the derivatives
            # exchange the exit order routes to and how many units it is. A
            # position outlives the process that opened it, so on resume this
            # cannot be inferred - it is read back from here. NULL reads as the
            # chart index, which is what every row written before the book
            # became multi-index is.
            _column("index_name", "TEXT", "text"),
            _column("quantity", "INTEGER", "int4"),
            # LIVE only: rupees the closed position made on its own broker
            # fills (entry and exit premiums), before charges. NULL when a fill
            # could not be read back; fill_price/exit_price stay index levels.
            _column("realized_pnl", "DOUBLE PRECISION", "float8"),
        ),
    ),
    TableSpec(
        "order_logs",
        (
            _column("id", "SERIAL PRIMARY KEY", "int4", add_definition="SERIAL"),
            _column("log_time", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("symbol1", "TEXT", "text"),
            _column("symbol2", "TEXT", "text"),
            _column("state1", "TEXT", "text"),
            _column("state2", "TEXT", "text"),
            _column("order_type1", "TEXT", "text"),
            _column("order_type2", "TEXT", "text"),
            _column("time1", "TIMESTAMPTZ", "timestamptz"),
            _column("time2", "TIMESTAMPTZ", "timestamptz"),
            _column("buy_orderno", "TEXT", "text"),
            _column("sell_orderno", "TEXT", "text"),
            _column("buy_exchordid", "TEXT", "text"),
            _column("sell_exchordid", "TEXT", "text"),
            _column("buy_fill_qty", "INTEGER", "int4"),
            _column("sell_fill_qty", "INTEGER", "int4"),
            _column("buy_avg_price", "DOUBLE PRECISION", "float8"),
            _column("sell_avg_price", "DOUBLE PRECISION", "float8"),
            _column("buy_exit_orderno", "TEXT", "text"),
            _column("sell_exit_orderno", "TEXT", "text"),
            _column("buy_exit_exchordid", "TEXT", "text"),
            _column("sell_exit_exchordid", "TEXT", "text"),
            _column("buy_exit_fill_qty", "INTEGER", "int4"),
            _column("sell_exit_fill_qty", "INTEGER", "int4"),
            _column("buy_exit_avg_price", "DOUBLE PRECISION", "float8"),
            _column("sell_exit_avg_price", "DOUBLE PRECISION", "float8"),
            _column("buy_exit_state", "TEXT", "text"),
            _column("sell_exit_state", "TEXT", "text"),
            _column("time1_exit", "TIMESTAMPTZ", "timestamptz"),
            _column("time2_exit", "TIMESTAMPTZ", "timestamptz"),
            _column("lifecycle_state", "TEXT", "text"),
        ),
    ),
    TableSpec(
        "order_attempts",
        (
            _column("id", "SERIAL PRIMARY KEY", "int4", add_definition="SERIAL"),
            _column("created_at", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("updated_at", "TIMESTAMPTZ NOT NULL", "timestamptz", add_definition="TIMESTAMPTZ"),
            _column("order_log_id", "INTEGER", "int4"),
            _column("client_ref", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("phase", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("leg", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("attempt_no", "INTEGER NOT NULL", "int4", add_definition="INTEGER"),
            _column("symbol", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("side", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("requested_qty", "INTEGER NOT NULL", "int4", add_definition="INTEGER"),
            _column("orderno", "TEXT", "text"),
            _column("exchordid", "TEXT", "text"),
            _column("status", "TEXT NOT NULL", "text", add_definition="TEXT"),
            _column("filled_qty", "INTEGER", "int4"),
            _column("avg_price", "DOUBLE PRECISION", "float8"),
            _column("error", "TEXT", "text"),
        ),
    ),
)


INDEXES: Final = (
    IndexSpec("forecast_logs_log_time_id_desc_idx", "forecast_logs", "(log_time DESC, id DESC)"),
    IndexSpec("order_attempts_client_ref_idx", "order_attempts", "(client_ref)", unique=True),
    IndexSpec("order_attempts_orderno_idx", "order_attempts", "(orderno)", unique=True),
    IndexSpec("trade_logs_execution_id_idx", "trade_logs", "(execution_id)", unique=True),
)

# Objects retired when the bot moved to the frozen carry strategy. The strategy has
# no take-profit, so the ATR take-profit stream and the NOT NULL `tp_price`
# column it fed are gone. Both are dropped explicitly: a leftover
# `tp_price NOT NULL` would reject every insert the new manager makes, and
# `_validate_existing_objects` only inspects columns the schema still declares,
# so it would never report the problem.
# Raw websocket price history and its dedicated backup are retired.
DROPPED_TABLES: Final = ("tp_logs", "websocket")
DROPPED_COLUMNS: Final = (
    ("trade_logs", "tp_price"),
    # Dead since the buy/sell leg split: never written (see buy_leg/sell_leg),
    # always NULL.
    ("forecast_logs", "updated_symbol"),
    # forecast_logs records pivot/symbol state only.
    ("forecast_logs", "live_price"),
)


FUNCTIONS: Final = {
    "prevent_symbol_change_trade_logs": """
        CREATE FUNCTION botdata.prevent_symbol_change_trade_logs()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.symbol IS DISTINCT FROM OLD.symbol)
               OR (NEW.buy_symbol IS DISTINCT FROM OLD.buy_symbol)
               OR (NEW.sell_symbol IS DISTINCT FROM OLD.sell_symbol) THEN
                RAISE EXCEPTION 'botdata.trade_logs: symbol fields are immutable once set';
            END IF;
            RETURN NEW;
        END; $$;
    """,
    "prevent_symbol_change_order_logs": """
        CREATE FUNCTION botdata.prevent_symbol_change_order_logs()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.symbol1 IS DISTINCT FROM OLD.symbol1)
               OR (NEW.symbol2 IS DISTINCT FROM OLD.symbol2) THEN
                RAISE EXCEPTION 'botdata.order_logs: symbol1/symbol2 are immutable once set';
            END IF;
            RETURN NEW;
        END; $$;
    """,
}


TRIGGERS: Final = (
    (
        "trg_prevent_symbol_change_trade_logs",
        "trade_logs",
        "prevent_symbol_change_trade_logs",
    ),
    (
        "trg_prevent_symbol_change_order_logs",
        "order_logs",
        "prevent_symbol_change_order_logs",
    ),
)


def _load_relations(cur) -> dict[str, str]:
    cur.execute(
        """
        SELECT c.relname AS table_name, c.relkind
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = ANY(%s)
        """,
        (SCHEMA_NAME, [table.name for table in TABLES]),
    )
    return {row["table_name"]: row["relkind"] for row in cur.fetchall()}


def _load_columns(cur) -> dict[tuple[str, str], str]:
    cur.execute(
        """
        SELECT table_name, column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = ANY(%s)
        """,
        (SCHEMA_NAME, [table.name for table in TABLES]),
    )
    return {
        (row["table_name"], row["column_name"]): row["udt_name"]
        for row in cur.fetchall()
    }


def _load_indexes(cur) -> dict[str, tuple[str, str]]:
    """Map index name -> (table name, definition) for the indexes we manage."""
    cur.execute(
        """
        SELECT ic.relname  AS index_name,
               tc.relname  AS table_name,
               pg_get_indexdef(i.indexrelid) AS indexdef
        FROM pg_index AS i
        JOIN pg_class AS ic ON ic.oid = i.indexrelid
        JOIN pg_class AS tc ON tc.oid = i.indrelid
        JOIN pg_namespace AS n ON n.oid = tc.relnamespace
        WHERE n.nspname = %s
          AND ic.relname = ANY(%s)
        """,
        (SCHEMA_NAME, [index.name for index in INDEXES]),
    )
    return {
        row["index_name"]: (row["table_name"], row["indexdef"])
        for row in cur.fetchall()
    }


def _load_primary_keys(cur) -> dict[str, tuple[str, ...]]:
    cur.execute(
        """
        SELECT tc.table_name,
               array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS columns
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON kcu.constraint_catalog = tc.constraint_catalog
         AND kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = %s
          AND tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = ANY(%s)
        GROUP BY tc.table_name
        """,
        (SCHEMA_NAME, [table.name for table in TABLES]),
    )
    return {
        row["table_name"]: tuple(row["columns"])
        for row in cur.fetchall()
    }


def _validate_existing_objects(
    relations: dict[str, str],
    columns: dict[tuple[str, str], str],
    primary_keys: dict[str, tuple[str, ...]],
) -> None:
    invalid_relations = [
        name for name, kind in relations.items() if kind not in {"r", "p"}
    ]
    if invalid_relations:
        raise SchemaCompatibilityError(
            "Expected tables but found other relation types: "
            + ", ".join(sorted(invalid_relations))
        )

    mismatches: list[str] = []
    for table in TABLES:
        for column in table.columns:
            actual = columns.get((table.name, column.name))
            if actual is not None and actual != column.postgres_type:
                mismatches.append(
                    f"{SCHEMA_NAME}.{table.name}.{column.name} "
                    f"is {actual}, expected {column.postgres_type}"
                )
        primary_key = primary_keys.get(table.name)
        if primary_key is not None and primary_key != ("id",):
            mismatches.append(
                f"{SCHEMA_NAME}.{table.name} primary key is {primary_key}, expected ('id',)"
            )

    if mismatches:
        raise SchemaCompatibilityError(
            "Existing schema is incompatible; no existing objects were modified: "
            + "; ".join(mismatches)
        )


def _index_matches(indexdef: str, spec: IndexSpec) -> bool:
    """
    Loose equivalence check against pg_get_indexdef output.

    Postgres normalises the definition it stores (quoting, spacing, explicit
    ASC/NULLS clauses), so exact string comparison would report false
    mismatches. Compare the bare key columns and their sort direction instead.
    """
    def _key(text: str) -> list[str]:
        inner = text[text.find("(") + 1: text.rfind(")")]
        parts = []
        for raw in inner.split(","):
            tokens = raw.replace('"', "").lower().split()
            if not tokens:
                continue
            # keep the column plus DESC if present; drop ASC / NULLS FIRST|LAST
            col = tokens[0]
            parts.append(f"{col} desc" if "desc" in tokens else col)
        return parts

    try:
        actual_unique = "CREATE UNIQUE INDEX" in indexdef.upper()
        return _key(indexdef) == _key(spec.key) and actual_unique == spec.unique
    except Exception:  # noqa: BLE001 - never let a cosmetic check break startup
        return True


def ensure_botdata_schema(conn) -> SchemaReport:
    """Create only missing botdata objects in one serialized transaction."""

    tables_created: list[str] = []
    columns_added: list[str] = []
    constraints_created: list[str] = []
    indexes_created: list[str] = []
    functions_created: list[str] = []
    triggers_created: list[str] = []

    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
            ("lune-schema", SCHEMA_NAME),
        )
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s) AS present",
            (SCHEMA_NAME,),
        )
        schema_created = not cur.fetchone()["present"]
        if schema_created:
            cur.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(SCHEMA_NAME))
            )

        # Retire objects the frozen strategy no longer uses before anything
        # inspects the schema, so a stale NOT NULL column cannot fail inserts.
        for dropped_table in DROPPED_TABLES:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(SCHEMA_NAME), sql.Identifier(dropped_table)
                )
            )
        for dropped_table, dropped_column in DROPPED_COLUMNS:
            cur.execute(
                sql.SQL("ALTER TABLE IF EXISTS {}.{} DROP COLUMN IF EXISTS {}").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(dropped_table),
                    sql.Identifier(dropped_column),
                )
            )

        relations = _load_relations(cur)
        columns = _load_columns(cur)
        primary_keys = _load_primary_keys(cur)
        _validate_existing_objects(relations, columns, primary_keys)

        for table in TABLES:
            if table.name in relations:
                continue
            definitions = [
                sql.SQL("{} {}").format(
                    sql.Identifier(column.name), sql.SQL(column.create_definition)
                )
                for column in table.columns
            ]
            cur.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(table.name),
                    sql.SQL(", ").join(definitions),
                )
            )
            tables_created.append(table.name)

        for table in TABLES:
            if table.name in tables_created:
                continue
            for column in table.columns:
                if (table.name, column.name) in columns:
                    continue
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                        sql.Identifier(SCHEMA_NAME),
                        sql.Identifier(table.name),
                        sql.Identifier(column.name),
                        sql.SQL(column.add_definition),
                    )
                )
                columns_added.append(f"{table.name}.{column.name}")

        for table in TABLES:
            if table.name in tables_created or table.name in primary_keys:
                continue
            constraint_name = f"{table.name}_pkey"
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(table.name),
                    sql.Identifier(constraint_name),
                    sql.Identifier("id"),
                )
            )
            constraints_created.append(constraint_name)

        # Indexes last among the structural steps: every managed table and
        # column exists by now, whether it was just created or already present.
        #
        # Plain CREATE INDEX (not CONCURRENTLY) is deliberate. CONCURRENTLY
        # cannot run inside a transaction block, and this whole function is one
        # transaction under an advisory lock. It is also unnecessary here:
        # ensure_schema() completes inside TimescaleDBConnector.connect(),
        # before main.py starts the ForecastLogger writer, so nothing is
        # inserting while the index builds. On the normal empty table it is
        # instant; on a table left populated by an unclean shutdown it briefly
        # blocks writes that are not running yet.
        existing_indexes = _load_indexes(cur)
        for index in INDEXES:
            present = existing_indexes.get(index.name)
            if present is not None:
                table_name, indexdef = present
                # Never modify an existing index, and never refuse to start over
                # one: a cosmetic mismatch must not keep a trading bot down.
                if table_name != index.table:
                    logging.warning(
                        "Index %s.%s exists on table %r, expected %r; leaving it alone.",
                        SCHEMA_NAME, index.name, table_name, index.table,
                    )
                elif not _index_matches(indexdef, index):
                    if index.unique:
                        raise SchemaCompatibilityError(
                            f"Safety-critical unique index {SCHEMA_NAME}.{index.name} "
                            f"has an incompatible definition: {indexdef}"
                        )
                    logging.warning(
                        "Index %s.%s exists with an unexpected definition (%s); "
                        "leaving it alone. Drop it manually to have it rebuilt as %s.",
                        SCHEMA_NAME, index.name, indexdef, index.key,
                    )
                continue

            cur.execute(
                sql.SQL("CREATE {}INDEX {} ON {}.{} {}").format(
                    sql.SQL("UNIQUE ") if index.unique else sql.SQL(""),
                    sql.Identifier(index.name),
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(index.table),
                    sql.SQL(index.key),
                )
            )
            indexes_created.append(index.name)

        cur.execute(
            """
            SELECT p.proname
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = %s
              AND p.pronargs = 0
              AND p.proname = ANY(%s)
            """,
            (SCHEMA_NAME, list(FUNCTIONS)),
        )
        existing_functions = {row["proname"] for row in cur.fetchall()}
        for function_name, definition in FUNCTIONS.items():
            if function_name not in existing_functions:
                cur.execute(definition)
                functions_created.append(function_name)

        cur.execute(
            """
            SELECT t.tgname, c.relname AS table_name
            FROM pg_trigger AS t
            JOIN pg_class AS c ON c.oid = t.tgrelid
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND NOT t.tgisinternal
              AND t.tgname = ANY(%s)
            """,
            (SCHEMA_NAME, [trigger[0] for trigger in TRIGGERS]),
        )
        existing_triggers = {
            (row["tgname"], row["table_name"]) for row in cur.fetchall()
        }
        for trigger_name, table_name, function_name in TRIGGERS:
            if (trigger_name, table_name) in existing_triggers:
                continue
            cur.execute(
                sql.SQL(
                    "CREATE TRIGGER {} BEFORE UPDATE ON {}.{} "
                    "FOR EACH ROW EXECUTE FUNCTION {}.{}()"
                ).format(
                    sql.Identifier(trigger_name),
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(table_name),
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(function_name),
                )
            )
            triggers_created.append(trigger_name)

        structure_changed = any(
            (
                schema_created,
                tables_created,
                columns_added,
                constraints_created,
            )
        )
        final_relations = _load_relations(cur) if structure_changed else relations
        final_columns = _load_columns(cur) if structure_changed else columns
        final_primary_keys = (
            _load_primary_keys(cur) if structure_changed else primary_keys
        )
        _validate_existing_objects(
            final_relations, final_columns, final_primary_keys
        )
        missing = [
            f"{table.name}.{column.name}"
            for table in TABLES
            for column in table.columns
            if (table.name, column.name) not in final_columns
        ]
        if missing:
            raise RuntimeError(
                "Schema provisioning did not create required columns: "
                + ", ".join(missing)
            )

    return SchemaReport(
        schema_created=schema_created,
        tables_created=tuple(tables_created),
        columns_added=tuple(columns_added),
        constraints_created=tuple(constraints_created),
        indexes_created=tuple(indexes_created),
        functions_created=tuple(functions_created),
        triggers_created=tuple(triggers_created),
    )
