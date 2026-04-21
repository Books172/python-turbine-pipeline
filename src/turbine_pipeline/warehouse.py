"""Warehouse: DuckDB storage with idempotent upserts.

DuckDB is used as a single-file embedded database — the brief asks for "a
database," DuckDB is one, and it avoids the SQLAlchemy boilerplate that
would otherwise dominate this module.

Every write is idempotent. Re-running yesterday's pipeline must not
double-insert, because the brief describes a daily append cadence where
re-runs (for fixes, backfills) are expected.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb
import pandas as pd

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS readings_clean (
    timestamp       TIMESTAMP NOT NULL,
    turbine_id      INTEGER   NOT NULL,
    wind_speed      DOUBLE,
    wind_direction  DOUBLE,
    power_output    DOUBLE,
    PRIMARY KEY (timestamp, turbine_id)
);

CREATE TABLE IF NOT EXISTS daily_stats (
    run_date    TIMESTAMP NOT NULL,
    turbine_id  INTEGER   NOT NULL,
    min_power   DOUBLE,
    max_power   DOUBLE,
    mean_power  DOUBLE,
    std_power   DOUBLE,
    count       INTEGER   NOT NULL,
    PRIMARY KEY (run_date, turbine_id)
);

CREATE TABLE IF NOT EXISTS anomalies (
    run_date          TIMESTAMP NOT NULL,
    turbine_id        INTEGER   NOT NULL,
    turbine_mean      DOUBLE    NOT NULL,
    fleet_mean        DOUBLE    NOT NULL,
    fleet_std         DOUBLE    NOT NULL,
    deviation_sigmas  DOUBLE    NOT NULL,
    PRIMARY KEY (run_date, turbine_id)
);
"""


@contextmanager
def connect(db_path: Path | str) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context-managed DuckDB connection with schema ensured."""
    con = duckdb.connect(str(db_path))
    try:
        con.execute(SCHEMA_SQL)
        yield con
    finally:
        con.close()


def _upsert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    key_columns: list[str],
) -> None:
    """Idempotent write: delete existing rows matching the key, then insert.

    DuckDB supports `INSERT ... ON CONFLICT` but the delete-then-insert
    pattern is dialect-portable and keeps the per-table logic readable.
    A single transaction makes it atomic.
    """
    if df.empty:
        return

    con.register("_staging", df)
    try:
        con.execute("BEGIN")
        key_values = (
            con.execute(f"SELECT DISTINCT {', '.join(key_columns)} FROM _staging")
            .fetchall()
        )
        if key_values:
            placeholders = ", ".join(["?"] * len(key_columns))
            where = f"({', '.join(key_columns)}) IN (VALUES " + ", ".join(
                [f"({placeholders})"] * len(key_values)
            ) + ")"
            flat = [v for row in key_values for v in row]
            con.execute(f"DELETE FROM {table} WHERE {where}", flat)
        con.execute(f"INSERT INTO {table} SELECT * FROM _staging")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("_staging")


def write_readings(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame
) -> None:
    _upsert(con, "readings_clean", df, ["timestamp", "turbine_id"])


def write_stats(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame
) -> None:
    _upsert(con, "daily_stats", df, ["run_date", "turbine_id"])


def write_anomalies(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame
) -> None:
    _upsert(con, "anomalies", df, ["run_date", "turbine_id"])