"""Warehouse: DuckDB storage with idempotent upserts.

DuckDB is used as a single-file embedded database — the brief asks for "a
database," DuckDB is one, and it avoids the SQLAlchemy boilerplate that
would otherwise dominate this module.

Every write is idempotent. Re-running yesterday's pipeline must not
double-insert, because the brief describes a daily append cadence where
re-runs (for fixes, backfills) are expected.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_readings (
    timestamp      TIMESTAMP NOT NULL,
    turbine_id     INTEGER   NOT NULL,
    wind_speed     DOUBLE,
    wind_direction DOUBLE,
    power_output   DOUBLE,
    PRIMARY KEY (timestamp, turbine_id)
);

CREATE TABLE IF NOT EXISTS readings_clean (
    timestamp       TIMESTAMP NOT NULL,
    turbine_id      INTEGER   NOT NULL,
    wind_speed      DOUBLE,
    wind_direction  DOUBLE,
    power_output    DOUBLE,
    PRIMARY KEY (timestamp, turbine_id)
);

CREATE TABLE IF NOT EXISTS daily_stats (
    run_date    DATE      NOT NULL,
    turbine_id  INTEGER   NOT NULL,
    min_power   DOUBLE,
    max_power   DOUBLE,
    mean_power  DOUBLE,
    std_power   DOUBLE,
    count       INTEGER   NOT NULL,
    PRIMARY KEY (run_date, turbine_id)
);

CREATE TABLE IF NOT EXISTS anomalies (
    run_date          DATE      NOT NULL,
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
    """Open a DuckDB connection and ensure the schema exists.

    Args:
        db_path: Path to the DuckDB file. Created if it does not exist.

    Yields:
        An open ``DuckDBPyConnection`` with all tables initialised.
    """
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
) -> None:
    """Replace rows whose primary key already exists, then insert the rest."""
    if df.empty:
        return

    con.register("_staging", df)
    try:
        con.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM _staging")
    finally:
        con.unregister("_staging")


def write_raw(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Upsert raw (pre-cleaning) readings into the ``raw_readings`` table.

    Args:
        con: Open DuckDB connection from :func:`connect`.
        df: Validated RawReading frame for the run-date window.
    """
    _upsert(con, "raw_readings", df)


def write_readings(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Upsert cleaned hourly readings into the ``readings_clean`` table.

    Args:
        con: Open DuckDB connection from :func:`connect`.
        df: Validated CleanReading frame.
    """
    _upsert(con, "readings_clean", df)


def write_stats(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Upsert per-turbine daily statistics into the ``daily_stats`` table.

    Args:
        con: Open DuckDB connection from :func:`connect`.
        df: Validated DailyStats frame.
    """
    _upsert(con, "daily_stats", df)


def write_anomalies(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Upsert detected anomalies into the ``anomalies`` table.

    Args:
        con: Open DuckDB connection from :func:`connect`.
        df: Validated Anomalies frame. May be empty.
    """
    _upsert(con, "anomalies", df)
