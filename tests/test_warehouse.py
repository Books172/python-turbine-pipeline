from datetime import date
from pathlib import Path

import pandas as pd

from turbine_pipeline import warehouse


def _sample_readings() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2022-03-15 00:00", "2022-03-15 01:00"]
            ),
            "turbine_id": [1, 1],
            "wind_speed": [12.0, 12.1],
            "wind_direction": [180.0, 181.0],
            "power_output": [3.0, 3.1],
        }
    )


def test_write_and_read_back(tmp_path: Path) -> None:
    db = tmp_path / "t.duckdb"
    with warehouse.connect(db) as con:
        warehouse.write_readings(con, _sample_readings())
        rows = con.execute(
            "SELECT COUNT(*) FROM readings_clean"
        ).fetchone()[0]
    assert rows == 2


def test_upsert_is_idempotent(tmp_path: Path) -> None:
    """The single most important property of this module: rerunning a day
    must not create duplicates."""
    db = tmp_path / "t.duckdb"
    df = _sample_readings()
    with warehouse.connect(db) as con:
        warehouse.write_readings(con, df)
        warehouse.write_readings(con, df)  # re-run same day
        warehouse.write_readings(con, df)  # and again
        rows = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[0]
    assert rows == 2


def test_upsert_updates_changed_values(tmp_path: Path) -> None:
    """If a re-run produces corrected values, the new values win."""
    db = tmp_path / "t.duckdb"
    with warehouse.connect(db) as con:
        warehouse.write_readings(con, _sample_readings())
        corrected = _sample_readings()
        corrected["power_output"] = [9.9, 9.9]
        warehouse.write_readings(con, corrected)
        vals = [
            r[0]
            for r in con.execute(
                "SELECT power_output FROM readings_clean ORDER BY timestamp"
            ).fetchall()
        ]
    assert vals == [9.9, 9.9]


def test_empty_write_is_noop(tmp_path: Path) -> None:
    """Anomaly table will often be empty — writing nothing must not raise."""
    db = tmp_path / "t.duckdb"
    empty = _sample_readings().iloc[0:0]
    with warehouse.connect(db) as con:
        warehouse.write_readings(con, empty)
        rows = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[0]
    assert rows == 0