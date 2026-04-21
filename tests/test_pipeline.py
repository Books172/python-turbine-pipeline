from datetime import date
from pathlib import Path

from conftest import TURBINE_IDS

from turbine_pipeline import pipeline, warehouse

_N = len(TURBINE_IDS)


def test_run_pipeline_populates_all_tables(
    uploads_dir: Path, tmp_path: Path, run_date: date
) -> None:
    db = tmp_path / "integration.duckdb"
    result = pipeline.run_pipeline(uploads_dir, run_date, db)

    assert len(result.readings) == _N * 24
    assert len(result.stats) == _N
    # turbine 8 was made a fleet outlier in the fixture
    assert 8 in result.anomalies["turbine_id"].values

    with warehouse.connect(db) as con:
        readings_count = con.execute(
            "SELECT COUNT(*) FROM readings_clean"
        ).fetchone()[0]
        stats_count = con.execute(
            "SELECT COUNT(*) FROM daily_stats"
        ).fetchone()[0]
        anomalies_count = con.execute(
            "SELECT COUNT(*) FROM anomalies"
        ).fetchone()[0]
        assert readings_count == _N * 24
        assert stats_count == _N
        assert anomalies_count >= 1


def test_run_pipeline_is_idempotent(
    uploads_dir: Path, tmp_path: Path, run_date: date
) -> None:
    db = tmp_path / "rerun.duckdb"
    pipeline.run_pipeline(uploads_dir, run_date, db)
    pipeline.run_pipeline(uploads_dir, run_date, db)

    with warehouse.connect(db) as con:
        readings_count = con.execute(
            "SELECT COUNT(*) FROM readings_clean"
        ).fetchone()[0]
        stats_count = con.execute(
            "SELECT COUNT(*) FROM daily_stats"
        ).fetchone()[0]
        assert readings_count == _N * 24
        assert stats_count == _N