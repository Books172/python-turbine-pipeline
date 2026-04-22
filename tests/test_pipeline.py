from datetime import date, timedelta
from pathlib import Path

import pytest
from conftest import MONTH_END, MONTH_START, RUN_DATE, TURBINE_IDS

from turbine_pipeline import pipeline, warehouse

_N = len(TURBINE_IDS)
_MONTH_DAYS = (MONTH_END - MONTH_START).days + 1


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
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        stats_count = con.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        anomalies_count = con.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
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
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        stats_count = con.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        assert readings_count == _N * 24
        assert stats_count == _N


def test_run_pipeline_raises_for_empty_date(uploads_dir: Path, tmp_path: Path) -> None:
    """run_pipeline raises ValueError when no data exists for the requested date."""
    db = tmp_path / "empty.duckdb"
    with pytest.raises(ValueError, match="No data found"):
        pipeline.run_pipeline(uploads_dir, date(2022, 1, 1), db)


def test_run_pipeline_range_processes_multiple_days(
    multi_day_uploads_dir: Path, tmp_path: Path
) -> None:
    """range run returns a result for each day that has data."""
    db = tmp_path / "range.duckdb"
    end = RUN_DATE + timedelta(days=1)
    results = pipeline.run_pipeline_range(multi_day_uploads_dir, RUN_DATE, end, db)

    assert set(results.keys()) == {RUN_DATE, RUN_DATE + timedelta(days=1)}
    assert len(results[RUN_DATE].readings) == _N * 24
    assert len(results[RUN_DATE + timedelta(days=1)].readings) == _N * 24

    with warehouse.connect(db) as con:
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        assert readings_count == _N * 24 * 2


def test_run_pipeline_range_skips_empty_days(uploads_dir: Path, tmp_path: Path) -> None:
    """Dates with no CSV data are absent from the result, not an error."""
    db = tmp_path / "skip.duckdb"
    # uploads_dir only has data for RUN_DATE; the day after has nothing
    end = RUN_DATE + timedelta(days=1)
    results = pipeline.run_pipeline_range(uploads_dir, RUN_DATE, end, db)

    assert RUN_DATE in results
    assert RUN_DATE + timedelta(days=1) not in results


def test_run_pipeline_range_is_idempotent(
    multi_day_uploads_dir: Path, tmp_path: Path
) -> None:
    """Running the same range twice produces the same warehouse state."""
    db = tmp_path / "range_idem.duckdb"
    end = RUN_DATE + timedelta(days=1)
    pipeline.run_pipeline_range(multi_day_uploads_dir, RUN_DATE, end, db)
    pipeline.run_pipeline_range(multi_day_uploads_dir, RUN_DATE, end, db)

    with warehouse.connect(db) as con:
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        assert readings_count == _N * 24 * 2


def test_run_pipeline_range_raises_for_inverted_dates(
    uploads_dir: Path, tmp_path: Path
) -> None:
    """end_date before start_date raises ValueError immediately."""
    db = tmp_path / "bad.duckdb"
    with pytest.raises(ValueError, match="must not be before"):
        pipeline.run_pipeline_range(
            uploads_dir, RUN_DATE, RUN_DATE - timedelta(days=1), db
        )


def test_run_pipeline_range_full_month(month_uploads_dir: Path, tmp_path: Path) -> None:
    """Processing a full month accumulates the correct row counts."""
    db = tmp_path / "month.duckdb"
    results = pipeline.run_pipeline_range(month_uploads_dir, MONTH_START, MONTH_END, db)

    assert len(results) == _MONTH_DAYS

    with warehouse.connect(db) as con:
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        stats_count = con.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        assert readings_count == _N * 24 * _MONTH_DAYS
        assert stats_count == _N * _MONTH_DAYS


def test_run_pipeline_range_skips_missing_days_in_month(
    month_uploads_dir_with_gap: Path, tmp_path: Path
) -> None:
    """Days absent from the CSVs are silently skipped; present days succeed."""
    db = tmp_path / "month_gap.duckdb"
    results = pipeline.run_pipeline_range(
        month_uploads_dir_with_gap, MONTH_START, MONTH_END, db
    )

    # 3 days removed from fixture (days 10, 11, 12 of March)
    assert len(results) == _MONTH_DAYS - 3
    assert date(2022, 3, 10) not in results
    assert date(2022, 3, 11) not in results
    assert date(2022, 3, 12) not in results


def test_run_pipeline_range_backfill(month_uploads_dir: Path, tmp_path: Path) -> None:
    """Running days 1-15 then 1-31 produces the same state as a single 1-31 run."""
    db = tmp_path / "backfill.duckdb"
    mid = date(2022, 3, 15)

    pipeline.run_pipeline_range(month_uploads_dir, MONTH_START, mid, db)
    pipeline.run_pipeline_range(month_uploads_dir, MONTH_START, MONTH_END, db)

    with warehouse.connect(db) as con:
        readings_count = con.execute("SELECT COUNT(*) FROM readings_clean").fetchone()[
            0
        ]
        stats_count = con.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        assert readings_count == _N * 24 * _MONTH_DAYS
        assert stats_count == _N * _MONTH_DAYS
