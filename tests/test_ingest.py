from datetime import date
from pathlib import Path

import pytest
from conftest import TURBINE_IDS

from turbine_pipeline import ingest


def test_read_raw_reads_all_groups(uploads_dir: Path) -> None:
    df = ingest.read_raw(uploads_dir)
    assert set(df["turbine_id"].unique()) == set(TURBINE_IDS)
    assert len(df) == len(TURBINE_IDS) * 24


def test_read_raw_empty_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ingest.read_raw(tmp_path)


def test_read_raw_skips_corrupt_file(uploads_dir: Path, caplog) -> None:
    """A garbage file is logged and skipped; good files still return data."""
    import logging

    (uploads_dir / "data_group_99.csv").write_text("not,a,valid\ncsv,at,all\n")
    with caplog.at_level(logging.WARNING, logger="turbine_pipeline.ingest"):
        df = ingest.read_raw(uploads_dir)
    assert set(df["turbine_id"].unique()) == set(TURBINE_IDS)
    assert any("Failed to read" in rec.message for rec in caplog.records)


def test_filter_to_window_calendar_aligned(uploads_dir: Path) -> None:
    df = ingest.read_raw(uploads_dir)
    windowed = ingest.filter_to_window(df, date(2022, 3, 15))
    assert len(windowed) == len(TURBINE_IDS) * 24
    assert windowed["timestamp"].min().date() == date(2022, 3, 15)
    assert windowed["timestamp"].max().date() == date(2022, 3, 15)


def test_filter_to_window_excludes_other_days(uploads_dir: Path) -> None:
    df = ingest.read_raw(uploads_dir)
    empty = ingest.filter_to_window(df, date(2022, 1, 1))
    assert empty.empty


def test_read_raw_all_corrupt_raises(tmp_path: Path) -> None:
    """RuntimeError when all matching CSVs fail to parse."""
    (tmp_path / "data_group_1.csv").write_bytes(b"\xff\xfe not valid utf-8 csv")
    with pytest.raises(RuntimeError, match="All CSVs"):
        ingest.read_raw(tmp_path)
