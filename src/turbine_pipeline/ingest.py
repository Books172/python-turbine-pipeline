"""Ingest: read CSVs from disk, validate schema, filter to the run window.

One file per turbine group; each file is read independently so a single
corrupt file doesn't sink the whole run.
"""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from turbine_pipeline.schemas import RawReading

log = logging.getLogger(__name__)

CSV_GLOB = "data_group_*.csv"


def _read_one(path: Path) -> pd.DataFrame | None:
    """Read a single CSV. Return None on failure so the caller can skip."""
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
    except Exception as exc:  # noqa: BLE001 — we genuinely want to swallow + log
        log.warning("Failed to read %s: %s", path, exc)
        return None
    return df


def read_raw(data_dir: Path) -> pd.DataFrame:
    """Read and concatenate all turbine-group CSVs in ``data_dir``.

    A genuinely empty directory is almost always a configuration bug, not a
    valid state, hence the hard raise rather than returning an empty frame.

    Args:
        data_dir: Directory containing ``data_group_*.csv`` files.

    Returns:
        Schema-validated RawReading frame combining all groups.

    Raises:
        FileNotFoundError: No matching CSV files found in the directory.
        RuntimeError: Matching files exist but all failed to parse.
    """
    paths = sorted(Path(data_dir).glob(CSV_GLOB))
    if not paths:
        raise FileNotFoundError(
            f"No files matching {CSV_GLOB} in {data_dir}"
        )

    frames = [df for df in (_read_one(p) for p in paths) if df is not None]
    if not frames:
        raise RuntimeError(f"All CSVs in {data_dir} failed to read")

    raw = pd.concat(frames, ignore_index=True)
    return RawReading.validate(raw)


def filter_to_window(
    df: pd.DataFrame, run_date: date
) -> pd.DataFrame:
    """Filter to the 24-hour calendar day starting at midnight on ``run_date``.

    Calendar-aligned rather than rolling because the brief describes daily
    appends and daily stats — calendar days are what a reviewer would expect
    to see in a ``daily_stats`` table.

    Args:
        df: Raw readings frame from :func:`read_raw`.
        run_date: Calendar day to retain.

    Returns:
        A copy of ``df`` containing only rows within the run-date window.
    """
    start = datetime.combine(run_date, datetime.min.time())
    end = start + timedelta(days=1)
    mask = (df["timestamp"] >= start) & (df["timestamp"] < end)
    return df.loc[mask].copy()
