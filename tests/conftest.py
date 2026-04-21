"""Shared pytest fixtures.

The real CSVs are clean, so fixtures here deliberately inject the failure
modes the brief describes (missing entries, outliers) so the cleaning and
anomaly logic actually gets exercised.
"""

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

RUN_DATE = date(2022, 3, 15)
TURBINE_IDS = list(range(1, 16))


def _build_day(turbine_ids: list[int], run_date: date) -> pd.DataFrame:
    """24 rows per turbine, hourly, plausible values with realistic variance.

    Power varies hour-to-hour (tracks wind) and has a small per-turbine
    baseline offset so each turbine's mean is slightly different — matches
    reality (different farm positions) and avoids degenerate cases where
    every value in a series is identical (floating-point equality quirks,
    z-score self-capping).
    """
    import math

    start = datetime.combine(run_date, datetime.min.time())
    hours = [start + timedelta(hours=h) for h in range(24)]
    rows = []
    for tid in turbine_ids:
        baseline = 3.0 + 0.05 * ((tid % 5) - 2)  # small per-turbine offset
        for i, ts in enumerate(hours):
            rows.append(
                {
                    "timestamp": ts,
                    "turbine_id": tid,
                    "wind_speed": 12.0 + 0.1 * i,
                    "wind_direction": 180,
                    "power_output": round(baseline + 0.3 * math.sin(i / 3), 3),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def run_date() -> date:
    return RUN_DATE


@pytest.fixture
def clean_day() -> pd.DataFrame:
    """5 turbines × 24 hours, no faults. Baseline for tests that don't
    care about the cleaning logic."""
    return _build_day([1, 2, 3, 4, 5], RUN_DATE)


@pytest.fixture
def day_with_gaps() -> pd.DataFrame:
    """A day where turbine 1 is missing 2 consecutive hours (recoverable)
    and turbine 2 is missing 5 consecutive hours (not recoverable)."""
    df = _build_day([1, 2, 3, 4, 5], RUN_DATE)
    # turbine 1: drop hours 5, 6 — within ffill bound
    mask1 = (df["turbine_id"] == 1) & df["timestamp"].dt.hour.isin([5, 6])
    # turbine 2: drop hours 10-14 — beyond ffill bound
    mask2 = (df["turbine_id"] == 2) & df["timestamp"].dt.hour.isin(
        [10, 11, 12, 13, 14]
    )
    return df.loc[~(mask1 | mask2)].reset_index(drop=True)


@pytest.fixture
def day_with_outliers() -> pd.DataFrame:
    """A day with injected sensor faults."""
    df = _build_day([1, 2, 3, 4, 5], RUN_DATE)
    # Negative power (physical impossibility)
    df.loc[
        (df["turbine_id"] == 1) & (df["timestamp"].dt.hour == 3),
        "power_output",
    ] = -5.0
    # Absurdly high power (stuck sensor)
    df.loc[
        (df["turbine_id"] == 3) & (df["timestamp"].dt.hour == 7),
        "power_output",
    ] = 999.0
    return df


@pytest.fixture
def uploads_dir(
    tmp_path: Path, clean_day: pd.DataFrame
) -> Path:
    """Write a clean day split across three CSVs in the group_N pattern."""
    groups = [TURBINE_IDS[i:i + 5] for i in range(0, len(TURBINE_IDS), 5)]
    for gid, tids in enumerate(groups, start=1):
        group = _build_day(tids, RUN_DATE)
        # make turbine 8 a fleet-wide anomaly for integration tests
        if 8 in tids:
            group.loc[group["turbine_id"] == 8, "power_output"] = 0.5
        group.to_csv(tmp_path / f"data_group_{gid}.csv", index=False)
    return tmp_path
