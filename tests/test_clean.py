from datetime import date

import pandas as pd

from turbine_pipeline import clean


def test_clean_reindexes_full_day(clean_day: pd.DataFrame, run_date: date) -> None:
    """Output has exactly 24 rows per turbine even when input is complete."""
    out = clean.clean(clean_day, run_date)
    counts = out.groupby("turbine_id").size()
    assert (counts == 24).all()


def test_clean_reindexes_missing_hours(
    day_with_gaps: pd.DataFrame, run_date: date
) -> None:
    """Missing hours are materialised as rows so downstream stats see them."""
    out = clean.clean(day_with_gaps, run_date)
    counts = out.groupby("turbine_id").size()
    assert (counts == 24).all()


def test_short_gap_is_filled(
    day_with_gaps: pd.DataFrame, run_date: date
) -> None:
    """Turbine 1 had a 2-hour gap — should be forward-filled."""
    out = clean.clean(day_with_gaps, run_date)
    t1 = out[out["turbine_id"] == 1].sort_values("timestamp")
    assert t1["power_output"].notna().all()


def test_long_gap_stays_nan(
    day_with_gaps: pd.DataFrame, run_date: date
) -> None:
    """Turbine 2 had a 5-hour gap — ffill limit is 2, so hours 12-14 stay NaN."""
    out = clean.clean(day_with_gaps, run_date)
    t2 = out[out["turbine_id"] == 2].sort_values("timestamp")
    nan_count = t2["power_output"].isna().sum()
    # Hours 10, 11 get filled from hour 9; hours 12, 13, 14 exceed the limit.
    assert nan_count == 3


def test_negative_power_is_nulled(
    day_with_outliers: pd.DataFrame, run_date: date
) -> None:
    """Physically impossible values get nulled, then potentially ffilled."""
    out = clean.clean(day_with_outliers, run_date)
    # No negatives survive cleaning regardless of fill decisions.
    assert (out["power_output"].dropna() >= 0).all()


def test_stuck_high_sensor_is_nulled(
    day_with_outliers: pd.DataFrame, run_date: date
) -> None:
    out = clean.clean(day_with_outliers, run_date)
    assert out["power_output"].dropna().max() < 100


def test_clean_does_not_modify_input(
    day_with_outliers: pd.DataFrame, run_date: date
) -> None:
    """Cleaning is a pure function — input frame untouched."""
    before = day_with_outliers.copy()
    clean.clean(day_with_outliers, run_date)
    pd.testing.assert_frame_equal(day_with_outliers, before)