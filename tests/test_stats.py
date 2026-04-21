from datetime import date

import pandas as pd

from turbine_pipeline import clean, stats


def test_stats_shape(clean_day: pd.DataFrame, run_date: date) -> None:
    cleaned = clean.clean(clean_day, run_date)
    out = stats.summarise(cleaned, run_date)
    assert len(out) == 5
    assert set(out.columns) == {
        "run_date",
        "turbine_id",
        "min_power",
        "max_power",
        "mean_power",
        "std_power",
        "count",
    }


def test_min_le_mean_le_max(clean_day: pd.DataFrame, run_date: date) -> None:
    """The property every stats function must satisfy."""
    cleaned = clean.clean(clean_day, run_date)
    out = stats.summarise(cleaned, run_date)
    assert (out["min_power"] <= out["mean_power"]).all()
    assert (out["mean_power"] <= out["max_power"]).all()


def test_count_reflects_unimputed_gaps(
    day_with_gaps: pd.DataFrame, run_date: date
) -> None:
    """Turbine 2's long gap should reduce its count below 24 so consumers
    can see the mean is based on partial data."""
    cleaned = clean.clean(day_with_gaps, run_date)
    out = stats.summarise(cleaned, run_date)
    t2_count = int(out.loc[out["turbine_id"] == 2, "count"].iloc[0])
    t1_count = int(out.loc[out["turbine_id"] == 1, "count"].iloc[0])
    assert t1_count == 24  # 2-hour gap was ffilled
    assert t2_count == 21  # 5-hour gap: 2 ffilled, 3 remain NaN


def test_run_date_stamped(clean_day: pd.DataFrame, run_date: date) -> None:
    cleaned = clean.clean(clean_day, run_date)
    out = stats.summarise(cleaned, run_date)
    assert (out["run_date"].dt.date == run_date).all()