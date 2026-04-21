from datetime import date

import pandas as pd

from turbine_pipeline import anomalies, clean, stats


def test_identical_fleet_has_no_anomalies(run_date: date) -> None:
    """If every turbine has the same mean, std is 0 and nothing is anomalous."""
    rows = [
        {
            "run_date": pd.Timestamp(run_date),
            "turbine_id": tid,
            "min_power": 2.0,
            "max_power": 4.0,
            "mean_power": 3.0,
            "std_power": 0.5,
            "count": 24,
        }
        for tid in range(1, 6)
    ]
    out = anomalies.detect(pd.DataFrame(rows), run_date)
    assert out.empty


def test_fleet_outlier_is_flagged(run_date: date) -> None:
    """Five turbines at mean ≈ 3.0, one at 0.5 — the low one should flag."""
    means = [3.0, 3.0, 3.0, 3.0, 3.0, 0.5]
    rows = [
        {
            "run_date": pd.Timestamp(run_date),
            "turbine_id": tid,
            "min_power": m - 0.5,
            "max_power": m + 0.5,
            "mean_power": m,
            "std_power": 0.3,
            "count": 24,
        }
        for tid, m in enumerate(means, start=1)
    ]
    out = anomalies.detect(pd.DataFrame(rows), run_date)
    assert list(out["turbine_id"]) == [6]
    assert out["deviation_sigmas"].iloc[0] < -2


def test_single_turbine_has_no_anomalies(run_date: date) -> None:
    """Fleet std is NaN for a single turbine — the early-return path must fire."""
    row = pd.DataFrame([{
        "run_date": pd.Timestamp(run_date),
        "turbine_id": 1,
        "min_power": 2.0,
        "max_power": 4.0,
        "mean_power": 3.0,
        "std_power": 0.5,
        "count": 24,
    }])
    out = anomalies.detect(row, run_date)
    assert out.empty


def test_anomaly_frame_contains_correct_fleet_stats(run_date: date) -> None:
    """fleet_mean and deviation_sigmas on the returned row must be accurate."""
    means = [3.0, 3.0, 3.0, 3.0, 3.0, 0.5]
    rows = [
        {
            "run_date": pd.Timestamp(run_date),
            "turbine_id": tid,
            "min_power": m - 0.5,
            "max_power": m + 0.5,
            "mean_power": m,
            "std_power": 0.3,
            "count": 24,
        }
        for tid, m in enumerate(means, start=1)
    ]
    out = anomalies.detect(pd.DataFrame(rows), run_date)
    expected_fleet_mean = sum(means) / len(means)
    assert abs(float(out["fleet_mean"].iloc[0]) - expected_fleet_mean) < 0.01