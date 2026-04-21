"""Clean: handle missing entries and outliers.

Two distinct problems the brief conflates under "missing values and outliers":

1. Missing entries — reindex against the expected hourly grid so gaps become
   explicit NaNs, then forward-fill bounded to FFILL_LIMIT hours. Longer gaps
   stay NaN and surface as reduced `count` in downstream stats.

2. Outliers — domain bounds first (physical plausibility), then per-turbine
   IQR. Deliberately NOT 2σ here: 2σ is the anomaly rule, and using it for
   cleaning would scrub the anomalies before detection.
"""

from datetime import date, datetime, timedelta

import pandas as pd

from turbine_pipeline.schemas import (
    POWER_OUTPUT_MAX,
    WIND_SPEED_MAX,
    CleanReading,
)

FFILL_LIMIT = 2  # hours — wind conditions are broadly stable over 1-2h
IQR_MULTIPLIER = 1.5


def _reindex_to_hourly_grid(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """Ensure every turbine has one row per hour of the run day.

    Missing rows become NaN rows. This is what turns "sensor dropped a
    reading" from an invisible absence into a visible gap the rest of the
    pipeline can reason about.
    """
    start = datetime.combine(run_date, datetime.min.time())
    expected_hours = pd.date_range(start, start + timedelta(hours=23), freq="h")

    pieces = []
    for turbine_id, group in df.groupby("turbine_id", sort=True):
        reindexed = (
            group.set_index("timestamp")
            .reindex(expected_hours)
            .rename_axis("timestamp")
            .reset_index()
        )
        reindexed["turbine_id"] = turbine_id
        pieces.append(reindexed)

    return pd.concat(pieces, ignore_index=True)


def _null_out_of_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Null values outside physical plausibility. These are sensor faults
    (negatives, 999 fill values, stuck-high readings), not weather."""
    df = df.copy()
    df.loc[
        (df["power_output"] < 0) | (df["power_output"] > POWER_OUTPUT_MAX),
        "power_output",
    ] = pd.NA
    df.loc[
        (df["wind_speed"] < 0) | (df["wind_speed"] > WIND_SPEED_MAX),
        "wind_speed",
    ] = pd.NA
    return df


def _null_iqr_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Null per-turbine IQR outliers in `column`.

    Per-turbine because turbines at different fleet positions see different
    wind regimes — a farm-wide IQR would flag the windiest turbines as
    outliers on every windy day.
    """
    df = df.copy()

    def _bounds(s: pd.Series) -> tuple[float, float]:
        q1, q3 = s.quantile([0.25, 0.75])
        iqr = q3 - q1
        return q1 - IQR_MULTIPLIER * iqr, q3 + IQR_MULTIPLIER * iqr

    for turbine_id, group in df.groupby("turbine_id"):
        values = group[column].dropna()
        if len(values) < 4:  # IQR on <4 points is noise
            continue
        lo, hi = _bounds(values)
        mask = (
            (df["turbine_id"] == turbine_id)
            & df[column].notna()
            & ((df[column] < lo) | (df[column] > hi))
        )
        df.loc[mask, column] = pd.NA

    return df


def _bounded_ffill(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill up to FFILL_LIMIT consecutive NaNs, per turbine.

    Bounded because imputing 8 hours of power output from a single prior
    reading is fabrication, not imputation.
    """
    df = df.sort_values(["turbine_id", "timestamp"]).copy()
    numeric_cols = ["wind_speed", "wind_direction", "power_output"]
    df[numeric_cols] = df.groupby("turbine_id")[numeric_cols].ffill(limit=FFILL_LIMIT)
    return df


def clean(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """Run the full cleaning pipeline: reindex → null-bad → null-IQR → ffill.

    Input is expected to already be filtered to the run-date window.

    Args:
        df: Raw readings filtered to the run-date window.
        run_date: Calendar day being cleaned, used to build the hourly grid.

    Returns:
        Validated CleanReading frame with one row per turbine per hour.
    """
    df = _reindex_to_hourly_grid(df, run_date)
    df = _null_out_of_bounds(df)
    df = _null_iqr_outliers(df, "power_output")
    df = _bounded_ffill(df)

    # Cast after cleaning so NaN-containing float cols validate cleanly.
    df["turbine_id"] = df["turbine_id"].astype(int)
    return CleanReading.validate(df)
