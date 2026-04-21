"""Stats: per-turbine summary statistics over the run window."""

from datetime import date, datetime

import pandas as pd

from turbine_pipeline.schemas import DailyStats


def summarise(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """Compute min/max/mean/std/count of power_output per turbine.

    `count` is the number of non-NaN hourly readings and is deliberately
    exposed so downstream consumers can judge how complete a day's data was.
    A mean over 12 hours is a different object to a mean over 24.
    """
    agg = (
        df.groupby("turbine_id")["power_output"]
        .agg(
            min_power="min",
            max_power="max",
            mean_power="mean",
            std_power="std",
            count="count",
        )
        .reset_index()
    )
    agg.insert(0, "run_date", datetime.combine(run_date, datetime.min.time()))

    # std is undefined for n<2; coerce to 0 rather than NaN so the schema
    # (non-negative, nullable) stays clean and downstream SQL isn't surprised.
    agg["std_power"] = agg["std_power"].fillna(0.0)
    agg["count"] = agg["count"].astype(int)

    return DailyStats.validate(agg)