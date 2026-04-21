"""Anomaly detection: flag turbines whose daily average deviates from the fleet.

Cross-turbine interpretation of the brief: for the run window, compute each
turbine's mean output, then flag any turbine whose mean sits more than
SIGMA_THRESHOLD standard deviations from the fleet-wide mean of those means.

The grammar of the brief ("turbines that have deviated") points at this
reading rather than the alternative (hourly-within-turbine). See README.
"""

from datetime import date, datetime

import pandas as pd

from turbine_pipeline.schemas import Anomalies, DailyStats

SIGMA_THRESHOLD = 2.0


def detect(stats: pd.DataFrame, run_date: date) -> pd.DataFrame:
    """Flag turbines whose daily mean power deviates from the fleet average.

    Empty output is valid — it means no turbine deviated enough to flag,
    which is the common case.

    Args:
        stats: Validated DailyStats frame from :func:`stats.summarise`.
        run_date: Calendar day the statistics were computed for.

    Returns:
        Validated Anomalies frame. May be empty.
    """
    DailyStats.validate(stats)

    means = stats["mean_power"]
    fleet_mean = means.mean()
    fleet_std = means.std(ddof=0)  # population std — we have the whole fleet

    if fleet_std == 0 or pd.isna(fleet_std):
        # All turbines identical (or ≤1 turbine). No anomalies definable.
        return Anomalies.validate(_empty_anomalies_frame())

    deviations = (means - fleet_mean) / fleet_std
    flagged = stats.assign(
        turbine_mean=means,
        fleet_mean=fleet_mean,
        fleet_std=fleet_std,
        deviation_sigmas=deviations,
    )
    flagged = flagged.loc[flagged["deviation_sigmas"].abs() > SIGMA_THRESHOLD]

    out = flagged[
        [
            "turbine_id",
            "turbine_mean",
            "fleet_mean",
            "fleet_std",
            "deviation_sigmas",
        ]
    ].copy()
    out.insert(0, "run_date", datetime.combine(run_date, datetime.min.time()))
    out = out.reset_index(drop=True)
    return Anomalies.validate(out)


def _empty_anomalies_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the correct dtypes for Anomalies validation."""
    return pd.DataFrame(
        {
            "run_date": pd.Series([], dtype="datetime64[ns]"),
            "turbine_id": pd.Series([], dtype="int64"),
            "turbine_mean": pd.Series([], dtype="float64"),
            "fleet_mean": pd.Series([], dtype="float64"),
            "fleet_std": pd.Series([], dtype="float64"),
            "deviation_sigmas": pd.Series([], dtype="float64"),
        }
    )
