"""Pandera schemas — contracts between pipeline layers.

Schemas are enforced at layer boundaries so each module can trust its inputs
and downstream consumers can trust its outputs. A schema failure is a bug,
not bad data — bad data should be handled by `clean.py` before validation.
"""

import pandera.pandas as pa
from pandera.typing import Series

# Physical plausibility bounds. These are deliberately loose — they catch
# sensor faults (negatives, 999 fill values) without second-guessing real
# meteorological variation. Tighter bounds belong in cleaning, not schema.
WIND_SPEED_MAX = 40.0  # m/s — well above cut-out for any commercial turbine
POWER_OUTPUT_MAX = 10.0  # MW — above nameplate for the largest onshore turbines


class RawReading(pa.DataFrameModel):
    """Shape of a row straight out of the CSV. Permissive on values."""

    timestamp: Series[pa.DateTime]
    turbine_id: Series[int] = pa.Field(ge=1)
    wind_speed: Series[float] = pa.Field(nullable=True)
    wind_direction: Series[float] = pa.Field(ge=0, le=360, nullable=True)
    power_output: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True


class CleanReading(pa.DataFrameModel):
    """Post-cleaning shape. Values within physical bounds; NaNs only where
    a gap was too long to honestly impute."""

    timestamp: Series[pa.DateTime]
    turbine_id: Series[int] = pa.Field(ge=1)
    wind_speed: Series[float] = pa.Field(ge=0, le=WIND_SPEED_MAX, nullable=True)
    wind_direction: Series[float] = pa.Field(ge=0, le=360, nullable=True)
    power_output: Series[float] = pa.Field(ge=0, le=POWER_OUTPUT_MAX, nullable=True)

    class Config:
        strict = True
        coerce = True


class DailyStats(pa.DataFrameModel):
    """Per-turbine summary over the run window."""

    run_date: Series[pa.DateTime]
    turbine_id: Series[int] = pa.Field(ge=1)
    min_power: Series[float] = pa.Field(ge=0, nullable=True)
    max_power: Series[float] = pa.Field(ge=0, nullable=True)
    mean_power: Series[float] = pa.Field(ge=0, nullable=True)
    std_power: Series[float] = pa.Field(ge=0)
    count: Series[int] = pa.Field(ge=0)

    class Config:
        strict = True
        coerce = True


class Anomalies(pa.DataFrameModel):
    """Turbines flagged as deviating from the fleet on the run date."""

    run_date: Series[pa.DateTime]
    turbine_id: Series[int] = pa.Field(ge=1)
    turbine_mean: Series[float] = pa.Field(ge=0)
    fleet_mean: Series[float] = pa.Field(ge=0)
    fleet_std: Series[float] = pa.Field(ge=0)
    deviation_sigmas: Series[float]

    class Config:
        strict = True
        coerce = True
