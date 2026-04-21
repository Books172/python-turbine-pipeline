"""Pipeline: orchestrate ingest → clean → stats → anomalies → store.

Idempotent for a given (data_dir, run_date) pair — running it twice produces
the same warehouse state.
"""

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from turbine_pipeline import anomalies, clean, ingest, stats, warehouse

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """Return value of `run_pipeline` — useful for tests and callers that
    want to act on the output without re-querying the warehouse."""

    readings: pd.DataFrame
    stats: pd.DataFrame
    anomalies: pd.DataFrame


def run_pipeline(
    data_dir: Path | str,
    run_date: date,
    db_path: Path | str,
) -> PipelineResult:
    """Run the full pipeline for one calendar day.

    Args:
        data_dir: Directory containing ``data_group_*.csv`` files.
        run_date: Calendar day to process (UTC, midnight-aligned).
        db_path: Path to the DuckDB file. Created if it does not exist.

    Returns:
        PipelineResult containing the cleaned readings, daily stats,
        and any detected anomalies for the run date.

    Raises:
        FileNotFoundError: No matching CSVs found in ``data_dir``.
        RuntimeError: Matching CSVs exist but all failed to parse.
    """
    log.info("Running pipeline for %s", run_date)

    raw = ingest.read_raw(Path(data_dir))
    windowed = ingest.filter_to_window(raw, run_date)
    log.info("Ingested %d raw rows in window", len(windowed))

    cleaned = clean.clean(windowed, run_date)
    log.info("Cleaned to %d rows", len(cleaned))

    summary = stats.summarise(cleaned, run_date)
    flagged = anomalies.detect(summary, run_date)
    log.info(
        "Computed stats for %d turbines, flagged %d anomalies",
        len(summary),
        len(flagged),
    )

    with warehouse.connect(db_path) as con:
        warehouse.write_readings(con, cleaned)
        warehouse.write_stats(con, summary)
        warehouse.write_anomalies(con, flagged)

    return PipelineResult(readings=cleaned, stats=summary, anomalies=flagged)


def cli() -> None:
    """Command-line entry: `turbine-pipeline --data-dir ... --run-date ...`"""
    parser = argparse.ArgumentParser(description="Run the turbine pipeline.")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument(
        "--run-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="Calendar day to process, YYYY-MM-DD.",
    )
    parser.add_argument("--db-path", type=Path, default=Path("turbines.duckdb"))
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    result = run_pipeline(args.data_dir, args.run_date, args.db_path)
    print(
        f"Processed {len(result.readings)} readings, "
        f"{len(result.stats)} turbines, "
        f"{len(result.anomalies)} anomalies → {args.db_path}"
    )


if __name__ == "__main__":
    cli()
