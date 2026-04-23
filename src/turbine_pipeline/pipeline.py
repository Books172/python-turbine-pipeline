"""Pipeline: orchestrate ingest → clean → stats → anomalies → store.

Idempotent for a given (data_dir, run_date) pair — running it twice produces
the same warehouse state.
"""

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from turbine_pipeline import anomalies, clean, ingest, stats, warehouse

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    """Outputs of a single pipeline run — avoids re-querying the warehouse."""

    readings: pd.DataFrame
    stats: pd.DataFrame
    anomalies: pd.DataFrame


def _process_date(
    raw: pd.DataFrame,
    run_date: date,
    con: duckdb.DuckDBPyConnection,
) -> PipelineResult | None:
    """Process one calendar day from a pre-read raw DataFrame.

    Args:
        raw: Full raw readings frame (may span multiple days).
        run_date: Calendar day to process.
        con: Open DuckDB connection from :func:`warehouse.connect`.

    Returns:
        PipelineResult for the day, or None if no data exists for that date.
    """
    windowed = ingest.filter_to_window(raw, run_date)
    if windowed.empty:
        log.warning("No data found for %s — skipping", run_date)
        return None

    log.info("Running pipeline for %s (%d rows)", run_date, len(windowed))

    cleaned = clean.clean(windowed, run_date)
    summary = stats.summarise(cleaned, run_date)
    flagged = anomalies.detect(summary, run_date)
    log.info("%s: %d turbines, %d anomalies", run_date, len(summary), len(flagged))

    warehouse.write_raw(con, windowed)
    warehouse.write_readings(con, cleaned)
    warehouse.write_stats(con, summary)
    warehouse.write_anomalies(con, flagged)

    return PipelineResult(readings=cleaned, stats=summary, anomalies=flagged)


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
        ValueError: CSVs were read successfully but contain no data for ``run_date``.
    """
    raw = ingest.read_raw(Path(data_dir))
    with warehouse.connect(db_path) as con:
        result = _process_date(raw, run_date, con)
    if result is None:
        raise ValueError(f"No data found for {run_date} in {data_dir}")
    return result


def run_pipeline_range(
    data_dir: Path | str,
    start_date: date,
    end_date: date,
    db_path: Path | str,
) -> dict[date, PipelineResult]:
    """Process all calendar days from start_date to end_date inclusive.

    CSVs are read once and a single database connection is held for the
    entire range, making this significantly more efficient than calling
    run_pipeline in a loop for large date ranges.

    Args:
        data_dir: Directory containing ``data_group_*.csv`` files.
        start_date: First calendar day to process.
        end_date: Last calendar day to process (inclusive).
        db_path: Path to the DuckDB file. Created if it does not exist.

    Returns:
        Mapping of run_date to PipelineResult for each day that had data.
        Dates with no data are skipped and absent from the result.

    Raises:
        FileNotFoundError: No matching CSVs found in ``data_dir``.
        RuntimeError: Matching CSVs exist but all failed to parse.
        ValueError: ``end_date`` is before ``start_date``.
    """
    if end_date < start_date:
        raise ValueError(f"end_date {end_date} must not be before start_date {start_date}")

    total_days = (end_date - start_date).days + 1
    log.info("Processing %s to %s (%d days)", start_date, end_date, total_days)

    raw = ingest.read_raw(Path(data_dir))

    results: dict[date, PipelineResult] = {}
    with warehouse.connect(db_path) as con:
        current = start_date
        while current <= end_date:
            result = _process_date(raw, current, con)
            if result is not None:
                results[current] = result
            current += timedelta(days=1)

    log.info("Completed %d/%d days with data", len(results), total_days)
    return results


def _parse_date(s: str) -> date:
    """Parse a YYYY-MM-DD string into a date for argparse ``type=`` callbacks."""
    return datetime.strptime(s, "%Y-%m-%d").date()


def cli() -> None:
    """Command-line entry point.

    Single day:  turbine-pipeline --data-dir ... --run-date YYYY-MM-DD
    Date range:  turbine-pipeline --data-dir ... --start-date YYYY-MM-DD --end-date YYYY-MM-DD
    """
    parser = argparse.ArgumentParser(description="Run the turbine pipeline.")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=Path("turbines.duckdb"))
    parser.add_argument("--log-level", default="INFO")

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--run-date",
        type=_parse_date,
        help="Single calendar day to process, YYYY-MM-DD.",
    )
    date_group.add_argument(
        "--start-date",
        type=_parse_date,
        help="Start of date range, YYYY-MM-DD. Requires --end-date.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        help="End of date range (inclusive), YYYY-MM-DD. Requires --start-date.",
    )
    args = parser.parse_args()

    if args.start_date and not args.end_date:
        parser.error("--end-date is required when --start-date is provided")

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.run_date:
        result = run_pipeline(args.data_dir, args.run_date, args.db_path)
        print(
            f"Processed {len(result.readings)} readings, "
            f"{len(result.stats)} turbines, "
            f"{len(result.anomalies)} anomalies → {args.db_path}"
        )
    else:
        results = run_pipeline_range(args.data_dir, args.start_date, args.end_date, args.db_path)
        total_readings = sum(len(r.readings) for r in results.values())
        print(f"Processed {len(results)} days, {total_readings} total readings → {args.db_path}")


if __name__ == "__main__":
    cli()
