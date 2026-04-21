# Turbine Pipeline

Local python turbine pipeline to ingest, clean, summerise, and store turbine data.

## Running

Requires [UV] -> https://docs.astral.sh/uv/

```bash
# Install deps
uv sync --group dev

# Format
uv run ruff format .

# Lint
uv run ruff check .

# Run the test suite
uv run pytest -v --tb=short --cov=turbine_pipeline --cov-report=term-missing

# Run the pipeline to ingest all csv data (time bounds are project specific)
d=2022-03-01; while [[ "$d" < "2022-04-01" ]]; do
  uv run turbine-pipeline --data-dir /path/to/csvs --run-date "$d" --db-path turbines.duckdb
  d=$(date -d "$d + 1 day" +%Y-%m-%d)
done

# Run the pipeline to ingest a specific days data
uv run turbine-pipeline \
    --data-dir /path/to/csvs \
    --run-date 2022-03-15 \
    --db-path turbines.duckdb
```
### Viewing Data

Requires [DuckDB UI] -> https://duckdb.org/2025/03/12/duckdb-ui

Add Database -> Path to local turbines.duckdb file

## Design
This small pipeline is designed to be portable, due to use case, aslongside being modular to enable unit testing.

```
ingest  ──▶  clean  ──▶  stats  ──▶  anomalies  ──▶  warehouse
                                      │
                                      └─▶  (DuckDB: 3 tables)
```

| Module         | Responsibility                                            |
| -------------- | --------------------------------------------------------- |
| `schemas.py`   | Pandera contracts enforced at layer boundaries            |
| `ingest.py`    | Read `data_group_*.csv`, validate, filter to run window   |
| `clean.py`     | Missing-value imputation + outlier removal                |
| `stats.py`     | Per-turbine min/max/mean/std/count                        |
| `anomalies.py` | Cross-turbine 2σ deviation detection                      |
| `warehouse.py` | Idempotent DuckDB upserts                                 |
| `pipeline.py`  | Orchestration + CLI                                       |

### Tooling choice

- **pandas** for business logic. More than capable for the amount of data we have.
- **Pandera** for schema enforcement.
- **DuckDB** for storage. Easy to set up and would scale due to being built for OLAP workloads.
- **UV** for managing environment. Fast, easy to use and set up, and enables easy porting.


## Assumptions
- **Power Units are non-negative**
- **Anomaly detection is cross-turbine**
- **Missing data longer than 2 hours are not included**

## Project layout

```
turbine_pipeline/
├── pyproject.toml
├── README.md
├── src/turbine_pipeline/
│   ├── __init__.py
│   ├── schemas.py
│   ├── ingest.py
│   ├── clean.py
│   ├── stats.py
│   ├── anomalies.py
│   ├── warehouse.py
│   └── pipeline.py
└── tests/
    ├── conftest.py
    ├── test_ingest.py
    ├── test_clean.py
    ├── test_stats.py
    ├── test_anomalies.py
    ├── test_warehouse.py
    └── test_pipeline.py
```