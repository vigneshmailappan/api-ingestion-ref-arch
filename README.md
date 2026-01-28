# API Ingestion Pipeline (Open-Meteo → GCS)

This repository contains a Python-based ingestion pipeline that
fetches data from the Open-Meteo API, transforms it into Parquet,
and writes partitioned files to GCS. It will later run in Cloud Run
Jobs and be deployed via Terraform.

# Different ways to Run 

```bash
# quick dry run (no writes)
python -m ingestion.app.main --dry-run

# fetch and write parquet + raw gzip
python -m ingestion.app.main --keep-raw-bytes --out-dir ./out --run-id my-run-123

# set KEEP_RAW_BYTES via env and request specific dates
KEEP_RAW_BYTES=1 python -m ingestion.app.main --start-date 2026-01-01 --end-date 2026-01-03

# custom location and hourly params
python -m ingestion.app.main --latitude 40.7128 --longitude -74.0060 --hourly temperature_2m,wind_speed_10m

# minimal custom run id (useful for repeatable tests)
python -m ingestion.app.main --run-id test-run-0001 --dry-run
```

## validatiaons

```bash

ruff check .
mypy .
pytest

```