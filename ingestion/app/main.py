from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import structlog
from pydantic import ValidationError

from .fetch import fetch_open_meteo, fetch_raw_bytes, gzip_bytes
from .schemas.open_meteo import OpenMeteoResponse
from .transform import normalize_open_meteo
from .write_gcs import (
    write_parquet_gcs,
    write_parquet_local,
    write_raw_bytes_gcs,
    write_raw_bytes_local,
)


def build_logger():
    """
    Configure structured JSON logs to stdout.
    Cloud Run Jobs will ship stdout/stderr to Cloud Logging automatically.
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo API → Parquet (local or GCS)")
    # Environment & source entity
    parser.add_argument("--env", default="dev", help="Environment: dev|test|prod")
    parser.add_argument("--source", default="open-meteo", help="Source system name")
    parser.add_argument("--entity", default="forecast", help="Logical entity name")

    # Coordinates & window
    parser.add_argument("--latitude", type=float, default=55.6050, help="Latitude (e.g., Malmö)")
    parser.add_argument("--longitude", type=float, default=13.0038, help="Longitude (e.g., Malmö)")
    parser.add_argument("--hourly", default="temperature_2m,relative_humidity_2m",
                        help="Open-Meteo hourly params (comma-separated)")
    parser.add_argument("--start-date", help="YYYY-MM-DD", default=None)
    parser.add_argument("--end-date", help="YYYY-MM-DD", default=None)

    # Output targets
    parser.add_argument("--out-dir", default="out", help="Local output base directory")
    parser.add_argument("--to-gcs", action="store_true", help="Write to GCS instead of local")
    parser.add_argument("--gcs-bucket", default=None, help="Target GCS bucket")

    # Run behavior
    parser.add_argument("--run-id", default=None, help="Override run_id (uuid if omitted)")
    parser.add_argument("--keep-raw-bytes", action="store_true", help="Store gzipped raw JSON too")
    parser.add_argument("--dry-run", action="store_true", help="Fetch/transform only; no writes")

    return parser.parse_args()


def main():
    log = build_logger()
    args = parse_args()

    # Resolve parameters
    env: str = args.env
    source: str = args.source
    entity: str = args.entity
    out_dir: str = args.out_dir
    run_id: str = args.run_id or str(uuid.uuid4())
    keep_raw_env: bool = os.getenv("KEEP_RAW_BYTES", "").lower() in {"1", "true", "yes"}
    keep_raw: bool = args.keep_raw_bytes or keep_raw_env

    # If writing to GCS, bucket must be provided
    gcs_bucket = args.gcs_bucket
    if args.to_gcs and not gcs_bucket:
        print("ERROR: --to-gcs requires --gcs-bucket (e.g., project-...-raw)", file=sys.stderr)
        sys.exit(2)

    start_ts = datetime.now(timezone.utc)

    try:
        # 1) Fetch JSON (retries handled in fetch_open_meteo)
        json_doc: Dict[str, Any] = fetch_open_meteo(
            latitude=args.latitude,
            longitude=args.longitude,
            hourly=args.hourly,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        # 2) Optional: fetch raw bytes for audit retention (Option A strict)
        raw_path = None
        gz = None
        if keep_raw:
            raw_bytes = fetch_raw_bytes(
                latitude=args.latitude,
                longitude=args.longitude,
                hourly=args.hourly,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            gz = gzip_bytes(raw_bytes)

        # 3) Schema validation (minimal)
        try:
            OpenMeteoResponse.model_validate(json_doc)
        except ValidationError as ve:
            log.error("schema_validation_failed", errors=ve.errors())
            raise

        # 4) Transform to a tidy hourly DataFrame
        df = normalize_open_meteo(json_doc)
        record_count = len(df)

        # 5) Write outputs (skip if --dry-run)
        parquet_path = None
        if not args.dry_run:
            meta = {
                "request_params": json.dumps(
                    {
                        "latitude": args.latitude,
                        "longitude": args.longitude,
                        "hourly": args.hourly,
                        "start_date": args.start_date,
                        "end_date": args.end_date,
                    }
                )
            }

            if args.to_gcs:
                parquet_path = write_parquet_gcs(
                    df,
                    bucket=gcs_bucket,
                    env=env,
                    source=source,
                    entity=entity,
                    run_id=run_id,
                    metadata=meta,
                )
                if keep_raw and gz is not None:
                    raw_path = write_raw_bytes_gcs(
                        gz,
                        bucket=gcs_bucket,
                        env=env,
                        source=source,
                        entity=entity,
                        run_id=run_id,
                    )
            else:
                parquet_path = write_parquet_local(
                    df,
                    base_dir=out_dir,
                    env=env,
                    source=source,
                    entity=entity,
                    run_id=run_id,
                    metadata=meta,
                )
                if keep_raw and gz is not None:
                    raw_path = write_raw_bytes_local(
                        gz, out_dir, env=env, source=source, entity=entity, run_id=run_id
                    )

        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        log.info(
            "ingestion_completed",
            env=env,
            source=source,
            entity=entity,
            run_id=run_id,
            record_count=record_count,
            parquet_path=parquet_path,
            raw_path=raw_path,
            duration_seconds=duration,
            dry_run=args.dry_run,
        )

    except Exception as e:
        # Log the error and exit non-zero for observability in Cloud Run Job and CI
        log = build_logger()
        log.error("ingestion_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()