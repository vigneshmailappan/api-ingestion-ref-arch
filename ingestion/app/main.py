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
from .transform import normalize_open_meteo
from .write_gcs import write_parquet_local, write_raw_bytes_local
from .schemas.open_meteo import OpenMeteoResponse


def build_logger():
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )
    return structlog.get_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo API → local Parquet")
    parser.add_argument("--env", default="dev", help="Environment: dev|test|prod")
    parser.add_argument("--latitude", type=float, default=55.6050, help="Latitude (e.g., Malmö)")
    parser.add_argument("--longitude", type=float, default=13.0038, help="Longitude (e.g., Malmö)")
    parser.add_argument("--hourly", default="temperature_2m,relative_humidity_2m",
                        help="Open-Meteo hourly params")
    parser.add_argument("--start-date", help="YYYY-MM-DD", default=None)
    parser.add_argument("--end-date", help="YYYY-MM-DD", default=None)
    parser.add_argument("--out-dir", default="out", help="Local output base directory")
    parser.add_argument("--entity", default="forecast", help="Logical entity name")
    parser.add_argument("--source", default="open-meteo", help="Source system name")
    parser.add_argument("--run-id", default=None, help="Override run_id (uuid if omitted)")
    parser.add_argument("--keep-raw-bytes", action="store_true", help="Store gzipped raw JSON too")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and transform only; no writes")
    return parser.parse_args()


def main():
    log = build_logger()
    args = parse_args()

    env = args.env
    source = args.source
    entity = args.entity
    out_dir = args.out_dir
    run_id = args.run_id or str(uuid.uuid4())
    keep_raw_env = os.getenv("KEEP_RAW_BYTES", "").lower() in {"1", "true", "yes"}
    keep_raw = args.keep_raw_bytes or keep_raw_env

    start_ts = datetime.now(timezone.utc)

    try:
        # 1) Fetch JSON (and optionally raw bytes)
        json_doc: Dict[str, Any] = fetch_open_meteo(
            latitude=args.latitude,
            longitude=args.longitude,
            hourly=args.hourly,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        raw_path = None
        if keep_raw:
            raw_bytes = fetch_raw_bytes(
                latitude=args.latitude,
                longitude=args.longitude,
                hourly=args.hourly,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            gz = gzip_bytes(raw_bytes)
            if not args.dry_run:
                raw_path = write_raw_bytes_local(
                    gz, out_dir, env=env, source=source, entity=entity, run_id=run_id
                )

        # 2) Validate minimal schema (optional but good practice)
        try:
            OpenMeteoResponse.model_validate(json_doc)
        except ValidationError as ve:
            log.error("schema_validation_failed", errors=ve.errors())
            raise

        # 3) Transform → DataFrame
        df = normalize_open_meteo(json_doc)
        record_count = len(df)

        # 4) Write parquet (unless dry-run)
        parquet_path = None
        if not args.dry_run:
            parquet_path = write_parquet_local(
                df,
                base_dir=out_dir,
                env=env,
                source=source,
                entity=entity,
                run_id=run_id,
                metadata={
                    "request_params": json.dumps(
                        {
                            "latitude": args.latitude,
                            "longitude": args.longitude,
                            "hourly": args.hourly,
                            "start_date": args.start_date,
                            "end_date": args.end_date,
                        }
                    )
                },
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
        log = build_logger()
        log.error("ingestion_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()