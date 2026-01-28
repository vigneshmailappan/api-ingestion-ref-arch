from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def ensure_dir(path: str | Path) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_parquet_local(
    df: pd.DataFrame,
    base_dir: str,
    env: str,
    source: str,
    entity: str,
    run_id: str,
    ingestion_date: Optional[str] = None,  # YYYY-MM-DD
    metadata: Optional[Dict[str, str]] = None,
) -> str:
    """
    Write df to Parquet under a partitioned path:
    out/env=<env>/source=<source>/entity=<entity>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/part-0001.parquet
    Returns the full file path.
    """
    if ingestion_date is None:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    out_dir = Path(base_dir) / f"env={env}" / f"source={source}" / f"entity={entity}" / \
        f"ingestion_date={ingestion_date}" / f"run_id={run_id}"
    ensure_dir(out_dir)

    file_path = out_dir / "part-0001.parquet"

    # Attach metadata via PyArrow schema metadata (pandas will pass through)
    # Note: pandas allows setting metadata via df.attrs is not persisted; schema metadata requires pyarrow Table.
    # We’ll write using pandas, but include an adjacent _metadata.json for audit simplicity.
    meta_json_path = out_dir / "_metadata.json"
    meta = metadata.copy() if metadata else {}
    meta["ingestion_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    meta["run_id"] = run_id
    meta["env"] = env
    meta["source"] = source
    meta["entity"] = entity

    with open(meta_json_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    df.to_parquet(file_path, engine="pyarrow", index=False)
    return str(file_path)


def write_raw_bytes_local(
    raw_bytes: bytes,
    base_dir: str,
    env: str,
    source: str,
    entity: str,
    run_id: str,
    ingestion_date: Optional[str] = None,  # YYYY-MM-DD
) -> str:
    """
    Store gzipped raw response:
    out/env=.../source=.../entity=.../ingestion_date=YYYY-MM-DD/run_id=<run_id>/_raw_bytes/response.json.gz
    """
    if ingestion_date is None:
        from datetime import datetime, timezone
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    out_dir = Path(base_dir) / f"env={env}" / f"source={source}" / f"entity={entity}" / \
        f"ingestion_date={ingestion_date}" / f"run_id={run_id}" / "_raw_bytes"
    ensure_dir(out_dir)

    fp = out_dir / "response.json.gz"
    with open(fp, "wb") as f:
        f.write(raw_bytes)
    return str(fp)