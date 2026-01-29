from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from google.cloud import storage


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


def _partition_prefix(env: str, source: str, entity: str, ingestion_date: Optional[str], run_id: str) -> str:
    if ingestion_date is None:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"env={env}/source={source}/entity={entity}/ingestion_date={ingestion_date}/run_id={run_id}"


def write_parquet_gcs(
    df: pd.DataFrame,
    bucket: str,
    env: str,
    source: str,
    entity: str,
    run_id: str,
    ingestion_date: Optional[str] = None,  # YYYY-MM-DD
    metadata: Optional[Dict[str, str]] = None,
) -> str:
    """
    Write df as Parquet to GCS under the partitioned path.
    Also write a JSON sidecar _metadata.json for easy audit.
    Returns the gs:// URI of the Parquet file.
    """
    client = storage.Client()
    bkt = client.bucket(bucket)

    prefix = _partition_prefix(env, source, entity, ingestion_date, run_id)
    parquet_blob_path = f"{prefix}/part-0001.parquet"
    meta_blob_path = f"{prefix}/_metadata.json"

    # Prepare Parquet bytes in-memory
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    parquet_blob = bkt.blob(parquet_blob_path)
    parquet_blob.content_type = "application/x-parquet"
    parquet_blob.metadata = {"env": env, "source": source, "entity": entity, "run_id": run_id}
    parquet_blob.upload_from_file(buf)

    # Prepare metadata JSON
    meta = metadata.copy() if metadata else {}
    meta["ingestion_timestamp_utc"] = datetime.now(timezone.utc).isoformat()
    meta["run_id"] = run_id
    meta["env"] = env
    meta["source"] = source
    meta["entity"] = entity

    meta_blob = bkt.blob(meta_blob_path)
    meta_blob.content_type = "application/json"
    meta_blob.metadata = {"env": env, "source": source, "entity": entity, "run_id": run_id}
    meta_blob.upload_from_string(json.dumps(meta, indent=2), content_type="application/json")

    return f"gs://{bucket}/{parquet_blob_path}"


def write_raw_bytes_gcs(
    gz_bytes: bytes,
    bucket: str,
    env: str,
    source: str,
    entity: str,
    run_id: str,
    ingestion_date: Optional[str] = None,  # YYYY-MM-DD
) -> str:
    """
    Store gzipped raw response to GCS:
    gs://<bucket>/.../run_id=<run_id>/_raw_bytes/response.json.gz
    """
    client = storage.Client()
    bkt = client.bucket(bucket)

    prefix = _partition_prefix(env, source, entity, ingestion_date, run_id)
    raw_blob_path = f"{prefix}/_raw_bytes/response.json.gz"

    raw_blob = bkt.blob(raw_blob_path)
    raw_blob.content_type = "application/gzip"
    raw_blob.metadata = {"env": env, "source": source, "entity": entity, "run_id": run_id}
    raw_blob.upload_from_string(gz_bytes, content_type="application/gzip")

    return f"gs://{bucket}/{raw_blob_path}"