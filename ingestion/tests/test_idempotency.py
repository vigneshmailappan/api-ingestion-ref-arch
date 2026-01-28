import uuid
from pathlib import Path
import pandas as pd

from ingestion.app.write_gcs import write_parquet_local

def test_partition_path_contains_run_id(tmp_path):
    df = pd.DataFrame([{"time": "2024-01-01T00:00", "temperature_2m": 1.0, "relative_humidity_2m": 80}])
    run_id = str(uuid.uuid4())
    fp = write_parquet_local(
        df, base_dir=str(tmp_path), env="dev", source="open-meteo", entity="forecast", run_id=run_id
    )
    assert run_id in fp
    assert Path(fp).exists()