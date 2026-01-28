from ingestion.app.transform import normalize_open_meteo

def test_normalize_open_meteo_basic():
    sample = {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [1.0, 2.5],
            "relative_humidity_2m": [80, 82],
        }
    }
    df = normalize_open_meteo(sample)
    assert len(df) == 2
    assert set(df.columns) == {"time", "temperature_2m", "relative_humidity_2m"}