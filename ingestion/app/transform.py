from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def normalize_open_meteo(json_doc: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Open-Meteo JSON into a tidy hourly DataFrame:
    columns: time (str ISO), temperature_2m (float), relative_humidity_2m (float)
    """
    hourly = json_doc.get("hourly") or {}
    times = hourly.get("time") or []
    temp = hourly.get("temperature_2m") or []
    rh = hourly.get("relative_humidity_2m") or []

    # Align lengths safely
    n = min(
          len(times)
        , len(temp) if temp else len(times)
        , len(rh) if rh else len(times)
        )
    
    rows: List[Dict[str, Any]] = []
    for i in range(n):
        rows.append(
            {
                "time": times[i],
                "temperature_2m": float(temp[i]) if i < len(temp) else None,
                "relative_humidity_2m": float(rh[i]) if i < len(rh) else None,
            }
        )
    df = pd.DataFrame(rows)
    return df