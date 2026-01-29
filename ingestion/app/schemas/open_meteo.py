from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class Hourly(BaseModel):
    time: List[str]
    temperature_2m: Optional[List[float]] = None
    relative_humidity_2m: Optional[List[float]] = None


class OpenMeteoResponse(BaseModel):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    hourly_units: dict
    hourly: Hourly