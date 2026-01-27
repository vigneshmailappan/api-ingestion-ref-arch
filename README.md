# API Ingestion Pipeline (Open-Meteo → GCS)

This repository contains a Python-based ingestion pipeline that
fetches data from the Open-Meteo API, transforms it into Parquet,
and writes partitioned files to GCS. It will later run in Cloud Run
Jobs and be deployed via Terraform.