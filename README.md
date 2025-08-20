# Airflow Playground
- A tiny Airflow project with one DAG that:
- fetches Stockholm weather from Open-Meteo,
- saves raw JSON,
- builds a small aggregated CSV,
- branches on a simple rule (max temp > 25 °C).

## Prerequisites
- Docker & Docker Compose v2+
- Port 8080 available

```shell
# 1) (recommended) set your user id for file permissions
export AIRFLOW_UID=$(id -u)

# 2) One-time init (sets up DB and admin user)
docker compose up airflow-init

# 3) Start Airflow
docker compose up -d
```

## Trigger DAG

UI: http://localhost:8080

Login: airflow / airflow

Toggle it On.

Click Trigger DAG (▶).

## Outputs

Raw JSON → ./data/raw/weather_<YYYY-MM-DD>.json

Aggregated CSV → ./data/processed/weather_daily_<YYYY-MM-DD>.csv

DAG logs the result → e.g `INFO - 😊 Not that hot for 2025-08-20`
