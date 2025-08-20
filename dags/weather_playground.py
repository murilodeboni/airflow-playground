from datetime import datetime, timedelta
from pathlib import Path
import json
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
PROC_DIR = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_ARGS = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(minutes=10),
}

def fetch_weather(ds, **_):
    # Stockholm
    lat, lon = 59.3293, 18.0686
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m"
        "&timezone=Europe%2FStockholm"
    )
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    raw_path = RAW_DIR / f"weather_{ds}.json"
    raw_path.write_text(r.text, encoding="utf-8")
    return str(raw_path)

def transform_weather(ti, ds, **_):
    raw_path = Path(ti.xcom_pull(task_ids="extract.fetch"))
    data = json.loads(raw_path.read_text(encoding="utf-8"))
    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    rh    = data["hourly"]["relative_humidity_2m"]
    df = pd.DataFrame({"time": times, "temp_c": temps, "rh_pct": rh})
    df["date"] = pd.to_datetime(df["time"]).dt.date
    agg = df.groupby("date").agg(
        max_temp=("temp_c", "max"),
        min_temp=("temp_c", "min"),
        avg_rh=("rh_pct", "mean"),
    ).reset_index()
    out_csv = PROC_DIR / f"weather_daily_{ds}.csv"
    agg.to_csv(out_csv, index=False)
    return {"csv_path": str(out_csv), "max_temp": float(agg["max_temp"].iloc[0])}

def is_hot(ti, **_):
    info = ti.xcom_pull(task_ids="transform")
    return "hot_path" if info["max_temp"] > 25 else "cool_path"

with DAG(
        dag_id="weather_playground",
        description="Tiny Airflow playground: fetch, transform, branch.",
        start_date=datetime(2025, 8, 1),
        schedule_interval="@daily",
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["playground", "weather"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("extract"):
        fetch = PythonOperator(
            task_id="fetch",
            python_callable=fetch_weather,
        )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_weather,
    )

    decide = BranchPythonOperator(
        task_id="decide_hot_or_cool",
        python_callable=is_hot,
    )

    hot = BashOperator(
        task_id="hot_path",
        bash_command='echo "🔥 Hot day detected (max > 25°C) for {{ ds }}"'
    )
    cool = BashOperator(
        task_id="cool_path",
        bash_command='echo "😊 Not that hot for {{ ds }}"'
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    start >> fetch >> transform >> decide
    decide >> [hot, cool] >> end
