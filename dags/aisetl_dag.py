from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aisetl import pipeline  # noqa: E402
from aisetl.config import Config  # noqa: E402

CONFIG = os.environ.get("AISETL_CONFIG", str(ROOT / "config.toml"))

SOURCE_TASK = "land" if Config.load(CONFIG).ais.source == "files" else "generate"

DEFAULTS = {
    "owner": "aisetl",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def execute(stage, **context):
    cfg = Config.load(CONFIG)
    cfg.paths.ensure()
    logical = context.get("ds")
    days = [logical] if logical in cfg.window.dates() else None
    if stage in {"port_calls", "voyages", "cpg", "gold", "quality", "maintenance"}:
        days = None
    return pipeline.run_stage(cfg, stage, days)


def task(dag, stage):
    return PythonOperator(
        task_id=stage, python_callable=execute, op_kwargs={"stage": stage}, dag=dag
    )


with DAG(
    dag_id="aisetl",
    description="AIS to supply chain lakehouse",
    default_args=DEFAULTS,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["aisetl", "spark", "delta"],
) as dag:
    source = task(dag, SOURCE_TASK)
    bronze = task(dag, "bronze") if SOURCE_TASK == "generate" else None
    silver_pings = task(dag, "silver_pings")
    port_calls = task(dag, "port_calls")
    legs = task(dag, "voyages")
    vessel_dim = task(dag, "vessel_dim")
    orders = task(dag, "cpg")
    marts = task(dag, "gold")
    gate = task(dag, "quality")
    stream = task(dag, "stream")
    tuning = task(dag, "tuning")
    upkeep = task(dag, "maintenance")

    landed = bronze if bronze is not None else source
    if bronze is not None:
        source >> bronze
    landed >> silver_pings >> port_calls >> legs >> marts
    port_calls >> orders >> marts >> gate
    landed >> vessel_dim >> orders
    landed >> stream
    landed >> tuning
    gate >> upkeep
