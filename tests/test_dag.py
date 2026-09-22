from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def bag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)


def test_dag_imports_without_errors(bag):
    assert bag.import_errors == {}
    assert "waybill" in bag.dags


def test_every_stage_has_a_task(bag):
    from waybill import pipeline

    dag = bag.dags["waybill"]
    assert set(dag.task_ids) == set(pipeline.STAGES) - {"download"}


def test_dependencies_follow_the_medallion_order(bag):
    dag = bag.dags["waybill"]
    assert dag.get_task("bronze").upstream_task_ids == {"generate"}
    assert dag.get_task("silver_pings").upstream_task_ids == {"bronze"}
    assert dag.get_task("port_calls").upstream_task_ids == {"silver_pings"}
    assert dag.get_task("voyages").upstream_task_ids == {"port_calls"}
    assert dag.get_task("cpg").upstream_task_ids == {"voyages", "vessel_dim"}
    assert dag.get_task("gold").upstream_task_ids == {"cpg"}
    assert dag.get_task("quality").upstream_task_ids == {"gold"}
    assert dag.get_task("maintenance").upstream_task_ids == {"quality"}
