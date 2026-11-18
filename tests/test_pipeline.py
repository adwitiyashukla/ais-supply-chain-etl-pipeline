from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from aisetl import delta, pipeline
from aisetl.generate import cpg
from aisetl.gold import otif, ports
from aisetl.ingest import bronze
from aisetl.quality import checks
from aisetl.silver import dims, portcalls
from tests.conftest import build_config


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory):
    cfg = build_config(tmp_path_factory.mktemp("pipeline"))
    cfg.paths.ensure()
    pipeline.run_stage(cfg, "generate", spark=spark)
    outcomes = pipeline.run_pipeline(cfg, spark=spark)
    return cfg, {item["stage"]: item["result"] for item in outcomes}


def test_every_stage_reports_rows(built):
    _, outcomes = built
    assert outcomes["bronze"]["days"][0]["rows_valid"] > 0
    assert outcomes["silver_pings"]["port_area_pings"] > 0
    assert outcomes["port_calls"]["port_calls"] > 0
    assert outcomes["voyages"]["voyages"] > 0
    assert outcomes["vessel_dim"]["dim_vessel"] > 0
    assert outcomes["cpg"]["order_lines"] > 0
    assert outcomes["gold"]["otif_daily"] > 0


def test_quality_gate_passes_on_a_clean_run(built):
    _, outcomes = built
    report = outcomes["quality"]
    assert report["failed"] == 0
    assert report["checks"] >= 25


def test_quality_report_is_written_to_disk(built):
    cfg, _ = built
    payload = json.loads(Path(cfg.paths.reports, "quality.json").read_text())
    assert payload["checks"] == len(payload["results"])
    assert {item["status"] for item in payload["results"]} == {"pass"}


def test_bookings_only_reference_real_port_calls(spark, built):
    cfg, _ = built
    calls = delta.read(spark, cfg.paths.table(*portcalls.CALLS)).select("call_id")
    books = delta.read(spark, cfg.paths.table(*cpg.BOOKINGS)).select("call_id").dropDuplicates()
    assert books.count() > 0
    assert books.join(calls, "call_id", "left_anti").count() == 0


def test_port_calls_sit_inside_the_generated_window(spark, built):
    cfg, _ = built
    calls = delta.read(spark, cfg.paths.table(*portcalls.CALLS))
    dates = cfg.window.dates()
    outside = calls.filter(
        "arrival_date < date '" + dates[0] + "' or arrival_date > date '" + dates[-1] + "'"
    )
    assert outside.count() == 0


def test_vessel_dimension_has_one_current_row_per_vessel(spark, built):
    cfg, _ = built
    dim = delta.read(spark, cfg.paths.table(*dims.VESSELS))
    current = dim.filter("is_current")
    assert current.count() == current.select("mmsi").distinct().count()


def test_gold_rates_stay_between_zero_and_one(spark, built):
    cfg, _ = built
    daily = delta.read(spark, cfg.paths.table(*otif.DAILY))
    assert daily.filter("otif_rate < 0 or otif_rate > 1").count() == 0
    performance = delta.read(spark, cfg.paths.table(*ports.PERFORMANCE))
    assert performance.filter("avg_berth_hours <= 0").count() == 0


def test_quality_gate_fails_when_a_rule_is_broken(spark, built):
    cfg, _ = built
    path = cfg.paths.table(*bronze.PINGS)
    broken = delta.read(spark, path).limit(5).withColumn("lat", F.expr("999.0"))
    delta.write(broken, path, ["file_day"], "append")
    with pytest.raises(ValueError, match="quality gate failed"):
        checks.run(spark, cfg)


def test_maintenance_compacts_and_vacuums(spark, built):
    cfg, _ = built
    result = pipeline.run_stage(cfg, "maintenance", spark=spark)["result"]
    assert result
    lookup = {".".join(item): item for item in pipeline.MAINTAINED}
    for table, numbers in result.items():
        assert numbers["files_after"] <= numbers["files_before"]
        assert delta.row_count(cfg.paths.table(*lookup[table])) > 0
