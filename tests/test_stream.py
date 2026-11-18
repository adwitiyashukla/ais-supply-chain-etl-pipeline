from __future__ import annotations

import json
from pathlib import Path

import pytest

from aisetl import delta, pipeline
from aisetl.ingest import bronze
from aisetl.stream import replay
from tests.conftest import build_config


@pytest.fixture(scope="module")
def streamed(spark, tmp_path_factory):
    cfg = build_config(tmp_path_factory.mktemp("stream"))
    cfg.paths.ensure()
    pipeline.run_stage(cfg, "generate", spark=spark)
    pipeline.run_stage(cfg, "bronze", spark=spark)
    report = pipeline.run_stage(cfg, "stream", spark=spark)["result"]
    return cfg, report


def test_stream_writes_its_own_bronze_table(streamed):
    cfg, report = streamed
    assert report["batches"] > 0
    assert report["written_rows"] > 0
    assert delta.exists(cfg.paths.table(*replay.STREAM))


def test_stream_and_batch_agree_on_the_replayed_day(spark, streamed):
    cfg, report = streamed
    day = report["days"][0]
    batch = delta.read(spark, cfg.paths.table(*bronze.PINGS)).filter(
        "file_day = date '" + day + "'"
    )
    streamed_rows = delta.read(spark, cfg.paths.table(*replay.STREAM)).filter(
        "file_day = date '" + day + "'"
    )
    difference = abs(batch.count() - streamed_rows.count())
    assert difference <= report["dropped_by_watermark"] + batch.count() * 0.01


def test_stream_report_lands_on_disk(streamed):
    cfg, report = streamed
    payload = json.loads(Path(cfg.paths.reports, "stream.json").read_text())
    assert payload["input_rows"] == report["input_rows"]
    assert payload["watermark_minutes"] == cfg.stream.watermark_minutes


def test_streamed_rows_are_typed_like_batch_rows(spark, streamed):
    cfg, _ = streamed
    batch = delta.read(spark, cfg.paths.table(*bronze.PINGS))
    live = delta.read(spark, cfg.paths.table(*replay.STREAM))
    assert set(live.columns) == set(batch.columns)
    assert live.filter("mmsi is null or ping_ts is null").count() == 0
