from __future__ import annotations

from pathlib import Path

import pytest

from waybill import delta


@pytest.fixture
def frame(spark):
    return spark.range(0, 400).selectExpr(
        "id",
        "case when id % 4 = 0 then '2024-01-01' else '2024-01-02' end as dt",
        "timestamp_seconds(1704067200 + id) as event_ts",
    )


def test_append_then_read_back(spark, tmp_path, frame):
    path = str(tmp_path / "table")
    assert delta.write(frame, path, ["dt"], "append", "table") == 400
    assert delta.row_count(path) == 400
    loaded = delta.read(spark, path)
    assert loaded.count() == 400
    assert set(loaded.columns) == {"id", "dt", "event_ts"}


def test_second_append_creates_a_new_version(spark, tmp_path, frame):
    path = str(tmp_path / "table")
    delta.write(frame, path, ["dt"], "append", "table")
    delta.write(frame.selectExpr("id + 1000 as id", "dt", "event_ts"), path, ["dt"], "append")
    assert delta.version(path) == 1
    assert delta.row_count(path) == 800
    assert delta.row_count(path, 0) == 400


def test_overwrite_replaces_every_row(spark, tmp_path, frame):
    path = str(tmp_path / "table")
    delta.write(frame, path, ["dt"], "append", "table")
    delta.write(frame.limit(7), path, ["dt"], "overwrite")
    assert delta.row_count(path) == 7
    assert delta.read(spark, path).count() == 7


def test_replace_touches_one_partition_only(spark, tmp_path, frame):
    path = str(tmp_path / "table")
    delta.write(frame, path, ["dt"], "append", "table")
    before = delta.read(spark, path).filter("dt = '2024-01-01'").count()
    replacement = frame.filter("dt = '2024-01-01'").limit(3)
    delta.replace(replacement, path, ["dt"], {"dt": "2024-01-01"})
    after = delta.read(spark, path)
    assert after.filter("dt = '2024-01-01'").count() == 3
    assert after.filter("dt = '2024-01-02'").count() == 400 - before
    assert delta.version(path) == 1


def test_empty_frame_still_creates_the_table(spark, tmp_path, frame):
    path = str(tmp_path / "empty")
    assert delta.write(frame.limit(0), path, ["dt"], "append", "empty") == 0
    assert delta.exists(path)
    assert delta.read(spark, path).count() == 0


def test_uncommitted_files_are_cleaned_before_a_write(spark, tmp_path, frame):
    path = str(tmp_path / "table")
    delta.write(frame, path, ["dt"], "append", "table")
    stray = Path(path, "dt=2024-01-01", "stray.parquet")
    stray.write_bytes(Path(delta.data_files(path)[0]).read_bytes())
    assert delta.clean_orphans(path) == 1
    assert not stray.exists()
    assert delta.row_count(path) == 400


def test_partition_values_are_parsed_from_the_path():
    assert delta.partition_values("dt=2024-01-01/part-0.parquet") == {"dt": "2024-01-01"}
    assert delta.partition_values("part-0.parquet") == {}
