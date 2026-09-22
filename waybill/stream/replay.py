from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F

from waybill import delta
from waybill.ingest import bronze

STREAM = ("bronze", "ais_stream")

DAY_PATTERN = r"(\\d{4}-\\d{2}-\\d{2})"


def landing(cfg):
    return Path(cfg.paths.raw) / "stream_landing"


def checkpoint(cfg):
    return Path(cfg.paths.checkpoints) / "ais_stream"


def stage(cfg, days=None):
    target = landing(cfg)
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    selected = days or cfg.window.dates()[: cfg.stream.days]
    staged = []
    for day in selected:
        source = Path(cfg.paths.raw) / "ais" / day
        for item in sorted(source.glob("*.csv")):
            destination = target / (day + "__" + item.name)
            destination.symlink_to(item.resolve())
            staged.append(destination.name)
    return {"days": selected, "files": len(staged)}


def source(spark, cfg):
    return (
        spark.readStream.option("header", True)
        .option("maxFilesPerTrigger", cfg.stream.files_per_trigger)
        .schema(bronze.RAW_SCHEMA)
        .csv(str(landing(cfg)))
        .withColumn("source_file", F.expr("input_file_name()"))
    )


def prepared(cfg, raw):
    day_expr = "regexp_extract(input_file_name(), '" + DAY_PATTERN + "', 1)"
    marked = bronze.flagged(bronze.typed(raw, day_expr))
    accepted = (
        marked.filter("reject_reason is null")
        .drop("reject_reason", "raw_mmsi", "raw_base_datetime", "raw_lat", "raw_lon")
        .withColumn("ping_date", F.expr("cast(ping_ts as date)"))
        .withColumn("ingested_at", F.current_timestamp())
    )
    return accepted.withWatermark("ping_ts", str(cfg.stream.watermark_minutes) + " minutes").dropDuplicates(
        ["mmsi", "ping_ts"]
    )


def run(spark, cfg, days=None, reset=True):
    staged = stage(cfg, days)
    path = cfg.paths.table(*STREAM)
    if reset:
        if Path(path).exists():
            shutil.rmtree(path)
        if checkpoint(cfg).exists():
            shutil.rmtree(checkpoint(cfg))
    started = time.time()
    frame = prepared(cfg, source(spark, cfg))
    query = (
        frame.writeStream.option("checkpointLocation", str(checkpoint(cfg)))
        .trigger(availableNow=True)
        .foreachBatch(
            lambda batch, epoch: delta.write(batch, path, ["file_day"], "append", "ais_stream")
        )
        .start()
    )
    query.awaitTermination()
    progress = list(query.recentProgress)
    rows = sum(item.get("numInputRows", 0) for item in progress)
    dropped = 0
    for item in progress:
        for operator in item.get("stateOperators", []) or []:
            dropped += operator.get("numRowsDroppedByWatermark", 0)
    report = {
        "days": staged["days"],
        "files": staged["files"],
        "batches": len(progress),
        "input_rows": int(rows),
        "written_rows": delta.row_count(path),
        "dropped_by_watermark": int(dropped),
        "watermark_minutes": cfg.stream.watermark_minutes,
        "seconds": round(time.time() - started, 2),
    }
    destination = Path(cfg.paths.reports) / "stream.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2))
    return report
