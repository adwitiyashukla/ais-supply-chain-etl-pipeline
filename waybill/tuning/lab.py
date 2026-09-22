from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from waybill import delta
from waybill.ingest import bronze
from waybill.silver import pings

STRATEGIES = [
    ("naive_shuffle", "shuffle", {"spark.sql.adaptive.enabled": "false", "spark.sql.autoBroadcastJoinThreshold": "-1"}),
    ("salted_shuffle", "salted", {"spark.sql.adaptive.enabled": "true", "spark.sql.autoBroadcastJoinThreshold": "-1"}),
    ("broadcast", "broadcast", {"spark.sql.adaptive.enabled": "true"}),
]

METRICS = [
    "shuffleReadBytes",
    "shuffleWriteBytes",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "numTasks",
    "executorRunTime",
]


def _api(port, path):
    url = "http://localhost:" + str(port) + "/api/v1" + path
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode())


def application(spark):
    port = int(spark.conf.get("spark.ui.port", "4040"))
    for candidate in (port, 4040, 4041, 4042):
        try:
            applications = _api(candidate, "/applications")
        except (urllib.error.URLError, OSError, ValueError):
            continue
        if applications:
            return candidate, applications[0]["id"]
    return None, None


def stage_metrics(port, app_id, after):
    if port is None:
        return {name: None for name in METRICS}, after
    try:
        stages = _api(port, "/applications/" + app_id + "/stages?status=COMPLETE")
    except (urllib.error.URLError, OSError, ValueError):
        return {name: None for name in METRICS}, after
    totals = dict.fromkeys(METRICS, 0)
    highest = after
    for stage in stages:
        identifier = stage.get("stageId", -1)
        if identifier <= after:
            continue
        highest = max(highest, identifier)
        for name in METRICS:
            totals[name] += stage.get(name, 0) or 0
    return totals, highest


def measure(spark, cfg, bronze_df, strategy, overrides, marker, port, app_id):
    previous = {key: spark.conf.get(key, None) for key in overrides}
    for key, value in overrides.items():
        spark.conf.set(key, value)
    partitions = (
        cfg.tuning.naive_shuffle_partitions if strategy == "shuffle" else cfg.spark.shuffle_partitions
    )
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    started = time.time()
    frame = pings.port_area_pings(spark, cfg, bronze_df, strategy=strategy)
    rows = frame.count()
    elapsed = round(time.time() - started, 2)
    totals, marker = stage_metrics(port, app_id, marker)
    for key, value in previous.items():
        if value is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, value)
    spark.conf.set("spark.sql.shuffle.partitions", cfg.spark.shuffle_partitions)
    result = {
        "strategy": strategy,
        "shuffle_partitions": partitions,
        "rows": rows,
        "seconds": elapsed,
    }
    for name in METRICS:
        value = totals.get(name)
        if name.endswith("Bytes") or name.endswith("Spilled"):
            result[name] = None if value is None else round(value / (1024 * 1024), 2)
        else:
            result[name] = value
    return result, marker


def run(spark, cfg, days=None):
    selected = days or cfg.window.dates()[: cfg.tuning.sample_days]
    frame = delta.read(spark, cfg.paths.table(*bronze.PINGS)).filter(
        "file_day in (" + ", ".join("date '" + day + "'" for day in selected) + ")"
    )
    port, app_id = application(spark)
    marker = -1
    if port is not None:
        _, marker = stage_metrics(port, app_id, -1)
    results = []
    for label, strategy, overrides in STRATEGIES:
        measured, marker = measure(spark, cfg, frame, strategy, overrides, marker, port, app_id)
        measured["label"] = label
        results.append(measured)
    baseline = results[0]
    for item in results:
        item["speedup_vs_naive"] = (
            round(baseline["seconds"] / item["seconds"], 2) if item["seconds"] else None
        )
    report = {"days": selected, "metrics_source": "spark rest api" if port else "timings only",
              "results": results}
    destination = Path(cfg.paths.reports) / "tuning.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2))
    return report
