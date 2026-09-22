from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import functions as F

from waybill import delta
from waybill.generate import cpg
from waybill.gold import otif, ports
from waybill.ingest import bronze
from waybill.silver import dims, portcalls, voyages


@dataclass(frozen=True)
class Expectation:
    table: str
    name: str
    expression: str
    max_failure_rate: float = 0.0


@dataclass(frozen=True)
class Uniqueness:
    table: str
    name: str
    keys: tuple


@dataclass(frozen=True)
class Reference:
    table: str
    name: str
    column: str
    parent: str
    parent_column: str


EXPECTATIONS = [
    Expectation("bronze.ais_pings", "mmsi_in_range", "mmsi between 100000000 and 999999999"),
    Expectation("bronze.ais_pings", "latitude_in_range", "lat between -90 and 90"),
    Expectation("bronze.ais_pings", "longitude_in_range", "lon between -180 and 180"),
    Expectation("bronze.ais_pings", "timestamp_present", "ping_ts is not null"),
    Expectation("bronze.ais_pings", "speed_plausible", "sog is null or sog between 0 and 102"),
    Expectation("bronze.ais_pings", "class_known", "transceiver_class in ('A', 'B')"),
    Expectation("silver.port_calls", "arrival_before_departure", "arrival_ts <= departure_ts"),
    Expectation("silver.port_calls", "berth_positive", "berth_hours > 0"),
    Expectation("silver.port_calls", "wait_not_negative", "anchor_wait_hours >= 0"),
    Expectation("silver.port_calls", "dwell_under_30_days", "dwell_hours <= 720"),
    Expectation("silver.voyages", "sea_time_positive", "sea_hours > 0"),
    Expectation("silver.voyages", "distance_positive", "distance_km > 0"),
    Expectation("silver.voyages", "speed_plausible", "avg_speed_knots between 0.5 and 40"),
    Expectation("silver.voyages", "ports_differ", "from_port != to_port"),
    Expectation("silver.order_lines", "shipped_not_over", "shipped_cases <= ordered_cases"),
    Expectation("silver.order_lines", "delivery_after_order", "delivered_ts > order_ts"),
    Expectation("silver.order_lines", "promise_after_order", "requested_delivery_date >= order_date"),
    Expectation("silver.order_lines", "import_has_booking", "not import_linked or booking_id is not null"),
    Expectation("gold.otif_daily", "rate_between_zero_and_one", "otif_rate between 0 and 1"),
    Expectation("gold.otif_daily", "lines_positive", "lines > 0"),
    Expectation("gold.port_performance", "dwell_positive", "avg_dwell_hours > 0"),
]

UNIQUENESS = [
    Uniqueness("silver.port_calls", "call_id_unique", ("call_id",)),
    Uniqueness("silver.voyages", "voyage_id_unique", ("voyage_id",)),
    Uniqueness("silver.order_lines", "line_id_unique", ("line_id",)),
    Uniqueness("silver.dim_vessel", "vessel_key_unique", ("vessel_key",)),
    Uniqueness("bronze.ais_pings", "ping_unique", ("mmsi", "ping_ts")),
]

REFERENCES = [
    Reference("silver.order_lines", "line_dc_exists", "dc_id", "silver.dim_dc", "dc_id"),
    Reference("silver.order_lines", "line_sku_exists", "sku_id", "silver.dim_sku", "sku_id"),
    Reference("silver.bookings", "booking_voyage_exists", "voyage_id", "silver.voyages", "voyage_id"),
    Reference("silver.voyages", "voyage_vessel_seen", "mmsi", "silver.dim_vessel", "mmsi"),
]

TABLES = {
    "bronze.ais_pings": bronze.PINGS,
    "bronze.ais_quarantine": bronze.QUARANTINE,
    "bronze.load_audit": bronze.AUDIT,
    "silver.port_calls": portcalls.CALLS,
    "silver.voyages": voyages.VOYAGES,
    "silver.dim_vessel": dims.VESSELS,
    "silver.dim_dc": cpg.CENTERS,
    "silver.dim_sku": cpg.SKUS,
    "silver.order_lines": cpg.LINES,
    "silver.bookings": cpg.BOOKINGS,
    "gold.otif_daily": otif.DAILY,
    "gold.port_performance": ports.PERFORMANCE,
}


def load(spark, cfg, name):
    if name not in TABLES:
        raise KeyError("unknown table: " + name)
    return delta.read(spark, cfg.paths.table(*TABLES[name]))


def run_expectations(spark, cfg):
    results = []
    grouped = {}
    for item in EXPECTATIONS:
        grouped.setdefault(item.table, []).append(item)
    for table, items in grouped.items():
        frame = load(spark, cfg, table)
        aggregates = [F.expr("count(*) as total_rows")]
        for position, item in enumerate(items):
            aggregates.append(
                F.expr("sum(case when not (" + item.expression + ") then 1 else 0 end) as failed_" + str(position))
            )
        row = frame.agg(*aggregates).collect()[0]
        total = row["total_rows"]
        for position, item in enumerate(items):
            failed = row["failed_" + str(position)] or 0
            rate = failed / total if total else 0.0
            results.append(
                {
                    "table": table,
                    "check": item.name,
                    "kind": "expectation",
                    "rows": total,
                    "failed": int(failed),
                    "rate": round(rate, 6),
                    "threshold": item.max_failure_rate,
                    "status": "pass" if rate <= item.max_failure_rate else "fail",
                }
            )
    return results


def run_uniqueness(spark, cfg):
    results = []
    for item in UNIQUENESS:
        frame = load(spark, cfg, item.table)
        row = frame.agg(
            F.expr("count(*) as total_rows"),
            F.expr("count(distinct " + ", ".join(item.keys) + ") as distinct_rows"),
        ).collect()[0]
        duplicates = row["total_rows"] - row["distinct_rows"]
        results.append(
            {
                "table": item.table,
                "check": item.name,
                "kind": "uniqueness",
                "rows": row["total_rows"],
                "failed": int(duplicates),
                "rate": round(duplicates / row["total_rows"], 6) if row["total_rows"] else 0.0,
                "threshold": 0.0,
                "status": "pass" if duplicates == 0 else "fail",
            }
        )
    return results


def run_references(spark, cfg):
    results = []
    for item in REFERENCES:
        child = load(spark, cfg, item.table).select(item.column).filter(item.column + " is not null")
        parent = load(spark, cfg, item.parent).select(
            F.col(item.parent_column).alias("parent_key")
        ).dropDuplicates()
        orphans = child.join(F.broadcast(parent), child[item.column] == parent["parent_key"], "left_anti")
        total = child.count()
        missing = orphans.count()
        results.append(
            {
                "table": item.table,
                "check": item.name,
                "kind": "reference",
                "rows": total,
                "failed": missing,
                "rate": round(missing / total, 6) if total else 0.0,
                "threshold": 0.0,
                "status": "pass" if missing == 0 else "fail",
            }
        )
    return results


def run_quarantine(spark, cfg):
    audit = load(spark, cfg, "bronze.load_audit")
    row = audit.agg(
        F.expr("sum(rows_read) as rows_read"),
        F.expr("sum(rows_quarantined) as rows_quarantined"),
        F.expr("sum(rows_duplicate) as rows_duplicate"),
    ).collect()[0]
    read = row["rows_read"] or 0
    quarantined = row["rows_quarantined"] or 0
    rate = quarantined / read if read else 0.0
    return [
        {
            "table": "bronze.ais_quarantine",
            "check": "quarantine_rate",
            "kind": "threshold",
            "rows": read,
            "failed": int(quarantined),
            "rate": round(rate, 6),
            "threshold": cfg.quality.max_quarantine_rate,
            "status": "pass" if rate <= cfg.quality.max_quarantine_rate else "fail",
        },
        {
            "table": "bronze.ais_pings",
            "check": "minimum_rows",
            "kind": "threshold",
            "rows": read,
            "failed": 0 if read >= cfg.quality.min_bronze_rows else 1,
            "rate": 0.0,
            "threshold": float(cfg.quality.min_bronze_rows),
            "status": "pass" if read >= cfg.quality.min_bronze_rows else "fail",
        },
    ]


def run(spark, cfg):
    started = time.time()
    results = (
        run_expectations(spark, cfg)
        + run_uniqueness(spark, cfg)
        + run_references(spark, cfg)
        + run_quarantine(spark, cfg)
    )
    failures = [item for item in results if item["status"] == "fail"]
    report = {
        "checks": len(results),
        "failed": len(failures),
        "seconds": round(time.time() - started, 2),
        "results": results,
    }
    destination = Path(cfg.paths.reports) / "quality.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2, default=str))
    if failures and cfg.quality.fail_on_error:
        names = ", ".join(item["table"] + "." + item["check"] for item in failures)
        raise ValueError("quality gate failed: " + names)
    return report
