from __future__ import annotations

import time

from waybill import delta, session
from waybill.generate import ais, cpg
from waybill.gold import inventory as gold_inventory
from waybill.gold import lanes as gold_lanes
from waybill.gold import otif as gold_otif
from waybill.gold import ports as gold_ports
from waybill.ingest import bronze, download
from waybill.quality import checks
from waybill.silver import dims, pings, portcalls, voyages

MAINTAINED = [
    bronze.PINGS,
    pings.PINGS,
    portcalls.CALLS,
    voyages.VOYAGES,
    cpg.LINES,
    cpg.ORDERS,
]


def bronze_frame(spark, cfg, days=None):
    frame = delta.read(spark, cfg.paths.table(*bronze.PINGS))
    selected = days or cfg.window.dates()
    return frame.filter("file_day in (" + ", ".join("date '" + day + "'" for day in selected) + ")")


def stage_generate(spark, cfg, days=None):
    cfg.paths.ensure()
    written = ais.run(spark, cfg)
    return {"days_generated": len(written)}


def stage_download(spark, cfg, days=None):
    cfg.paths.ensure()
    return {"files": download.run(cfg, days)}


def stage_bronze(spark, cfg, days=None):
    return {"days": bronze.run(spark, cfg, days)}


def stage_silver_pings(spark, cfg, days=None):
    return pings.run(spark, cfg, bronze_frame(spark, cfg, days))


def stage_port_calls(spark, cfg, days=None):
    area = delta.read(spark, cfg.paths.table(*pings.PINGS))
    return portcalls.run(spark, cfg, area)


def stage_voyages(spark, cfg, days=None):
    calls = delta.read(spark, cfg.paths.table(*portcalls.CALLS))
    return voyages.run(spark, cfg, calls)


def stage_vessel_dim(spark, cfg, days=None):
    return dims.run(spark, cfg, bronze_frame(spark, cfg, days))


def stage_cpg(spark, cfg, days=None):
    legs = delta.read(spark, cfg.paths.table(*voyages.VOYAGES))
    return cpg.run(spark, cfg, legs)


def stage_gold(spark, cfg, days=None):
    lines = delta.read(spark, cfg.paths.table(*cpg.LINES))
    legs = delta.read(spark, cfg.paths.table(*voyages.VOYAGES))
    books = delta.read(spark, cfg.paths.table(*cpg.BOOKINGS))
    calls = delta.read(spark, cfg.paths.table(*portcalls.CALLS))
    positions = delta.read(spark, cfg.paths.table(*cpg.INVENTORY))
    skus = delta.read(spark, cfg.paths.table(*cpg.SKUS))
    result = {}
    result.update(gold_otif.run(cfg, lines))
    result.update(gold_lanes.run(cfg, legs, books))
    result.update(gold_ports.run(cfg, calls))
    result.update(gold_inventory.run(cfg, positions, skus))
    return result


def stage_quality(spark, cfg, days=None):
    return checks.run(spark, cfg)


def stage_stream(spark, cfg, days=None):
    from waybill.stream import replay

    return replay.run(spark, cfg, days)


def stage_tuning(spark, cfg, days=None):
    from waybill.tuning import lab

    if not cfg.tuning.enabled:
        return {"skipped": True}
    return lab.run(spark, cfg, days)


def stage_maintenance(spark, cfg, days=None):
    return {
        ".".join(table): delta.maintenance(cfg.paths.table(*table))
        for table in MAINTAINED
        if delta.exists(cfg.paths.table(*table))
    }


STAGES = {
    "generate": stage_generate,
    "download": stage_download,
    "bronze": stage_bronze,
    "silver_pings": stage_silver_pings,
    "port_calls": stage_port_calls,
    "voyages": stage_voyages,
    "vessel_dim": stage_vessel_dim,
    "cpg": stage_cpg,
    "gold": stage_gold,
    "quality": stage_quality,
    "stream": stage_stream,
    "tuning": stage_tuning,
    "maintenance": stage_maintenance,
}

PIPELINE = [
    "bronze",
    "silver_pings",
    "port_calls",
    "voyages",
    "vessel_dim",
    "cpg",
    "gold",
    "quality",
]


def run_stage(cfg, name, days=None, spark=None):
    if name not in STAGES:
        raise KeyError("unknown stage: " + name)
    owned = spark is None
    active = spark or session.build(cfg, "waybill-" + name)
    started = time.time()
    try:
        result = STAGES[name](active, cfg, days)
    finally:
        if owned:
            active.stop()
    return {"stage": name, "seconds": round(time.time() - started, 2), "result": result}


def run_pipeline(cfg, days=None, stages=None, spark=None):
    owned = spark is None
    active = spark or session.build(cfg, "waybill")
    outcomes = []
    try:
        for name in stages or PIPELINE:
            outcomes.append(run_stage(cfg, name, days, active))
    finally:
        if owned:
            active.stop()
    return outcomes
