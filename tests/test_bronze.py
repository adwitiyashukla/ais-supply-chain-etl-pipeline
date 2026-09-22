from __future__ import annotations

from waybill import delta
from waybill.generate import ais
from waybill.ingest import bronze


def prepare(spark, cfg):
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference_ports(spark))
    for position, day in enumerate(cfg.window.dates()):
        follower = cfg.window.dates()[position + 1] if position + 1 < cfg.window.days else None
        ais.write_day(spark, cfg, plan, day, follower)


def reference_ports(spark):
    from waybill import reference

    return reference.ports(spark)


def test_ingest_splits_valid_rows_from_quarantine(spark, cfg):
    prepare(spark, cfg)
    day = cfg.window.dates()[0]
    outcome = bronze.ingest_day(spark, cfg, day)
    assert outcome["rows_read"] > 0
    assert outcome["rows_valid"] > 0
    assert outcome["rows_quarantined"] > 0
    assert outcome["rows_read"] == outcome["rows_valid"] + outcome["rows_quarantined"] + outcome["rows_duplicate"]


def test_bronze_rows_are_typed_and_clean(spark, cfg):
    prepare(spark, cfg)
    bronze.ingest_day(spark, cfg, cfg.window.dates()[0])
    frame = delta.read(spark, cfg.paths.table(*bronze.PINGS))
    assert frame.filter("mmsi is null or ping_ts is null").count() == 0
    assert frame.filter("lat < -90 or lat > 90 or lon < -180 or lon > 180").count() == 0
    assert frame.filter("sog is not null and sog > 102").count() == 0
    assert frame.filter("heading is not null and heading >= 360").count() == 0
    assert frame.filter("transceiver_class not in ('A', 'B')").count() == 0


def test_quarantine_keeps_the_reason_and_the_raw_values(spark, cfg):
    prepare(spark, cfg)
    bronze.ingest_day(spark, cfg, cfg.window.dates()[0])
    frame = delta.read(spark, cfg.paths.table(*bronze.QUARANTINE))
    reasons = {row["reject_reason"] for row in frame.select("reject_reason").distinct().collect()}
    assert reasons
    assert reasons.issubset({"bad_mmsi", "bad_timestamp", "bad_latitude", "bad_longitude",
                             "bad_transceiver_class"})
    assert frame.filter("raw_base_datetime is null and raw_mmsi is null").count() == 0


def test_reingesting_a_day_is_idempotent(spark, cfg):
    prepare(spark, cfg)
    day = cfg.window.dates()[0]
    first = bronze.ingest_day(spark, cfg, day)
    rows = delta.row_count(cfg.paths.table(*bronze.PINGS))
    second = bronze.ingest_day(spark, cfg, day)
    assert first["rows_valid"] == second["rows_valid"]
    assert delta.row_count(cfg.paths.table(*bronze.PINGS)) == rows


def test_duplicate_pings_are_dropped(spark, cfg):
    prepare(spark, cfg)
    day = cfg.window.dates()[0]
    outcome = bronze.ingest_day(spark, cfg, day)
    assert outcome["rows_duplicate"] > 0
    frame = delta.read(spark, cfg.paths.table(*bronze.PINGS))
    assert frame.count() == frame.dropDuplicates(["mmsi", "ping_ts"]).count()


def test_audit_row_is_written_per_day(spark, cfg):
    prepare(spark, cfg)
    day = cfg.window.dates()[0]
    bronze.ingest_day(spark, cfg, day)
    audit = delta.read(spark, cfg.paths.table(*bronze.AUDIT))
    assert audit.count() == 1
    row = audit.collect()[0]
    assert str(row["file_day"]) == day
    assert row["rows_read"] >= row["rows_valid"]
