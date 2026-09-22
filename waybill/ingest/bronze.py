from __future__ import annotations

import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from waybill import delta
from waybill.generate.ais import AIS_COLUMNS

RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in AIS_COLUMNS])

PINGS = ("bronze", "ais_pings")
QUARANTINE = ("bronze", "ais_quarantine")
AUDIT = ("bronze", "load_audit")

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def source_directory(cfg, day):
    return str(Path(cfg.paths.raw) / "ais" / day)


def read_raw(spark, cfg, day):
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(source_directory(cfg, day))
        .withColumn("source_file", F.expr("input_file_name()"))
    )


def typed(df, day_expr):
    return df.selectExpr(
        "try_cast(MMSI as long) as mmsi",
        "try_to_timestamp(BaseDateTime, \"" + TIMESTAMP_FORMAT + "\") as ping_ts",
        "try_cast(LAT as double) as lat",
        "try_cast(LON as double) as lon",
        "case when try_cast(SOG as double) >= 102.3 then null else try_cast(SOG as double) end as sog",
        "case when try_cast(COG as double) >= 360 then null else try_cast(COG as double) end as cog",
        "case when try_cast(Heading as int) >= 360 then null else try_cast(Heading as int) end as heading",
        "nullif(trim(VesselName), '') as vessel_name",
        "nullif(trim(IMO), '') as imo",
        "nullif(trim(CallSign), '') as call_sign",
        "try_cast(VesselType as int) as vessel_type",
        "try_cast(Status as int) as status",
        "try_cast(Length as double) as length_m",
        "try_cast(Width as double) as width_m",
        "try_cast(Draft as double) as draft_m",
        "try_cast(Cargo as int) as cargo",
        "nullif(trim(TransceiverClass), '') as transceiver_class",
        "MMSI as raw_mmsi",
        "BaseDateTime as raw_base_datetime",
        "LAT as raw_lat",
        "LON as raw_lon",
        "source_file",
        "to_date(" + day_expr + ") as file_day",
    )


def flagged(df):
    return df.withColumn(
        "reject_reason",
        F.expr(
            "case"
            " when mmsi is null or mmsi < 100000000 or mmsi > 999999999 then 'bad_mmsi'"
            " when ping_ts is null then 'bad_timestamp'"
            " when lat is null or lat < -90 or lat > 90 then 'bad_latitude'"
            " when lon is null or lon < -180 or lon > 180 then 'bad_longitude'"
            " when transceiver_class not in ('A', 'B') then 'bad_transceiver_class'"
            " end"
        ),
    )


def ingest_day(spark, cfg, day):
    started = time.time()
    raw = read_raw(spark, cfg, day)
    rows_read = raw.count()
    marked = flagged(typed(raw, "'" + day + "'"))
    accepted = (
        marked.filter("reject_reason is null")
        .drop("reject_reason", "raw_mmsi", "raw_base_datetime", "raw_lat", "raw_lon")
        .withColumn("ping_date", F.expr("cast(ping_ts as date)"))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["mmsi", "ping_ts"])
    )
    rejected = marked.filter("reject_reason is not null").selectExpr(
        "reject_reason",
        "raw_mmsi",
        "raw_base_datetime",
        "raw_lat",
        "raw_lon",
        "source_file",
        "file_day",
    ).withColumn("ingested_at", F.current_timestamp())
    pings_path = cfg.paths.table(*PINGS)
    quarantine_path = cfg.paths.table(*QUARANTINE)
    rows_valid = delta.replace(accepted, pings_path, ["file_day"], {"file_day": day}, "ais_pings")
    rows_rejected = delta.replace(
        rejected, quarantine_path, ["file_day"], {"file_day": day}, "ais_quarantine"
    )
    audit = spark.createDataFrame(
        [
            (
                day,
                int(rows_read),
                int(rows_valid),
                int(rows_rejected),
                int(rows_read - rows_valid - rows_rejected),
                round(time.time() - started, 2),
            )
        ],
        "file_day string, rows_read long, rows_valid long, rows_quarantined long,"
        " rows_duplicate long, seconds double",
    ).withColumn("file_day", F.expr("to_date(file_day)")).withColumn("ingested_at", F.current_timestamp())
    delta.replace(audit, cfg.paths.table(*AUDIT), ["file_day"], {"file_day": day}, "load_audit")
    return {
        "day": day,
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_quarantined": rows_rejected,
        "rows_duplicate": rows_read - rows_valid - rows_rejected,
        "seconds": round(time.time() - started, 2),
    }


def run(spark, cfg, days=None):
    return [ingest_day(spark, cfg, day) for day in (days or cfg.window.dates())]
