from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from waybill import delta, reference

VESSELS = ("silver", "dim_vessel")

ATTRIBUTES = [
    "vessel_name",
    "imo",
    "call_sign",
    "vessel_type",
    "length_m",
    "width_m",
    "transceiver_class",
]


def daily_attributes(bronze):
    struct_expr = "max_by(struct(" + ", ".join(ATTRIBUTES) + "), ping_ts) as attributes"
    grouped = bronze.groupBy("mmsi", "ping_date").agg(F.expr(struct_expr), F.expr("count(*) as pings"))
    return grouped.select(
        "mmsi", "ping_date", "pings", *[F.col("attributes." + name).alias(name) for name in ATTRIBUTES]
    )


def vessel_scd2(bronze):
    daily = daily_attributes(bronze)
    fingerprint = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(name).cast("string"), F.lit("")) for name in ATTRIBUTES]), 256)
    ordering = Window.partitionBy("mmsi").orderBy("ping_date")
    running = ordering.rowsBetween(Window.unboundedPreceding, 0)
    marked = (
        daily.withColumn("fingerprint", fingerprint)
        .withColumn("previous", F.lag("fingerprint").over(ordering))
        .withColumn(
            "changed", F.expr("case when previous is null or previous != fingerprint then 1 else 0 end")
        )
        .withColumn("version", F.sum("changed").over(running))
    )
    grouped = marked.groupBy("mmsi", "version").agg(
        F.expr("min(ping_date) as valid_from"),
        F.expr("max(ping_date) as last_seen"),
        F.expr("sum(pings) as pings"),
        *[F.expr("min(" + name + ") as " + name) for name in ATTRIBUTES],
    )
    versions = Window.partitionBy("mmsi").orderBy("version")
    closed = grouped.withColumn("next_from", F.lead("valid_from").over(versions))
    enriched = closed.withColumn("category", reference.category_expr("vessel_type"))
    return enriched.selectExpr(
        "concat_ws('-', cast(mmsi as string), cast(version as string)) as vessel_key",
        "mmsi",
        "version",
        "vessel_name",
        "imo",
        "call_sign",
        "vessel_type",
        "category",
        "round(length_m, 1) as length_m",
        "round(width_m, 1) as width_m",
        "transceiver_class",
        "valid_from",
        "case when next_from is null then date '9999-12-31' else date_sub(next_from, 1) end as valid_to",
        "next_from is null as is_current",
        "last_seen",
        "pings",
    )


def run(spark, cfg, bronze):
    dim = vessel_scd2(bronze)
    rows = delta.write(dim, cfg.paths.table(*VESSELS), None, "overwrite", "dim_vessel")
    return {"dim_vessel": rows}
