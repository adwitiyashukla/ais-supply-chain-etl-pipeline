from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from waybill import delta, reference
from waybill.silver.pings import distance_expr

VOYAGES = ("silver", "voyages")


def voyages(spark, cfg, calls):
    ordering = Window.partitionBy("mmsi").orderBy("arrival_ts")
    fleet = Window.partitionBy("mmsi")
    scaled = calls.withColumn(
        "vessel_max_draft_m", F.max("draft_departure_m").over(fleet)
    )
    chained = (
        scaled.withColumn("from_port", F.lag("port_id").over(ordering))
        .withColumn("from_port_name", F.lag("port_name").over(ordering))
        .withColumn("departed_ts", F.lag("departure_ts").over(ordering))
        .withColumn("from_call_id", F.lag("call_id").over(ordering))
        .withColumn("from_draft_m", F.lag("draft_departure_m").over(ordering))
        .withColumn("from_cargo_move", F.lag("cargo_move").over(ordering))
    )
    legs = chained.filter("from_port is not null and from_port != port_id and departed_ts < arrival_ts")
    ports = reference.ports(spark)
    origin = ports.selectExpr("port_id as from_port", "lat as from_lat", "lon as from_lon")
    destination = ports.selectExpr("port_id as to_port", "lat as to_lat", "lon as to_lon")
    joined = legs.withColumnRenamed("port_id", "to_port").join(F.broadcast(origin), "from_port").join(
        F.broadcast(destination), "to_port"
    )
    measured = joined.selectExpr(
        "*",
        distance_expr("from_lat", "from_lon", "to_lat", "to_lon") + " as distance_km",
        "cast((unix_timestamp(arrival_ts) - unix_timestamp(departed_ts)) / 3600.0 as double) as sea_hours",
    )
    adjusted = measured.withColumn(
        "sea_distance_km",
        F.expr("distance_km - " + str(cfg.ports.radius_km * 2)),
    )
    return adjusted.filter("sea_hours >= 0.5 and sea_hours <= 720 and sea_distance_km >= 1").selectExpr(
        "concat_ws('-', from_call_id, call_id) as voyage_id",
        "mmsi",
        "from_port",
        "from_port_name",
        "to_port",
        "port_name as to_port_name",
        "concat_ws('>', from_port, to_port) as lane",
        "departed_ts",
        "arrival_ts",
        "berth_start_ts",
        "round(sea_hours, 3) as sea_hours",
        "round(anchor_wait_hours, 3) as anchor_wait_hours",
        "round(sea_distance_km, 2) as distance_km",
        "round(distance_km, 2) as port_distance_km",
        "cast(round(sea_distance_km / 1.852 / sea_hours, 2) as double) as avg_speed_knots",
        "round(from_draft_m, 2) as departure_draft_m",
        "round(draft_arrival_m, 2) as arrival_draft_m",
        "round(vessel_max_draft_m, 2) as vessel_max_draft_m",
        "from_cargo_move as origin_cargo_move",
        "case when from_draft_m >= 0.8 * vessel_max_draft_m then 'laden' else 'ballast' end as leg_state",
        "gateway as to_gateway",
        "cast(arrival_ts as date) as arrival_date",
    )


def run(spark, cfg, calls):
    legs = voyages(spark, cfg, calls)
    rows = delta.write(legs, cfg.paths.table(*VOYAGES), ["arrival_date"], "overwrite", "voyages")
    return {"voyages": rows}
