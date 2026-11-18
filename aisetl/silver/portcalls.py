from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from aisetl import delta
from aisetl.silver.pings import distance_expr

CALLS = ("silver", "port_calls")


def sessionize(cfg, area_pings):
    ordering = Window.partitionBy("mmsi").orderBy("ping_ts")
    running = ordering.rowsBetween(Window.unboundedPreceding, 0)
    berth_km = cfg.ports.radius_km * 0.25
    tagged = (
        area_pings.withColumn(
            "at_berth",
            F.expr(
                "sog is not null and sog <= " + str(cfg.ports.berth_speed_knots)
                + " and (status = 5 or (coalesce(status, -1) not in (1, 5) and distance_km <= "
                + str(berth_km) + "))"
            ),
        )
        .withColumn(
            "at_anchor",
            F.expr(
                "sog is not null and sog <= 1.0 and not at_berth and (status = 1 or distance_km > "
                + str(berth_km) + ")"
            ),
        )
    )
    chained = (
        tagged.withColumn("prev_ts", F.lag("ping_ts").over(ordering))
        .withColumn(
            "gap_hours", F.expr("(unix_timestamp(ping_ts) - unix_timestamp(prev_ts)) / 3600.0")
        )
        .withColumn("prev_port_lat", F.lag("port_lat").over(ordering))
        .withColumn("prev_port_lon", F.lag("port_lon").over(ordering))
    )
    moved = chained.withColumn(
        "port_gap_km",
        F.expr(distance_expr("port_lat", "port_lon", "prev_port_lat", "prev_port_lon")),
    )
    return moved.withColumn(
        "call_start",
        F.expr(
            "case when prev_ts is null or gap_hours > " + str(cfg.ports.call_gap_hours)
            + " or port_gap_km > " + str(cfg.ports.radius_km * 2) + " then 1 else 0 end"
        ),
    ).withColumn("call_seq", F.sum("call_start").over(running))


def port_calls(cfg, area_pings):
    sessions = sessionize(cfg, area_pings)
    grouped = sessions.groupBy("mmsi", "call_seq").agg(
        F.expr("min_by(port_id, case when at_berth then ping_ts end) as port_id"),
        F.expr("min_by(port_name, case when at_berth then ping_ts end) as port_name"),
        F.expr("min_by(coast, case when at_berth then ping_ts end) as coast"),
        F.expr("min_by(gateway, case when at_berth then ping_ts end) as gateway"),
        F.expr("min(ping_ts) as arrival_ts"),
        F.expr("max(ping_ts) as departure_ts"),
        F.expr("min(case when at_berth then ping_ts end) as berth_start_ts"),
        F.expr("max(case when at_berth then ping_ts end) as berth_end_ts"),
        F.expr("count(*) as pings"),
        F.expr("sum(case when at_anchor then 1 else 0 end) as anchor_pings"),
        F.expr("sum(case when at_berth then 1 else 0 end) as berth_pings"),
        F.expr("min_by(draft_m, ping_ts) as draft_arrival_m"),
        F.expr("max_by(draft_m, ping_ts) as draft_departure_m"),
        F.expr("min(distance_km) as closest_km"),
    )
    derived = grouped.selectExpr(
        "*",
        "cast((unix_timestamp(departure_ts) - unix_timestamp(arrival_ts)) / 3600.0 as double)"
        " as dwell_hours",
        "cast((unix_timestamp(berth_end_ts) - unix_timestamp(berth_start_ts)) / 3600.0 as double)"
        " as berth_hours",
        "cast((unix_timestamp(berth_start_ts) - unix_timestamp(arrival_ts)) / 3600.0 as double)"
        " as anchor_wait_hours",
        "cast(arrival_ts as date) as arrival_date",
    )
    filtered = derived.filter(
        "berth_start_ts is not null and berth_hours * 60 >= " + str(cfg.ports.min_berth_minutes)
    )
    return filtered.selectExpr(
        "concat_ws('-', cast(mmsi as string), port_id,"
        " cast(unix_timestamp(arrival_ts) as string)) as call_id",
        "mmsi",
        "port_id",
        "port_name",
        "coast",
        "gateway",
        "arrival_ts",
        "berth_start_ts",
        "berth_end_ts",
        "departure_ts",
        "round(dwell_hours, 3) as dwell_hours",
        "round(berth_hours, 3) as berth_hours",
        "round(greatest(anchor_wait_hours, 0), 3) as anchor_wait_hours",
        "pings",
        "anchor_pings",
        "berth_pings",
        "round(draft_arrival_m, 2) as draft_arrival_m",
        "round(draft_departure_m, 2) as draft_departure_m",
        "round(draft_departure_m - draft_arrival_m, 2) as draft_change_m",
        "case when draft_departure_m - draft_arrival_m > 0.3 then 'loaded'"
        " when draft_departure_m - draft_arrival_m < -0.3 then 'discharged'"
        " else 'unchanged' end as cargo_move",
        "round(closest_km, 3) as closest_km",
        "arrival_date",
    )


def run(spark, cfg, area_pings):
    calls = port_calls(cfg, area_pings)
    rows = delta.write(calls, cfg.paths.table(*CALLS), ["arrival_date"], "overwrite", "port_calls")
    return {"port_calls": rows}
