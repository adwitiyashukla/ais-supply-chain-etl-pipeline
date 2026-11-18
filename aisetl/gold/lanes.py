from __future__ import annotations

from pyspark.sql import functions as F

from aisetl import delta

LANES = ("gold", "lane_reliability")


def lane_reliability(voyage_df):
    grouped = voyage_df.groupBy("lane", "from_port", "to_port", "from_port_name", "to_port_name").agg(
        F.expr("count(*) as voyages"),
        F.expr("count(distinct mmsi) as vessels"),
        F.expr("round(avg(sea_hours), 3) as avg_sea_hours"),
        F.expr("round(percentile_approx(sea_hours, 0.5), 3) as median_sea_hours"),
        F.expr("round(percentile_approx(sea_hours, 0.9), 3) as p90_sea_hours"),
        F.expr("round(stddev_samp(sea_hours), 3) as stddev_sea_hours"),
        F.expr("round(avg(distance_km), 2) as distance_km"),
        F.expr("round(avg(avg_speed_knots), 2) as avg_speed_knots"),
        F.expr("round(avg(anchor_wait_hours), 3) as avg_anchor_wait_hours"),
        F.expr("sum(case when leg_state = 'laden' then 1 else 0 end) as laden_voyages"),
    )
    scored = grouped.selectExpr(
        "*",
        "round(coalesce(stddev_sea_hours, 0) / greatest(avg_sea_hours, 0.01), 4) as transit_variability",
        "round(p90_sea_hours - median_sea_hours, 3) as tail_hours",
        "round(laden_voyages / voyages, 4) as laden_share",
    )
    return scored.filter("voyages >= 3")


def run(cfg, voyage_df):
    return {
        "lane_reliability": delta.write(
            lane_reliability(voyage_df), cfg.paths.table(*LANES), None,
            "overwrite", "lane_reliability",
        )
    }
