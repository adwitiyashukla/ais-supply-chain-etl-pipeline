from __future__ import annotations

from pyspark.sql import functions as F

from waybill import delta

PERFORMANCE = ("gold", "port_performance")
CONGESTION = ("gold", "port_congestion_daily")


def port_performance(call_df):
    grouped = call_df.groupBy("port_id", "port_name", "coast", "gateway").agg(
        F.expr("count(*) as calls"),
        F.expr("count(distinct mmsi) as vessels"),
        F.expr("round(avg(dwell_hours), 3) as avg_dwell_hours"),
        F.expr("round(percentile_approx(dwell_hours, 0.5), 3) as median_dwell_hours"),
        F.expr("round(percentile_approx(dwell_hours, 0.9), 3) as p90_dwell_hours"),
        F.expr("round(avg(berth_hours), 3) as avg_berth_hours"),
        F.expr("round(avg(anchor_wait_hours), 3) as avg_anchor_wait_hours"),
        F.expr("round(percentile_approx(anchor_wait_hours, 0.9), 3) as p90_anchor_wait_hours"),
        F.expr("sum(case when cargo_move = 'loaded' then 1 else 0 end) as loaded_calls"),
        F.expr("sum(case when cargo_move = 'discharged' then 1 else 0 end) as discharged_calls"),
    )
    return grouped.selectExpr(
        "*",
        "round(avg_anchor_wait_hours / greatest(avg_berth_hours, 0.01), 4) as wait_to_berth_ratio",
        "round(loaded_calls / greatest(calls, 1), 4) as load_share",
    )


def port_congestion_daily(call_df):
    grouped = call_df.groupBy("arrival_date", "port_id", "port_name", "gateway").agg(
        F.expr("count(*) as calls"),
        F.expr("round(avg(anchor_wait_hours), 3) as avg_anchor_wait_hours"),
        F.expr("round(max(anchor_wait_hours), 3) as max_anchor_wait_hours"),
        F.expr("round(avg(berth_hours), 3) as avg_berth_hours"),
        F.expr("sum(case when anchor_wait_hours >= 12 then 1 else 0 end) as calls_waiting_12h"),
    )
    return grouped.selectExpr(
        "*", "round(calls_waiting_12h / greatest(calls, 1), 4) as share_waiting_12h"
    )


def run(cfg, call_df):
    return {
        "port_performance": delta.write(
            port_performance(call_df), cfg.paths.table(*PERFORMANCE), None, "overwrite", "port_performance"
        ),
        "port_congestion_daily": delta.write(
            port_congestion_daily(call_df), cfg.paths.table(*CONGESTION), ["arrival_date"],
            "overwrite", "port_congestion_daily",
        ),
    }
