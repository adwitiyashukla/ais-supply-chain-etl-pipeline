from __future__ import annotations

from pyspark.sql import functions as F

from aisetl import delta

HEALTH = ("gold", "inventory_health")


def inventory_health(position_df, sku_df):
    joined = position_df.join(F.broadcast(sku_df.select("sku_id", "category")), "sku_id")
    grouped = joined.groupBy("dc_id", "category").agg(
        F.expr("count(*) as positions"),
        F.expr("sum(cases_received) as cases_received"),
        F.expr("sum(cases_shipped) as cases_shipped"),
        F.expr("round(avg(closing_cases), 1) as avg_closing_cases"),
        F.expr("round(percentile_approx(days_of_supply, 0.5), 2) as median_days_of_supply"),
        F.expr("sum(case when closing_cases <= 0 then 1 else 0 end) as stockout_positions"),
        F.expr("count(distinct sku_id) as skus"),
    )
    return grouped.selectExpr(
        "*",
        "round(stockout_positions / greatest(positions, 1), 4) as stockout_rate",
        "round(cases_received - cases_shipped, 1) as net_case_change",
    )


def run(cfg, position_df, sku_df):
    return {
        "inventory_health": delta.write(
            inventory_health(position_df, sku_df), cfg.paths.table(*HEALTH), None,
            "overwrite", "inventory_health",
        )
    }
