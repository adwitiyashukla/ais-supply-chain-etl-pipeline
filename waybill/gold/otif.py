from __future__ import annotations

from pyspark.sql import functions as F

from waybill import delta

DAILY = ("gold", "otif_daily")
SERVICE = ("gold", "customer_service")
IMPACT = ("gold", "import_impact")


def otif_daily(line_df):
    grouped = line_df.groupBy("delivered_date", "customer_region", "channel", "dc_id").agg(
        F.expr("count(*) as lines"),
        F.expr("count(distinct order_id) as orders"),
        F.expr("sum(ordered_cases) as ordered_cases"),
        F.expr("sum(shipped_cases) as shipped_cases"),
        F.expr("sum(case when otif then 1 else 0 end) as otif_lines"),
        F.expr("sum(case when on_time then 1 else 0 end) as on_time_lines"),
        F.expr("sum(case when in_full then 1 else 0 end) as in_full_lines"),
        F.expr("sum(case when import_linked then 1 else 0 end) as import_lines"),
        F.expr("round(sum(shipped_value_usd), 2) as shipped_value_usd"),
        F.expr("round(avg(delivery_variance_days), 3) as avg_variance_days"),
    )
    return grouped.selectExpr(
        "*",
        "round(otif_lines / lines, 4) as otif_rate",
        "round(on_time_lines / lines, 4) as on_time_rate",
        "round(in_full_lines / lines, 4) as in_full_rate",
        "round(shipped_cases / ordered_cases, 4) as case_fill_rate",
    )


def customer_service(line_df):
    grouped = line_df.groupBy("account_tier", "channel", "customer_region").agg(
        F.expr("count(distinct customer_id) as customers"),
        F.expr("count(*) as lines"),
        F.expr("sum(case when otif then 1 else 0 end) as otif_lines"),
        F.expr("sum(ordered_cases - shipped_cases) as cases_short"),
        F.expr("round(sum(shipped_value_usd), 2) as shipped_value_usd"),
        F.expr("round(avg(delivery_variance_days), 3) as avg_variance_days"),
        F.expr("round(percentile_approx(delivery_variance_days, 0.9), 2) as p90_variance_days"),
    )
    return grouped.selectExpr("*", "round(otif_lines / lines, 4) as otif_rate")


def import_impact(line_df):
    bucketed = line_df.withColumn(
        "delay_bucket",
        F.expr(
            "case when not import_linked then 'domestic'"
            " when slack_hours is null then 'no_container'"
            " when slack_hours < -24 then 'container_late_24h_plus'"
            " when slack_hours < 0 then 'container_late_0_24h'"
            " when slack_hours < 48 then 'slack_0_48h'"
            " else 'slack_48h_plus' end"
        ),
    )
    grouped = bucketed.groupBy("delay_bucket").agg(
        F.expr("count(*) as lines"),
        F.expr("sum(case when otif then 1 else 0 end) as otif_lines"),
        F.expr("sum(case when on_time then 1 else 0 end) as on_time_lines"),
        F.expr("sum(ordered_cases - shipped_cases) as cases_short"),
        F.expr("round(avg(coalesce(ocean_delay_hours, 0)), 2) as avg_ocean_delay_hours"),
        F.expr("round(avg(coalesce(port_wait_hours, 0)), 2) as avg_port_wait_hours"),
        F.expr("round(avg(delivery_variance_days), 3) as avg_variance_days"),
        F.expr("sum(case when allocation_late then 1 else 0 end) as allocation_late_lines"),
        F.expr("sum(case when unserved then 1 else 0 end) as unserved_lines"),
        F.expr("round(avg(slack_hours), 2) as avg_slack_hours"),
    )
    return grouped.selectExpr(
        "*",
        "round(otif_lines / lines, 4) as otif_rate",
        "round(on_time_lines / lines, 4) as on_time_rate",
        "round(allocation_late_lines / lines, 4) as allocation_late_rate",
        "round(unserved_lines / lines, 4) as unserved_rate",
        "round(cases_short / greatest(lines, 1), 2) as cases_short_per_line",
    )


def run(cfg, line_df):
    return {
        "otif_daily": delta.write(
            otif_daily(line_df), cfg.paths.table(*DAILY), ["delivered_date"], "overwrite", "otif_daily"
        ),
        "customer_service": delta.write(
            customer_service(line_df), cfg.paths.table(*SERVICE), None, "overwrite", "customer_service"
        ),
        "import_impact": delta.write(
            import_impact(line_df), cfg.paths.table(*IMPACT), None, "overwrite", "import_impact"
        ),
    }
