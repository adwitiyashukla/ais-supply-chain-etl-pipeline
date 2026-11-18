from __future__ import annotations

from datetime import date

from aisetl.gold import inventory, lanes, otif, ports

LINE_SCHEMA = (
    "line_id string, order_id string, customer_id string, channel string, account_tier string,"
    " customer_region string, dc_id string, sku_id string, category string, delivered_date date,"
    " ordered_cases int, shipped_cases int, shipped_value_usd double, delivery_variance_days int,"
    " on_time boolean, in_full boolean, otif boolean, import_linked boolean,"
    " port_wait_hours double, allocation_late boolean,"
    " unserved boolean, slack_hours double"
)


def make_line(identifier, on_time, in_full, imported, delay):
    return (
        identifier,
        "SO" + identifier,
        "CU1",
        "mass",
        "key",
        "midwest",
        "DC001",
        "SKU1",
        "fabric_care",
        date(2024, 1, 5),
        100,
        100 if in_full else 60,
        2000.0,
        0 if on_time else 2,
        on_time,
        in_full,
        on_time and in_full,
        imported,
        3.0,
        bool(delay is not None and delay > 12),
        False,
        None if delay is None else (56.0 if delay < 12 else -20.0),
    )


def lines(spark):
    return spark.createDataFrame(
        [
            make_line("1", True, True, False, None),
            make_line("2", True, True, True, 4.0),
            make_line("3", False, True, True, 40.0),
            make_line("4", True, False, False, None),
        ],
        LINE_SCHEMA,
    )


def test_otif_rate_matches_the_lines(spark, cfg):
    row = otif.otif_daily(lines(spark)).collect()[0]
    assert row["lines"] == 4
    assert row["otif_lines"] == 2
    assert float(row["otif_rate"]) == 0.5
    assert float(row["on_time_rate"]) == 0.75
    assert float(row["case_fill_rate"]) == round(360 / 400, 4)


def test_import_impact_splits_by_container_slack(spark, cfg):
    rows = {row["delay_bucket"]: row for row in otif.import_impact(lines(spark)).collect()}
    assert set(rows) == {"domestic", "slack_48h_plus", "container_late_0_24h"}
    assert float(rows["container_late_0_24h"]["otif_rate"]) == 0.0
    assert float(rows["slack_48h_plus"]["otif_rate"]) == 1.0


def test_customer_service_counts_short_cases(spark, cfg):
    row = otif.customer_service(lines(spark)).collect()[0]
    assert row["cases_short"] == 40
    assert row["customers"] == 1


def test_lane_reliability_needs_three_voyages(spark, cfg):
    voyages = spark.createDataFrame(
        [
            ("V" + str(index), 366100001 + index, "USLAX", "Los Angeles", "USOAK", "Oakland",
             "USLAX>USOAK", 40.0 + index, 2.0, 600.0, 14.0, "laden")
            for index in range(4)
        ],
        "voyage_id string, mmsi long, from_port string, from_port_name string, to_port string,"
        " to_port_name string, lane string, sea_hours double, anchor_wait_hours double,"
        " distance_km double, avg_speed_knots double, leg_state string",
    )
    row = lanes.lane_reliability(voyages).collect()[0]
    assert row["voyages"] == 4
    assert row["vessels"] == 4
    assert float(row["laden_share"]) == 1.0
    assert row["median_sea_hours"] is not None


def test_port_performance_summarises_calls(spark, cfg):
    calls = spark.createDataFrame(
        [
            ("C1", 366100001, "USLAX", "Los Angeles", "west", True, 30.0, 20.0, 6.0, "loaded",
             date(2024, 1, 2)),
            ("C2", 366100002, "USLAX", "Los Angeles", "west", True, 50.0, 24.0, 20.0, "discharged",
             date(2024, 1, 3)),
        ],
        "call_id string, mmsi long, port_id string, port_name string, coast string, gateway boolean,"
        " dwell_hours double, berth_hours double, anchor_wait_hours double, cargo_move string,"
        " arrival_date date",
    )
    row = ports.port_performance(calls).collect()[0]
    assert row["calls"] == 2
    assert float(row["avg_dwell_hours"]) == 40.0
    assert row["loaded_calls"] == 1
    daily = ports.port_congestion_daily(calls).collect()
    assert len(daily) == 2
    assert sum(item["calls_waiting_12h"] for item in daily) == 1


def test_inventory_health_flags_stockouts(spark, cfg):
    positions = spark.createDataFrame(
        [
            ("DC001", "SKU1", date(2024, 1, 2), 100, 40, 500, 560, 14.0),
            ("DC001", "SKU1", date(2024, 1, 3), 0, 600, 560, -40, 0.0),
        ],
        "dc_id string, sku_id string, position_date date, cases_received long, cases_shipped long,"
        " opening_cases int, closing_cases long, days_of_supply double",
    )
    skus = spark.createDataFrame([("SKU1", "fabric_care")], "sku_id string, category string")
    row = inventory.inventory_health(positions, skus).collect()[0]
    assert row["positions"] == 2
    assert row["stockout_positions"] == 1
    assert float(row["stockout_rate"]) == 0.5
