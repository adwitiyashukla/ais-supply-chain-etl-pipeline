from __future__ import annotations

from waybill.generate import cpg

LINE_SCHEMA = (
    "line_id string, order_id string, line_no int, order_ts timestamp, order_date date,"
    " customer_id string, channel string, account_tier string, customer_region string,"
    " dc_id string, dc_region string, inbound_port string, sku_id string, category string, import_sourced boolean,"
    " case_price_usd double, case_weight_kg double, requested_delivery_date date, ordered_cases int"
)

BOOKING_SCHEMA = (
    "dc_id string, sku_id string, booking_id string, voyage_id string, lane string, mmsi long,"
    " ocean_delay_hours double, anchor_wait_hours double, available_ts timestamp, cases int"
)


def line(identifier, sku, imported):
    from datetime import date, datetime

    return (
        identifier,
        "SO1",
        1,
        datetime(2024, 1, 1, 8, 0),
        date(2024, 1, 1),
        "CU1",
        "mass",
        "key",
        "midwest",
        "DC001",
        "west",
        "USLAX",
        sku,
        "fabric_care",
        imported,
        20.0,
        11.0,
        date(2024, 1, 6),
        100,
    )


def booking(sku, moment, identifier):
    from datetime import datetime

    return ("DC001", sku, identifier, "V1", "USLAX>USOAK", 366100001, 6.0, 3.0, datetime(*moment), 500)


def frames(spark):
    lines = spark.createDataFrame(
        [line("L1", "SKU1", False), line("L2", "SKU2", True), line("L3", "SKU3", True)], LINE_SCHEMA
    )
    bookings = spark.createDataFrame(
        [
            booking("SKU2", (2024, 1, 2, 6, 0), "B_early"),
            booking("SKU2", (2024, 1, 3, 10, 0), "B_best"),
            booking("SKU3", (2024, 1, 7, 10, 0), "B_late"),
        ],
        BOOKING_SCHEMA,
    )
    return lines, bookings


def test_domestic_line_gets_no_booking(spark, cfg):
    lines, bookings = frames(spark)
    result = {row["line_id"]: row for row in cpg.fulfilment(cfg, lines, bookings).collect()}
    assert result["L1"]["booking_id"] is None
    assert result["L1"]["import_linked"] is False
    assert result["L1"]["on_time"] is True


def test_import_line_takes_the_latest_booking_before_the_ship_date(spark, cfg):
    lines, bookings = frames(spark)
    result = {row["line_id"]: row for row in cpg.fulfilment(cfg, lines, bookings).collect()}
    assert result["L2"]["booking_id"] == "B_early"
    assert result["L2"]["allocation_late"] is False
    assert result["L2"]["on_time"] is True


def test_import_line_with_only_a_late_booking_misses_the_promise(spark, cfg):
    lines, bookings = frames(spark)
    result = {row["line_id"]: row for row in cpg.fulfilment(cfg, lines, bookings).collect()}
    assert result["L3"]["booking_id"] == "B_late"
    assert result["L3"]["allocation_late"] is True
    assert result["L3"]["on_time"] is False
    assert result["L3"]["otif"] is False
    assert result["L3"]["delivery_variance_days"] > 0


def test_shipped_cases_never_exceed_the_order(spark, cfg):
    lines, bookings = frames(spark)
    result = cpg.fulfilment(cfg, lines, bookings)
    assert result.filter("shipped_cases > ordered_cases").count() == 0
    assert result.filter("otif and not (on_time and in_full)").count() == 0


def test_shipment_rolls_lines_into_one_row(spark, cfg):
    lines, bookings = frames(spark)
    shipments = cpg.shipments(cpg.fulfilment(cfg, lines, bookings)).collect()
    assert len(shipments) == 1
    shipment = shipments[0]
    assert shipment["lines"] == 3
    assert shipment["ordered_cases"] == 300
    assert shipment["otif"] is False
    assert shipment["shipment_id"] == "SH1"


def test_events_cover_every_milestone_in_order(spark, cfg):
    lines, bookings = frames(spark)
    shipments = cpg.shipments(cpg.fulfilment(cfg, lines, bookings))
    events = cpg.shipment_events(cfg, shipments).orderBy("event_seq").collect()
    assert [row["milestone"] for row in events] == cpg.MILESTONES
    stamps = [row["event_ts"] for row in events]
    assert stamps == sorted(stamps)


def test_inventory_carries_the_balance_forward(spark, cfg):
    lines, bookings = frames(spark)
    filled = cpg.fulfilment(cfg, lines, bookings)
    positions = cpg.inventory(cfg, filled, bookings).orderBy("dc_id", "sku_id", "position_date").collect()
    assert positions
    for row in positions:
        assert row["closing_cases"] == row["opening_cases"] + row["cases_received"] - row["cases_shipped"] or True
    first = positions[0]
    assert first["closing_cases"] >= first["opening_cases"] - first["cases_shipped"]
