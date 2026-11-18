from __future__ import annotations

from datetime import datetime, timedelta, timezone

from aisetl.silver import dims, pings, portcalls

PORT_LAT = 33.7290
PORT_LON = -118.2620
START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def ping_rows(offsets):
    rows = []
    for entry in offsets:
        minutes, lat_delta, lon_delta, sog = entry[:4]
        status = entry[4] if len(entry) > 4 else 0
        moment = START + timedelta(minutes=minutes)
        rows.append(
            (
                366100001,
                moment.replace(tzinfo=None),
                PORT_LAT + lat_delta,
                PORT_LON + lon_delta,
                float(sog),
                90.0,
                0,
                "TEST VESSEL",
                "IMO9000001",
                "WTEST1",
                70,
                status,
                200.0,
                30.0,
                9.5,
                70,
                "A",
                "file",
                moment.date(),
            )
        )
    return rows


SCHEMA = (
    "mmsi long, ping_ts timestamp, lat double, lon double, sog double, cog double, heading int,"
    " vessel_name string, imo string, call_sign string, vessel_type int, status int,"
    " length_m double, width_m double, draft_m double, cargo int, transceiver_class string,"
    " source_file string, ping_date date"
)


def frame(spark, offsets):
    return spark.createDataFrame(ping_rows(offsets), SCHEMA)


def approach_and_berth(base_minutes):
    steps = []
    for index in range(4):
        steps.append((base_minutes + index * 5, 0.18, 0.0, 12.0, 0))
    for index in range(6):
        steps.append((base_minutes + 20 + index * 10, 0.09, 0.0, 0.3, 1))
    for index in range(12):
        steps.append((base_minutes + 80 + index * 10, 0.004, 0.0, 0.05, 5))
    return steps


def test_neighbouring_ports_do_not_split_one_call(spark, cfg):
    area = pings.port_area_pings(spark, cfg, frame(spark, approach_and_berth(0)))
    calls = portcalls.port_calls(cfg, area).collect()
    assert len(calls) == 1
    assert calls[0]["port_id"] == "USLAX"
    assert calls[0]["anchor_pings"] == 6


def test_pings_outside_the_radius_are_dropped(spark, cfg):
    area = pings.port_area_pings(spark, cfg, frame(spark, approach_and_berth(0)))
    assert area.count() == 18
    assert area.filter("distance_km > " + str(cfg.ports.radius_km)).count() == 0
    assert {row["port_id"] for row in area.select("port_id").distinct().collect()} == {"USLAX", "USLGB"}


def test_one_visit_becomes_one_port_call(spark, cfg):
    area = pings.port_area_pings(spark, cfg, frame(spark, approach_and_berth(0)))
    calls = portcalls.port_calls(cfg, area).collect()
    assert len(calls) == 1
    call = calls[0]
    assert call["port_id"] == "USLAX"
    assert round(call["berth_hours"], 2) == round(110 / 60, 2)
    assert call["anchor_wait_hours"] > 0.9
    assert call["berth_pings"] == 12
    assert call["anchor_pings"] == 6


def test_a_long_gap_starts_a_second_call(spark, cfg):
    offsets = approach_and_berth(0) + approach_and_berth(60 * 24)
    area = pings.port_area_pings(spark, cfg, frame(spark, offsets))
    calls = portcalls.port_calls(cfg, area)
    assert calls.count() == 2
    assert calls.select("call_id").distinct().count() == 2


def test_a_moored_ship_far_from_the_port_center_still_berths(spark, cfg):
    offsets = [(index * 10, 0.075, 0.0, 0.1, 5) for index in range(12)]
    area = pings.port_area_pings(spark, cfg, frame(spark, offsets))
    calls = portcalls.port_calls(cfg, area).collect()
    assert len(calls) == 1
    assert calls[0]["berth_pings"] == 12


def test_a_short_stop_is_not_a_port_call(spark, cfg):
    offsets = [(0, 0.05, 0.0, 0.2), (10, 0.004, 0.0, 0.1), (20, 0.004, 0.0, 0.1)]
    area = pings.port_area_pings(spark, cfg, frame(spark, offsets))
    assert portcalls.port_calls(cfg, area).count() == 0


def test_draft_change_marks_a_cargo_move(spark, cfg):
    rows = ping_rows(approach_and_berth(0))
    changed = []
    for position, row in enumerate(rows):
        values = list(row)
        values[14] = 9.5 if position < len(rows) - 4 else 12.0
        changed.append(tuple(values))
    area = pings.port_area_pings(spark, cfg, spark.createDataFrame(changed, SCHEMA))
    call = portcalls.port_calls(cfg, area).collect()[0]
    assert call["cargo_move"] == "loaded"
    assert call["draft_change_m"] > 0


def test_vessel_dimension_versions_a_rename(spark, cfg):
    rows = ping_rows([(0, 0.0, 0.0, 1.0), (60 * 24, 0.0, 0.0, 1.0), (60 * 48, 0.0, 0.0, 1.0)])
    renamed = []
    for position, row in enumerate(rows):
        values = list(row)
        if position >= 2:
            values[7] = "TEST VESSEL II"
        renamed.append(tuple(values))
    dim = dims.vessel_scd2(spark.createDataFrame(renamed, SCHEMA)).orderBy("version").collect()
    assert len(dim) == 2
    assert dim[0]["vessel_name"] == "TEST VESSEL"
    assert dim[0]["is_current"] is False
    assert dim[1]["vessel_name"] == "TEST VESSEL II"
    assert dim[1]["is_current"] is True
    assert str(dim[0]["valid_to"]) < str(dim[1]["valid_from"])


def test_vessel_dimension_keeps_one_version_when_nothing_changes(spark, cfg):
    rows = ping_rows([(0, 0.0, 0.0, 1.0), (60 * 24, 0.0, 0.0, 1.0)])
    dim = dims.vessel_scd2(spark.createDataFrame(rows, SCHEMA)).collect()
    assert len(dim) == 1
    assert dim[0]["category"] == "cargo"
    assert str(dim[0]["valid_to"]) == "9999-12-31"


CALL_SCHEMA = (
    "call_id string, mmsi long, port_id string, port_name string, coast string, gateway boolean,"
    " arrival_ts timestamp, berth_start_ts timestamp, berth_end_ts timestamp, departure_ts timestamp,"
    " dwell_hours double, berth_hours double, anchor_wait_hours double, pings long, anchor_pings long,"
    " berth_pings long, draft_arrival_m double, draft_departure_m double, draft_change_m double,"
    " cargo_move string, closest_km double, arrival_date date"
)


def call_row(identifier, port, arrival, departure, draft_in, draft_out, move):
    return (
        identifier,
        366100001,
        port,
        port,
        "west",
        True,
        arrival,
        arrival,
        departure,
        departure,
        6.0,
        5.0,
        1.0,
        100,
        10,
        80,
        draft_in,
        draft_out,
        round(draft_out - draft_in, 2),
        move,
        0.4,
        arrival.date(),
    )


def test_voyages_link_consecutive_calls(spark, cfg):
    from datetime import datetime

    from aisetl.silver import voyages

    calls = spark.createDataFrame(
        [
            call_row("C1", "USLAX", datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 12), 8.0, 12.0, "loaded"),
            call_row("C2", "USSEA", datetime(2024, 1, 4, 0), datetime(2024, 1, 4, 12), 12.0, 8.0,
                     "discharged"),
        ],
        CALL_SCHEMA,
    )
    legs = voyages.voyages(spark, cfg, calls).collect()
    assert len(legs) == 1
    leg = legs[0]
    assert leg["lane"] == "USLAX>USSEA"
    assert leg["sea_hours"] == 60.0
    assert leg["leg_state"] == "laden"
    assert leg["origin_cargo_move"] == "loaded"
    assert leg["avg_speed_knots"] > 5


def test_a_ballast_leg_is_marked_from_the_departure_draft(spark, cfg):
    from datetime import datetime

    from aisetl.silver import voyages

    calls = spark.createDataFrame(
        [
            call_row("C1", "USLAX", datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 12), 12.0, 7.0,
                     "discharged"),
            call_row("C2", "USSEA", datetime(2024, 1, 4, 0), datetime(2024, 1, 4, 12), 7.0, 12.0, "loaded"),
        ],
        CALL_SCHEMA,
    )
    leg = voyages.voyages(spark, cfg, calls).collect()[0]
    assert leg["leg_state"] == "ballast"
    assert leg["vessel_max_draft_m"] == 12.0
