from __future__ import annotations

import struct
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from aisetl import delta
from aisetl.ingest import bronze, geoparquet

DAY = "2024-01-01"


def point(lon, lat, order="<"):
    flag = 1 if order == "<" else 0
    return struct.pack(order + "bIdd", flag, 1, lon, lat)


def test_wkb_points_decode_in_both_byte_orders():
    values = pa.array([point(-70.25298, 43.65322), point(-118.262, 33.729, ">"), None, b"\x01\x01"],
                      type=pa.binary())
    lon, lat = geoparquet.wkb_xy(values)
    assert lon.to_pylist()[:2] == [-70.25298, -118.262]
    assert lat.to_pylist()[:2] == [43.65322, 33.729]
    assert lon.to_pylist()[2:] == [None, None]
    assert lat.null_count == 2


def test_non_point_geometry_is_rejected():
    line = struct.pack("<bII", 1, 2, 0) + b"\x00" * 16
    lon, lat = geoparquet.wkb_xy(pa.array([line], type=pa.binary()))
    assert lon.to_pylist() == [None]
    assert lat.to_pylist() == [None]


def write_day(cfg, rows):
    directory = Path(cfg.paths.raw) / "ais" / DAY
    directory.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "mmsi": pa.array([row[0] for row in rows], type=pa.int32()),
            "base_date_time": pa.array([row[1] for row in rows], type=pa.timestamp("ms")),
            "sog": pa.array([row[2] for row in rows], type=pa.float32()),
            "cog": pa.array([90.0] * len(rows), type=pa.float32()),
            "heading": pa.array([511] * len(rows), type=pa.int32()),
            "vessel_name": pa.array(["TEST VESSEL"] * len(rows)),
            "imo": pa.array([""] * len(rows)),
            "call_sign": pa.array(["WTEST1"] * len(rows)),
            "vessel_type": pa.array([70] * len(rows), type=pa.int32()),
            "status": pa.array([0] * len(rows), type=pa.int32()),
            "length": pa.array([200.0] * len(rows), type=pa.float32()),
            "width": pa.array([30] * len(rows), type=pa.int32()),
            "draft": pa.array([9.5] * len(rows), type=pa.float32()),
            "cargo": pa.array([70] * len(rows), type=pa.int32()),
            "transceiver": pa.array(["A"] * len(rows)),
            "geometry": pa.array([row[3] for row in rows], type=pa.binary()),
        }
    )
    pq.write_table(table, directory / ("ais-" + DAY + ".parquet"))


def test_geoparquet_day_lands_in_bronze_with_decoded_positions(spark, cfg):
    rows = [
        (366100001, datetime(2024, 1, 1, 0, 0), 12.5, point(-118.262, 33.729)),
        (366100001, datetime(2024, 1, 1, 0, 1), 102.3, point(-118.263, 33.730)),
        (366100002, datetime(2024, 1, 1, 0, 2), 0.1, b"\x01"),
        (366100002, datetime(2024, 1, 1, 0, 2), 0.1, point(-74.05, 40.67)),
    ]
    write_day(cfg, rows)
    outcome = bronze.ingest_day(spark, cfg, DAY)
    assert outcome["rows_read"] == 4
    assert outcome["rows_valid"] == 3
    assert outcome["rows_quarantined"] == 1
    frame = delta.read(spark, cfg.paths.table(*bronze.PINGS)).orderBy("mmsi", "ping_ts").collect()
    assert [(row["lon"], row["lat"]) for row in frame] == [
        (-118.262, 33.729),
        (-118.263, 33.73),
        (-74.05, 40.67),
    ]
    assert frame[0]["sog"] == 12.5
    assert frame[1]["sog"] is None
    assert frame[0]["heading"] is None
    assert frame[0]["imo"] is None
    assert str(frame[0]["file_day"]) == DAY


def test_quarantine_keeps_the_bad_geometry_row(spark, cfg):
    rows = [
        (366100003, datetime(2024, 1, 1, 0, 0), 1.0, None),
        (366100004, datetime(2024, 1, 1, 0, 0), 1.0, point(-80.0, 25.0)),
    ]
    write_day(cfg, rows)
    bronze.ingest_day(spark, cfg, DAY)
    rejected = delta.read(spark, cfg.paths.table(*bronze.QUARANTINE)).collect()
    assert len(rejected) == 1
    assert rejected[0]["reject_reason"] == "bad_latitude"
    assert rejected[0]["raw_mmsi"] == "366100003"
