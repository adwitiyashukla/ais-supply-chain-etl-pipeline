from __future__ import annotations

import numpy as np
import pyarrow as pa
from pyspark.sql.types import DoubleType, StructField, StructType

POINT_BYTES = 21


def wkb_xy(array):
    count = len(array)
    if count == 0:
        empty = pa.array([], type=pa.float64())
        return empty, empty
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if pa.types.is_large_binary(array.type):
        array = array.cast(pa.binary())
    _, offset_buffer, data_buffer = array.buffers()
    offsets = np.frombuffer(offset_buffer, dtype=np.int32, count=count + 1, offset=array.offset * 4)
    data = np.frombuffer(data_buffer, dtype=np.uint8) if data_buffer is not None else np.zeros(0, np.uint8)
    starts = offsets[:-1].astype(np.int64)
    lengths = (offsets[1:] - offsets[:-1]).astype(np.int64)
    valid = lengths >= POINT_BYTES
    if array.null_count:
        valid &= np.asarray(array.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
    lon = np.full(count, np.nan)
    lat = np.full(count, np.nan)
    rows = np.nonzero(valid)[0]
    if rows.size:
        base = starts[rows]
        order = data[base]
        window = data[base[:, None] + np.arange(1, POINT_BYTES)]
        little = order == 1
        kind = np.where(
            little,
            window[:, 0:4].copy().view("<u4").ravel(),
            window[:, 0:4].copy().view(">u4").ravel(),
        )
        x_little = window[:, 4:12].copy().view("<f8").ravel()
        y_little = window[:, 12:20].copy().view("<f8").ravel()
        x_big = window[:, 4:12].copy().view(">f8").ravel()
        y_big = window[:, 12:20].copy().view(">f8").ravel()
        point = (kind == 1) & ((order == 0) | (order == 1))
        lon[rows] = np.where(point, np.where(little, x_little, x_big), np.nan)
        lat[rows] = np.where(point, np.where(little, y_little, y_big), np.nan)
    missing = np.isnan(lon) | np.isnan(lat)
    return pa.array(lon, mask=missing), pa.array(lat, mask=missing)


def _decode(batches):
    for batch in batches:
        names = [name for name in batch.schema.names if name != "geometry"]
        lon, lat = wkb_xy(batch.column("geometry"))
        columns = [batch.column(name) for name in names] + [lon, lat]
        yield pa.RecordBatch.from_arrays(columns, names=names + ["lon", "lat"])


def decode_points(frame):
    fields = [field for field in frame.schema.fields if field.name != "geometry"]
    schema = StructType(fields + [StructField("lon", DoubleType()), StructField("lat", DoubleType())])
    return frame.mapInArrow(_decode, schema)


def typed(frame, day_expr):
    return frame.selectExpr(
        "cast(mmsi as bigint) as mmsi",
        "cast(base_date_time as timestamp) as ping_ts",
        "round(lat, 5) as lat",
        "round(lon, 5) as lon",
        "case when sog >= 102.3 then null else round(cast(sog as double), 1) end as sog",
        "case when cog >= 360 then null else round(cast(cog as double), 1) end as cog",
        "case when heading >= 360 then null else heading end as heading",
        "nullif(trim(vessel_name), '') as vessel_name",
        "nullif(trim(imo), '') as imo",
        "nullif(trim(call_sign), '') as call_sign",
        "vessel_type",
        "status",
        "round(cast(length as double), 1) as length_m",
        "round(cast(width as double), 1) as width_m",
        "round(cast(draft as double), 1) as draft_m",
        "cargo",
        "nullif(trim(transceiver), '') as transceiver_class",
        "cast(mmsi as string) as raw_mmsi",
        "cast(base_date_time as string) as raw_base_datetime",
        "cast(lat as string) as raw_lat",
        "cast(lon as string) as raw_lon",
        "source_file",
        "to_date(" + day_expr + ") as file_day",
    )
