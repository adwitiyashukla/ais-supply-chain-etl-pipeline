from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DIRECTORY = Path(__file__).resolve().parents[1] / "reference"

PORT_SCHEMA = StructType(
    [
        StructField("port_id", StringType(), False),
        StructField("port_name", StringType(), False),
        StructField("locode", StringType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("coast", StringType(), False),
        StructField("gateway", BooleanType(), False),
    ]
)

VESSEL_TYPE_SCHEMA = StructType(
    [
        StructField("min_code", IntegerType(), False),
        StructField("max_code", IntegerType(), False),
        StructField("category", StringType(), False),
    ]
)

NAV_STATUS_SCHEMA = StructType(
    [
        StructField("status_code", IntegerType(), False),
        StructField("status_name", StringType(), False),
    ]
)


def _read(spark, name, schema):
    path = DIRECTORY / name
    if not path.exists():
        raise FileNotFoundError(f"reference file missing: {path}")
    return spark.read.csv(str(path), header=True, schema=schema)


def ports(spark):
    return _read(spark, "ports.csv", PORT_SCHEMA)


def vessel_types(spark):
    return _read(spark, "vessel_types.csv", VESSEL_TYPE_SCHEMA)


def nav_status(spark):
    return _read(spark, "nav_status.csv", NAV_STATUS_SCHEMA)


def port_rows():
    import csv

    with (DIRECTORY / "ports.csv").open(newline="") as handle:
        return list(csv.DictReader(handle))


def ports_by_coast():
    grouped = {}
    for row in port_rows():
        grouped.setdefault(row["coast"], []).append(row["port_id"])
    return grouped


def category_expr(column):
    rows = []
    with (DIRECTORY / "vessel_types.csv").open() as handle:
        header = handle.readline()
        if not header:
            raise ValueError("vessel_types.csv is empty")
        for line in handle:
            low, high, category = line.strip().split(",")
            rows.append((int(low), int(high), category))
    expression = F.lit("other")
    for low, high, category in reversed(rows):
        expression = F.when(F.col(column).between(low, high), F.lit(category)).otherwise(expression)
    return F.when(F.col(column).isNull(), F.lit("unknown")).otherwise(expression)
