from __future__ import annotations

import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, Schema
from deltalake.transaction import AddAction, RemoveAction, create_table_with_add_actions

LOG_DIR = "_delta_log"


def normalize(path):
    return str(Path(path).resolve()).replace("\\", "/")


def exists(path):
    return Path(path, LOG_DIR).exists()


def table(path, version=None):
    root = normalize(path)
    if not exists(root):
        raise FileNotFoundError(f"no delta table at {root}")
    return DeltaTable(root, version=version) if version is not None else DeltaTable(root)


def relative_files(path):
    root = normalize(path)
    if not exists(root):
        return set()
    prefix = root + "/"
    return {uri.replace("\\", "/").split(prefix, 1)[-1] for uri in DeltaTable(root).file_uris()}


def data_files(root):
    base = Path(root)
    return [item for item in sorted(base.rglob("*.parquet")) if LOG_DIR not in item.parts]


def clean_orphans(path):
    root = normalize(path)
    if not exists(root):
        return 0
    known = relative_files(root)
    removed = 0
    for item in data_files(root):
        if str(item.relative_to(root)).replace("\\", "/") not in known:
            item.unlink()
            removed += 1
    for marker in Path(root).rglob("_SUCCESS"):
        marker.unlink()
    return removed


def _actions(root, previous):
    actions = []
    schema = None
    for item in data_files(root):
        relative = str(item.relative_to(root)).replace("\\", "/")
        if relative in previous:
            continue
        metadata = pq.read_metadata(item)
        if schema is None:
            schema = pq.read_schema(item)
        values = {}
        for segment in relative.split("/")[:-1]:
            if "=" in segment:
                key, value = segment.split("=", 1)
                values[key] = value
        actions.append(
            AddAction(
                relative,
                item.stat().st_size,
                values,
                int(item.stat().st_mtime * 1000),
                True,
                json.dumps({"numRecords": metadata.num_rows}),
            )
        )
    return actions, schema


def write(df, path, partition_by=None, mode="append", name=None):
    root = normalize(path)
    Path(root).mkdir(parents=True, exist_ok=True)
    clean_orphans(root)
    previous = relative_files(root)
    columns = list(partition_by or [])
    writer = df.write.mode("append")
    if columns:
        writer = writer.partitionBy(*columns)
    writer.parquet(root)
    actions, arrow_schema = _actions(root, previous)
    if not actions:
        if not exists(root):
            create_empty(root, df.schema, columns, name)
        return 0
    arrow_schema = with_partition_fields(arrow_schema, df.schema, columns)
    schema = Schema.from_arrow(arrow_schema)
    if exists(root):
        DeltaTable(root).create_write_transaction(actions, mode=mode, schema=schema, partition_by=columns)
    else:
        create_table_with_add_actions(root, schema, actions, mode="error", partition_by=columns, name=name)
    if mode == "overwrite":
        clean_orphans(root)
    return sum(json.loads(action.stats)["numRecords"] for action in actions)


def create_empty(path, spark_schema, partition_by=None, name=None):
    root = normalize(path)
    arrow_schema = to_arrow(spark_schema)
    DeltaTable.create(root, Schema.from_arrow(arrow_schema), partition_by=list(partition_by or []), name=name)


def partition_values(relative_path):
    values = {}
    for segment in relative_path.split("/")[:-1]:
        if "=" in segment:
            key, value = segment.split("=", 1)
            values[key] = value
    return values


def replace(df, path, partition_by, values, name=None):
    root = normalize(path)
    Path(root).mkdir(parents=True, exist_ok=True)
    clean_orphans(root)
    previous = relative_files(root)
    removals = []
    if exists(root):
        stamp = int(time.time() * 1000)
        for relative in sorted(previous):
            current = partition_values(relative)
            if all(current.get(key) == value for key, value in values.items()):
                target = Path(root, relative)
                removals.append(
                    RemoveAction(
                        path=relative,
                        data_change=True,
                        deletion_timestamp=stamp,
                        size=target.stat().st_size if target.exists() else None,
                        partition_values=current,
                    )
                )
    writer = df.write.mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(root)
    additions, arrow_schema = _actions(root, previous)
    if not additions and not removals:
        if not exists(root):
            create_empty(root, df.schema, partition_by, name)
        return 0
    if arrow_schema is None:
        arrow_schema = to_arrow(df.schema)
    arrow_schema = with_partition_fields(arrow_schema, df.schema, partition_by or [])
    schema = Schema.from_arrow(arrow_schema)
    if exists(root):
        DeltaTable(root).create_write_transaction(
            additions + removals, mode="append", schema=schema, partition_by=list(partition_by or [])
        )
    else:
        create_table_with_add_actions(
            root, schema, additions, mode="error", partition_by=list(partition_by or []), name=name
        )
    clean_orphans(root)
    return sum(json.loads(action.stats)["numRecords"] for action in additions)


def with_partition_fields(arrow_schema, spark_schema, columns):
    if not columns:
        return arrow_schema
    source = to_arrow(spark_schema)
    for column in columns:
        if column in arrow_schema.names:
            continue
        field = source.field(column) if column in source.names else pa.field(column, pa.string())
        arrow_schema = arrow_schema.append(field)
    return arrow_schema


def to_arrow(spark_schema):
    from pyspark.sql.pandas.types import to_arrow_schema

    return to_arrow_schema(spark_schema)


def ddl(path, version=None):
    fields = table(path, version).schema().fields
    return ", ".join(f"`{field.name}` {_spark_type(field.type)}" for field in fields)


def _spark_type(delta_type):
    name = getattr(delta_type, "type", None)
    if name is None:
        return "string"
    if name == "decimal":
        return f"decimal({delta_type.precision},{delta_type.scale})"
    if name == "array":
        return f"array<{_spark_type(delta_type.element_type)}>"
    if name == "struct":
        inner = ", ".join(f"`{f.name}`: {_spark_type(f.type)}" for f in delta_type.fields)
        return f"struct<{inner}>"
    if name == "map":
        return f"map<{_spark_type(delta_type.key_type)}, {_spark_type(delta_type.value_type)}>"
    return name


def read(spark, path, version=None, columns=None):
    root = normalize(path)
    handle = table(root, version)
    uris = handle.file_uris()
    if not uris:
        df = spark.createDataFrame([], ddl(root, version))
    else:
        df = spark.read.option("basePath", root).parquet(*uris)
    return df.select(*columns) if columns else df


def row_count(path, version=None):
    return table(path, version).to_pyarrow_dataset().count_rows()


def version(path):
    return table(path).version()


def history(path, limit=20):
    return table(path).history(limit)


def maintenance(path, retention_hours=0):
    handle = table(path)
    before = len(handle.file_uris())
    handle.optimize.compact()
    handle.create_checkpoint()
    removed = handle.vacuum(retention_hours=retention_hours, dry_run=False, enforce_retention_duration=False)
    after = len(DeltaTable(normalize(path)).file_uris())
    return {"files_before": before, "files_after": after, "files_removed": len(removed)}
