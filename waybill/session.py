from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def build(cfg, app_name, overrides=None):
    Path(cfg.paths.tmp).mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master(cfg.spark.master)
        .appName(app_name)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", cfg.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", str(cfg.spark.aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(cfg.spark.aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(cfg.spark.aqe).lower())
        .config("spark.sql.autoBroadcastJoinThreshold", cfg.spark.broadcast_threshold_mb * 1024 * 1024)
        .config("spark.sql.files.maxPartitionBytes", cfg.spark.max_partition_mb * 1024 * 1024)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.local.dir", str(Path(cfg.paths.tmp).resolve()))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    )
    for key, value in (overrides or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.spark.log_level)
    return spark
