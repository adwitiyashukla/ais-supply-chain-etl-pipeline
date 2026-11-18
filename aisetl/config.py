from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import tomllib


class ConfigError(ValueError):
    pass


def _need(condition, message):
    if not condition:
        raise ConfigError(message)


def _section(raw, name):
    _need(name in raw, f"config is missing the [{name}] section")
    return raw[name]


def _get(section, name, kind, where):
    _need(name in section, f"[{where}] is missing {name}")
    value = section[name]
    if kind is float and isinstance(value, int):
        value = float(value)
    _need(isinstance(value, kind), f"[{where}].{name} must be {kind.__name__}, got {type(value).__name__}")
    return value


def _ratio(section, name, where):
    value = _get(section, name, float, where)
    _need(0.0 <= value <= 1.0, f"[{where}].{name} must be between 0 and 1, got {value}")
    return value


def _positive(section, name, where, kind=int):
    value = _get(section, name, kind, where)
    _need(value > 0, f"[{where}].{name} must be greater than 0, got {value}")
    return value


@dataclass(frozen=True)
class Paths:
    root: str
    raw: str
    lake: str
    checkpoints: str
    reports: str
    tmp: str

    def table(self, layer, name):
        return str(Path(self.lake) / layer / name)

    def ensure(self):
        for value in (self.root, self.raw, self.lake, self.checkpoints, self.reports, self.tmp):
            Path(value).mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class SparkSettings:
    master: str
    driver_memory: str
    shuffle_partitions: int
    aqe: bool
    broadcast_threshold_mb: int
    max_partition_mb: int
    log_level: str


@dataclass(frozen=True)
class Window:
    start_date: str
    days: int

    def dates(self):
        first = date.fromisoformat(self.start_date)
        return [(first + timedelta(days=offset)).isoformat() for offset in range(self.days)]


@dataclass(frozen=True)
class AisSettings:
    source: str
    url_template: str
    keep_raw_days: int
    vessels: int
    ping_interval_seconds: int
    class_b_share: float
    gap_share: float
    duplicate_rate: float
    corrupt_rate: float
    late_share: float


@dataclass(frozen=True)
class CpgSettings:
    plants: int
    distribution_centers: int
    customers: int
    skus: int
    orders_per_day: int
    max_lines_per_order: int
    import_share: float
    containers_per_call: int
    promised_transit_days: int
    inland_transit_hours: float


@dataclass(frozen=True)
class PortSettings:
    radius_km: float
    berth_speed_knots: float
    min_berth_minutes: int
    call_gap_hours: int
    grid_degrees: float


@dataclass(frozen=True)
class QualitySettings:
    max_null_rate: float
    min_bronze_rows: int
    max_quarantine_rate: float
    fail_on_error: bool


@dataclass(frozen=True)
class StreamSettings:
    files_per_trigger: int
    watermark_minutes: int
    trigger_seconds: int
    dedupe_minutes: int
    days: int


@dataclass(frozen=True)
class TuningSettings:
    enabled: bool
    sample_days: int
    salt_buckets: int
    naive_shuffle_partitions: int


@dataclass(frozen=True)
class Config:
    paths: Paths
    spark: SparkSettings
    window: Window
    ais: AisSettings
    cpg: CpgSettings
    ports: PortSettings
    quality: QualitySettings
    stream: StreamSettings
    tuning: TuningSettings

    @staticmethod
    def load(path="config.toml"):
        location = Path(path)
        _need(location.exists(), f"config file not found at {location}")
        with location.open("rb") as handle:
            raw = tomllib.load(handle)
        return Config.from_dict(raw)

    @staticmethod
    def from_dict(raw):
        paths_raw = _section(raw, "paths")
        paths = Paths(
            root=_get(paths_raw, "root", str, "paths"),
            raw=_get(paths_raw, "raw", str, "paths"),
            lake=_get(paths_raw, "lake", str, "paths"),
            checkpoints=_get(paths_raw, "checkpoints", str, "paths"),
            reports=_get(paths_raw, "reports", str, "paths"),
            tmp=_get(paths_raw, "tmp", str, "paths"),
        )

        spark_raw = _section(raw, "spark")
        log_level = _get(spark_raw, "log_level", str, "spark")
        _need(log_level in {"ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"},
              f"[spark].log_level is not a Spark level: {log_level}")
        spark = SparkSettings(
            master=_get(spark_raw, "master", str, "spark"),
            driver_memory=_get(spark_raw, "driver_memory", str, "spark"),
            shuffle_partitions=_positive(spark_raw, "shuffle_partitions", "spark"),
            aqe=_get(spark_raw, "aqe", bool, "spark"),
            broadcast_threshold_mb=_positive(spark_raw, "broadcast_threshold_mb", "spark"),
            max_partition_mb=_positive(spark_raw, "max_partition_mb", "spark"),
            log_level=log_level,
        )

        window_raw = _section(raw, "window")
        start_date = _get(window_raw, "start_date", str, "window")
        try:
            date.fromisoformat(start_date)
        except ValueError as error:
            raise ConfigError(f"[window].start_date must be YYYY-MM-DD, got {start_date}") from error
        window = Window(start_date=start_date, days=_positive(window_raw, "days", "window"))
        _need(window.days <= 366, f"[window].days must be 366 or fewer, got {window.days}")

        ais_raw = _section(raw, "ais")
        source = _get(ais_raw, "source", str, "ais")
        _need(source in {"generated", "files"}, f"[ais].source must be generated or files, got {source}")
        template = _get(ais_raw, "url_template", str, "ais")
        for token in ("{year}", "{month}", "{day}"):
            _need(token in template, f"[ais].url_template must contain {token}")
        ais = AisSettings(
            source=source,
            url_template=template,
            keep_raw_days=_get(ais_raw, "keep_raw_days", int, "ais"),
            vessels=_positive(ais_raw, "vessels", "ais"),
            ping_interval_seconds=_positive(ais_raw, "ping_interval_seconds", "ais"),
            class_b_share=_ratio(ais_raw, "class_b_share", "ais"),
            gap_share=_ratio(ais_raw, "gap_share", "ais"),
            duplicate_rate=_ratio(ais_raw, "duplicate_rate", "ais"),
            corrupt_rate=_ratio(ais_raw, "corrupt_rate", "ais"),
            late_share=_ratio(ais_raw, "late_share", "ais"),
        )
        _need(ais.ping_interval_seconds <= 3600, "[ais].ping_interval_seconds must be 3600 or fewer")
        _need(0 <= ais.keep_raw_days <= window.days,
              "[ais].keep_raw_days must be between 0 and [window].days (" + str(window.days) + ")")

        cpg_raw = _section(raw, "cpg")
        cpg = CpgSettings(
            plants=_positive(cpg_raw, "plants", "cpg"),
            distribution_centers=_positive(cpg_raw, "distribution_centers", "cpg"),
            customers=_positive(cpg_raw, "customers", "cpg"),
            skus=_positive(cpg_raw, "skus", "cpg"),
            orders_per_day=_positive(cpg_raw, "orders_per_day", "cpg"),
            max_lines_per_order=_positive(cpg_raw, "max_lines_per_order", "cpg"),
            import_share=_ratio(cpg_raw, "import_share", "cpg"),
            containers_per_call=_positive(cpg_raw, "containers_per_call", "cpg"),
            promised_transit_days=_positive(cpg_raw, "promised_transit_days", "cpg"),
            inland_transit_hours=_positive(cpg_raw, "inland_transit_hours", "cpg", float),
        )
        _need(cpg.max_lines_per_order <= 20, "[cpg].max_lines_per_order must be 20 or fewer")
        _need(cpg.containers_per_call <= 1000, "[cpg].containers_per_call must be 1000 or fewer")

        ports_raw = _section(raw, "ports")
        ports = PortSettings(
            radius_km=_positive(ports_raw, "radius_km", "ports", float),
            berth_speed_knots=_positive(ports_raw, "berth_speed_knots", "ports", float),
            min_berth_minutes=_positive(ports_raw, "min_berth_minutes", "ports"),
            call_gap_hours=_positive(ports_raw, "call_gap_hours", "ports"),
            grid_degrees=_positive(ports_raw, "grid_degrees", "ports", float),
        )
        _need(ports.radius_km <= 100, "[ports].radius_km must be 100 or fewer")
        _need(ports.grid_degrees >= 0.05, "[ports].grid_degrees must be 0.05 or more")

        quality_raw = _section(raw, "quality")
        quality = QualitySettings(
            max_null_rate=_ratio(quality_raw, "max_null_rate", "quality"),
            min_bronze_rows=_positive(quality_raw, "min_bronze_rows", "quality"),
            max_quarantine_rate=_ratio(quality_raw, "max_quarantine_rate", "quality"),
            fail_on_error=_get(quality_raw, "fail_on_error", bool, "quality"),
        )

        stream_raw = _section(raw, "stream")
        stream = StreamSettings(
            files_per_trigger=_positive(stream_raw, "files_per_trigger", "stream"),
            watermark_minutes=_positive(stream_raw, "watermark_minutes", "stream"),
            trigger_seconds=_positive(stream_raw, "trigger_seconds", "stream"),
            dedupe_minutes=_positive(stream_raw, "dedupe_minutes", "stream"),
            days=_positive(stream_raw, "days", "stream"),
        )
        _need(stream.dedupe_minutes <= stream.watermark_minutes,
              "[stream].dedupe_minutes must not exceed [stream].watermark_minutes")
        _need(stream.days <= window.days,
              "[stream].days must not exceed [window].days (" + str(window.days) + ")")
        _need(ais.source != "files" or stream.days <= ais.keep_raw_days,
              "[stream].days must not exceed [ais].keep_raw_days when [ais].source is files")

        tuning_raw = _section(raw, "tuning")
        tuning = TuningSettings(
            enabled=_get(tuning_raw, "enabled", bool, "tuning"),
            sample_days=_positive(tuning_raw, "sample_days", "tuning"),
            salt_buckets=_positive(tuning_raw, "salt_buckets", "tuning"),
            naive_shuffle_partitions=_positive(tuning_raw, "naive_shuffle_partitions", "tuning"),
        )
        _need(tuning.sample_days <= window.days,
              f"[tuning].sample_days must not exceed [window].days ({window.days})")

        return Config(paths, spark, window, ais, cpg, ports, quality, stream, tuning)
