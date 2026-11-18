from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aisetl import session  # noqa: E402
from aisetl.config import Config  # noqa: E402

BASE = {
    "paths": {
        "root": "data",
        "raw": "data/raw",
        "lake": "data/lake",
        "checkpoints": "data/checkpoints",
        "reports": "data/reports",
        "tmp": "data/tmp",
    },
    "spark": {
        "master": "local[2]",
        "driver_memory": "1g",
        "shuffle_partitions": 4,
        "aqe": True,
        "broadcast_threshold_mb": 16,
        "max_partition_mb": 32,
        "log_level": "ERROR",
    },
    "window": {"start_date": "2024-01-01", "days": 2},
    "ais": {
        "source": "generated",
        "url_template": "https://example.org/{year}/AIS_{year}_{month}_{day}.zip",
        "keep_raw_days": 1,
        "vessels": 40,
        "ping_interval_seconds": 120,
        "class_b_share": 0.3,
        "gap_share": 0.03,
        "duplicate_rate": 0.01,
        "corrupt_rate": 0.01,
        "late_share": 0.02,
    },
    "cpg": {
        "plants": 3,
        "distribution_centers": 13,
        "customers": 12,
        "skus": 20,
        "orders_per_day": 40,
        "max_lines_per_order": 3,
        "import_share": 0.3,
        "containers_per_call": 20,
        "promised_transit_days": 5,
        "inland_transit_hours": 34.0,
    },
    "ports": {
        "radius_km": 14.0,
        "berth_speed_knots": 0.7,
        "min_berth_minutes": 45,
        "call_gap_hours": 4,
        "grid_degrees": 0.25,
    },
    "quality": {
        "max_null_rate": 0.02,
        "min_bronze_rows": 10,
        "max_quarantine_rate": 0.05,
        "fail_on_error": True,
    },
    "stream": {
        "files_per_trigger": 2,
        "watermark_minutes": 120,
        "trigger_seconds": 5,
        "dedupe_minutes": 30,
        "days": 1,
    },
    "tuning": {
        "enabled": True,
        "sample_days": 1,
        "salt_buckets": 4,
        "naive_shuffle_partitions": 8,
    },
}


def build_config(tmp_path, **overrides):
    raw = {section: dict(values) for section, values in BASE.items()}
    for section, values in overrides.items():
        raw.setdefault(section, {}).update(values)
    for key in raw["paths"]:
        raw["paths"][key] = str(tmp_path / raw["paths"][key])
    return Config.from_dict(raw)


@pytest.fixture(scope="session")
def spark():
    cfg = Config.from_dict(BASE)
    active = session.build(cfg, "aisetl-tests")
    yield active
    active.stop()


@pytest.fixture
def cfg(tmp_path):
    built = build_config(tmp_path)
    built.paths.ensure()
    return built
