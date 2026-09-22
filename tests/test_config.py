from __future__ import annotations

import pytest

from tests.conftest import BASE, build_config
from waybill.config import Config, ConfigError


def test_dates_cover_the_window(tmp_path):
    cfg = build_config(tmp_path, window={"start_date": "2024-03-01", "days": 3})
    assert cfg.window.dates() == ["2024-03-01", "2024-03-02", "2024-03-03"]


def test_missing_section_is_rejected():
    raw = {section: dict(values) for section, values in BASE.items()}
    del raw["ports"]
    with pytest.raises(ConfigError, match="ports"):
        Config.from_dict(raw)


def test_ratio_outside_range_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="gap_share"):
        build_config(tmp_path, ais={"gap_share": 1.4})


def test_zero_vessels_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="vessels"):
        build_config(tmp_path, ais={"vessels": 0})


def test_bad_start_date_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="start_date"):
        build_config(tmp_path, window={"start_date": "01-03-2024"})


def test_url_template_must_have_tokens(tmp_path):
    with pytest.raises(ConfigError, match="url_template"):
        build_config(tmp_path, ais={"url_template": "https://example.org/file.zip"})


def test_tuning_days_cannot_exceed_window(tmp_path):
    with pytest.raises(ConfigError, match="sample_days"):
        build_config(tmp_path, tuning={"sample_days": 9})


def test_dedupe_window_cannot_exceed_watermark(tmp_path):
    with pytest.raises(ConfigError, match="dedupe_minutes"):
        build_config(tmp_path, stream={"dedupe_minutes": 500})


def test_table_paths_sit_under_the_lake(tmp_path):
    cfg = build_config(tmp_path)
    assert cfg.paths.table("bronze", "ais_pings").endswith("lake/bronze/ais_pings")
