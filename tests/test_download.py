from __future__ import annotations

from pathlib import Path

from aisetl.ingest import download


def test_url_is_built_from_the_template(cfg):
    assert download.file_url(cfg, "2024-03-09").endswith("2024/AIS_2024_03_09.zip")


def test_target_directory_is_named_after_the_day(cfg):
    assert download.target_directory(cfg, "2024-03-09").name == "2024-03-09"


def test_present_days_are_not_downloaded_again(cfg):
    directory = download.target_directory(cfg, "2024-03-09")
    directory.mkdir(parents=True)
    (directory / "AIS_2024_03_09.csv").write_text("MMSI\n366000001\n")
    assert download.already_present(cfg, "2024-03-09")
    assert download.fetch_day(cfg, "2024-03-09")["downloaded"] is False


def test_dropping_a_day_frees_its_files(cfg):
    directory = download.target_directory(cfg, "2024-03-09")
    directory.mkdir(parents=True)
    payload = "MMSI\n366000001\n"
    (directory / "AIS_2024_03_09.csv").write_text(payload)
    outcome = download.drop_day(cfg, "2024-03-09")
    assert outcome["files_removed"] == 1
    assert outcome["bytes_freed"] == len(payload)
    assert not directory.exists()


def test_dropping_a_missing_day_is_harmless(cfg):
    outcome = download.drop_day(cfg, "2024-03-10")
    assert outcome["files_removed"] == 0


def test_only_the_first_days_are_kept(cfg):
    days = ["2024-01-0" + str(index) for index in range(1, 8)]
    assert download.days_to_keep(cfg, days) == {"2024-01-01"}
    assert Path(cfg.paths.raw).exists()
