from __future__ import annotations

import zipfile
from pathlib import Path

import requests


def target_directory(cfg, day):
    return Path(cfg.paths.raw) / "ais" / day


def file_url(cfg, day):
    year, month, dayofmonth = day.split("-")
    return cfg.ais.url_template.format(year=year, month=month, day=dayofmonth)


def already_present(cfg, day):
    directory = target_directory(cfg, day)
    return directory.exists() and (any(directory.glob("*.csv")) or any(directory.glob("*.parquet")))


def fetch_day(cfg, day, timeout=600):
    directory = target_directory(cfg, day)
    if already_present(cfg, day):
        return {"day": day, "downloaded": False, "bytes": 0, "path": str(directory)}
    directory.mkdir(parents=True, exist_ok=True)
    url = file_url(cfg, day)
    name = url.rsplit("/", 1)[-1]
    if name.endswith(".parquet"):
        target = directory / name
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)
        return {"day": day, "downloaded": True, "bytes": target.stat().st_size, "path": str(directory)}
    archive = directory / name
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with archive.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1 << 20):
                handle.write(chunk)
    size = archive.stat().st_size
    with zipfile.ZipFile(archive) as bundle:
        members = [item for item in bundle.namelist() if item.lower().endswith(".csv")]
        if not members:
            raise ValueError("no csv inside " + str(archive))
        for member in members:
            with bundle.open(member) as source, (directory / Path(member).name).open("wb") as sink:
                while True:
                    block = source.read(1 << 20)
                    if not block:
                        break
                    sink.write(block)
    archive.unlink()
    return {"day": day, "downloaded": True, "bytes": size, "path": str(directory)}


def drop_day(cfg, day):
    directory = target_directory(cfg, day)
    removed = 0
    freed = 0
    if not directory.exists():
        return {"day": day, "files_removed": 0, "bytes_freed": 0}
    for item in sorted(directory.glob("*")):
        if item.is_file():
            freed += item.stat().st_size
            item.unlink()
            removed += 1
    directory.rmdir()
    return {"day": day, "files_removed": removed, "bytes_freed": freed}


def days_to_keep(cfg, days):
    return set(days[: cfg.ais.keep_raw_days])


def run(cfg, days=None):
    return [fetch_day(cfg, day) for day in (days or cfg.window.dates())]
