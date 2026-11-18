from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from aisetl import reference
from aisetl.generate import ais


def test_every_vessel_has_a_unique_mmsi(spark, cfg):
    vessels = ais.vessels(spark, cfg)
    assert vessels.count() == cfg.ais.vessels
    assert vessels.select("mmsi").distinct().count() == cfg.ais.vessels
    assert vessels.filter("mmsi < 100000000 or mmsi > 999999999").count() == 0


def test_voyage_legs_chain_port_to_port(spark, cfg):
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    ordering = Window.partitionBy("vessel_idx").orderBy("leg_idx")
    broken = plan.withColumn("previous", F.lag("to_port").over(ordering)).filter(
        "previous is not null and previous != from_port"
    )
    assert plan.filter("from_port = to_port").count() == 0
    assert broken.count() == 0


def test_leg_phases_run_in_order(spark, cfg):
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    assert plan.filter("transit_end_epoch < leg_start_epoch").count() == 0
    assert plan.filter("anchor_end_epoch < transit_end_epoch").count() == 0
    assert plan.filter("berth_end_epoch < anchor_end_epoch").count() == 0


def test_feed_matches_the_published_column_list(spark, cfg):
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    feed = ais.to_feed(cfg, ais.pings(cfg, plan, cfg.window.dates()[0]))
    assert feed.drop("late").columns == ais.AIS_COLUMNS


def test_pings_stay_inside_the_requested_day(spark, cfg):
    day = cfg.window.dates()[0]
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    feed = ais.to_feed(cfg, ais.pings(cfg, plan, day)).filter("BaseDateTime != '0000-00-00T00:00:00'")
    outside = feed.filter("substring(BaseDateTime, 1, 10) != '" + day + "'")
    assert outside.count() == 0


def test_corrupt_rows_are_rare_but_present(spark, cfg):
    day = cfg.window.dates()[0]
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    feed = ais.to_feed(cfg, ais.pings(cfg, plan, day)).cache()
    total = feed.count()
    broken = feed.filter("MMSI = '' or LAT = '91.00000' or BaseDateTime = '0000-00-00T00:00:00'").count()
    assert 0 < broken < total * 0.05


def test_written_day_lands_in_its_own_directory(spark, cfg):
    day = cfg.window.dates()[0]
    follower = cfg.window.dates()[1]
    plan = ais.schedule(spark, cfg, ais.vessels(spark, cfg), reference.ports(spark))
    ais.write_day(spark, cfg, plan, day, follower)
    written = spark.read.option("header", True).csv(ais.day_directory(cfg, day))
    assert written.count() > 0
    assert written.columns == ais.AIS_COLUMNS
