from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from aisetl import reference

AIS_COLUMNS = [
    "MMSI",
    "BaseDateTime",
    "LAT",
    "LON",
    "SOG",
    "COG",
    "Heading",
    "VesselName",
    "IMO",
    "CallSign",
    "VesselType",
    "Status",
    "Length",
    "Width",
    "Draft",
    "Cargo",
    "TransceiverClass",
]

NAME_HEADS = [
    "ATLANTIC", "PACIFIC", "NORTHERN", "SOUTHERN", "CORAL", "AMBER", "IRON", "SILVER",
    "HARBOR", "MERIDIAN", "SUMMIT", "CASCADE", "DELTA", "ORION", "PELICAN", "TRADEWIND",
    "GRANITE", "COPPER", "LUNAR", "SABLE",
]

NAME_TAILS = [
    "STAR", "TRADER", "VOYAGER", "SPIRIT", "PIONEER", "MARINER", "CARRIER", "EXPRESS",
    "HORIZON", "BRIDGE", "RUNNER", "SENTINEL",
]

FLAG_MIDS = [366, 367, 368, 538, 636, 477, 215, 249]

COAST_WEIGHTS = [("west", 31), ("east", 31), ("gulf", 28), ("lakes", 6), ("pacific", 4)]


def rnd(*parts):
    return "cast(pmod(hash(" + ", ".join(parts) + "), 100000) / 100000.0 as double)"


def tag(name):
    return "'" + name + "'"


def array_literal(values):
    return "array(" + ", ".join("'" + str(value) + "'" for value in values) + ")"


def coast_case():
    grouped = reference.ports_by_coast()
    branches = " ".join(
        "when '" + coast + "' then " + array_literal(ids) for coast, ids in grouped.items()
    )
    return "case coast " + branches + " end"


def coast_pick():
    total = sum(weight for _, weight in COAST_WEIGHTS)
    branches = []
    cursor = 0
    for coast, weight in COAST_WEIGHTS:
        cursor += weight
        branches.append(
            "when pmod(hash(vessel_idx, 'coast'), " + str(total) + ") < " + str(cursor)
            + " then '" + coast + "'"
        )
    return "case " + " ".join(branches) + " end"


def _by_category(mapping, default, seed):
    branches = []
    for category, (base, span) in mapping.items():
        branches.append(
            "when '" + category + "' then " + str(base) + " + "
            + rnd("vessel_idx", tag(seed)) + " * " + str(span)
        )
    fallback = str(default[0]) + " + " + rnd("vessel_idx", tag(seed)) + " * " + str(default[1])
    return "case category " + " ".join(branches) + " else " + fallback + " end"


LENGTHS = {"cargo": (120, 230), "tanker": (150, 180), "tug": (20, 25), "fishing": (12, 50),
           "passenger": (40, 260), "pleasure": (7, 25)}
SPEEDS = {"cargo": (10, 9), "tanker": (9, 6), "tug": (6, 5), "fishing": (4, 6),
          "passenger": (13, 9), "pleasure": (5, 10)}
BERTHS = {"cargo": (16, 28), "tanker": (18, 30), "tug": (1, 5), "fishing": (3, 10),
          "passenger": (4, 10), "pleasure": (8, 30)}


def vessels(spark, cfg):
    flags = ", ".join(str(mid) for mid in FLAG_MIDS)
    base = spark.range(cfg.ais.vessels).selectExpr(
        "id as vessel_idx",
        "element_at(array(" + flags + "), 1 + pmod(hash(id, 'flag'), " + str(len(FLAG_MIDS)) + "))"
        " * 1000000 + 100000 + id as mmsi",
        "case"
        " when pmod(hash(id, 'type'), 100) < 34 then 70 + pmod(hash(id, 'sub'), 10)"
        " when pmod(hash(id, 'type'), 100) < 54 then 80 + pmod(hash(id, 'sub'), 10)"
        " when pmod(hash(id, 'type'), 100) < 66 then 52"
        " when pmod(hash(id, 'type'), 100) < 76 then 30"
        " when pmod(hash(id, 'type'), 100) < 82 then 60 + pmod(hash(id, 'sub'), 10)"
        " when pmod(hash(id, 'type'), 100) < 96 then 37"
        " else 90 + pmod(hash(id, 'sub'), 10) end as vessel_type",
    )
    return base.withColumn("category", reference.category_expr("vessel_type")).selectExpr(
        "vessel_idx",
        "mmsi",
        "vessel_type",
        "category",
        "concat(element_at(" + array_literal(NAME_HEADS) + ", 1 + pmod(hash(vessel_idx, 'head'), "
        + str(len(NAME_HEADS)) + ")), ' ', element_at(" + array_literal(NAME_TAILS)
        + ", 1 + pmod(hash(vessel_idx, 'tail'), " + str(len(NAME_TAILS)) + "))) as vessel_name",
        "case when category in ('cargo', 'tanker', 'passenger') then"
        " concat('IMO', cast(9000000 + pmod(hash(vessel_idx, 'imo'), 999999) as string)) end as imo",
        "concat('W', upper(substring(hex(hash(vessel_idx, 'call')), 1, 3)),"
        " cast(pmod(hash(vessel_idx, 'call2'), 10) as string)) as call_sign",
        _by_category(LENGTHS, (30, 80), "len") + " as length_m",
        _by_category(SPEEDS, (8, 6), "spd") + " as speed_knots",
        "case when category in ('pleasure', 'fishing') then 'B' when "
        + rnd("vessel_idx", tag("cls")) + " < 0.05 then 'B' else 'A' end as transceiver_class",
        "case when category in ('cargo', 'tanker') then vessel_type end as cargo_code",
        coast_pick() + " as coast",
        "case when category in ('cargo', 'tanker') then 4 + " + rnd("vessel_idx", tag("drf"))
        + " * 11 else 1.5 + " + rnd("vessel_idx", tag("drf")) + " * 4 end as draft_m",
    ).withColumn("width_m", F.expr("length_m * (0.12 + " + rnd("vessel_idx", tag("wid")) + " * 0.05)"))


def schedule(spark, cfg, vessel_df, port_df):
    legs = max(6, int((cfg.window.days + 8) * 24 / 30) + 2)
    expanded = (
        vessel_df.withColumn("coast_ports", F.expr(coast_case()))
        .withColumn("leg_idx", F.explode(F.sequence(F.lit(0), F.lit(legs - 1))))
        .withColumn("port_count", F.size("coast_ports"))
        .withColumn("start_slot", F.expr("pmod(hash(vessel_idx, 'start'), port_count)"))
        .withColumn("step", F.expr("1 + pmod(hash(vessel_idx, leg_idx, 'step'), greatest(port_count - 1, 1))"))
    )
    inclusive = Window.partitionBy("vessel_idx").orderBy("leg_idx").rowsBetween(Window.unboundedPreceding, 0)
    exclusive = Window.partitionBy("vessel_idx").orderBy("leg_idx").rowsBetween(Window.unboundedPreceding, -1)
    expanded = (
        expanded.withColumn("cum_step", F.sum("step").over(inclusive))
        .withColumn("prev_step", F.coalesce(F.sum("step").over(exclusive), F.lit(0)))
        .withColumn("to_slot", F.expr("cast(pmod(start_slot + cum_step, port_count) as int)"))
        .withColumn("from_slot", F.expr("cast(pmod(start_slot + prev_step, port_count) as int)"))
        .withColumn("from_port", F.expr("element_at(coast_ports, from_slot + 1)"))
        .withColumn("to_port", F.expr("element_at(coast_ports, to_slot + 1)"))
        .drop("coast_ports", "port_count", "from_slot", "to_slot", "start_slot", "step", "cum_step", "prev_step")
    )
    origin = port_df.selectExpr("port_id as from_port", "lat as from_lat", "lon as from_lon")
    destination = port_df.selectExpr(
        "port_id as to_port", "lat as to_lat", "lon as to_lon", "gateway as to_gateway"
    )
    joined = expanded.join(F.broadcast(origin), "from_port").join(F.broadcast(destination), "to_port")
    with_geometry = joined.selectExpr(
        "*",
        "2 * 6371.0 * asin(sqrt(pow(sin(radians(to_lat - from_lat) / 2), 2)"
        " + cos(radians(from_lat)) * cos(radians(to_lat))"
        " * pow(sin(radians(to_lon - from_lon) / 2), 2))) as distance_km",
        "pmod(degrees(atan2(sin(radians(to_lon - from_lon)) * cos(radians(to_lat)),"
        " cos(radians(from_lat)) * sin(radians(to_lat))"
        " - sin(radians(from_lat)) * cos(radians(to_lat)) * cos(radians(to_lon - from_lon)))) + 360, 360)"
        " as bearing",
    )
    with_time = with_geometry.selectExpr(
        "*",
        "greatest(1.5, distance_km / 1.852 / speed_knots) as transit_hours",
        "case when to_gateway then 3 + " + rnd("vessel_idx", "leg_idx", tag("anch")) + " * 26 else "
        + rnd("vessel_idx", "leg_idx", tag("anch")) + " * 6 end as anchor_hours",
        _by_category(BERTHS, (6, 20), "berth").replace("vessel_idx,", "vessel_idx, leg_idx,")
        + " as berth_hours",
    )
    ordering = Window.partitionBy("vessel_idx").orderBy("leg_idx").rowsBetween(Window.unboundedPreceding, -1)
    start = datetime.fromisoformat(cfg.window.start_date).replace(tzinfo=timezone.utc)
    anchor_epoch = int((start - timedelta(days=4)).timestamp())
    return (
        with_time.withColumn("cycle_hours", F.expr("transit_hours + anchor_hours + berth_hours"))
        .withColumn("leg_offset_hours", F.coalesce(F.sum("cycle_hours").over(ordering), F.lit(0.0)))
        .withColumn(
            "leg_start_epoch",
            F.expr(
                str(anchor_epoch) + " + (leg_offset_hours + " + rnd("vessel_idx", tag("shift"))
                + " * 96) * 3600"
            ),
        )
        .withColumn("transit_end_epoch", F.expr("leg_start_epoch + transit_hours * 3600"))
        .withColumn("anchor_end_epoch", F.expr("transit_end_epoch + anchor_hours * 3600"))
        .withColumn("berth_end_epoch", F.expr("anchor_end_epoch + berth_hours * 3600"))
    )


def _phases(schedule_df):
    exploded = schedule_df.selectExpr(
        "*",
        "explode(array("
        "struct('transit' as phase, leg_start_epoch as phase_start, transit_end_epoch as phase_end),"
        "struct('anchor' as phase, transit_end_epoch as phase_start, anchor_end_epoch as phase_end),"
        "struct('berth' as phase, anchor_end_epoch as phase_start, berth_end_epoch as phase_end)"
        ")) as ph",
    )
    return exploded.selectExpr(
        "*", "ph.phase as phase", "ph.phase_start as phase_start", "ph.phase_end as phase_end"
    ).drop("ph")


def pings(cfg, schedule_df, day):
    interval = cfg.ais.ping_interval_seconds
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())
    end = start + 86400
    rename_epoch = int(
        (
            datetime.fromisoformat(cfg.window.start_date).replace(tzinfo=timezone.utc)
            + timedelta(days=max(1, cfg.window.days // 2))
        ).timestamp()
    )
    gap_modulus = max(2, round(1 / cfg.ais.gap_share)) if cfg.ais.gap_share > 0 else 0
    windowed = _phases(schedule_df).filter(
        "phase_end > " + str(start) + " and phase_start < " + str(end) + " and phase_end > phase_start"
    )
    indexed = windowed.selectExpr(
        "*",
        "cast(greatest(0, ceil((" + str(start) + " - phase_start) / " + str(interval) + ")) as int) as first_idx",
        "cast(ceil((least(phase_end, " + str(end) + ") - phase_start) / " + str(interval)
        + ") - 1 as int) as last_idx",
    ).filter("last_idx >= first_idx")
    expanded = indexed.withColumn("idx", F.explode(F.expr("sequence(first_idx, last_idx)")))
    kept = expanded.filter("transceiver_class = 'A' or pmod(idx, 3) = 0")
    if gap_modulus:
        kept = kept.filter(
            "pmod(cast(floor(idx / 40) as int), " + str(gap_modulus) + ")"
            " != pmod(hash(mmsi, leg_idx), " + str(gap_modulus) + ")"
        )
    positioned = kept.selectExpr(
        "*",
        "phase_start + idx * " + str(interval) + " as ping_epoch",
        "case when phase = 'transit'"
        " then least(1.0, (idx * " + str(interval) + ") / greatest(phase_end - phase_start, 1))"
        " else 1.0 end as progress",
        rnd("vessel_idx", "leg_idx", tag("dir")) + " * 6.283185 as anchor_dir",
        "0.05 + " + rnd("vessel_idx", "leg_idx", tag("off")) + " * 0.06 as anchor_offset",
    ).selectExpr(
        "*",
        "case phase"
        " when 'transit' then from_lat + (to_lat - from_lat) * progress"
        " + (" + rnd("mmsi", "idx", tag("nlat")) + " - 0.5) * 0.002"
        " when 'anchor' then to_lat + anchor_offset * cos(anchor_dir) + sin(idx / 40.0) * 0.002"
        " else to_lat + (" + rnd("mmsi", "idx", tag("blat")) + " - 0.5) * 0.006 end as lat",
        "case phase"
        " when 'transit' then from_lon + (to_lon - from_lon) * progress"
        " + (" + rnd("mmsi", "idx", tag("nlon")) + " - 0.5) * 0.002"
        " when 'anchor' then to_lon + anchor_offset * sin(anchor_dir) / cos(radians(to_lat))"
        " + cos(idx / 40.0) * 0.002"
        " else to_lon + (" + rnd("mmsi", "idx", tag("blon")) + " - 0.5) * 0.006 end as lon",
        "case phase"
        " when 'transit' then speed_knots * (0.85 + " + rnd("mmsi", "idx", tag("sog")) + " * 0.3)"
        " when 'anchor' then " + rnd("mmsi", "idx", tag("asog")) + " * 0.9"
        " else " + rnd("mmsi", "idx", tag("bsog")) + " * 0.25 end as sog",
        "case when phase = 'transit' then pmod(bearing + (" + rnd("mmsi", "idx", tag("cog"))
        + " - 0.5) * 8 + 360, 360) else " + rnd("mmsi", "idx", tag("dcog")) + " * 360 end as cog",
        "case phase when 'transit' then case when category = 'sailing' then 8 else 0 end"
        " when 'anchor' then 1 else 5 end as status",
        "case when " + rnd("mmsi", "idx", tag("corrupt")) + " < " + str(cfg.ais.corrupt_rate)
        + " then pmod(hash(mmsi, idx, 'kind'), 3) else -1 end as corrupt_kind",
        rnd("mmsi", "idx", tag("late")) + " < " + str(cfg.ais.late_share) + " as late",
        "case when pmod(hash(vessel_idx, leg_idx, 'laden'), 2) = 0 then draft_m"
        " else draft_m * 0.66 end as draft_report",
        "case when " + rnd("vessel_idx", tag("ren")) + " < 0.015 and phase_start + idx * "
        + str(interval) + " > " + str(rename_epoch)
        + " then concat(vessel_name, ' II') else vessel_name end as name_report",
    )
    return positioned


def to_feed(cfg, ping_df):
    formatted = ping_df.selectExpr(
        "case when corrupt_kind = 1 then '' else cast(mmsi as string) end as MMSI",
        "case when corrupt_kind = 2 then '0000-00-00T00:00:00'"
        " else date_format(timestamp_seconds(ping_epoch), \"yyyy-MM-dd'T'HH:mm:ss\") end as BaseDateTime",
        "case when corrupt_kind = 0 then '91.00000' else format_string('%.5f', lat) end as LAT",
        "case when corrupt_kind = 0 then '181.00000' else format_string('%.5f', lon) end as LON",
        "case when " + rnd("mmsi", "idx", tag("nosog")) + " < 0.002 then '102.3'"
        " else format_string('%.1f', sog) end as SOG",
        "case when " + rnd("mmsi", "idx", tag("nocog")) + " < 0.01 then '360.0'"
        " else format_string('%.1f', cog) end as COG",
        "case when transceiver_class = 'B' or " + rnd("mmsi", "idx", tag("nohdg")) + " < 0.18 then '511'"
        " else cast(cast(pmod(cog + (" + rnd("mmsi", "idx", tag("hdg")) + " - 0.5) * 6 + 360, 360) as int)"
        " as string) end as Heading",
        "name_report as VesselName",
        "coalesce(imo, '') as IMO",
        "call_sign as CallSign",
        "cast(vessel_type as string) as VesselType",
        "cast(status as string) as Status",
        "format_string('%.1f', length_m) as Length",
        "format_string('%.1f', width_m) as Width",
        "format_string('%.1f', draft_report) as Draft",
        "coalesce(cast(cargo_code as string), '') as Cargo",
        "transceiver_class as TransceiverClass",
        "late",
    )
    duplicates = formatted.filter(
        rnd("MMSI", "BaseDateTime", tag("dup")) + " < " + str(cfg.ais.duplicate_rate)
    )
    return formatted.unionByName(duplicates)


def day_directory(cfg, day):
    return str(Path(cfg.paths.raw) / "ais" / day)


def write_day(spark, cfg, schedule_df, day, next_day=None):
    feed = to_feed(cfg, pings(cfg, schedule_df, day))
    on_time = feed.filter("not late").drop("late")
    on_time.write.mode("overwrite").option("header", True).csv(day_directory(cfg, day))
    if next_day is not None:
        late = feed.filter("late").drop("late")
        late.write.mode("append").option("header", True).csv(day_directory(cfg, next_day))


def run(spark, cfg):
    plan = schedule(spark, cfg, vessels(spark, cfg), reference.ports(spark)).cache()
    plan.count()
    days = cfg.window.dates()
    written = []
    for position, day in enumerate(days):
        follower = days[position + 1] if position + 1 < len(days) else None
        write_day(spark, cfg, plan, day, follower)
        written.append(day)
    plan.unpersist()
    return written
