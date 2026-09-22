from __future__ import annotations

from pyspark.sql import functions as F

from waybill import delta, reference

PINGS = ("silver", "port_area_pings")
VESSEL_DAYS = ("silver", "vessel_days")

HAVERSINE = (
    "2 * 6371.0 * asin(sqrt(pow(sin(radians({lat2} - {lat1}) / 2), 2)"
    " + cos(radians({lat1})) * cos(radians({lat2}))"
    " * pow(sin(radians({lon2} - {lon1}) / 2), 2)))"
)


def distance_expr(lat1, lon1, lat2, lon2):
    return HAVERSINE.format(lat1=lat1, lon1=lon1, lat2=lat2, lon2=lon2)


def cell_columns(df, grid, lat="lat", lon="lon"):
    return df.withColumn("cell_lat", F.expr("cast(floor(" + lat + " / " + str(grid) + ") as int)")).withColumn(
        "cell_lon", F.expr("cast(floor(" + lon + " / " + str(grid) + ") as int)")
    )


def port_cells(spark, cfg):
    grid = cfg.ports.grid_degrees
    base = cell_columns(reference.ports(spark), grid)
    spread = base.withColumn("delta_lat", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1)))).withColumn(
        "delta_lon", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1)))
    )
    return spread.selectExpr(
        "port_id",
        "port_name",
        "coast",
        "gateway",
        "lat as port_lat",
        "lon as port_lon",
        "cell_lat + delta_lat as cell_lat",
        "cell_lon + delta_lon as cell_lon",
    ).dropDuplicates(["port_id", "cell_lat", "cell_lon"])


def port_area_pings(spark, cfg, bronze, strategy="broadcast", salt_buckets=None):
    grid = cfg.ports.grid_degrees
    buckets = salt_buckets or cfg.tuning.salt_buckets
    pings = cell_columns(bronze, grid)
    cells = port_cells(spark, cfg)
    if strategy == "broadcast":
        joined = pings.join(F.broadcast(cells), ["cell_lat", "cell_lon"])
    elif strategy == "shuffle":
        joined = pings.join(cells, ["cell_lat", "cell_lon"])
    elif strategy == "salted":
        salted_pings = pings.withColumn("salt", F.expr("pmod(hash(mmsi, ping_ts), " + str(buckets) + ")"))
        salted_cells = cells.withColumn(
            "salt", F.explode(F.expr("sequence(0, " + str(buckets - 1) + ")"))
        )
        joined = salted_pings.join(salted_cells, ["cell_lat", "cell_lon", "salt"]).drop("salt")
    else:
        raise ValueError("unknown join strategy: " + strategy)
    measured = joined.withColumn(
        "distance_km", F.expr(distance_expr("lat", "lon", "port_lat", "port_lon"))
    ).filter("distance_km <= " + str(cfg.ports.radius_km))
    nearest = measured.groupBy("mmsi", "ping_ts").agg(
        F.expr("min_by(struct(port_id, port_name, coast, gateway, distance_km, port_lat, port_lon),"
               " distance_km) as pick"),
        F.expr("min(lat) as lat"),
        F.expr("min(lon) as lon"),
        F.expr("min(sog) as sog"),
        F.expr("min(cog) as cog"),
        F.expr("min(status) as status"),
        F.expr("min(draft_m) as draft_m"),
        F.expr("min(ping_date) as ping_date"),
    )
    return nearest.selectExpr(
        "mmsi",
        "ping_ts",
        "ping_date",
        "pick.port_id as port_id",
        "pick.port_name as port_name",
        "pick.coast as coast",
        "pick.gateway as gateway",
        "pick.distance_km as distance_km",
        "pick.port_lat as port_lat",
        "pick.port_lon as port_lon",
        "lat",
        "lon",
        "sog",
        "cog",
        "status",
        "draft_m",
    )


def vessel_days(bronze):
    ordered = bronze.selectExpr(
        "mmsi",
        "ping_date",
        "ping_ts",
        "lat",
        "lon",
        "sog",
        "lag(lat) over (partition by mmsi, ping_date order by ping_ts) as prev_lat",
        "lag(lon) over (partition by mmsi, ping_date order by ping_ts) as prev_lon",
    )
    stepped = ordered.withColumn(
        "step_km",
        F.expr(
            "case when prev_lat is null then 0 else "
            + distance_expr("lat", "lon", "prev_lat", "prev_lon")
            + " end"
        ),
    )
    return stepped.groupBy("mmsi", "ping_date").agg(
        F.expr("count(*) as pings"),
        F.expr("sum(case when step_km < 60 then step_km else 0 end) as distance_km"),
        F.expr("round(avg(sog), 2) as avg_sog"),
        F.expr("max(sog) as max_sog"),
        F.expr("sum(case when sog >= 0.7 then 1 else 0 end) as moving_pings"),
        F.expr("min(ping_ts) as first_ping"),
        F.expr("max(ping_ts) as last_ping"),
    )


def run(spark, cfg, bronze):
    area = port_area_pings(spark, cfg, bronze)
    rows = delta.write(area, cfg.paths.table(*PINGS), ["ping_date"], "overwrite", "port_area_pings")
    days = delta.write(
        vessel_days(bronze), cfg.paths.table(*VESSEL_DAYS), ["ping_date"], "overwrite", "vessel_days"
    )
    return {"port_area_pings": rows, "vessel_days": days}
