from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from waybill import delta, reference
from waybill.generate.ais import array_literal, rnd, tag

PLANTS = ("silver", "dim_plant")
CENTERS = ("silver", "dim_dc")
CUSTOMERS = ("silver", "dim_customer")
SKUS = ("silver", "dim_sku")
BOOKINGS = ("silver", "bookings")
ORDERS = ("silver", "orders")
LINES = ("silver", "order_lines")
EVENTS = ("silver", "shipment_events")
INVENTORY = ("silver", "inventory_position")

REGIONS = ["northeast", "southeast", "midwest", "south", "west"]
CHANNELS = ["mass", "grocery", "club", "ecommerce", "drug"]
CATEGORIES = ["fabric_care", "home_care", "baby_care", "family_care", "grooming", "oral_care", "hair_care"]
MILESTONES = ["order_created", "picked", "loaded", "departed", "arrived", "delivered"]


def plants(spark, cfg):
    return spark.range(cfg.cpg.plants).selectExpr(
        "concat('PL', lpad(cast(id + 1 as string), 3, '0')) as plant_id",
        "element_at(" + array_literal(REGIONS) + ", 1 + pmod(hash(id, 'region'), " + str(len(REGIONS))
        + ")) as region",
        "element_at(" + array_literal(CATEGORIES) + ", 1 + pmod(hash(id, 'cat'), " + str(len(CATEGORIES))
        + ")) as primary_category",
        "cast(40000 + pmod(hash(id, 'cap'), 60000) as int) as daily_case_capacity",
    )


def centers(spark, cfg, gateway_ports):
    count = len(gateway_ports)
    return spark.range(cfg.cpg.distribution_centers).selectExpr(
        "concat('DC', lpad(cast(id + 1 as string), 3, '0')) as dc_id",
        "element_at(" + array_literal(REGIONS) + ", 1 + pmod(hash(id, 'region'), " + str(len(REGIONS))
        + ")) as region",
        "element_at(" + array_literal(gateway_ports) + ", 1 + pmod(hash(id, 'port'), " + str(count)
        + ")) as inbound_port",
        "cast(120000 + pmod(hash(id, 'slots'), 380000) as int) as case_capacity",
    )


def customers(spark, cfg):
    return spark.range(cfg.cpg.customers).selectExpr(
        "concat('CU', lpad(cast(id + 1 as string), 5, '0')) as customer_id",
        "element_at(" + array_literal(CHANNELS) + ", 1 + pmod(hash(id, 'chan'), " + str(len(CHANNELS))
        + ")) as channel",
        "element_at(" + array_literal(REGIONS) + ", 1 + pmod(hash(id, 'region'), " + str(len(REGIONS))
        + ")) as region",
        "case when pmod(hash(id, 'tier'), 100) < 8 then 'strategic'"
        " when pmod(hash(id, 'tier'), 100) < 35 then 'key' else 'standard' end as account_tier",
    )


def skus(spark, cfg):
    return spark.range(cfg.cpg.skus).selectExpr(
        "concat('SKU', lpad(cast(id + 1 as string), 5, '0')) as sku_id",
        "element_at(" + array_literal(CATEGORIES) + ", 1 + pmod(hash(id, 'cat'), " + str(len(CATEGORIES))
        + ")) as category",
        "round(4 + " + rnd("id", tag("wt")) + " * 22, 2) as case_weight_kg",
        "round(0.02 + " + rnd("id", tag("cube")) + " * 0.09, 4) as case_cube_m3",
        "round(12 + " + rnd("id", tag("price")) + " * 48, 2) as case_price_usd",
        "pmod(hash(id, 'import'), 100) < 35 as import_sourced",
        "cast(36 + pmod(hash(id, 'pallet'), 60) as int) as cases_per_pallet",
    )


def lane_plan(voyages):
    grouped = voyages.groupBy("lane").agg(
        F.expr("count(*) as lane_voyages"),
        F.expr("cast(percentile_approx(sea_hours, 0.5) as double) as lane_median_hours"),
    )
    return grouped.selectExpr(
        "lane",
        "lane_voyages",
        "case when lane_voyages >= 3 then lane_median_hours end as lane_median_hours",
    )


def bookings(cfg, voyages, center_df, sku_df):
    laden = voyages.filter("leg_state = 'laden' and to_gateway").join(
        F.broadcast(lane_plan(voyages)), "lane", "left"
    )
    per_voyage = laden.withColumn(
        "container_idx", F.explode(F.expr("sequence(0, cast(11 + pmod(hash(voyage_id), 25) as int))"))
    )
    keyed = per_voyage.selectExpr(
        "*",
        "concat_ws('-', voyage_id, cast(container_idx as string)) as booking_id",
        "to_port as inbound_port",
        "cast(round(coalesce(lane_median_hours, distance_km / 1.852 / 15.5 + 18), 2) as double)"
        " as planned_sea_hours",
    )
    with_dc = keyed.join(
        F.broadcast(center_df.selectExpr("dc_id", "inbound_port", "region as dc_region")), "inbound_port"
    )
    imports = sku_df.filter("import_sourced").select("sku_id", "category")
    allocated = with_dc.withColumn(
        "sku_slot", F.expr("pmod(hash(booking_id, dc_id, 'sku'), " + str(1 << 30) + ")")
    )
    numbered = imports.withColumn(
        "sku_index", F.expr("row_number() over (order by sku_id) - 1")
    )
    sku_count = numbered.count()
    joined = allocated.withColumn(
        "sku_index", F.expr("pmod(sku_slot, " + str(max(sku_count, 1)) + ")")
    ).join(F.broadcast(numbered), "sku_index")
    return joined.selectExpr(
        "booking_id",
        "voyage_id",
        "mmsi",
        "from_port",
        "to_port",
        "lane",
        "dc_id",
        "sku_id",
        "category",
        "departed_ts",
        "timestamp_seconds(unix_timestamp(departed_ts) + cast(planned_sea_hours * 3600 as bigint))"
        " as planned_arrival_ts",
        "arrival_ts as actual_arrival_ts",
        "berth_start_ts",
        "anchor_wait_hours",
        "sea_hours",
        "cast(round(sea_hours - planned_sea_hours, 3) as double) as ocean_delay_hours",
        "cast(1500 + pmod(hash(booking_id, 'cases'), 2500) as int) as cases",
        "round(6 + " + rnd("booking_id", tag("disch")) + " * 20, 2) as discharge_hours",
    ).selectExpr(
        "*",
        "timestamp_seconds(unix_timestamp(berth_start_ts)"
        " + cast((discharge_hours + " + str(cfg.cpg.inland_transit_hours) + ") * 3600 as bigint))"
        " as available_ts",
    )


def orders(spark, cfg, customer_df, center_df):
    total = cfg.cpg.orders_per_day * cfg.window.days
    start = int(datetime.fromisoformat(cfg.window.start_date).replace(tzinfo=timezone.utc).timestamp())
    per_day = cfg.cpg.orders_per_day
    base = spark.range(total).selectExpr(
        "id as order_idx",
        "cast(id / " + str(per_day) + " as int) as day_offset",
        "concat('SO', lpad(cast(id + 1 as string), 9, '0')) as order_id",
        "pmod(hash(id, 'cust'), " + str(cfg.cpg.customers) + ") as customer_slot",
        "pmod(hash(id, 'dc'), " + str(cfg.cpg.distribution_centers) + ") as dc_slot",
    )
    timed = base.selectExpr(
        "*",
        "timestamp_seconds(" + str(start) + " + day_offset * 86400"
        " + cast(21600 + " + rnd("order_idx", tag("hour")) + " * 43200 as bigint)) as order_ts",
    )
    customers_indexed = customer_df.withColumn(
        "customer_slot", F.expr("row_number() over (order by customer_id) - 1")
    )
    centers_indexed = center_df.withColumnRenamed("region", "dc_region").withColumn(
        "dc_slot", F.expr("row_number() over (order by dc_id) - 1")
    )
    joined = timed.join(F.broadcast(customers_indexed), "customer_slot").join(
        F.broadcast(centers_indexed), "dc_slot"
    )
    return joined.selectExpr(
        "order_id",
        "order_idx",
        "order_ts",
        "cast(order_ts as date) as order_date",
        "customer_id",
        "channel",
        "account_tier",
        "region as customer_region",
        "dc_id",
        "dc_region",
        "inbound_port",
        "case when account_tier = 'strategic' then " + str(max(cfg.cpg.promised_transit_days - 1, 1))
        + " else " + str(cfg.cpg.promised_transit_days) + " end as promised_days",
    ).selectExpr(
        "*",
        "date_add(order_date, promised_days) as requested_delivery_date",
    )


def order_lines(cfg, order_df, sku_df):
    exploded = order_df.withColumn(
        "line_no",
        F.explode(
            F.expr("sequence(1, cast(1 + pmod(hash(order_idx, 'lines'), "
                   + str(cfg.cpg.max_lines_per_order) + ") as int))")
        ),
    )
    indexed = sku_df.withColumn("sku_slot", F.expr("row_number() over (order by sku_id) - 1"))
    keyed = exploded.withColumn(
        "sku_slot", F.expr("pmod(hash(order_idx, line_no, 'sku'), " + str(cfg.cpg.skus) + ")")
    )
    return keyed.join(F.broadcast(indexed), "sku_slot").selectExpr(
        "concat_ws('-', order_id, cast(line_no as string)) as line_id",
        "order_id",
        "order_idx",
        "line_no",
        "order_ts",
        "order_date",
        "customer_id",
        "channel",
        "account_tier",
        "customer_region",
        "dc_id",
        "dc_region",
        "inbound_port",
        "sku_id",
        "category",
        "import_sourced",
        "case_price_usd",
        "case_weight_kg",
        "requested_delivery_date",
        "cast(20 + pmod(hash(order_idx, line_no, 'cases'), 260) as int) as ordered_cases",
    )


def fulfilment(cfg, lines, booking_df):
    required = lines.withColumn(
        "required_ship_ts",
        F.expr(
            "timestamp_seconds(unix_timestamp(cast(requested_delivery_date as timestamp))"
            " - cast(" + str(cfg.cpg.inland_transit_hours) + " * 3600 as bigint))"
        ),
    )
    queue = Window.partitionBy("dc_id", "sku_id").orderBy("required_ship_ts", "line_id").rowsBetween(
        Window.unboundedPreceding, 0
    )
    demand = required.withColumn(
        "cumulative_cases",
        F.when(F.col("import_sourced"), F.sum("ordered_cases").over(queue)),
    )
    arrivals = Window.partitionBy("dc_id", "sku_id").orderBy("available_ts", "booking_id").rowsBetween(
        Window.unboundedPreceding, 0
    )
    supply = booking_df.withColumn("cumulative_supply", F.sum("cases").over(arrivals))
    slots = supply.groupBy("dc_id", "sku_id").agg(
        F.expr(
            "sort_array(collect_list(struct(cumulative_supply, available_ts, booking_id, voyage_id,"
            " lane, mmsi, ocean_delay_hours, anchor_wait_hours))) as slots"
        )
    )
    joined = demand.join(F.broadcast(slots), ["dc_id", "sku_id"], "left")
    chosen = joined.selectExpr(
        "*",
        "case when import_sourced and slots is not null then coalesce("
        " try_element_at(filter(slots, s -> s.cumulative_supply >= cumulative_cases), 1),"
        " try_element_at(slots, -1)) end as slot",
        "case when import_sourced and slots is not null then"
        " try_element_at(filter(slots, s -> s.cumulative_supply >= cumulative_cases), 1) is null"
        " else false end as unserved",
    ).drop("slots")
    timed = chosen.selectExpr(
        "*",
        "timestamp_seconds(unix_timestamp(order_ts)"
        " + cast((4 + " + rnd("line_id", tag("pick")) + " * 14) * 3600 as bigint)) as pick_ready_ts",
    ).selectExpr(
        "*",
        "case when slot is null then pick_ready_ts"
        " else greatest(pick_ready_ts, slot.available_ts) end as ship_ready_ts",
        "cast(round(" + str(cfg.cpg.inland_transit_hours) + " * (0.8 + "
        + rnd("line_id", tag("road")) + " * 0.5), 2) as double) as inland_hours",
    )
    delivered = timed.selectExpr(
        "*",
        "timestamp_seconds(unix_timestamp(ship_ready_ts) + cast(inland_hours * 3600 as bigint))"
        " as delivered_ts",
    )
    scored = delivered.selectExpr(
        "*",
        "cast(delivered_ts as date) as delivered_date",
        "slot.booking_id as booking_id",
        "slot.voyage_id as voyage_id",
        "slot.lane as lane",
        "slot.mmsi as mmsi",
        "slot.ocean_delay_hours as ocean_delay_hours",
        "slot.anchor_wait_hours as port_wait_hours",
        "slot is not null as import_linked",
        "case when slot is not null then"
        " cast((unix_timestamp(required_ship_ts) - unix_timestamp(slot.available_ts)) / 3600.0 as double)"
        " end as slack_hours",
        "case when slot is not null and slot.available_ts > required_ship_ts then true"
        " else false end as allocation_late",
    )
    return scored.selectExpr(
        "line_id",
        "order_id",
        "line_no",
        "order_ts",
        "order_date",
        "customer_id",
        "channel",
        "account_tier",
        "customer_region",
        "dc_id",
        "dc_region",
        "inbound_port",
        "sku_id",
        "category",
        "import_sourced",
        "import_linked",
        "unserved",
        "booking_id",
        "voyage_id",
        "lane",
        "mmsi",
        "ocean_delay_hours",
        "port_wait_hours",
        "round(slack_hours, 2) as slack_hours",
        "allocation_late",
        "requested_delivery_date",
        "required_ship_ts",
        "pick_ready_ts",
        "ship_ready_ts",
        "delivered_ts",
        "delivered_date",
        "inland_hours",
        "ordered_cases",
        "case when unserved then cast(ordered_cases * (0.3 + " + rnd("line_id", tag("cut0")) + " * 0.4) as int)"
        " when allocation_late and " + rnd("line_id", tag("cut")) + " < 0.45"
        " then cast(ordered_cases * (0.35 + " + rnd("line_id", tag("fill")) + " * 0.5) as int)"
        " when " + rnd("line_id", tag("short")) + " < 0.05"
        " then cast(ordered_cases * (0.55 + " + rnd("line_id", tag("fill2")) + " * 0.4) as int)"
        " else ordered_cases end as shipped_cases",
        "case_price_usd",
        "case_weight_kg",
    ).selectExpr(
        "*",
        "delivered_date <= requested_delivery_date as on_time",
        "shipped_cases >= ordered_cases as in_full",
        "(delivered_date <= requested_delivery_date and shipped_cases >= ordered_cases) as otif",
        "round(shipped_cases * case_price_usd, 2) as shipped_value_usd",
        "datediff(delivered_date, requested_delivery_date) as delivery_variance_days",
    )


def shipments(line_df):
    grouped = line_df.groupBy("order_id", "dc_id", "customer_id", "channel", "account_tier",
                             "customer_region", "order_date").agg(
        F.expr("concat('SH', substring(order_id, 3)) as shipment_id"),
        F.expr("min(order_ts) as order_ts"),
        F.expr("min(pick_ready_ts) as pick_ready_ts"),
        F.expr("max(ship_ready_ts) as ship_ready_ts"),
        F.expr("max(delivered_ts) as delivered_ts"),
        F.expr("max(requested_delivery_date) as requested_delivery_date"),
        F.expr("count(*) as lines"),
        F.expr("sum(ordered_cases) as ordered_cases"),
        F.expr("sum(shipped_cases) as shipped_cases"),
        F.expr("sum(shipped_value_usd) as shipped_value_usd"),
        F.expr("round(sum(shipped_cases * case_weight_kg), 1) as shipped_weight_kg"),
        F.expr("max(import_linked) as import_linked"),
        F.expr("max(allocation_late) as allocation_late"),
        F.expr("max(coalesce(ocean_delay_hours, 0)) as ocean_delay_hours"),
        F.expr("max(coalesce(port_wait_hours, 0)) as port_wait_hours"),
        F.expr("min(case when otif then 1 else 0 end) as otif_flag"),
        F.expr("min(case when on_time then 1 else 0 end) as on_time_flag"),
        F.expr("min(case when in_full then 1 else 0 end) as in_full_flag"),
    )
    return grouped.selectExpr(
        "shipment_id",
        "order_id",
        "dc_id",
        "customer_id",
        "channel",
        "account_tier",
        "customer_region",
        "order_date",
        "order_ts",
        "pick_ready_ts",
        "ship_ready_ts",
        "delivered_ts",
        "cast(delivered_ts as date) as delivered_date",
        "requested_delivery_date",
        "lines",
        "ordered_cases",
        "shipped_cases",
        "shipped_value_usd",
        "shipped_weight_kg",
        "import_linked",
        "allocation_late",
        "round(ocean_delay_hours, 2) as ocean_delay_hours",
        "round(port_wait_hours, 2) as port_wait_hours",
        "otif_flag = 1 as otif",
        "on_time_flag = 1 as on_time",
        "in_full_flag = 1 as in_full",
    )


def shipment_events(cfg, shipment_df):
    milestones = shipment_df.selectExpr(
        "shipment_id",
        "order_id",
        "dc_id",
        "order_date",
        "explode(array("
        "struct('order_created' as milestone, 1 as event_seq, order_ts as event_ts),"
        "struct('picked' as milestone, 2 as event_seq,"
        " timestamp_seconds(unix_timestamp(pick_ready_ts) - 7200) as event_ts),"
        "struct('loaded' as milestone, 3 as event_seq,"
        " timestamp_seconds(unix_timestamp(ship_ready_ts) - 1800) as event_ts),"
        "struct('departed' as milestone, 4 as event_seq, ship_ready_ts as event_ts),"
        "struct('arrived' as milestone, 5 as event_seq,"
        " timestamp_seconds(unix_timestamp(delivered_ts) - 1800) as event_ts),"
        "struct('delivered' as milestone, 6 as event_seq, delivered_ts as event_ts)"
        ")) as event",
    )
    return milestones.selectExpr(
        "concat_ws('-', shipment_id, cast(event.event_seq as string)) as event_id",
        "shipment_id",
        "order_id",
        "dc_id",
        "event.milestone as milestone",
        "event.event_seq as event_seq",
        "event.event_ts as event_ts",
        "cast(event.event_ts as date) as event_date",
        rnd("shipment_id", "event.milestone", tag("late")) + " < " + str(cfg.cpg.import_share * 0.1)
        + " as late_arrival",
    )


def inventory(cfg, line_df, booking_df):
    shipped = line_df.groupBy("dc_id", "sku_id", "delivered_date").agg(
        F.expr("sum(shipped_cases) as cases_shipped")
    ).withColumnRenamed("delivered_date", "position_date")
    received = booking_df.selectExpr(
        "dc_id", "sku_id", "cast(available_ts as date) as position_date", "cases"
    ).groupBy("dc_id", "sku_id", "position_date").agg(F.expr("sum(cases) as cases_received"))
    combined = shipped.join(received, ["dc_id", "sku_id", "position_date"], "full_outer").fillna(
        {"cases_shipped": 0, "cases_received": 0}
    )
    ordering = Window.partitionBy("dc_id", "sku_id").orderBy("position_date").rowsBetween(
        Window.unboundedPreceding, 0
    )
    opening = F.expr("cast(1800 + pmod(hash(dc_id, sku_id), 5200) as int)")
    return (
        combined.withColumn("opening_cases", opening)
        .withColumn("cumulative", F.sum(F.expr("cases_received - cases_shipped")).over(ordering))
        .selectExpr(
            "dc_id",
            "sku_id",
            "position_date",
            "cases_received",
            "cases_shipped",
            "opening_cases",
            "opening_cases + cumulative as closing_cases",
            "round((opening_cases + cumulative) / greatest(cases_shipped, 1), 2) as days_of_supply",
        )
    )


def run(spark, cfg, voyage_df):
    ports = reference.ports(spark)
    gateway_ports = [row["port_id"] for row in ports.filter("gateway").select("port_id").collect()]
    plant_df = plants(spark, cfg)
    center_df = centers(spark, cfg, gateway_ports)
    customer_df = customers(spark, cfg)
    sku_df = skus(spark, cfg)
    booking_df = bookings(cfg, voyage_df, center_df, sku_df).cache()
    order_df = orders(spark, cfg, customer_df, center_df)
    line_df = fulfilment(cfg, order_lines(cfg, order_df, sku_df), booking_df).cache()
    shipment_df = shipments(line_df).cache()
    written = {
        "dim_plant": delta.write(plant_df, cfg.paths.table(*PLANTS), None, "overwrite", "dim_plant"),
        "dim_dc": delta.write(center_df, cfg.paths.table(*CENTERS), None, "overwrite", "dim_dc"),
        "dim_customer": delta.write(
            customer_df, cfg.paths.table(*CUSTOMERS), None, "overwrite", "dim_customer"
        ),
        "dim_sku": delta.write(sku_df, cfg.paths.table(*SKUS), None, "overwrite", "dim_sku"),
        "bookings": delta.write(booking_df, cfg.paths.table(*BOOKINGS), None, "overwrite", "bookings"),
        "orders": delta.write(
            shipment_df, cfg.paths.table(*ORDERS), ["order_date"], "overwrite", "orders"
        ),
        "order_lines": delta.write(
            line_df, cfg.paths.table(*LINES), ["order_date"], "overwrite", "order_lines"
        ),
        "shipment_events": delta.write(
            shipment_events(cfg, shipment_df), cfg.paths.table(*EVENTS), ["event_date"],
            "overwrite", "shipment_events",
        ),
        "inventory_position": delta.write(
            inventory(cfg, line_df, booking_df), cfg.paths.table(*INVENTORY), ["position_date"],
            "overwrite", "inventory_position",
        ),
    }
    line_df.unpersist()
    shipment_df.unpersist()
    booking_df.unpersist()
    return written
