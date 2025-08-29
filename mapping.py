import os
import sys
import datetime as dt
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ============================================================================
# CONFIG / CONNECTION
# ============================================================================
load_dotenv("config/.env")  # expects DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

CHANNELS = ["lazada", "shopee", "tiktok", "pos"]
SRC_SCHEMAS = {ch: f"src_{ch}" for ch in CHANNELS}

# ---- OPTIONAL: initial master product seeding and bridge mapping overrides ---
# 1) Seed your golden catalog here (only needed once; safe to keep – it's upsert)

df = pd.read_csv("data/master_product.csv")

MASTER_PRODUCT_SEED = [
    (
        row["master_product_code"],
        row["name"],
        row["category"],
        row["brand"],
        int(row["starting_inventory"]),
        dt.datetime.fromisoformat(row["created_at"]) if "created_at" in row and not pd.isna(row["created_at"]) 
            else dt.datetime.now()
    )
    for _, row in df.iterrows()
]

# 2) Manual bridge overrides (source_product_id -> master_product_code)
# You can key by (channel, source_product_id) to avoid ambiguity
BRIDGE_OVERRIDES = {
    # ("shopee", "12345"): "SKU-001",
    # ("lazada", "LZ-987"): "SKU-001",
    # ("tiktok", "TT1122"): "SKU-001",
    # ("pos",    "A1001"): "SKU-001",
}

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

# ============================================================================
# HELPERS
# ============================================================================
def fetchall_dict(cur) -> List[Dict[str, Any]]:
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def get_channel_id(cur, channel_name: str) -> int:
    cur.execute("SELECT channel_id FROM wh.dim_channel WHERE name = %s;", (channel_name,))
    r = cur.fetchone()
    if not r:
        raise RuntimeError(f"Channel '{channel_name}' not found in wh.dim_channel")
    return r[0]

def ensure_dim_date(cur, start_date: dt.date, end_date: dt.date):
    if not start_date or not end_date or start_date > end_date:
        return
    batch = []
    d = start_date
    while d <= end_date:
        batch.append(
            (d, d.year, (d.month - 1)//3 + 1, d.month, d.day, int(d.strftime("%U")), d.weekday() >= 5)
        )
        d += dt.timedelta(days=1)

    execute_values(cur, """
        INSERT INTO wh.dim_date (date_key, year, quarter, month, day, week_of_year, is_weekend)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING;
    """, batch)

def upsert_fx_myr_passthrough(cur, date_key: dt.date):
    cur.execute("""
        INSERT INTO wh.fx_rates (date_key, currency, to_myr)
        VALUES (%s, 'MYR', 1.0)
        ON CONFLICT (date_key, currency) DO NOTHING;
    """, (date_key,))

# ============================================================================
# SEED MASTER PRODUCTS (OPTIONAL)
# ============================================================================
def seed_master_products(MASTER_PRODUCT_SEED, conn):
    if not MASTER_PRODUCT_SEED:
        print("No master products to seed.")
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO wh.dim_product (
                master_product_code, name, category, brand, starting_inventory, created_at
            ) VALUES %s
            ON CONFLICT (master_product_code) DO UPDATE
            SET name = EXCLUDED.name,
                category = EXCLUDED.category,
                brand = EXCLUDED.brand,
                starting_inventory = EXCLUDED.starting_inventory,
                created_at = EXCLUDED.created_at;
        """, MASTER_PRODUCT_SEED)
        print(f"Seeded/Updated {len(MASTER_PRODUCT_SEED)} master products.")
    conn.commit()

# ============================================================================
# BRIDGE PRODUCT MAPPING
# ============================================================================
def upsert_bridge_for_channel(conn, channel: str):
    """
    Always upsert all products that can be mapped (manual override or auto-matched by SKU).
    Any change in src_{channel}.products will be reflected in wh.bridge_product_source.
    """
    schema = SRC_SCHEMAS[channel]
    with conn.cursor() as cur:
        # Read all products from source
        cur.execute(f"""
            SELECT product_id, sku, name, category, brand, cost, price, currency, updated_at
            FROM {schema}.products;
        """)
        src_products = fetchall_dict(cur)
        if not src_products:
            return

        # Map master_product_code -> product_sk
        cur.execute("SELECT product_sk, master_product_code FROM wh.dim_product;")
        master_map = {r[1]: r[0] for r in cur.fetchall() if r[1]}

        rows = []
        for p in src_products:
            spid = p["product_id"]
            key = (channel, spid)
            # Prefer manual override if present
            if key in BRIDGE_OVERRIDES and BRIDGE_OVERRIDES[key] in master_map:
                product_sk = master_map[BRIDGE_OVERRIDES[key]]
            else:
                sku = (p.get("sku") or "").strip()
                # Try exact match first
                if sku and sku in master_map:
                    product_sk = master_map[sku]
                else:
                    # Try matching by suffix after last dash
                    sku_suffix = sku.split("-")[-1] if "-" in sku else sku
                    if sku_suffix and sku_suffix in master_map:
                        product_sk = master_map[sku_suffix]
                    else:
                        continue  # can't map, skip
            rows.append((
                product_sk, channel, spid, p.get("sku"), p.get("name"),
                p.get("cost"), p.get("currency"), p.get("updated_at"), p.get("price")
            ))

        if rows:
            print(f"Upserting {len(rows)} products to bridge for channel {channel}")
            for r in rows:
                print(r)
            execute_values(cur, """
                INSERT INTO wh.bridge_product_source (
                    product_sk, source_channel, source_product_id, source_sku, source_name,
                    cost_native, currency_native, updated_at, price_native
                ) VALUES %s
                ON CONFLICT (source_product_id, source_channel) DO UPDATE
                SET product_sk      = EXCLUDED.product_sk,
                    source_sku      = EXCLUDED.source_sku,
                    source_name     = EXCLUDED.source_name,
                    cost_native     = EXCLUDED.cost_native,
                    currency_native = EXCLUDED.currency_native,
                    updated_at      = EXCLUDED.updated_at,
                    price_native    = EXCLUDED.price_native;
            """, rows)

    conn.commit()

def upsert_bridge_all(conn):
    for ch in CHANNELS:
        upsert_bridge_for_channel(conn, ch)

# ============================================================================
# DIM LOADERS
# ============================================================================
def load_dim_store(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT store_id, name, region, timezone FROM src_pos.stores;")
        rows = fetchall_dict(cur)
        if not rows:
            return
        data = [(r["store_id"], r.get("name"), r.get("region"), r.get("timezone")) for r in rows]
        execute_values(cur, """
            INSERT INTO wh.dim_store (store_id, name, region, timezone)
            VALUES %s
            ON CONFLICT (store_id) DO UPDATE
            SET name = EXCLUDED.name,
                region = EXCLUDED.region,
                timezone = EXCLUDED.timezone;
        """, data)
    conn.commit()

def load_dim_customer(conn, channel: str):
    schema = SRC_SCHEMAS[channel]
    with conn.cursor() as cur:
        id_col = "buyer_id" if channel in ("lazada", "shopee", "tiktok") else "customer_id"
        cur.execute(f"""
            SELECT {id_col} AS source_customer_id, region, created_at
            FROM {schema}.customers;
        """)
        rows = fetchall_dict(cur)
        if not rows:
            return

        data = []
        for r in rows:
            data.append((
                r["source_customer_id"],
                r.get("region"),
                r.get("created_at"),
                channel
            ))
        execute_values(cur, """
            INSERT INTO wh.dim_customer (
                source_customer_id, region, first_seen_at, source_channel
            ) VALUES %s
            ON CONFLICT (source_customer_id, source_channel)
            DO UPDATE SET
                region = COALESCE(EXCLUDED.region, wh.dim_customer.region),
                first_seen_at = LEAST(wh.dim_customer.first_seen_at, EXCLUDED.first_seen_at);
        """, data)
    conn.commit()

def load_dim_campaign(conn):
    with conn.cursor() as cur:
        ch_id = get_channel_id(cur, "tiktok")
        cur.execute("SELECT campaign_id, name, start_at, end_at, budget FROM src_tiktok.campaigns;")
        rows = fetchall_dict(cur)
        if not rows:
            return
        data = []
        for r in rows:
            data.append((
                r["campaign_id"], r.get("name"), ch_id,
                r.get("start_at"), r.get("end_at"),
                r.get("budget"), "MYR"
            ))
        execute_values(cur, """
            INSERT INTO wh.dim_campaign (
              source_campaign_id, name, channel_id, start_at, end_at, budget_native, currency_native
            ) VALUES %s
            ON CONFLICT (source_campaign_id) DO UPDATE
            SET name = EXCLUDED.name,
                channel_id = EXCLUDED.channel_id,
                start_at = EXCLUDED.start_at,
                end_at = EXCLUDED.end_at,
                budget_native = EXCLUDED.budget_native,
                currency_native = EXCLUDED.currency_native;
        """, data)
    conn.commit()

# ============================================================================
# FACT LOADERS
# ============================================================================
def load_fact_orders_and_items_marketplace(conn, channel: str):
    schema = SRC_SCHEMAS[channel]
    with conn.cursor() as cur:
        channel_id = get_channel_id(cur, channel)

        # Orders
        cur.execute(f"""
            SELECT
                o.order_id, o.buyer_id, o.created_at, o.status, o.currency,
                o.total_amount, o.shipping_fee, o.tax_total, o.voucher_amount
            FROM {schema}.orders o;
        """)
        orders = fetchall_dict(cur)
        if orders:
            rows = []
            min_d, max_d = None, None
            for o in orders:
                ts = o.get("created_at")
                if ts:
                    d = ts.date()
                    min_d = d if not min_d or d < min_d else min_d
                    max_d = d if not max_d or d > max_d else max_d
                    upsert_fx_myr_passthrough(cur, d)

                cur.execute("""
                    SELECT customer_sk
                    FROM wh.dim_customer
                    WHERE source_customer_id=%s AND source_channel=%s
                """, (o["buyer_id"], channel))
                c = cur.fetchone()
                customer_sk = c[0] if c else None

                gross = o.get("total_amount") or 0
                net = gross - (o.get("voucher_amount") or 0)

                rows.append((
                    o["order_id"], channel_id, customer_sk, None,  # store_sk None for marketplaces
                    o.get("created_at"), o.get("status"),
                    o.get("currency"), gross, net,
                    o.get("shipping_fee"), o.get("tax_total"), o.get("voucher_amount")
                ))
            if rows:
                execute_values(cur, """
                    INSERT INTO wh.fact_orders (
                        order_id, channel_id, customer_sk, store_sk, order_ts, status,
                        currency_native, order_total_gross, order_total_net,
                        shipping_fee, tax_total, voucher_amount
                    ) VALUES %s
                    ON CONFLICT (order_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        customer_sk = COALESCE(EXCLUDED.customer_sk, wh.fact_orders.customer_sk),
                        store_sk = COALESCE(EXCLUDED.store_sk, wh.fact_orders.store_sk),
                        currency_native = EXCLUDED.currency_native,
                        order_total_gross = EXCLUDED.order_total_gross,
                        order_total_net = EXCLUDED.order_total_net,
                        shipping_fee = EXCLUDED.shipping_fee,
                        tax_total = EXCLUDED.tax_total,
                        voucher_amount = EXCLUDED.voucher_amount;
                """, rows)
                if min_d and max_d:
                    ensure_dim_date(cur, min_d, max_d)

        # Order Items (join via BRIDGE → product_sk)
        cur.execute(f"""
            SELECT oi.order_id, oi.product_id, oi.qty, oi.price, oi.discount
            FROM {schema}.order_items oi;
        """)
        items = fetchall_dict(cur)
        if items:
            cur.execute("SELECT order_sk, order_id FROM wh.fact_orders WHERE channel_id=%s;", (channel_id,))
            order_map = {r[1]: r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT b.source_product_id, b.product_sk
                FROM wh.bridge_product_source b
                WHERE b.source_channel=%s;
            """, (channel,))
            bridge_map = {r[0]: r[1] for r in cur.fetchall()}

            rows = []
            for it in items:
                order_sk = order_map.get(it["order_id"])
                product_sk = bridge_map.get(it["product_id"])
                if not order_sk or not product_sk:
                    # unmapped: either order or product missing; skip
                    continue
                qty = it.get("qty") or 0
                price = it.get("price") or 0
                disc = it.get("discount") or 0
                revenue_net = (price - disc) * qty

                # optional: cost from bridge (latest)
                cur.execute("""
                    SELECT cost_native
                    FROM wh.bridge_product_source
                    WHERE product_sk=%s AND source_channel=%s AND source_product_id=%s
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1;
                """, (product_sk, channel, it["product_id"]))
                c = cur.fetchone()
                cost_each = c[0] if c and c[0] is not None else 0
                cost_total = cost_each * qty
                margin = revenue_net - cost_total

                rows.append((
                    order_sk, product_sk, qty, price, disc,
                    revenue_net, cost_total, margin
                ))

            if rows:
                execute_values(cur, """
                    INSERT INTO wh.fact_order_items (
                        order_sk, product_sk, qty, price, discount,
                        revenue_net, cost, margin
                        
                    ) VALUES %s
                    ON CONFLICT (order_sk, product_sk) DO UPDATE
                    SET qty = EXCLUDED.qty,
                        price = EXCLUDED.price,
                        discount = EXCLUDED.discount,
                        revenue_net = EXCLUDED.revenue_net,
                        cost = EXCLUDED.cost,
                        margin = EXCLUDED.margin
                        
                """, rows)

    conn.commit()

def load_fact_orders_and_items_pos(conn):
    with conn.cursor() as cur:
        channel_id = get_channel_id(cur, "pos")

        # Orders
        cur.execute("""
            SELECT r.receipt_id AS order_id, r.customer_id, r.store_id, r.sold_at AS order_ts,
                   r.status, r.currency, r.subtotal, r.discount_total, r.tax_total,
                   r.shipping_fee, r.grand_total
            FROM src_pos.receipts r;
        """)
        recs = fetchall_dict(cur)
        if recs:
            cur.execute("SELECT store_sk, store_id FROM wh.dim_store;")
            store_map = {r[1]: r[0] for r in cur.fetchall()}

            rows = []
            min_d, max_d = None, None
            for r in recs:
                ts = r.get("order_ts")
                if ts:
                    d = ts.date()
                    min_d = d if not min_d or d < min_d else min_d
                    max_d = d if not max_d or d > max_d else max_d
                    upsert_fx_myr_passthrough(cur, d)

                cur.execute("""
                    SELECT customer_sk FROM wh.dim_customer
                    WHERE source_customer_id=%s AND source_channel='pos';
                """, (r["customer_id"],))
                c = cur.fetchone()
                customer_sk = c[0] if c else None

                store_sk = store_map.get(r["store_id"])
                gross = r.get("grand_total") or 0
                net = (r.get("subtotal") or 0) - (r.get("discount_total") or 0)

                rows.append((
                    r["order_id"], channel_id, customer_sk, store_sk, r.get("order_ts"),
                    r.get("status"), r.get("currency"), gross, net,
                    r.get("shipping_fee"), r.get("tax_total"), r.get("discount_total")
                ))

            if rows:
                execute_values(cur, """
                    INSERT INTO wh.fact_orders (
                        order_id, channel_id, customer_sk, store_sk, order_ts, status,
                        currency_native, order_total_gross, order_total_net,
                        shipping_fee, tax_total, voucher_amount
                    ) VALUES %s
                    ON CONFLICT (order_id) DO UPDATE
                    SET status = EXCLUDED.status,
                        customer_sk = COALESCE(EXCLUDED.customer_sk, wh.fact_orders.customer_sk),
                        store_sk = COALESCE(EXCLUDED.store_sk, wh.fact_orders.store_sk),
                        currency_native = EXCLUDED.currency_native,
                        order_total_gross = EXCLUDED.order_total_gross,
                        order_total_net = EXCLUDED.order_total_net,
                        shipping_fee = EXCLUDED.shipping_fee,
                        tax_total = EXCLUDED.tax_total,
                        voucher_amount = EXCLUDED.voucher_amount;
                """, rows)
                if min_d and max_d:
                    ensure_dim_date(cur, min_d, max_d)

        # Items
        cur.execute("""
            SELECT rl.receipt_id AS order_id, rl.product_id, rl.qty, rl.unit_price, rl.line_discount
            FROM src_pos.receipt_lines rl;
        """)
        lines = fetchall_dict(cur)
        if lines:
            cur.execute("SELECT order_sk, order_id FROM wh.fact_orders WHERE channel_id=%s;", (channel_id,))
            order_map = {r[1]: r[0] for r in cur.fetchall()}

            cur.execute("""
                SELECT b.source_product_id, b.product_sk
                FROM wh.bridge_product_source b
                WHERE b.source_channel='pos';
            """)
            bridge_map = {r[0]: r[1] for r in cur.fetchall()}

            rows = []
            for it in lines:
                order_sk = order_map.get(it["order_id"])
                product_sk = bridge_map.get(it["product_id"])
                if not order_sk or not product_sk:
                    continue
                qty = it.get("qty") or 0
                price = it.get("unit_price") or 0
                disc = it.get("line_discount") or 0
                revenue_net = (price - disc) * qty

                # get cost if available from bridge
                cur.execute("""
                    SELECT cost_native
                    FROM wh.bridge_product_source
                    WHERE product_sk=%s AND source_channel='pos' AND source_product_id=%s
                    ORDER BY updated_at DESC NULLS LAST LIMIT 1;
                """, (product_sk, it["product_id"]))
                c = cur.fetchone()
                cost_each = c[0] if c and c[0] is not None else 0
                cost_total = cost_each * qty
                margin = revenue_net - cost_total

                rows.append((order_sk, product_sk, qty, price, disc,
                             revenue_net, cost_total, margin))

            if rows:
                execute_values(cur, """
                    INSERT INTO wh.fact_order_items (
                        order_sk, product_sk, qty, price, discount,
                        revenue_net, cost, margin
                        
                    ) VALUES %s
                    ON CONFLICT (order_sk, product_sk) DO UPDATE
                    SET qty = EXCLUDED.qty,
                        price = EXCLUDED.price,
                        discount = EXCLUDED.discount,
                        revenue_net = EXCLUDED.revenue_net,
                        cost = EXCLUDED.cost,
                        margin = EXCLUDED.margin
                        
                """, rows)

    conn.commit()

def load_fact_refunds(conn, channel: str):
    schema = SRC_SCHEMAS[channel]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT refund_id, order_id, amount, reason, processed_at
            FROM {schema}.refunds;
        """)
        rows = fetchall_dict(cur)
        if not rows:
            return
        channel_id = get_channel_id(cur, channel)

        cur.execute("SELECT order_sk, order_id FROM wh.fact_orders WHERE channel_id=%s;", (channel_id,))
        order_map = {r[1]: r[0] for r in cur.fetchall()}

        out = []
        min_d, max_d = None, None
        for r in rows:
            order_sk = order_map.get(r["order_id"])
            if not order_sk:
                continue
            ts = r.get("processed_at")
            if ts:
                d = ts.date()
                min_d = d if not min_d or d < min_d else min_d
                max_d = d if not max_d or d > max_d else max_d
                upsert_fx_myr_passthrough(cur, d)
            out.append((
                r["refund_id"], order_sk, None,  # product_sk unknown here
                r.get("amount"), r.get("reason"), r.get("processed_at")
            ))

        if out:
            execute_values(cur, """
                INSERT INTO wh.fact_refunds (
                    refund_id, order_sk, product_sk, amount_native, reason, processed_ts
                ) VALUES %s
                ON CONFLICT (refund_id) DO UPDATE
                SET order_sk = EXCLUDED.order_sk,
                    product_sk = COALESCE(EXCLUDED.product_sk, wh.fact_refunds.product_sk),
                    amount_native = EXCLUDED.amount_native,
                    
                    reason = EXCLUDED.reason,
                    processed_ts = EXCLUDED.processed_ts;
            """, out)
            if min_d and max_d:
                ensure_dim_date(cur, min_d, max_d)

    conn.commit()

# ============================================================================
# INVENTORY SNAPSHOT (MASTER)
# ============================================================================
def recompute_fact_inventory(conn, start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None):
    """
    Rebuilds wh.fact_inventory (per master product) for a date window:
      stock(t) = starting_inventory
                 + cumulative( POS inventory_movements.qty_delta )
                 - cumulative( all channels order_items.qty )
                 + cumulative( refunded qty if tracked per item)  <-- not available in schema; omitted
    This function truncates the date window first, then re-inserts.
    """
    if start_date is None or end_date is None:
        # pick a safe window: min(order date) .. today
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(order_ts)::date FROM wh.fact_orders;")
            mind = cur.fetchone()[0] or dt.date.today()
        start_date = mind
        end_date = dt.date.today()

    with conn.cursor() as cur:
        # ensure dim_date
        ensure_dim_date(cur, start_date, end_date)

        # 1) clear window
        cur.execute("""
            DELETE FROM wh.fact_inventory
            WHERE snapshot_date BETWEEN %s AND %s;
        """, (start_date, end_date))

        # 2) build daily deltas (per master product)
        #    - orders: negative qty per (product_sk, date)
        cur.execute("""
            WITH order_days AS (
                SELECT i.product_sk,
                       DATE(o.order_ts) AS d,
                       SUM(i.qty) AS sold_qty
                FROM wh.fact_order_items i
                JOIN wh.fact_orders o ON o.order_sk = i.order_sk
                WHERE o.order_ts::date BETWEEN %s AND %s
                GROUP BY i.product_sk, DATE(o.order_ts)
            ),
            pos_mov AS (
                -- Map POS product movements via bridge to master product
                SELECT b.product_sk,
                       DATE(m.moved_at) AS d,
                       SUM(m.qty_delta) AS delta_qty
                FROM src_pos.inventory_movements m
                JOIN wh.bridge_product_source b
                  ON b.source_channel = 'pos'
                 AND b.source_product_id = m.product_id
                WHERE m.moved_at::date BETWEEN %s AND %s
                    AND m.movement_type != 'Sale' -- exclude sales, only restocks/adjustments
                GROUP BY b.product_sk, DATE(m.moved_at)
            ),
            all_days AS (
                SELECT product_sk, d, SUM(delta) AS day_delta
                FROM (
                    SELECT product_sk, d, (-1.0)*sold_qty AS delta
                    FROM order_days
                    UNION ALL
                    SELECT product_sk, d, delta_qty AS delta
                    FROM pos_mov
                ) u
                GROUP BY product_sk, d
            )
            SELECT product_sk, d::date AS snapshot_date, day_delta
            FROM all_days
            ORDER BY product_sk, d;
        """, (start_date, end_date, start_date, end_date))
        day_deltas = fetchall_dict(cur)

        # 3) current starting_inventory per product
        cur.execute("SELECT product_sk, starting_inventory, created_at FROM wh.dim_product;")
        rows = cur.fetchall()
        start_inv = {r[0]: (r[1] or 0) for r in rows}
        created_at_map = {r[0]: r[2] for r in rows}

        # 4) accumulate per product
        # Build per-product daily cumulative total from starting_inventory + sum(deltas up to that day)
        from collections import defaultdict
        per_prod = defaultdict(list)
        for row in day_deltas:
            per_prod[row["product_sk"]].append((row["snapshot_date"], float(row["day_delta"] or 0)))

        # Expand to every date in range
        all_dates = []
        d = start_date
        while d <= end_date:
            all_dates.append(d)
            d += dt.timedelta(days=1)

        inserts = []
        for psk in start_inv.keys():
            running = float(start_inv.get(psk, 0))
            product_created = created_at_map.get(psk)
            # create a dict for quick day lookup
            delta_map = {d: 0.0 for d in all_dates}
            for (d, v) in per_prod.get(psk, []):
                delta_map[d] = delta_map.get(d, 0.0) + float(v)

            for d in all_dates:
                if product_created and d < product_created.date():
                    continue
                running += delta_map[d]
                inserts.append((d, psk, running))

        if inserts:
            execute_values(cur, """
                INSERT INTO wh.fact_inventory (snapshot_date, product_sk, stock_qty)
                VALUES %s
                ON CONFLICT (snapshot_date, product_sk) DO UPDATE
                SET stock_qty = EXCLUDED.stock_qty;
            """, inserts)

        # Keep fx passthrough for window
        dd = start_date
        while dd <= end_date:
            upsert_fx_myr_passthrough(cur, dd)
            dd += dt.timedelta(days=1)

    conn.commit()

# ============================================================================
# ORCHESTRATION
# ============================================================================
def main():
    conn = get_db_connection()
    try:
        # 0) seed master catalog (optional but recommended before first run)
        seed_master_products(MASTER_PRODUCT_SEED, conn)

        # 1) dimensions (non-product)
        load_dim_store(conn)
        for ch in CHANNELS:
            load_dim_customer(conn, ch)
        load_dim_campaign(conn)  # TikTok only

        # 2) bridge product mapping (critical for dedupe)
        upsert_bridge_all(conn)

        # 3) facts: orders + items
        for ch in ["lazada", "shopee", "tiktok"]:
            load_fact_orders_and_items_marketplace(conn, ch)
        load_fact_orders_and_items_pos(conn)

        # 4) refunds
        for ch in ["lazada", "shopee", "tiktok"]:
            load_fact_refunds(conn, ch)

        # 5) inventory snapshots (master-level)
        #    You can pass an explicit window, or let it auto-pick min(order_ts)..today
        recompute_fact_inventory(conn)

        print("✅ ETL completed successfully.")

    except Exception as e:
        conn.rollback()
        print("❌ ETL failed:", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
