import os, sys, secrets
from datetime import datetime
from pathlib import Path

import pytz
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values

# ---------- ENV ----------
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root
DOTENV_PATH = BASE_DIR / "config" / ".env"
if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)
else:
    load_dotenv(BASE_DIR / ".env")

sys.path.insert(0, str(BASE_DIR))

# ---------- DB POOL (Supabase Postgres) ----------
DB_KW = dict(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "6543"),
    dbname=os.getenv("DB_NAME", "postgres"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode=os.getenv("DB_SSLMODE", "require"),
    target_session_attrs=os.getenv("DB_TARGET_SESSION_ATTRS", "read-write"),
)
_missing = [k for k, v in DB_KW.items() if v in (None, "")]
if _missing:
    raise RuntimeError(f"Missing DB env values for: {', '.join(_missing)}. Check {DOTENV_PATH}")

pool = SimpleConnectionPool(1, 12, **DB_KW)
MY_TZ = pytz.timezone("Asia/Kuala_Lumpur")

def get_conn():
    return pool.getconn()

def put_conn(conn):
    if conn: pool.putconn(conn)

# ---------- Helpers ----------
def table_has_col(schema: str, table: str, column: str) -> bool:
    sql = """
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema=%s AND table_name=%s AND column_name=%s
      LIMIT 1
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (schema, table, column))
            return cur.fetchone() is not None
    finally:
        put_conn(conn)

def table_exists(schema: str, table: str) -> bool:
    sql = """
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema=%s AND table_name=%s
      LIMIT 1
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (schema, table))
            return cur.fetchone() is not None
    finally:
        put_conn(conn)

def next_id(schema: str, table: str, column: str, prefix: str, width: int = 3) -> str:
    sql = f"""
      SELECT COALESCE(MAX(CAST(SUBSTRING({column} FROM %s) AS INTEGER)), 0)
      FROM {schema}.{table}
      WHERE {column} ~ %s
    """
    pattern = f'^{prefix}(\\d+)$'
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (pattern, pattern))
            n = cur.fetchone()[0] or 0
            return f"{prefix}{str(n + 1).zfill(width)}"
    finally:
        put_conn(conn)

def next_id_batch(schema: str, table: str, column: str, prefix: str, width: int, count: int):
    sql = f"""
      SELECT COALESCE(MAX(CAST(SUBSTRING({column} FROM %s) AS INTEGER)), 0)
      FROM {schema}.{table}
      WHERE {column} ~ %s
    """
    pattern = f'^{prefix}(\\d+)$'
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (pattern, pattern))
            n = cur.fetchone()[0] or 0
            start = n + 1
            return [f"{prefix}{str(start + i).zfill(width)}" for i in range(count)]
    finally:
        put_conn(conn)

def _resolve_store_col(table: str) -> str | None:
    candidates = ("store_id", "store", "store_code", "outlet_id", "branch_id")
    for c in candidates:
        if table_has_col("src_pos", table, c):
            return c
    return None

# ---------- APP ----------
app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)

# ---------- STATIC ----------
@app.get("/")
def index():
    return send_from_directory("static", "index.html")

@app.get("/api/health")
def api_health():
    return jsonify({"ok": True})

# ---------- LOOKUPS ----------
@app.get("/api/stores")
def api_stores():
    sql = "SELECT store_id, COALESCE(name, store_id) AS name FROM src_pos.stores ORDER BY 2;"
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return jsonify([{"store_id": r[0], "name": r[1]} for r in rows])
    finally:
        put_conn(conn)

@app.get("/api/terminals")
def api_terminals():
    """
    Optional filter: /api/terminals?store_id=STR01
    Returns [{terminal_id, name}]
    Column-safe discovery so we never reference a missing column.
    """
    store_id = (request.args.get("store_id") or "").strip()

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
            """, ("src_pos", "terminals"))
            cols = {r[0] for r in cur.fetchall()}

        term_id_col = next((c for c in ("terminal_id", "terminal_code", "id", "code") if c in cols), None)
        if not term_id_col:
            return jsonify([])  # no usable columns

        name_col = next((c for c in ("name", "terminal_name", "display_name", "label", "description") if c in cols), term_id_col)
        store_col = next((c for c in ("store_id", "store", "store_code", "outlet_id", "branch_id") if c in cols), None)

        name_expr = (f"COALESCE({name_col}::text, {term_id_col}::text)" if name_col != term_id_col else f"{term_id_col}::text")
        base = f"SELECT {term_id_col} AS terminal_id, {name_expr} AS name FROM src_pos.terminals"
        params = []
        if store_id and store_col:
            sql = f"{base} WHERE {store_col}=%s ORDER BY 2"
            params = [store_id]
        else:
            sql = f"{base} ORDER BY 2"

        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return jsonify([{"terminal_id": r[0], "name": r[1]} for r in rows])
    finally:
        put_conn(conn)

@app.get("/api/cashiers")
def api_cashiers():
    """
    Optional filter: /api/cashiers?store_id=STR01
    Returns [{cashier_id, name}]
    Column-safe discovery.
    """
    store_id = (request.args.get("store_id") or "").strip()

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
            """, ("src_pos", "cashiers"))
            cols = {r[0] for r in cur.fetchall()}

        cash_id_col = next((c for c in ("cashier_id", "cashier_code", "id", "code") if c in cols), None)
        if not cash_id_col:
            return jsonify([])

        name_col = next((c for c in ("name", "cashier_name", "display_name", "label", "description") if c in cols), cash_id_col)
        store_col = next((c for c in ("store_id", "store", "store_code", "outlet_id", "branch_id") if c in cols), None)

        name_expr = (f"COALESCE({name_col}::text, {cash_id_col}::text)" if name_col != cash_id_col else f"{cash_id_col}::text")
        base = f"SELECT {cash_id_col} AS cashier_id, {name_expr} AS name FROM src_pos.cashiers"
        params = []
        if store_id and store_col:
            sql = f"{base} WHERE {store_col}=%s ORDER BY 2"
            params = [store_id]
        else:
            sql = f"{base} ORDER BY 2"

        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return jsonify([{"cashier_id": r[0], "name": r[1]} for r in rows])
    finally:
        put_conn(conn)

@app.get("/api/customers")
def api_customers():
    sql = """
      SELECT customer_id, COALESCE(name, customer_id) AS name
      FROM src_pos.customers
      ORDER BY 2
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return jsonify([{"customer_id": r[0], "name": r[1]} for r in rows])
    finally:
        put_conn(conn)

# ---------- CUSTOMERS ----------
@app.post("/api/customers")
def api_create_customer():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Missing field: name"}), 400

    cid = next_id("src_pos", "customers", "customer_id", "POSC", 8)

    cols = ["customer_id", "name"]
    vals = [cid, name]

    if "region" not in data and data.get("state"):
        data["region"] = data.get("state")

    optional_cols = ["email", "phone", "address", "city", "state", "region", "postcode", "country"]
    for c in optional_cols:
        if table_has_col("src_pos", "customers", c) and data.get(c) not in (None, ""):
            cols.append(c); vals.append(str(data[c]).strip())

    if table_has_col("src_pos", "customers", "created_at"):
        cols.append("created_at"); vals.append(datetime.now(tz=MY_TZ))

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT customer_id FROM src_pos.customers WHERE LOWER(name)=LOWER(%s) LIMIT 1",
                (name,)
            )
            if cur.fetchone():
                return jsonify({"ok": False, "error": "Customer exists (same name)"}), 409

            ph = ", ".join(["%s"] * len(cols))
            cur.execute(f"INSERT INTO src_pos.customers ({', '.join(cols)}) VALUES ({ph})", vals)

        return jsonify({"ok": True, "customer_id": cid}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        put_conn(conn)

@app.get("/api/customers/exists")
def api_customer_exists():
    name = (request.args.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Provide name"}), 400
    sql = "SELECT customer_id, name FROM src_pos.customers WHERE LOWER(name)=LOWER(%s) LIMIT 1"
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (name,))
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "customer_id": row[0], "name": row[1]})
            return jsonify({"exists": False})
    finally:
        put_conn(conn)

# ---------- PRODUCTS ----------
@app.get("/api/products")
def api_products():
    q = (request.args.get("search") or "").strip()
    clauses, params = [], []
    if q:
        clauses.append("(sku ILIKE %s OR name ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = (
        "SELECT product_id, sku, name, category, brand, "
        "COALESCE(price,0)::numeric AS price, COALESCE(cost,0)::numeric AS cost, currency "
        "FROM src_pos.products" + where_sql + " ORDER BY name LIMIT 50"
    )
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return jsonify([
                {
                    "product_id": r[0], "sku": r[1], "name": r[2],
                    "category": r[3], "brand": r[4],
                    "price": float(r[5]), "cost": float(r[6]), "currency": r[7]
                }
                for r in rows
            ])
    finally:
        put_conn(conn)

@app.post("/api/products")
def api_create_product():
    data = request.get_json(force=True)
    required = ["sku", "name", "category", "brand", "price", "currency"]
    for k in required:
        if k not in data or str(data[k]).strip() == "":
            return jsonify({"error": f"Missing field: {k}"}), 400

    pid = next_id("src_pos", "products", "product_id", "POS-PROD", 4)
    now_ts = datetime.now(tz=MY_TZ)

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT product_id FROM src_pos.products WHERE LOWER(sku)=LOWER(%s) OR LOWER(name)=LOWER(%s) LIMIT 1",
                (data["sku"], data["name"]),
            )
            if cur.fetchone():
                return jsonify({"ok": False, "error": "Product exists (same SKU or Name)"}), 409

            cols = ["product_id","sku","name","category","brand","cost","price","currency"]
            vals = [
                pid,
                data["sku"].strip(), data["name"].strip(),
                data["category"].strip(), data["brand"].strip(),
                float(data.get("cost") or 0), float(data["price"]),
                data["currency"].strip()
            ]
            if table_has_col("src_pos", "products", "created_at"):
                cols.append("created_at"); vals.append(now_ts)
            if table_has_col("src_pos", "products", "updated_at"):
                cols.append("updated_at"); vals.append(now_ts)

            ph = ", ".join(["%s"] * len(cols))
            cur.execute(f"INSERT INTO src_pos.products ({', '.join(cols)}) VALUES ({ph})", vals)

        return jsonify({"ok": True, "product_id": pid}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        put_conn(conn)

@app.get("/api/products/exists")
def api_product_exists():
    sku = (request.args.get("sku") or "").strip()
    name = (request.args.get("name") or "").strip()
    if not sku and not name:
        return jsonify({"error": "Provide sku or name"}), 400
    clauses, params = [], []
    if sku:  clauses.append("LOWER(sku) = LOWER(%s)");  params.append(sku)
    if name: clauses.append("LOWER(name) = LOWER(%s)"); params.append(name)
    sql = "SELECT product_id, sku, name FROM src_pos.products WHERE " + " OR ".join(clauses) + " LIMIT 1"
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row:
                return jsonify({"exists": True, "product_id": row[0], "sku": row[1], "name": row[2]})
            return jsonify({"exists": False})
    finally:
        put_conn(conn)

@app.patch("/api/products/<product_id>")
def api_update_product(product_id):
    data = request.get_json(force=True)
    allowed = {"name", "price", "brand", "category", "cost", "currency"}
    sets, vals = [], []
    for k, v in data.items():
        if k in allowed:
            sets.append(f"{k}=%s"); vals.append(v)
    if table_has_col("src_pos", "products", "updated_at"):
        sets.append("updated_at=%s"); vals.append(datetime.now(tz=MY_TZ))
    if not sets:
        return jsonify({"error": "No updatable fields provided"}), 400
    sql = "UPDATE src_pos.products SET " + ", ".join(sets) + " WHERE product_id=%s"
    vals.append(product_id)
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, vals)
            return jsonify({"ok": True, "updated": cur.rowcount})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        put_conn(conn)

# ---------- SALES (receipt + lines + payment) ----------
def _gen_payment_ref(method: str) -> str:
    """Generate ref_no with required prefix + 10 digits."""
    m = (method or "").strip().upper()
    prefix = {
        "TOUCH N GO": "TNGO",
        "CASH": "CASH",
        "GRAB PAY": "GRAB",
        "CREDIT CARD": "CRDC",
        "DEBIT CARD": "DBTC",
    }.get(m, "PAY")
    digits = "".join(secrets.choice("0123456789") for _ in range(10))
    return prefix + digits

@app.post("/api/receipts")
def api_create_receipt():
    """
    Body:
    {
      "store_id": "...", "terminal_id": "...", "cashier_id": "...",
      "customer_id": "...", "currency": "MYR",
      "tax_rate": 6.0, "order_discount": 0.0, "shipping_fee": 0.0,
      "payment_method": "CASH" | "TOUCH N GO" | "GRAB PAY" | "CREDIT CARD" | "DEBIT CARD",
      "payment_ref": "CASHxxxxxxxxxx" (optional; auto-generated if empty),
      "items": [{product_id, qty, unit_price, line_discount}]
    }
    """
    data = request.get_json(force=True)

    required = ["store_id", "customer_id", "currency", "items"]
    for k in required:
        if k not in data:
            return jsonify({"error": f"Missing field: {k}"}), 400

    items = data["items"] or []
    if not items:
        return jsonify({"error": "Cart is empty"}), 400

    terminal_id  = (data.get("terminal_id")  or "").strip()
    cashier_id   = (data.get("cashier_id")   or "").strip()
    cashier_code = (data.get("cashier_code") or "").strip()

    pay_method = (data.get("payment_method") or "").strip()
    pay_ref    = (data.get("payment_ref") or "").strip()

    order_discount = float(data.get("order_discount", 0.0))
    tax_rate       = float(data.get("tax_rate", 0.0))
    shipping_fee   = float(data.get("shipping_fee", 0.0))

    subtotal       = sum(float(i["unit_price"]) * int(i["qty"]) for i in items)
    line_discounts = sum(float(i.get("line_discount", 0.0)) * int(i["qty"]) for i in items)
    after_line     = max(subtotal - line_discounts, 0.0)
    after_order    = max(after_line - order_discount, 0.0)
    tax_total      = round(after_order * tax_rate / 100.0, 2)
    grand_total    = after_order + tax_total + shipping_fee

    rid = next_id("src_pos", "receipts", "receipt_id", "REC", 8)
    sold_at = datetime.now(tz=MY_TZ)

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # ----- receipts
                cols = [
                    "receipt_id","customer_id","store_id","sold_at","status","currency",
                    "subtotal","discount_total","tax_total","shipping_fee","grand_total"
                ]
                vals = [
                    rid, data["customer_id"], data["store_id"], sold_at, "COMPLETED",
                    data["currency"], subtotal, line_discounts + order_discount,
                    tax_total, shipping_fee, grand_total
                ]
                if table_has_col("src_pos", "receipts", "cashier_id") and cashier_id:
                    cols.append("cashier_id"); vals.append(cashier_id)
                elif table_has_col("src_pos", "receipts", "cashier_code") and cashier_code:
                    cols.append("cashier_code"); vals.append(cashier_code)
                if table_has_col("src_pos", "receipts", "terminal_id") and terminal_id:
                    cols.append("terminal_id"); vals.append(terminal_id)

                ph = ", ".join(["%s"] * len(cols))
                cur.execute(f"INSERT INTO src_pos.receipts ({', '.join(cols)}) VALUES ({ph})", vals)

                # ----- preload product meta
                pids = tuple({i["product_id"] for i in items})
                prod_map = {}
                if pids:
                    cur.execute(
                        "SELECT product_id, name, category, sku FROM src_pos.products WHERE product_id IN %s",
                        (pids,)
                    )
                    for pid, nm, cat, sku in cur.fetchall():
                        prod_map[pid] = {"name": nm, "category": cat, "sku": sku}

                has_line = {
                    "product_name": table_has_col("src_pos", "receipt_lines", "product_name"),
                    "category":     table_has_col("src_pos", "receipt_lines", "category"),
                    "sku":          table_has_col("src_pos", "receipt_lines", "sku"),
                    "line_tax":     table_has_col("src_pos", "receipt_lines", "line_tax"),
                    "line_total":   table_has_col("src_pos", "receipt_lines", "line_total"),
                }

                line_cols = ["line_id","receipt_id","product_id","qty","unit_price","line_discount"]
                if has_line["product_name"]: line_cols.append("product_name")
                if has_line["category"]:     line_cols.append("category")
                if has_line["sku"]:          line_cols.append("sku")
                if has_line["line_tax"]:     line_cols.append("line_tax")
                if has_line["line_total"]:   line_cols.append("line_total")

                line_ids = next_id_batch("src_pos", "receipt_lines", "line_id", "LINE", 8, len(items))
                rows = []
                for idx, i in enumerate(items):
                    pid  = i["product_id"]
                    qty  = int(i["qty"])
                    unit = float(i["unit_price"])
                    disc = float(i.get("line_discount", 0.0))
                    line_amount = max((unit - disc) * qty, 0.0)
                    line_tax    = round(line_amount * tax_rate / 100.0, 2)
                    pm = prod_map.get(pid, {})
                    row = [line_ids[idx], rid, pid, qty, unit, disc]
                    if has_line["product_name"]: row.append(pm.get("name"))
                    if has_line["category"]:     row.append(pm.get("category"))
                    if has_line["sku"]:          row.append(pm.get("sku"))
                    if has_line["line_tax"]:     row.append(line_tax)
                    if has_line["line_total"]:   row.append(line_amount)
                    rows.append(tuple(row))

                execute_values(
                    cur,
                    f"INSERT INTO src_pos.receipt_lines ({', '.join(line_cols)}) VALUES %s",
                    rows
                )

                # ----- inventory_movements (optional)
                if table_exists("src_pos", "inventory_movements"):
                    move_ids = next_id_batch("src_pos", "inventory_movements", "movement_id", "IMV", 6, len(items))
                    inv_rows = [
                        (
                            move_ids[idx],
                            i["product_id"], data["store_id"], "Sale",
                            -int(i["qty"]), rid, sold_at
                        )
                        for idx, i in enumerate(items)
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO src_pos.inventory_movements
                          (movement_id, product_id, store_id, movement_type, qty_delta, reference_id, moved_at)
                        VALUES %s
                        """,
                        inv_rows
                    )

                # ----- payment (optional; table name assumed "payments")
                if table_exists("src_pos", "payments"):
                    # discover available columns
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema=%s AND table_name=%s
                    """, ("src_pos", "payments"))
                    pcols = {r[0] for r in cur.fetchall()}

                    payment_id = next_id("src_pos", "payments", "payment_id", "PAY", 8) if "payment_id" in pcols else None
                    method = pay_method or "CASH"
                    ref_no = pay_ref or _gen_payment_ref(method)
                    paid_at = sold_at

                    cols = []
                    vals = []

                    if "payment_id" in pcols and payment_id:
                        cols.append("payment_id"); vals.append(payment_id)
                    if "receipt_id" in pcols:
                        cols.append("receipt_id"); vals.append(rid)
                    if "method" in pcols:
                        cols.append("method"); vals.append(method)
                    if "amount" in pcols:
                        cols.append("amount"); vals.append(grand_total)
                    if "ref_no" in pcols:
                        cols.append("ref_no"); vals.append(ref_no)
                    if "paid_at" in pcols:
                        cols.append("paid_at"); vals.append(paid_at)

                    if cols:
                        placeholders = ", ".join(["%s"] * len(cols))
                        cur.execute(
                            f"INSERT INTO src_pos.payments ({', '.join(cols)}) VALUES ({placeholders})",
                            vals
                        )

        return jsonify({
            "ok": True,
            "receipt_id": rid,
            "totals": {
                "subtotal": subtotal,
                "line_discounts": line_discounts,
                "order_discount": order_discount,
                "tax_total": tax_total,
                "shipping_fee": shipping_fee,
                "grand_total": grand_total
            }
        }), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        put_conn(conn)

# ---------- VIEW SALES ----------
@app.get("/api/receipts")
def api_list_receipts():
    start = request.args.get("start")
    end = request.args.get("end")
    store_id = request.args.get("store_id")
    q = (request.args.get("q") or "").strip()
    limit = max(1, min(int(request.args.get("limit", 50)), 200))
    offset = max(0, int(request.args.get("offset", 0)))

    clauses, params = [], []
    if start:    clauses.append("r.sold_at >= %s"); params.append(start)
    if end:      clauses.append("r.sold_at < %s");  params.append(end)
    if store_id: clauses.append("r.store_id = %s"); params.append(store_id)
    if q:
        if table_has_col("src_pos", "receipts", "cashier_id"):
            clauses.append("(r.receipt_id ILIKE %s OR r.customer_id ILIKE %s OR r.cashier_id ILIKE %s)")
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        elif table_has_col("src_pos", "receipts", "cashier_code"):
            clauses.append("(r.receipt_id ILIKE %s OR r.customer_id ILIKE %s OR r.cashier_code ILIKE %s)")
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        else:
            clauses.append("(r.receipt_id ILIKE %s OR r.customer_id ILIKE %s)")
            params.extend([f"%{q}%", f"%{q}%"])

    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    has = {
        "cashier_id":  table_has_col("src_pos", "receipts", "cashier_id"),
        "cashier_code":table_has_col("src_pos", "receipts", "cashier_code"),
        "terminal_id": table_has_col("src_pos", "receipts", "terminal_id"),
        "currency":     table_has_col("src_pos", "receipts", "currency"),
        "subtotal":     table_has_col("src_pos", "receipts", "subtotal"),
        "discount_total": table_has_col("src_pos", "receipts", "discount_total"),
        "tax_total":    table_has_col("src_pos", "receipts", "tax_total"),
        "shipping_fee": table_has_col("src_pos", "receipts", "shipping_fee"),
        "grand_total":  table_has_col("src_pos", "receipts", "grand_total"),
    }

    sel_parts = [
        "r.receipt_id",
        "r.sold_at",
        "r.store_id",
        "s.name AS store_name",
        "r.customer_id",
        ("r.cashier_id"  if has["cashier_id"]  else "'' AS cashier_id"),
        ("r.cashier_code" if has["cashier_code"] else "'' AS cashier_code"),
        ("r.terminal_id" if has["terminal_id"] else "'' AS terminal_id"),
        ("r.currency"     if has["currency"]     else "'' AS currency"),
        ("r.subtotal"     if has["subtotal"]     else "0::numeric AS subtotal"),
        ("r.discount_total" if has["discount_total"] else "0::numeric AS discount_total"),
        ("r.tax_total"    if has["tax_total"]    else "0::numeric AS tax_total"),
        ("r.shipping_fee" if has["shipping_fee"] else "0::numeric AS shipping_fee"),
        ("r.grand_total"  if has["grand_total"]  else "0::numeric AS grand_total"),
    ]

    sql = f"""
      SELECT {', '.join(sel_parts)}
      FROM src_pos.receipts r
      LEFT JOIN src_pos.stores s ON s.store_id = r.store_id
      {where_sql}
      ORDER BY r.sold_at DESC
      LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            out = []
            for r in rows:
                out.append({
                    "receipt_id":     r[0],
                    "sold_at":        r[1].isoformat(),
                    "store_id":       r[2],
                    "store_name":     r[3],
                    "customer_id":    r[4],
                    "cashier_id":     r[5],
                    "cashier_code":   r[6],
                    "terminal_id":    r[7],
                    "currency":       r[8],
                    "subtotal":       float(r[9]),
                    "discount_total": float(r[10]),
                    "tax_total":      float(r[11]),
                    "shipping_fee":   float(r[12]),
                    "grand_total":    float(r[13]),
                })
            return jsonify(out)
    finally:
        put_conn(conn)

@app.get("/api/receipts/<receipt_id>/lines")
def api_receipt_lines(receipt_id):
    has = {
        "product_name": table_has_col("src_pos", "receipt_lines", "product_name"),
        "category":     table_has_col("src_pos", "receipt_lines", "category"),
        "sku":          table_has_col("src_pos", "receipt_lines", "sku"),
        "line_tax":     table_has_col("src_pos", "receipt_lines", "line_tax"),
        "line_total":   table_has_col("src_pos", "receipt_lines", "line_total"),
    }

    if any(has.values()):
        cols = [
            "l.product_id",
            ("l.sku"          if has["sku"]          else "p.sku"),
            ("l.product_name" if has["product_name"] else "p.name"),
            "l.qty", "l.unit_price", "l.line_discount",
            ("l.line_tax"     if has["line_tax"]     else "0::numeric AS line_tax"),
            ("l.line_total"   if has["line_total"]   else "(l.unit_price - COALESCE(l.line_discount,0))*l.qty AS line_total"),
        ]
        sql = f"""
          SELECT {', '.join(cols)}
          FROM src_pos.receipt_lines l
          LEFT JOIN src_pos.products p ON p.product_id = l.product_id
          WHERE l.receipt_id = %s
          ORDER BY 3
        """
    else:
        sql = """
          SELECT l.product_id, p.sku, p.name, l.qty, l.unit_price, l.line_discount,
                 0::numeric AS line_tax,
                 (l.unit_price - COALESCE(l.line_discount,0))*l.qty AS line_total
          FROM src_pos.receipt_lines l
          LEFT JOIN src_pos.products p ON p.product_id = l.product_id
          WHERE l.receipt_id = %s
          ORDER BY p.name
        """

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (receipt_id,))
            rows = cur.fetchall()
            lines = []
            for r in rows:
                lines.append({
                    "product_id":   r[0],
                    "sku":          r[1],
                    "name":         r[2],
                    "qty":          int(r[3]),
                    "unit_price":   float(r[4]),
                    "line_discount":float(r[5] or 0),
                    "line_tax":     float(r[6]),
                    "line_total":   float(r[7]),
                })
            return jsonify(lines)
    finally:
        put_conn(conn)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)
