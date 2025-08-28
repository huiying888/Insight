import streamlit as st
import pandas as pd
from collections import Counter
from mapping import get_db_connection, fetchall_dict  # use your helpers

st.set_page_config(page_title="Product Overview", layout="wide", page_icon="🛒")
st.title("🛒 Product Overview")

def fetch_product_channel_summary():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_sk, master_product_code, name, category, brand FROM wh.dim_product;")
            products = fetchall_dict(cur)
            cur.execute("""
                SELECT product_sk, source_channel
                FROM wh.bridge_product_source;
            """)
            bridges = fetchall_dict(cur)

        prod_map = {p["product_sk"]: { "master_product_code": p["master_product_code"],
                                       "name": p["name"],
                                       "category": p["category"],
                                       "brand": p["brand"],
                                       "channels": [] } for p in products}

        for b in bridges:
            if b["product_sk"] in prod_map:
                prod_map[b["product_sk"]]["channels"].append(b["source_channel"])

        df = pd.DataFrame(prod_map.values())
        return df
    finally:
        conn.close()

# -----------------------
# Compute summary
# -----------------------
df_products = fetch_product_channel_summary()
total_products = len(df_products)
all_channels = [ch for sublist in df_products["channels"] for ch in sublist]
total_channels_used = len(set(all_channels))
products_by_channel = Counter(all_channels)

# -----------------------
# Display KPIs
# -----------------------
col1, col2 = st.columns(2)
col1.metric("Total Products", total_products)
col2.metric("Channels Used", total_channels_used)

# Products by channel table
st.subheader("Products by Channel")
prod_channel_df = pd.DataFrame(products_by_channel.items(), columns=["Channel", "Product Count"])
st.dataframe(prod_channel_df)

# -----------------------
# Detailed table
# -----------------------
st.subheader("All Products with Channels")
df_products["channels"] = df_products["channels"].apply(lambda x: ", ".join(x))
st.dataframe(df_products, use_container_width=True)
