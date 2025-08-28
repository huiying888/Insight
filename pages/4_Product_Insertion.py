import streamlit as st
import pandas as pd
import psycopg2
import os
from mapping import seed_master_products, get_db_connection   

st.set_page_config(page_title="Upload Master Products", page_icon="📦", layout="wide")
st.title("📦 Upload Master Products")

uploaded_file = st.file_uploader("Upload CSV/Excel file", type=["csv", "xlsx"])

if uploaded_file is not None:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.write("✅ Preview of uploaded file:")
    st.dataframe(df)

    # Ensure required columns exist
    required_cols = ["master_product_code", "name", "category", "brand", "starting_inventory"]
    if not all(col in df.columns for col in required_cols):
        st.error(f"Uploaded file must contain columns: {required_cols}")
    else:
        conn = get_db_connection()
        existing_codes = pd.read_sql("SELECT master_product_code FROM wh.dim_product", conn)["master_product_code"].tolist()

        # Mark existing vs new
        df["exists_in_db"] = df["master_product_code"].isin(existing_codes)

        already = df[df["exists_in_db"] == True]
        new = df[df["exists_in_db"] == False]

        if not already.empty:
            st.warning("⚠️ These master_product_codes already exist in DB:")
            st.write(", ".join(already["master_product_code"].astype(str).tolist()))

        if not new.empty:
            st.success("✅ These will be inserted as new products:")
            st.write(", ".join(new["master_product_code"].astype(str).tolist()))

        if st.button("Insert into Database"):
            try:
                MASTER_PRODUCT_SEED = [
                    (r["master_product_code"], r["name"], r["category"], r["brand"], r["starting_inventory"], pd.Timestamp.now())
                    for _, r in new.iterrows()  # only insert new ones
                ]

                with conn.cursor() as cur:
                    cur.execute("SELECT current_database();")
                    print("Connected to DB:", cur.fetchone())

                if MASTER_PRODUCT_SEED:  # insert only if new rows exist
                    seed_master_products(MASTER_PRODUCT_SEED, conn)

                conn.close()
                st.success("🎉 Products inserted/updated successfully!")

            except Exception as e:
                st.error(f"❌ Error inserting products: {e}")
