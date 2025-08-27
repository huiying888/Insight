import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
from utils import get_db_connection, load_data, generate_insight

st.set_page_config(page_title="KPI Dashboard", page_icon="🦙", layout="wide")
st.title("📊 Business KPI Dashboard")
st.markdown("KPIs across **Shopee, Lazada, TikTok, POS** from warehouse DB.")

# -----------------------------
# DATE FILTER
# -----------------------------
date_range = load_data("SELECT MIN(date_key) as min_date, MAX(date_key) as max_date FROM wh.dim_date;")
min_date, max_date = pd.to_datetime(date_range.iloc[0]['min_date']).date(), pd.to_datetime(date_range.iloc[0]['max_date']).date()

start_date, end_date = st.date_input(
    "📅 Select Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

date_params = {"start_date": start_date, "end_date": end_date}

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

revenue_df = load_data("""
    SELECT SUM(order_total_gross) as revenue 
    FROM wh.fact_orders o
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s;
""", date_params)
col1.metric("Total Revenue", f"RM{revenue_df['revenue'][0]:,.2f}")

orders_df = load_data("""
    SELECT COUNT(*) as orders 
    FROM wh.fact_orders o
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s;
""", date_params)
col2.metric("Total Orders", f"{orders_df['orders'][0]:,}")

customers_df = load_data("""
    SELECT COUNT(DISTINCT customer_sk) as customers 
    FROM wh.fact_orders o
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s;
""", date_params)
col3.metric("Customers", f"{customers_df['customers'][0]:,}")

aov_df = load_data("""
    SELECT SUM(order_total_gross)/COUNT(*) as avg_order
    FROM wh.fact_orders o
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s;
""", date_params)
col4.metric("Avg Order Value", f"RM{aov_df['avg_order'][0]:,.2f}")

# -----------------------------
# INSIGHT BOX FRAME
# -----------------------------
import re

def storytelling_box(content: str, color="#BCE5BE", bgcolor="#E8FFE8"):
    # Replace markdown bold with HTML bold
    safe_content = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", content)

    st.markdown(f"""
        <div style="
            border: 2px solid {color};
            border-radius: 10px;
            padding: 12px;
            background-color: {bgcolor};">
            <strong>🤖 Storytelling</strong>
            <ul>
                {''.join(f"<li>{line.strip('* ').strip()}</li>" for line in safe_content.splitlines() if line.strip())}
            </ul>
        </div>
    """, unsafe_allow_html=True)


# -----------------------------
# REVENUE TREND
# -----------------------------
st.subheader("📈 Revenue Trend")
trend_df = load_data("""
    SELECT order_ts::date as order_date, SUM(order_total_gross) as revenue
    FROM wh.fact_orders
    WHERE order_ts::date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY order_date
    ORDER BY order_date;
""", date_params)
col_chart, col_memo = st.columns([3, 1])
with col_chart:
    fig = px.line(trend_df, x="order_date", y="revenue", title="Revenue Over Time")
    st.plotly_chart(fig, use_container_width=True)
with col_memo:
    storytelling_box(generate_insight("Summarize revenue trend briefly.", trend_df))


# -----------------------------
# REVENUE BY CHANNEL
# -----------------------------
st.subheader("🏪 Revenue by Channel")
channel_df = load_data("""
    SELECT c.name as channel, SUM(o.order_total_gross) as revenue
    FROM wh.fact_orders o
    JOIN wh.dim_channel c ON o.channel_id = c.channel_id
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY c.name;
""", date_params)
col_chart, col_memo = st.columns([3, 1])
with col_chart:
    fig = px.pie(channel_df, names="channel", values="revenue", title="Revenue Share by Channel")
    st.plotly_chart(fig, use_container_width=True)
with col_memo:
    storytelling_box(generate_insight("Summarize revenue by channel briefly.", channel_df))


# -----------------------------
# TOP PRODUCTS
# -----------------------------
st.subheader("🔥 Top Products by Revenue")
top_products = load_data("""
    SELECT 
        p.name AS product, 
        SUM(oi.revenue_net) AS revenue
    FROM wh.fact_order_items oi
    JOIN wh.dim_product p ON oi.product_sk = p.product_sk
    JOIN wh.fact_orders o ON oi.order_sk = o.order_sk
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY p.name
    ORDER BY revenue DESC
    LIMIT 10;
""", date_params)
col_chart, col_memo = st.columns([3, 1])
with col_chart:
    fig = px.bar(top_products, x="revenue", y="product", orientation="h", title="Top 10 Products")
    st.plotly_chart(fig, use_container_width=True)
with col_memo:
    storytelling_box(generate_insight("Summarize top products by revenue briefly.", top_products))


# -----------------------------
# DAILY SALES PER PRODUCT
# -----------------------------
st.subheader("📊 Daily Sales Amount per Product")

daily_sales = load_data("""
    SELECT 
        o.order_ts::date AS order_date,
        p.name AS product,
        SUM(oi.revenue_net) AS daily_sales
    FROM wh.fact_order_items oi
    JOIN wh.fact_orders o ON oi.order_sk = o.order_sk
    JOIN wh.dim_product p ON oi.product_sk = p.product_sk
    WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY o.order_ts::date, p.name
    ORDER BY o.order_ts::date, daily_sales DESC;
""", date_params)

col_chart, col_memo = st.columns([3, 1])
with col_chart:
    if not daily_sales.empty:
        fig = px.line(
        daily_sales,
        x="order_date",
        y="daily_sales",
        color="product",
        title="Daily Sales Amount per Product"
        )

        # Force x-axis to be daily ticks
        fig.update_xaxes(
            dtick="D1",           # one tick per day
            tickformat="%Y-%m-%d" # show as YYYY-MM-DD
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No sales data available for the selected date range.")
with col_memo:
    storytelling_box(generate_insight("Summarize daily sales per product briefly.", daily_sales))


# -----------------------------
# INVENTORY HEALTH
# -----------------------------
st.subheader("📦 Inventory Health")
inv_df = load_data("""
    SELECT p.name as product, i.stock_qty
    FROM wh.fact_inventory i
    JOIN wh.dim_product p ON i.product_sk = p.product_sk
    WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM wh.fact_inventory)
    ORDER BY i.stock_qty ASC
    LIMIT 15;
""")
col_chart, col_memo = st.columns([3, 1])
with col_chart:
    st.dataframe(inv_df)
with col_memo:
    storytelling_box(generate_insight("Summarize inventory health briefly.", inv_df))

