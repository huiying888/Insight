import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
from utils import get_db_connection, load_data, generate_insight
import requests, json
from datetime import timedelta

st.set_page_config(page_title="KPI Dashboard", page_icon="🦙", layout="wide")
st.title("📊 Business KPI Dashboard")
st.markdown("KPIs across **Shopee, Lazada, TikTok, POS** from warehouse DB.")

from datetime import timedelta

# -----------------------------
# DATE FILTER
# -----------------------------
date_range = load_data("SELECT MIN(date_key) as min_date, MAX(date_key) as max_date FROM wh.dim_date;")
min_date, max_date = pd.to_datetime(date_range.iloc[0]['min_date']).date(), pd.to_datetime(date_range.iloc[0]['max_date']).date()

default_start = max(min_date, max_date - timedelta(days=30))
default_end = max_date

start_date, end_date = st.date_input(
    "📅 Select Date Range",
    value=(default_start, default_end),  
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
# ORGANIZED LAYOUT WITH TABS
# -----------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Total Revenue", 
    "🏪 Channels", 
    "🔥 Products", 
    "📦 Inventory",
    "👥 Customers"
])

# -----------------------------
# STORYTELLING BOX FRAME
# -----------------------------
import re

def storytelling_box(content: str, color="#BDCDD6", bgcolor="#D7E3E6"):
    # Replace markdown bold with HTML bold
    safe_content = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", content)

    st.markdown(f"""
        <div style="
            border: 2px solid #DAD2FF;
            border-radius: 18px;
            padding: 12px;
            background: radial-gradient(circle at top left, #FFF8D6, #EDEAFF, #D4CCFF);
            color: #2B2B2B;
            font-family: 'Segoe UI', sans-serif;
            box-shadow: 0 6px 14px rgba(0, 0, 0, 0.12);
            transition: all 0.3s ease;
        "
            onmouseover="this.style.boxShadow='0 8px 18px rgba(0,0,0,0.18)'; this.style.transform='scale(1.03)';"
            onmouseout="this.style.boxShadow='0 6px 14px rgba(0,0,0,0.12)'; this.style.transform='scale(1)';"
        >
            <div style="font-size: 20px; font-weight: bold; margin-bottom: 12px; color:#493D9E;">
                🤖 Storytelling
            </div>
            <ul style="padding-left: 22px; line-height: 1.6; list-style-type: '✨ ';">
                {''.join(f"<li style='margin-bottom: 8px; color:#2B2B2B;'>{line.strip('* ').strip()}</li>" for line in safe_content.splitlines() if line.strip())}
            </ul>
        </div>
    """, unsafe_allow_html=True)


# -----------------------------
# REVENUE TREND
# -----------------------------
with tab1:
    st.subheader("📈 Total Revenue Trend")
    trend_df = load_data("""
        SELECT order_ts::date as order_date, SUM(order_total_gross) as revenue
        FROM wh.fact_orders
        WHERE order_ts::date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY order_date
        ORDER BY order_date;
    """, date_params)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not trend_df.empty:
            fig = px.line(trend_df, x="order_date", y="revenue", title="Revenue Over Time", color_discrete_sequence=["#B2A5FF"])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize revenue trend briefly.", trend_df))


# -----------------------------
# REVENUE BY CHANNEL
# -----------------------------
with tab2:
    st.subheader("🏪 Revenue by Channel")
    channel_df = load_data("""
        SELECT c.name as channel, SUM(o.order_total_gross) as revenue
        FROM wh.fact_orders o
        JOIN wh.dim_channel c ON o.channel_id = c.channel_id
        WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY c.name;
    """, date_params)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not channel_df.empty:
            fig = px.pie(channel_df, names="channel", values="revenue", title="Revenue Share by Channel", color_discrete_sequence=px.colors.qualitative.Pastel1)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No channel revenue data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize revenue by channel briefly.", channel_df))

    st.subheader("📈 Revenue Trend by Channel")
    trend_df = load_data("""
        SELECT o.order_ts::date AS order_date, c.name AS channel, SUM(o.order_total_gross) AS revenue
        FROM wh.fact_orders o
        JOIN wh.dim_channel c ON o.channel_id = c.channel_id
        WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY o.order_ts::date, c.name
        ORDER BY order_date;
    """, date_params)
    all_dates = pd.date_range(trend_df["order_date"].min(), trend_df["order_date"].max(), freq="D")
    all_channels = trend_df["channel"].unique()
    full_index = pd.MultiIndex.from_product([all_dates, all_channels], names=["order_date", "channel"])
    trend_df = trend_df.set_index(["order_date", "channel"]).reindex(full_index, fill_value=0).reset_index()
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not trend_df.empty:
            fig = px.line(trend_df, x="order_date", y="revenue", color="channel", title="Daily Revenue Trend by Channel", markers=True, color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_xaxes(
                dtick="D1",  
                tickformat="%Y-%m-%d",
                tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue trend data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize revenue trend by channel briefly.", trend_df))

# -----------------------------
# PRODUCTS BY REVENUE
# -----------------------------
with tab3:
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
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not top_products.empty:
            fig = px.bar(top_products, x="revenue", y="product", orientation="h", title="Top 10 Products", color="product", color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No product revenue data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize top products by revenue briefly.", top_products))

    st.subheader("📊 Daily Sales Amount per Category")
    daily_sales = load_data("""
        SELECT 
            o.order_ts::date AS order_date,
            p.category AS category,   -- 👈 change here
            SUM(oi.revenue_net) AS daily_sales
        FROM wh.fact_order_items oi
        JOIN wh.fact_orders o ON oi.order_sk = o.order_sk
        JOIN wh.dim_product p ON oi.product_sk = p.product_sk
        WHERE o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY o.order_ts::date, p.category
        ORDER BY o.order_ts::date, daily_sales DESC;
    """, date_params)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not daily_sales.empty:
            fig = px.line(daily_sales, x="order_date", y="daily_sales", color="category",   title="Daily Sales Amount per Category", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_xaxes(
                dtick="D1",           
                tickformat="%Y-%m-%d"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sales data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize daily sales per category briefly.", daily_sales))

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
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not daily_sales.empty:
            fig = px.line(daily_sales, x="order_date", y="daily_sales", color="product", title="Daily Sales Amount per Product", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_xaxes(
                dtick="D1",           
                tickformat="%Y-%m-%d" 
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No sales data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize daily sales per product briefly.", daily_sales))
    
    

# -----------------------------
# INVENTORY HEALTH
# -----------------------------
with tab4:
    st.subheader("📦 Inventory Health")
    inv_df = load_data("""
        SELECT p.master_product_code, p.name as product, i.stock_qty
        FROM wh.fact_inventory i
        JOIN wh.dim_product p ON i.product_sk = p.product_sk
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM wh.fact_inventory)
        ORDER BY i.stock_qty ASC;
    """)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not inv_df.empty:
            st.dataframe(inv_df)
        else:
            st.info("No inventory data available.")
    with col_memo:
        storytelling_box(generate_insight("Summarize inventory health briefly.", inv_df))

    st.subheader("📦 Inventory by Category")
    inventory_df = load_data("""
        SELECT p.category AS category,
        SUM(i.stock_qty) AS stock_qty
        FROM wh.fact_inventory i
        JOIN wh.dim_product p ON i.product_sk = p.product_sk
        WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM wh.fact_inventory)
        GROUP BY p.category
        ORDER BY stock_qty DESC;
    """)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not inventory_df.empty:
            fig = px.bar(inventory_df, x="category", y="stock_qty", title="Current Inventory by Category", text_auto=True, color="category", color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(xaxis_title="Category", yaxis_title="Quantity")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No inventory category data available.")
    with col_memo:
        storytelling_box(
            generate_insight("Summarize inventory distribution by category briefly.", inventory_df)
        )

# -----------------------------
# CUSTOMER SEGMENT BY STATE 
# -----------------------------
with tab5:
    st.subheader("🌏 Customer Segment by State")
    region_sales = load_data("""
        SELECT
            COALESCE(c.region, 'Unknown') AS region,
            SUM(o.order_total_net) AS total_revenue,
            COUNT(DISTINCT c.source_customer_id) AS total_customers
        FROM wh.dim_customer c
        LEFT JOIN wh.fact_orders o
        ON c.customer_sk = o.customer_sk
        AND o.order_ts::date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY c.region
    """, date_params)

    region_sales['region'] = region_sales['region'].replace({None: 'Unknown', '': 'Unknown'})
    region_sales = region_sales[region_sales['region'] != 'Unknown']
    with open("assets/malaysia_states.geo.json", "r", encoding="utf-8") as f:
        geojson = json.load(f)
    all_regions = [feature['properties']['name'] for feature in geojson['features']]
    all_regions_df = pd.DataFrame({'region': all_regions})
    region_sales_full = all_regions_df.merge(region_sales, on='region', how='left')
    region_sales_full['total_revenue'] = region_sales_full['total_revenue'].fillna(0)
    region_sales_full['total_customers'] = region_sales_full['total_customers'].fillna(0)
    col_chart, col_memo = st.columns([2, 1])
    with col_chart:
        if not region_sales_full.empty:
            fig = px.choropleth(
                region_sales_full,
                geojson=geojson,
                locations='region',                 
                featureidkey='properties.name',  
                color='total_revenue',
                hover_data=['total_customers','total_revenue'],
                color_continuous_scale="Blues",
                title='Total Revenue by State',

            )
            fig.update_layout(
                margin={"r":0,"t":50,"l":0,"b":0},  
                height=600,
                paper_bgcolor="#FFFDF7",  
                plot_bgcolor="#FFFDF7"                     
            )
            fig.update_geos(
                fitbounds="locations",
                visible=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No regional sales data available for the selected date range.")
    with col_memo:
        storytelling_box(generate_insight("Summarize sales by state briefly.", region_sales_full))

