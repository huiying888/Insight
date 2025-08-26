import os
import urllib.parse
from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI
import plotly.express as px
import streamlit as st
import pandas as pd
import psycopg2
from langchain_community.chat_models import ChatOllama
load_dotenv("config/.env") 

# ============================================================================
# INIT SUPABASE DATABASE CONNECTION
# ============================================================================
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def load_data(query, params=None):
    conn = get_db_connection()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def init_supabase() -> SQLDatabase:
    """
    Initialize Supabase connection using environment variables and get_db_connection.
    """
    user = os.getenv("DB_USER")
    password = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    database = os.getenv("DB_NAME")
    db_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return SQLDatabase.from_uri(db_uri)

def load_schema_from_file(path="config/schema.sql") -> str:
    with open(path, "r") as f:
        return f.read()
# ============================================================================
# SQL GENERATION CHAIN
# ============================================================================
def get_sql_chain(db: SQLDatabase, schema: str):
    template = """
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the table schema below, write a SQL query that would answer the user's question. 
    Always use schema-qualified table names in the format schema.table (e.g., wh.dim_product, wh.fact_orders). Never prepend extra letters or aliases to the schema name.
    Take the conversation history into account.
    
    
    <SCHEMA>{schema}</SCHEMA>
    
    Conversation History: {chat_history}
    
    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks.
    No explanation. No formatting. No backticks.

    For example:
    Question: how many products do I have?
    SQL Query: SELECT "ArtistId", COUNT(*) as track_count FROM "Track" GROUP BY "ArtistId" ORDER BY track_count DESC LIMIT 3;
    
    Question: Name 10 artists
    SQL Query: SELECT "Name" FROM "Artist" LIMIT 10;
    
    Your turn:
    Question: {question}
    SQL Query:
    """

    prompt = ChatPromptTemplate.from_template(template)

    # Use LLaMA 3.2 via OpenAI-compatible endpoint (set in .env as OPENAI_API_KEY + BASE_URL if needed)
    llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash", 
    google_api_key=os.getenv("GEMINI_API_KEY"),  # make sure this matches your .env variable name
    temperature=0
)

    return (
        RunnablePassthrough.assign(schema=lambda _: schema)
        | prompt
        | llm
        | StrOutputParser()
    )

# ============================================================================
# NATURAL LANGUAGE RESPONSE CHAIN
# ============================================================================
def get_response(user_query: str, db: SQLDatabase, schema: str, chat_history: list):
    sql_chain = get_sql_chain(db, schema)

    template = """
    You are a helpful data analyst. Based on the schema, user question, SQL query, and SQL response,
    write a clear and concise natural language answer.

    <SCHEMA>{schema}</SCHEMA>
    Conversation History: {chat_history}
    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response: {response}
    """

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash", 
    google_api_key=os.getenv("GEMINI_API_KEY"),  # make sure this matches your .env variable name
    temperature=0
)
    chain = (
        RunnablePassthrough.assign(query=sql_chain).assign(
            schema=lambda _: schema,
            response=lambda vars: db.run(vars["query"]),
        )
        | prompt
        | llm
        | StrOutputParser()
    )
    return chain.invoke({
        "question": user_query,
        "chat_history": chat_history,
    })

# ============================================================================
# STREAMLIT APP
# ============================================================================
# ==========================================================
# PAGE CONFIG
# ==========================================================
st.set_page_config(page_title="AI + KPI Dashboard", layout="wide")

# st.title("🤖 Chat & 📊 Business KPI Dashboard")

# ==========================================================
# SIDEBAR MENU
# ==========================================================
page = st.sidebar.radio(
    "Choose View:",
    ["📊 KPI Dashboard","💬 Chat with AI" ]
)


# ==========================================================
# DASHBOARD MODE
# ==========================================================
if page == "📊 KPI Dashboard":
    st.header("📊 Business KPI Dashboard")
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
    fig = px.line(trend_df, x="order_date", y="revenue", title="Revenue Over Time")
    st.plotly_chart(fig, use_container_width=True)

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
    fig = px.pie(channel_df, names="channel", values="revenue", title="Revenue Share by Channel")
    st.plotly_chart(fig, use_container_width=True)

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
    fig = px.bar(top_products, x="revenue", y="product", orientation="h", title="Top 10 Products")
    st.plotly_chart(fig, use_container_width=True)

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
    st.dataframe(inv_df)


elif page == "💬 Chat with AI":
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            AIMessage(content="Hello! I'm a SQL assistant connected to Supabase. Ask me anything about your database."),
        ]

    if "db" not in st.session_state:
        st.session_state.db = init_supabase()
    if "schema_info" not in st.session_state:
        st.session_state.schema_info = load_schema_from_file("config/schema.sql")

    # # --- DEBUG: Print schema structure ---
    # st.subheader("Database Schema (for debugging)")
    # st.code(st.session_state.schema_info, language="sql")

    st.set_page_config(page_title="Chat with Supabase", page_icon="🦙")

    


    # Show chat history
    for message in st.session_state.chat_history:
        if isinstance(message, AIMessage):
            with st.chat_message("AI"):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message("Human"):
                st.markdown(message.content)

    # Handle new user input
    user_query = st.chat_input("Ask me something about your Supabase database...")
    if user_query and "db" in st.session_state:
        st.session_state.chat_history.append(HumanMessage(content=user_query))

        with st.chat_message("Human"):
            st.markdown(user_query)

        with st.chat_message("AI"):
            response = get_response(user_query, st.session_state.db, st.session_state.schema_info,st.session_state.chat_history)
            st.markdown(response)

        st.session_state.chat_history.append(AIMessage(content=response))
