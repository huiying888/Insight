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
    model="gemma-3-27b-it", 
    google_api_key=os.getenv("GEMINI_API_KEY"),  # make sure this matches your .env variable name
    temperature=0.1
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
    model="gemma-3-27b-it", 
    google_api_key=os.getenv("GEMINI_API_KEY"),  # make sure this matches your .env variable name
    temperature=0.1
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
