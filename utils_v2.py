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
import google.generativeai as genai
import json

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

# ============================================================================
# LOAD SCHEMA FROM JSON
# ============================================================================
def load_schema_from_file(path="config/schema.json") -> dict:
    with open(path, "r") as f:
        return json.load(f)

# ============================================================================
# SQL GENERATION CHAIN
# ============================================================================
def generate_sql(user_question: str, schema_info: dict) -> str:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel("gemini-2.5-flash")

    prompt = f"""
    You are a data analyst. 
    Here is the database schema:
    {schema_info}

    ⚠️ IMPORTANT:
    - Always use schema-qualified table names in the format wh.table_name
    - When calculating revenue, ALWAYS use the column 'order_total_gross' from 'wh.fact_orders'. 
      Do NOT use revenue_net, order_total_net, or join to fact_order_items.
    - Use PostgreSQL syntax
    - Return only the SQL query, no markdown, no explanation

    User question: {user_question}
    """

    response = model.generate_content(prompt)
    sql_query = response.text.strip()

    if sql_query.startswith("```"):
        sql_query = sql_query.strip("`")
        sql_query = sql_query.replace("sql\n", "")
        sql_query = sql_query.replace("sql", "")
        sql_query = sql_query.replace("```", "")

    return sql_query.strip()


# ============================================================================
# NATURAL LANGUAGE RESPONSE CHAIN
# ============================================================================
def summarize_result(result, sql_query, user_question: str) -> str:
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel("gemini-2.5-flash")
    summary_prompt = f"""
    The SQL query returned: {result}
    User question: {user_question}
    SQL query executed: {sql_query}

    You are a helpful data analyst. Based on the results, user question, and SQL query executed,
    write a clear and concise natural language answer and suggest business insights if applicable.
    
    - Use MR as currency unit if relevant.
    - Keep it concise, structured, and easy to understand.
    """
    answer = model.generate_content(summary_prompt)
    return answer.text.strip()


# ============================================================================
# MAIN FLOW (replacement for get_response)
# ============================================================================
def get_response(user_question: str):
    schema_info = load_schema_from_file()
    sql_query = generate_sql(user_question, schema_info)
    print("Generated SQL:", sql_query)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql_query)
    result = cursor.fetchall()
    print("SQL Result:", result)
    final_answer = summarize_result(result, sql_query, user_question)
    print("Final Answer:", final_answer)
    return final_answer

def generate_insight(user_query: str, df: pd.DataFrame):
    template = """
        You are a business data analyst.
        Respond in 1–3 concise and consistent bullet points only.
        - Start each bullet with a bolded short label.
        - Give key business insights directly (no intro phrases).
        - Suggest an action if useful.
        No code or technical details.

        Question: {question}
        Data:
        {data}
    """

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatGoogleGenerativeAI(
        model="gemma-3-27b-it",
        google_api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0.1
    )

    chain = (
        prompt
        | llm
        | StrOutputParser()
    )

    return chain.invoke({
        "question": user_query,
        "data": df.to_string(index=False) 
    })