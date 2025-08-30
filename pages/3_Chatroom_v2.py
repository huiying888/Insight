import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
from langchain_core.messages import AIMessage, HumanMessage
from utils import get_db_connection, load_data, init_supabase, load_schema_from_file, get_response

load_dotenv()

st.set_page_config(page_title="Chat with Supabase", page_icon="🦙", layout="wide")
st.title("💬 Chat with your AI Assistant")

st.markdown(
    """
    <style>
    .chat-container {
        max-width: 600px;
        margin: auto;
        font-family: Arial, sans-serif;
    }
    .message {
        display: inline-block;
        padding: 10px 15px;
        border-radius: 20px;
        margin: 8px;
        word-wrap: break-word;
        max-width: 70%;  /* bubble width adapts to text */
    }
    .user {
        background-color: #dcf8c6;
        margin-left: auto;
        float: right;
        text-align: right;
    }
    .assistant {
        background-color: #ffffff;
        border: 1px solid #ddd;
        margin-right: auto;
        text-align: left;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------------------
# Initialize chat + schema
# ------------------------------------------------------------------
if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(content="Hi there 👋 I’m your AI assistant. Ask me anything about your database and I’ll help you uncover business insights."),
    ]

if "db" not in st.session_state:
    st.session_state.db = init_supabase()
if "schema_info" not in st.session_state:
    st.session_state.schema_info = load_schema_from_file("config/schema.sql")
if "pending_ai" not in st.session_state:
    st.session_state["pending_ai"] = False

# ------------------------------------------------------------------
# FAQ section 
# ------------------------------------------------------------------
st.write("### ❓ Frequently Asked Questions")

faq_items = [
    {"q": "💰 What is the total revenue this quarter?"},
    {"q": "📈 What is the monthly sales trend this year?"},
    {"q": "🏬 Which customer region had the highest revenue this month?"},
    {"q": "⚡ What is the average order value last month?"},
]

cols = st.columns(len(faq_items))

button_style = """
    <style>
    div[data-testid="stButton"] button {
        border-radius: 12px;
        background-color: #f5f5f5;
        color: #333;
        border: 1px solid #ddd;
        padding: 0.6em 1em;
        font-weight: 500;
        transition: 0.2s;
    }
    div[data-testid="stButton"] button:hover {
        background-color: #e0f0ff;
        border-color: #3399ff;
        color: #000;
    }
    </style>
"""
st.markdown(button_style, unsafe_allow_html=True)

for i, item in enumerate(faq_items):
    with cols[i]:
        if st.button(item["q"], key=f"faq_{i}"):
            st.session_state.chat_history.append(HumanMessage(content=item["q"]))
            st.session_state["pending_ai"] = True
            st.session_state["last_query"] = item["q"]
            st.rerun()


# ------------------------------------------------------------------
# Display chat with bubbles
# ------------------------------------------------------------------
st.markdown('<div class="chat-container">', unsafe_allow_html=True)
for message in st.session_state.chat_history:
    if isinstance(message, HumanMessage):
        role_class = "user"
    else:
        role_class = "assistant"
    st.markdown(
        f'<div class="message {role_class}">{message.content}</div>',
        unsafe_allow_html=True,
    )
st.markdown("</div>", unsafe_allow_html=True)

# ------------------------------------------------------------------
# Input box
# ------------------------------------------------------------------
user_query = st.chat_input("Ask me something about your Supabase database...")
if user_query:
    st.session_state.chat_history.append(HumanMessage(content=user_query))
    st.session_state["pending_ai"] = True
    st.session_state["last_query"] = user_query
    st.rerun()

# ------------------------------------------------------------------
# AI response handling
# ------------------------------------------------------------------
if st.session_state["pending_ai"]:
    last_query = st.session_state.get("last_query", None)
    if last_query:
        with st.spinner("Thinking..."):
            response = get_response(last_query)
        st.session_state.chat_history.append(AIMessage(content=response))
        st.session_state["pending_ai"] = False
        st.rerun()
