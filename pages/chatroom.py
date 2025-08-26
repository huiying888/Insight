import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
from langchain_core.messages import AIMessage, HumanMessage
from utils import get_db_connection, load_data, init_supabase, load_schema_from_file, get_response
load_dotenv()

st.set_page_config(page_title="Chat with Supabase", page_icon="🦙")
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

# Initialize chat + schema
if "chat_history" not in st.session_state:
    st.session_state.chat_history = [
        AIMessage(content="Hello! I'm a SQL assistant connected to Supabase. Ask me anything about your database."),
    ]

if "db" not in st.session_state:
    st.session_state.db = init_supabase()
if "schema_info" not in st.session_state:
    st.session_state.schema_info = load_schema_from_file("config/schema.sql")
if "pending_ai" not in st.session_state:
    st.session_state["pending_ai"] = False

# --- Display chat with bubbles ---
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

# --- Input box ---
user_query = st.chat_input("Ask me something about your Supabase database...")
if user_query:
    # Add user bubble
    st.session_state.chat_history.append(HumanMessage(content=user_query))
    st.session_state["pending_ai"] = True
    st.rerun()

if st.session_state["pending_ai"]:
    # Get AI response
    with st.spinner("Thinking..."):
        response = get_response(
            user_query,
            st.session_state.db,
            st.session_state.schema_info,
            st.session_state.chat_history
        )

    # Add AI bubble
    st.session_state.chat_history.append(AIMessage(content=response))
    st.session_state["pending_ai"] = False
    st.rerun()
