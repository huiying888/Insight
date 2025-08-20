import streamlit as st
import ollama

st.set_page_config(page_title="Chatroom", layout="wide")

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


st.title("💬 WhatsApp-style Chatroom")

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "pending_ai" not in st.session_state:
    st.session_state["pending_ai"] = False

# Chat display
st.markdown('<div class="chat-container">', unsafe_allow_html=True)
for msg in st.session_state["messages"]:
    role_class = "user" if msg["role"] == "user" else "assistant"
    st.markdown(
        f'<div class="message {role_class}">{msg["content"]}</div>',
        unsafe_allow_html=True,
    )
st.markdown("</div>", unsafe_allow_html=True)

# Chat input
if prompt := st.chat_input("Type your message..."):
    st.session_state["messages"].append({"role": "user", "content": prompt})
    st.session_state["pending_ai"] = True
    st.rerun()

if st.session_state["pending_ai"]:
    with st.spinner("Thinking..."):
        response = ollama.chat(
            model="llama3.2:1b",  
            messages=st.session_state["messages"],
        )
        reply = response["message"]["content"]

    st.session_state["messages"].append({"role": "assistant", "content": reply})
    st.session_state["pending_ai"] = False
    st.rerun()

