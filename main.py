import streamlit as st

# ---- Page Config ----
st.set_page_config(
    page_title="Insight by LAICHAI",
    page_icon="🚀",
    layout="wide",
)

st.markdown(
    """
    <style>
    .hero {
        background: linear-gradient(135deg, #DAD2FF, #B2A5FF, #FFF2AF);
        color: white;
        padding: 50px;
        border-radius: 20px;
        margin-bottom: 20px;
        text-align: center;
    }
    .hero h1 {
        font-size: 2.5em;
        margin-bottom: 10px;
    }
    .hero p {
        font-size: 1.2em;
        margin-top: 0;
    }
    .hero {
    animation: fadeIn 1.5s ease-in-out;
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    </style>

    <div class="hero">
        <h1>🚀 INSIGHT </h1>
        <p>Transforming business data into insights, clarity, and growth.</p>
    </div>

    <style>
    /* Center everything */
    .main {
        text-align: center;
    }
    /* Hero title */
    h1 {
        font-size: 3em !important;
        color: #2E86C1;
    }
    /* Subheader */
    .subtitle {
        font-size: 1.5em;
        color: #555;
        margin-bottom: 20px;
    }
    /* Card container */
    .card {
        background-color: #f9f9f9;
        border-radius: 15px;
        padding: 25px;
        margin: 10px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }
    .card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 15px rgba(0,0,0,0.15);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ---- Feature Highlights ----
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
        <div class="card">
            <h3>📊 Cross-Platform View</h3>
            <p>We seamlessly integrate <b>Lazada</b>, <b>Shopee</b>, <b>TikTok Shop</b>, and <b>POS sales</b> into one single source of truth.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
        <div class="card">
            <h3>📈 Live Monitor</h3>
            <p>Monitor your business performance instantly with <b>real-time updates</b> and <b>interactive visualizations</b>.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        """
        <div class="card">
            <h3>🤖 Smart AI Chatbot</h3>
            <p>Ask questions and get <b>actionable insights</b> from your sales data powered by <b>AI-driven analytics</b>.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


st.divider()


st.markdown("<h3 style='text-align:center; color:#493D9E; margin-bottom:20px;'>🚩 Quick Navigation</h2>", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

def gradient_button(label, href, description):
    color_start = "#FFF2AF"
    color_end = "#DAD2FF"
    hover_start = "#DAD2FF"
    hover_end = "#B2A5FF"
    text_color = "#333"
    
    button_html = f"""
    <div style='text-align:center;'>
        <a href='{href}' style='
            background: linear-gradient(135deg, {color_start}, {color_end});
            padding:16px 28px; 
            border-radius:25px; 
            color:{text_color}; 
            text-decoration:none; 
            font-weight:bold; 
            font-size:16px; 
            display:block; 
            margin-bottom:8px; 
            transition:0.3s;
            box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        '>{label}</a>
        <span style='color:#555;'>{description}</span>
    </div>
    <style>
        a[href='{href}']:hover {{
            background: linear-gradient(135deg, {hover_start}, {hover_end});
            transform: scale(1.05); 
            box-shadow: 0 6px 15px rgba(0,0,0,0.2);
        }}
    </style>
    """
    st.markdown(button_html, unsafe_allow_html=True)

with col1:
    gradient_button("📊 Dashboard", "/dashboard", "View real-time metrics & analytics")

with col2:
    gradient_button("💬 Chatroom", "/chatroom", "Ask questions & get AI-driven insights")

with col3:
    gradient_button("🛒 Product Insertion", "/master_product_insertion", "Add or update products in your database")

