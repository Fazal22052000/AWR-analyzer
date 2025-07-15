import streamlit as st
import hashlib

# Toggle authentication
ENABLE_AUTH = True

# Load USERS from Streamlit secrets
try:
    USERS = st.secrets["users"]
except Exception:
    USERS = {}

def check_login(username, password):
    """Verify if the username and password match."""
    hashed_pw = hashlib.sha256(password.encode()).hexdigest()

    # 🔍 DEBUG LOGGING
    st.write("🔐 DEBUG: Username entered:", username)
    st.write("🔐 DEBUG: SHA-256 of entered password:", hashed_pw)
    st.write("🔐 DEBUG: Stored hash from secrets:", USERS.get(username))

    return USERS.get(username) == hashed_pw

def login():
    """Modern, aligned login UI with blue theme and no scroll."""
    st.markdown("""
        <style>
        html, body, .main, .block-container {
            height: 100vh;
            overflow: hidden;
        }
        .login-wrapper {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100%;
            background: linear-gradient(135deg, #e0f2ff, #f8fbff);
            font-family: 'Segoe UI', sans-serif;
        }
        .login-card {
            background: #ffffff;
            border-radius: 12px;
            padding: 2.5rem 2rem;
            width: 100%;
            max-width: 420px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.1);
            text-align: center;
        }
        .login-header {
            display: flex;
            flex-direction: column;
            align-items: center;
            margin-bottom: 25px;
        }
        .login-header .icon {
            font-size: 2rem;
            color: #1e3a8a;
            margin-bottom: 5px;
        }
        .login-header .title {
            font-size: 1.7rem;
            font-weight: 600;
            color: #1e3a8a;
            margin: 0;
        }
        .login-footer {
            text-align: center;
            font-size: 0.8rem;
            color: #777;
            margin-top: 25px;
        }
        input[type="password"] {
            border-radius: 6px !important;
            padding: 10px 40px 10px 10px !important;
            font-size: 1rem !important;
            box-sizing: border-box;
        }
        [data-testid="stTextInput"] button {
            position: absolute;
            top: 50% !important;
            transform: translateY(-50%) !important;
            right: 10px !important;
        }
        button {
            font-weight: 600;
            background-color: #1e3a8a !important;
            color: white !important;
            border-radius: 6px !important;
            margin-top: 15px;
        }
        </style>

        <div class="login-wrapper">
          <div class="login-card">
            <div class="login-header">
              <div class="icon">🔐</div>
              <div class="title">Oracle AWR Analyzer</div>
            </div>
    """, unsafe_allow_html=True)

    if "login_failed" not in st.session_state:
        st.session_state.login_failed = False
    if "empty_fields" not in st.session_state:
        st.session_state.empty_fields = False

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            if not username or not password:
                st.session_state.empty_fields = True
            elif check_login(username, password):
                st.session_state.authenticated = True
                st.session_state.username = username
                st.session_state.login_failed = False
                st.session_state.empty_fields = False
                st.rerun()
            else:
                st.session_state.login_failed = True
                st.session_state.empty_fields = False

    if st.session_state.empty_fields:
        st.warning("⚠️ Both username and password are required.")
    elif st.session_state.login_failed:
        st.error("❌ Invalid username or password.")

    st.markdown("""
            <div class="login-footer">© 2025 Oracle AWR Analyzer by Fazal</div>
          </div>
        </div>
    """, unsafe_allow_html=True)

    st.stop()

def logout():
    """Styled logout panel in sidebar."""
    with st.sidebar:
        st.markdown("---")
        st.markdown(f"👤 **Logged in as:** `{st.session_state.username}`")
        if st.button("🔓 Logout"):
            st.session_state.authenticated = False
            st.session_state.username = ""
            st.rerun()
