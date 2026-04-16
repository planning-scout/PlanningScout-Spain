import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import re
from urllib.parse import unquote
import html as html_lib
import html as _html_esc  # alias used in card builder
import os
import base64

# ════════════════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout — Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════════════════════════
# LOGO — base64-encoded for crisp display (no blur)
# File lives in same folder as this dashboard.py
# ════════════════════════════════════════════════════════════
def load_logo_b64():
    # Try same directory as this file first, then common paths
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "navbar.png"),
        "navbar.png",
        "core/navbar.png",
    ]
    for path in candidates:
        try:
            with open(path, "rb") as f:
                return base64.b64encode(f.read()).decode()
        except Exception:
            continue
    return None

LOGO_B64 = load_logo_b64()
LOGO_HTML = (
    f'<img src="data:image/png;base64,{LOGO_B64}" '
    f'style="width:180px;height:auto;display:block;" alt="PlanningScout">'
    if LOGO_B64 else
    '<span style="font-size:17px;font-weight:700;color:#0d1a2b;">🏗️ PlanningScout</span>'
)

# ════════════════════════════════════════════════════════════
# USER STORE — Google Sheets "Users" tab
# Sheet columns (row 1 = headers):  email | password | active
# Inga adds rows here to grant access. No Streamlit redeploy needed.
# Fallback: st.secrets["users"] still works for backward compat.
# ════════════════════════════════════════════════════════════
@st.cache_data(ttl=60)
def load_users_from_sheet():
    """Load users from the 'Users' worksheet.
    Returns ({email: password}, {email: perfil}).
    Sheet columns: email | password | active | perfil
    The 'perfil' column is optional — missing/empty = '' (fallback to login-form dropdown).
    Returns ({}, {}) on any error.
    """
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", "")).worksheet("Users")
        rows = ws.get_all_records()
        passwords = {}
        profiles  = {}
        for row in rows:
            email    = str(row.get("email", "") or "").strip().lower()
            password = str(row.get("password", "") or "").strip()
            active   = str(row.get("active", "TRUE") or "TRUE").strip().upper()
            perfil   = str(row.get("perfil", "") or "").strip().lower()
            if email and password and active != "FALSE":
                passwords[email] = password
                if perfil:
                    profiles[email] = perfil
        return passwords, profiles
    except Exception:
        return {}, {}

def update_password_in_sheet(email, new_password):
    """Update password for a user in the 'Users' worksheet. Returns True on success."""
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", "")).worksheet("Users")
        # Column A = email; Column B = password
        email_cells = ws.findall(email, in_column=1)
        for cell in email_cells:
            if cell.row > 1:  # skip header row
                ws.update_cell(cell.row, 2, new_password)
                return True
        return False
    except Exception:
        return False

def log_activity(email, action="login"):
    """Append a login event to the 'Activity' worksheet (timestamp | email | action).
    Creates the sheet with headers if it doesn't exist yet.
    Never raises — login must not be blocked by a logging failure."""
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc     = gspread.authorize(creds)
        wb     = gc.open_by_key(st.secrets.get("SHEET_ID", ""))
        # Get or create the Activity worksheet
        try:
            ws = wb.worksheet("Activity")
        except Exception:
            ws = wb.add_worksheet(title="Activity", rows=1000, cols=3)
            ws.append_row(["timestamp", "email", "action"])
        ws.append_row([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            email,
            action,
        ])
    except Exception:
        pass  # never block login due to logging failure

# ════════════════════════════════════════════════════════════
# AUTH
# Two access paths:
#   1. Token URL  ?token=carlos_vimad  → maps to profile, bypasses login (existing clients)
#   2. Email + password login          → checks Google Sheets "Users" tab, then st.secrets["users"]
#
# Add approved users in Google Sheets "Users" tab (email | password | active)
# OR in Streamlit Cloud secrets as fallback:
#   [users]
#   "leandro@kinepolis.com" = "welcome1"
#   "carlos@empresa.es"     = "OtraClave24"
#
# Clients NEVER need a Streamlit account. They open the URL, see the login form.
# ════════════════════════════════════════════════════════════
qp          = st.query_params
url_token   = qp.get("token", "")
url_profile = unquote(qp.get("perfil", ""))   # decode %20, %2F, emoji encoding etc.

client_tokens = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

# ── Initialise session state ──
for _k, _v in [("authenticated", False), ("user_email", ""), ("login_error", ""), ("user_perfil", "")]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── Token URL bypass (personalised links sent to existing clients) ──
_token_profile = None
if url_token and url_token in client_tokens:
    _token_profile = client_tokens[url_token]
    st.session_state["authenticated"] = True
    st.session_state["user_email"]    = f"token:{url_token}"

# ── Login gate: show branded form if not yet authenticated ──
if not st.session_state["authenticated"]:
    # Passwords come from [users] in secrets (and Google Sheets as secondary fallback)
    _sheet_u, _ = load_users_from_sheet()   # sheet is secondary fallback only
    _secret_u   = {}
    try:
        _su = st.secrets.get("users", {})
        _secret_u = {k.strip().lower(): v for k, v in dict(_su).items()} if _su else {}
    except Exception:
        pass
    _users = {**_sheet_u, **_secret_u}   # secrets override sheet for same email

    # Profiles come from [profiles] in secrets — this is the authoritative source
    _secret_profiles: dict = {}
    try:
        _pr = st.secrets.get("profiles", {})
        _secret_profiles = {k.strip().lower(): str(v).strip() for k, v in dict(_pr).items()} if _pr else {}
    except Exception:
        pass

    # Login-page CSS: block-container IS the card — one unified white box, no second container
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,600;0,9..144,700&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
.stApp { background: #f0f2f5 !important; }
[data-testid="stSidebar"]      { display: none !important; }
header[data-testid="stHeader"] { display: none !important; }

/* THE CARD — entire block-container is the white card */
.block-container {
    background: #fff !important;
    border-radius: 20px !important;
    border: 1px solid #e2e8f0 !important;
    box-shadow: 0 4px 32px rgba(0,0,0,.09), 0 1px 4px rgba(0,0,0,.05) !important;
    padding: 40px 36px 36px !important;
    max-width: 420px !important;
    margin: 7vh auto 0 !important;
}

/* Strip Streamlit's own form border so form blends into the card */
[data-testid="stForm"] {
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
}

/* Inputs — slightly tinted so they read on the white card */
.stTextInput > div > div > input {
    background: #f8fafc !important;
    border: 1.5px solid #e2e8f0 !important;
    border-radius: 8px !important;
    color: #0d1a2b !important;
    font-size: 14px !important;
    padding: 11px 14px !important;
}
.stTextInput > div > div > input:focus {
    background: #fff !important;
    border-color: #1e3a5f !important;
    box-shadow: 0 0 0 3px rgba(30,58,95,.1) !important;
    outline: none !important;
}
.stTextInput label p,
.stTextInput [data-testid="stWidgetLabel"] p {
    color: #334155 !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif !important;
}

/* Submit button */
[data-testid="stFormSubmitButton"] > button {
    background: #1e3a5f !important;
    color: #fff !important;
    border: none !important;
    border-radius: 10px !important;
    font-size: 15px !important;
    font-weight: 600 !important;
    width: 100% !important;
    padding: 13px 0 !important;
    margin-top: 4px !important;
    transition: background .15s;
}
[data-testid="stFormSubmitButton"] > button:hover {
    background: #162d4a !important;
}

/* Hide Streamlit's "Press Enter to submit form" tooltip */
[data-testid="InputInstructions"] { display: none !important; }
</style>""", unsafe_allow_html=True)

    # Header HTML — sits directly in block-container, no wrapper div
    st.markdown(f"""
<div style="text-align:center;margin-bottom:28px;">
  <div style="margin-bottom:20px;">{LOGO_HTML}</div>
  <p style="font-size:14px;color:#64748b;margin:0;
       font-family:'Plus Jakarta Sans',system-ui,sans-serif;line-height:1.5;">
    Introduce tus credenciales para acceder al radar.
  </p>
</div>
<div style="height:1px;background:#edf0f4;margin:0 0 24px;"></div>
""", unsafe_allow_html=True)

    # Login form — email + password only. Profile is set by Inga in [profiles] secrets.
    with st.form("login_form"):
        _email_in = st.text_input("Email profesional", placeholder="tu@empresa.com")
        _pass_in  = st.text_input("Contraseña", type="password", placeholder="••••••••")
        _submit   = st.form_submit_button("Acceder al radar →", use_container_width=True)

    if _submit:
        _e = _email_in.strip().lower()
        _p = _pass_in.strip()
        if _e in _users and _users[_e] == _p:
            _assigned = _secret_profiles.get(_e, "general")
            st.session_state["authenticated"] = True
            st.session_state["user_email"]    = _e
            st.session_state["login_error"]   = ""
            st.session_state["user_perfil"]   = _assigned
            log_activity(_e, "login")
            # ── Transition screen ─────────────────────────────────────────────
            # Hide the login card and show a loading spinner for this render
            # cycle. Streamlit completes this render, THEN re-executes from the
            # top where authenticated=True → dashboard renders. One clean transition,
            # no flash of the login form.
            st.markdown("""
<style>
/* Instantly hide the login card and form during this render cycle */
.block-container {
    background: transparent !important;
    box-shadow: none !important;
    border: none !important;
    padding: 0 !important;
}
[data-testid="stForm"],
[data-testid="stTextInput"],
[data-testid="stFormSubmitButton"],
div[data-testid="stMarkdownContainer"] { display: none !important; }
@keyframes _spin { to { transform: rotate(360deg); } }
</style>
<div style="min-height:90vh;display:flex;align-items:center;justify-content:center;">
  <div style="text-align:center;">
    <div style="width:36px;height:36px;border:3px solid #e2e8f0;border-top-color:#1e3a5f;
         border-radius:50%;animation:_spin .65s linear infinite;margin:0 auto 18px;"></div>
    <p style="font-family:'Plus Jakarta Sans',system-ui,sans-serif;font-size:15px;
       color:#1e3a5f;font-weight:600;margin:0;">Cargando tu radar&hellip;</p>
  </div>
</div>""", unsafe_allow_html=True)
            st.rerun()
        else:
            st.session_state["login_error"] = "Credenciales incorrectas. Verifica tu email y contraseña."

    if st.session_state["login_error"]:
        st.markdown(
            f'<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;'
            f'padding:10px 14px;font-size:13px;color:#dc2626;text-align:center;'
            f'font-family:\'Plus Jakarta Sans\',system-ui,sans-serif;margin-top:4px;">'
            f'{html_lib.escape(st.session_state["login_error"])}</div>',
            unsafe_allow_html=True,
        )

    # Footer — no closing </div> needed (block-container is the card)
    st.markdown("""
<div style="text-align:center;margin-top:20px;padding-top:16px;border-top:1px solid #f1f5f9;">
  <p style="font-size:12px;color:#94a3b8;margin:0 0 5px;
       font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
    ¿A&uacute;n no tienes acceso?
  </p>
  <a href="https://planningscout.com"
     style="font-size:12px;color:#1e3a5f;font-weight:600;text-decoration:none;">
    Solicitar acceso en planningscout.com &rarr;
  </a>
</div>""", unsafe_allow_html=True)

    st.stop()

# ── After successful auth: resolve profile ──────────────────────────────────
# Rules:
#   Token URL → profile from client_tokens, profile selector VISIBLE (Inga's admin view)
#   Email login → ALWAYS use session_state["user_perfil"] set at login from [profiles] secrets
#                 ?perfil= URL params are IGNORED for email users — cannot be spoofed
_is_email_user = (
    st.session_state.get("authenticated", False) and
    not st.session_state.get("user_email", "").startswith("token:")
)

forced_profile_key = ""
if _token_profile:
    forced_profile_key = _token_profile          # Inga's admin token: use token mapping
elif _is_email_user:
    forced_profile_key = st.session_state.get("user_perfil", "general") or "general"

SHEET_ID = st.secrets.get("SHEET_ID", "")

# User store accessible to sidebar (for password change)
_store_secret = {}
try:
    _ss = st.secrets.get("users", {})
    _store_secret = dict(_ss) if _ss else {}
except Exception:
    pass
_store_sheet, _ = load_users_from_sheet()   # only need passwords dict here
_all_pw         = {**_store_secret, **_store_sheet}

# ════════════════════════════════════════════════════════════
# GLOBAL CSS — only for Streamlit chrome, not card content
# Card content uses 100% inline styles to bypass Markdown parser
# ════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,600;0,9..144,700;1,9..144,400&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

#MainMenu { visibility: hidden; }
footer { visibility: hidden !important; }

/* Hide ALL Streamlit attribution — "Made with Streamlit", "Created by ingatech-hub", etc.
   Streamlit Cloud shows the deploying GitHub org in several elements; hide them all. */
[data-testid="stToolbar"]         { display: none !important; }
#stDecoration                      { display: none !important; }
.viewerBadge_container__1QSob,
.viewerBadge_link__1S137,
.viewerBadge_text__1JaDK           { display: none !important; }
[data-testid="stStatusWidget"]     { display: none !important; }
[data-testid="manage-app-button"]  { display: none !important; }

.stApp { background: #f0f2f5 !important; }

/* Main content padding — breathing room both sides */
.block-container {
    padding-top: 28px !important;
    padding-bottom: 48px !important;
    padding-left: 48px !important;
    padding-right: 48px !important;
    max-width: 1100px !important;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
}
[data-testid="stSidebarContent"] {
    padding: 20px 20px 32px 20px !important;
}

/* Sidebar text contrast — all labels dark */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] .stRadio > div label span {
    color: #334155 !important;
}
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #334155 !important;
}

/* Download button */
.stDownloadButton button {
    background: #fff !important;
    color: #1e3a5f !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    font-weight: 600 !important;
}
.stDownloadButton button:hover {
    border-color: #1e3a5f !important;
    background: #eff4fb !important;
}

/* Refresh button */
.stButton button {
    background: #fff !important;
    color: #334155 !important;
    border: 1.5px solid #e2e8f0 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
}
.stButton button:hover {
    border-color: #1e3a5f !important;
    color: #1e3a5f !important;
}

/* Mobile */
@media (max-width: 768px) {
    .block-container {
        padding-left: 16px !important;
        padding-right: 16px !important;
        padding-top: 14px !important;
    }
}

/* ── DARK MODE ───────────────────────────────────────────────────────────────
   Fires only when the user's OS/browser is set to dark mode.
   Targets the Streamlit chrome (toolbar, sidebar, app shell) — not the cards,
   which use inline styles and intentionally stay light (white cards on dark
   background is standard dark-mode UI, same as Notion / Linear / Vercel).
────────────────────────────────────────────────────────────────────────────── */
@media (prefers-color-scheme: dark) {

    /* App background */
    .stApp { background: #0f1724 !important; }

    /* Top toolbar — the white bar that looks broken in dark mode */
    header[data-testid="stHeader"] {
        background: #1a2535 !important;
        border-bottom: 1px solid #2d3f55 !important;
    }
    header[data-testid="stHeader"] button,
    header[data-testid="stHeader"] a {
        color: #94a3b8 !important;
    }
    header[data-testid="stHeader"] button:hover {
        background: rgba(255,255,255,0.08) !important;
        color: #e2e8f0 !important;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: #1a2535 !important;
        border-right: 1px solid #2d3f55 !important;
    }
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] .stRadio > div label span {
        color: #cbd5e1 !important;
    }
    [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
        color: #94a3b8 !important;
    }

    /* Dropdowns, number inputs */
    .stSelectbox > div > div,
    .stNumberInput > div > div > input {
        background: #1a2535 !important;
        color: #e2e8f0 !important;
        border-color: #2d3f55 !important;
    }

    /* Buttons */
    .stButton button {
        background: #1a2535 !important;
        color: #cbd5e1 !important;
        border-color: #2d3f55 !important;
    }
    .stButton button:hover {
        border-color: #4a8ec2 !important;
        color: #e2e8f0 !important;
    }
    .stDownloadButton button {
        background: #1a2535 !important;
        color: #93c5fd !important;
        border-color: #2d3f55 !important;
    }
}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# INLINE STYLE CONSTANTS
# All card HTML uses these — bypasses Streamlit's Markdown parser.
# Single quotes inside double-quoted Python strings = valid, no escaping.
# ════════════════════════════════════════════════════════════
_F  = "font-family:'Plus Jakarta Sans',system-ui,sans-serif"
_FH = "font-family:'Fraunces',Georgia,serif"
_FM = "font-family:'JetBrains Mono',monospace"

# Card wrapper
SC = "background:#fff;border:1.5px solid #e2e8f0;border-radius:14px;overflow:hidden;margin-bottom:14px;box-shadow:0 2px 8px rgba(0,0,0,.05);"
# Header
SH  = "background:#f7f8fa;border-bottom:1px solid #e2e8f0;padding:13px 20px;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;"
SLO = "display:flex;align-items:flex-start;gap:8px;min-width:0;flex:1;"
SDO = "width:8px;height:8px;border-radius:50%;background:#16a34a;flex-shrink:0;margin-top:4px;"
SMU = f"{_FH};font-size:14px;font-weight:700;color:#0d1a2b;line-height:1.3;"
SBD = "display:flex;align-items:center;gap:6px;flex-shrink:0;flex-wrap:wrap;justify-content:flex-end;"
# Score pills
SSPG = f"{_FM};font-size:12px;font-weight:500;padding:4px 11px;border-radius:100px;white-space:nowrap;color:#fff;background:#15803d;"
SSPO = f"{_FM};font-size:12px;font-weight:500;padding:4px 11px;border-radius:100px;white-space:nowrap;color:#fff;background:#b45309;"
SSPN = f"{_FM};font-size:12px;font-weight:500;padding:4px 11px;border-radius:100px;white-space:nowrap;color:#fff;background:#1e3a5f;"
SSPD = f"{_FM};font-size:12px;font-weight:500;padding:4px 11px;border-radius:100px;white-space:nowrap;color:#fff;background:#94a3b8;"
# Status badges (outlined)
SSBG = f"{_FM};font-size:10px;font-weight:500;padding:4px 10px;border-radius:100px;white-space:nowrap;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0;"
SSBA = f"{_FM};font-size:10px;font-weight:500;padding:4px 10px;border-radius:100px;white-space:nowrap;background:#fffbeb;color:#b45309;border:1px solid #fde68a;"
SSBN = f"{_FM};font-size:10px;font-weight:500;padding:4px 10px;border-radius:100px;white-space:nowrap;background:#eff4fb;color:#1e3a5f;border:1px solid #bfdbfe;"
# Body
SBO  = "padding:16px 20px;"
SRF  = f"{_FM};font-size:10.5px;color:#94a3b8;margin-bottom:5px;letter-spacing:.03em;"
STI  = f"{_FH};font-size:17px;font-weight:600;color:#0d1a2b;margin-bottom:5px;line-height:1.3;"
SAD  = f"{_F};font-size:13px;color:#64748b;display:flex;align-items:flex-start;gap:5px;margin-bottom:14px;line-height:1.4;"
# Table
STA  = "border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;margin-bottom:12px;"
SRB  = f"{_F};display:flex;justify-content:space-between;align-items:center;padding:9px 14px;border-bottom:1px solid #f1f5f9;gap:12px;"
SRL  = f"{_F};display:flex;justify-content:space-between;align-items:center;padding:9px 14px;gap:12px;"
SKE  = f"{_FM};font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;flex-shrink:0;min-width:75px;"
SVA  = f"{_F};font-size:13px;color:#334155;text-align:right;line-height:1.4;"
SVP  = f"{_FH};font-size:17px;font-weight:700;color:#1e3a5f;"
STG  = "display:flex;gap:5px;justify-content:flex-end;flex-wrap:wrap;"
STA2 = f"{_FM};font-size:10.5px;background:#fffbeb;color:#b45309;border:1px solid #fde68a;padding:3px 8px;border-radius:5px;"
STN  = f"{_FM};font-size:10.5px;background:#eff4fb;color:#1e3a5f;border:1px solid #bfdbfe;padding:3px 8px;border-radius:5px;"
STG2 = f"{_FM};font-size:10.5px;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0;padding:3px 8px;border-radius:5px;"
# Footer
SFO  = "background:#f7f8fa;border-top:1px solid #e2e8f0;padding:10px 20px;display:flex;align-items:center;gap:7px;flex-wrap:wrap;"
SBP  = f"{_F};display:inline-flex;align-items:center;gap:4px;font-size:12px;font-weight:600;color:#fff;background:#1e3a5f;border:1px solid #1e3a5f;padding:5px 12px;border-radius:7px;text-decoration:none;white-space:nowrap;"
SBT  = f"{_F};display:inline-flex;align-items:center;gap:4px;font-size:12px;font-weight:600;color:#334155;background:#fff;border:1px solid #cbd5e1;padding:5px 12px;border-radius:7px;text-decoration:none;white-space:nowrap;"
SNO  = f"{_FM};font-size:10px;color:#94a3b8;margin-left:auto;"

# ════════════════════════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════════════════════════
PROFILES = {
    "🔧 Instaladores MEP": {
        "key": "instaladores",
        "tip": "💡 <strong>Contacta al promotor 6-12 meses antes</strong> de que empiece la obra — antes de que cierre los contratos de instalaciones. La licencia concedida es tu señal de arranque.",
        "min_score": 0, "min_value": 80_000, "days": 365,
        "types": ["obra mayor", "obra mayor nueva construcción", "obra mayor rehabilitación",
                  "cambio de uso", "declaración responsable", "licencia primera ocupación",
                  "urbanización", "demolición y nueva planta"],
    },
    "🏪 Expansión Retail": {
        "key": "expansion",
        "tip": "💡 <strong>Una urbanización aprobada = nuevo barrio en 2-3 años.</strong> Identifica la ubicación de tu próxima apertura antes de que suba el precio del suelo.",
        "min_score": 0, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial", "plan especial / parcial", "plan parcial",
                  "cambio de uso", "licencia de actividad", "obra mayor nueva construcción"],
    },
    "📐 Promotores / RE": {
        "key": "promotores",
        "tip": "💡 <strong>Reparcelación aprobada = suelo urbanizable.</strong> Contacta a la Junta de Compensación antes de que la operación salga al mercado.",
        "min_score": 20, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan parcial", "plan especial", "plan especial / parcial",
                  "obra mayor nueva construcción", "cambio de uso"],
    },
    "🏢 Gran Constructora": {
        "key": "constructora",
        "tip": "💡 <strong>Aprobación definitiva = licitación en 12-18 meses.</strong> Prepara el dossier técnico y las alianzas antes que la competencia.",
        "min_score": 35, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial", "plan especial / parcial", "plan parcial",
                  "obra mayor industrial", "obra mayor nueva construcción", "licitación de obras"],
    },
    "🏗️ Gran Infraestructura": {
        "key": "infrastructura",
        "tip": "💡 <strong>Las Tablas Oeste €106M, Los Cerros, Tres Cantos UE.5 €17M.</strong> Anticipación de 12-18 meses antes de licitación. Las Juntas de Compensación activas son tu señal.",
        "min_score": 40, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial / parcial", "plan parcial", "licitación de obras"],
    },
    "🏭 Industrial / Log.": {
        "key": "industrial",
        "tip": "💡 <strong>Licencia de nave = obra en 3-6 meses.</strong> Contacta al promotor para demolición previa o ejecución completa.",
        "min_score": 0, "min_value": 200_000, "days": 365,
        "types": ["obra mayor industrial", "urbanización", "obra mayor nueva construcción",
                  "cambio de uso", "licitación de obras"],
    },
    "🚧 Alquiler Maquinaria": {
        "key": "alquiler",
        "tip": "💡 <strong>Llega al constructor ANTES de que empiece la obra.</strong> Cualquier proyecto >€200K = excavadoras, plataformas elevadoras, robots de demolición.",
        "min_score": 0, "min_value": 200_000, "days": 60,
        "types": [],
    },
    "🛒 Compras / Materiales": {
        "key": "compras",
        "tip": "💡 <strong>Todos los proyectos grandes = oportunidad de suministro.</strong> Preséntate antes de que la constructora adjudique materiales.",
        "min_score": 0, "min_value": 150_000, "days": 365,
        "types": [],
    },
    "🏙️ Vista General": {
        "key": "general",
        "tip": "Vista completa de todos los proyectos. Selecciona un perfil en el panel izquierdo para ver solo los leads relevantes para tu sector.",
        "min_score": 0, "min_value": 0, "days": 365,
        "types": [],
    },
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def esc(v):
    """html.escape all data before inserting into HTML."""
    s = str(v or "").strip()
    return html_lib.escape(s) if s not in ("nan", "None", "—", "") else ""

def parse_val(v):
    if not v or str(v).strip() in ("", "—", "N/A", "nan"):
        return 0.0
    s = re.sub(r'[^\d,.]', '', str(v))
    if s.count(',') == 1 and s.count('.') >= 1:
        s = s.replace('.', '').replace(',', '.')
    elif s.count(',') == 1:
        s = s.replace(',', '.')
    elif s.count('.') > 1:
        s = s.replace('.', '')
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_est_pem_numeric(text):
    """
    Extract the first numeric value from a rich Estimated PEM text string.
    Handles formats like:
      'Estimación PEM: €800K–€2.5M · ...'  → 800_000
      '✅ PEM confirmado: €17,361,664'       → 17_361_664
      '⚪ Sin datos PEM en BOCM'             → 0
    Used only for filtering/sorting — display uses the raw text.
    """
    if not text or str(text).strip().lower() in ("", "nan", "none"):
        return 0.0
    t = str(text)
    # Millions: €17.4M or €2.5M
    for m in re.finditer(r'€\s*([\d]+(?:[.,]\d+)?)\s*[Mm]', t):
        try: return float(m.group(1).replace(',', '.')) * 1_000_000
        except: pass
    # Thousands: €800K
    for m in re.finditer(r'€\s*([\d]+(?:[.,]\d+)?)\s*[Kk]', t):
        try: return float(m.group(1).replace(',', '.')) * 1_000
        except: pass
    # Plain number after €: €17,361,664 or €17.361.664
    for m in re.finditer(r'€\s*([\d][0-9.,]+)', t):
        try: return parse_val(m.group(1))
        except: pass
    return 0.0

def fmt(v):
    if v == 0:    return "—"
    if v >= 1e6:  return f"€{v/1e6:.1f}M"
    if v >= 1000: return f"€{int(v/1000)}K"
    return f"€{int(v):,}"

def sc_pill(sc):
    e = "🟢" if sc >= 65 else "🟠" if sc >= 40 else "🟡" if sc >= 20 else "⚪"
    s = SSPG if sc >= 65 else SSPO if sc >= 40 else SSPN if sc >= 20 else SSPD
    return f'<span style="{s}">{e} {sc} / 100</span>'

def build_card(row):
    """
    Build one lead card with ONLY inline styles.
    This guarantees correct rendering regardless of Streamlit's Markdown parser.
    All data values are html.escape()'d to prevent broken HTML.
    """
    sc    = parse_sc(row.get("score_raw", 0))
    pem   = parse_val(row.get("pem_raw", ""))         # declared PEM (numeric col F)
    pem_est_text = str(row.get("pem_est_raw", "") or "").strip()  # raw text from col R
    pem_e = parse_est_pem_numeric(pem_est_text)        # numeric extraction for sorting only
    pem_c = pem if pem > 0 else pem_e                  # combined for display of declared
    pem_is_declared  = pem > 0
    # Estimated: show when no declared PEM and raw text has meaningful content
    _est_empty = pem_est_text.lower() in ("", "nan", "none", "⚪ sin datos pem en bocm")
    pem_is_estimated = not pem_is_declared and bool(pem_est_text) and not _est_empty
    muni  = esc(row.get("municipio", "")) or "Madrid"
    addr  = esc(row.get("direccion", ""))
    prom  = esc(row.get("promotor", ""))
    tipo  = esc(row.get("tipo", ""))
    desc  = esc(row.get("descripcion", ""))
    fecha = esc(row.get("fecha", ""))
    fnd   = esc(row.get("fecha_encontrado", ""))
    maps  = str(row.get("maps", "") or "").strip()
    bocm  = str(row.get("bocm_url", "") or "").strip()
    pdf   = str(row.get("pdf_url", "") or "").strip()
    expd  = esc(row.get("expediente", ""))
    conf  = str(row.get("confianza", "") or "").strip()

    pem_s = fmt(pem_c)

    # BOCM / BOE reference + date
    ref_parts = []
    if bocm:
        _bocm_is_boe = bocm.lower().startswith("https://www.boe.es") or bocm.lower().startswith("https://boe.es")
        if _bocm_is_boe:
            boe_m = re.search(r'BOE[-_]?([A-Z]-\d{4}-\d+|\d{8}[-_]\d+)', bocm, re.I)
            ref_parts.append(f"BOE-{boe_m.group(1)}" if boe_m else "BOE")
        else:
            m = re.search(r'BOCM[-_](\d{8})', bocm, re.I)
            if m:
                ref_parts.append(f"BOCM-{m.group(1)}")
    pub = fnd[:10] if fnd else fecha
    if pub:
        try:
            dt = datetime.strptime(pub, "%Y-%m-%d")
            ref_parts.append(f"Publicado: {dt.strftime('%-d %b %Y')}")
        except Exception:
            ref_parts.append(pub)
    ref_str = " · ".join(ref_parts)

    # Title
    title = addr if addr else (desc[:90] if desc else tipo)

    # Status badge
    tl = tipo.lower() + " " + desc.lower()
    if "definitiv" in tl:
        sbadge = f'<span style="{SSBG}">Aprobación definitiva</span>'
    elif "inicial" in tl:
        sbadge = f'<span style="{SSBA}">Aprobación inicial</span>'
    elif "concede" in tl or "otorga" in tl:
        sbadge = f'<span style="{SSBG}">Licencia concedida</span>'
    elif tipo:
        sbadge = f'<span style="{SSBN}">{tipo[:28]}</span>'
    else:
        sbadge = ""

    # ── NEW badge: shown if published within the last 7 days ──
    _new_badge = ""
    _pub_for_new = fnd[:10] if fnd else fecha
    if _pub_for_new:
        try:
            _pub_dt = datetime.strptime(_pub_for_new, "%Y-%m-%d")
            if (datetime.now() - _pub_dt).days <= 7:
                _new_badge = (
                    "<span style='font-family:\"JetBrains Mono\",monospace;font-size:9px;"
                    "font-weight:700;letter-spacing:.08em;text-transform:uppercase;"
                    "background:#dc2626;color:#fff;border-radius:4px;padding:2px 7px;"
                    "margin-right:4px;'>Nuevo</span>"
                )
        except Exception:
            pass

    # ─ HEADER (inline styles, guaranteed to render) ─
    head = (
        f'<div style="{SH}">'
        f'  <div style="{SLO}">'
        f'    <div style="{SDO}"></div>'
        f'    <span style="{SMU}">{muni}</span>'
        f'  </div>'
        f'  <div style="{SBD}">{_new_badge}{sbadge}{sc_pill(sc)}</div>'
        f'</div>'
    )

    # ─ BODY ─
    ref_html   = f'<div style="{SRF}">{ref_str}</div>' if ref_str else ""
    title_html = f'<div style="{STI}">{title}</div>'
    addr_html  = f'<div style="{SAD}"><span>📍</span><span>{addr}</span></div>' if addr and addr != title else ""

    # ── Description: always visible inline (≤2 lines), "leer más" for the rest ──
    # UX principle: description is the first thing a user wants to know.
    # Show a 2-line preview inline so they can scan without clicking.
    # Only add the dropdown when there is more text to reveal.
    _SDESC = (
        "font-family:'Plus Jakarta Sans',system-ui,sans-serif;"
        "font-size:13px;color:#475569;line-height:1.55;margin:6px 0 12px 0;"
    )
    desc_preview_html = ""
    if desc and len(desc) > 5:
        # CSS line-clamp: browsers clip at 2 lines regardless of char count
        clamp_style = _SDESC + "display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;"
        desc_preview_html = f'<div style="{clamp_style}">{desc}</div>'

    # ── TABLE ─────────────────────────────────────────────────────────────────
    table_rows = []
    all_row_data = []
    if tipo:
        all_row_data.append(("Tipo", f'<span style="{SVA}">{tipo}</span>'))

    # PEM row: declared (navy, confirmed badge) OR estimated (amber, IA badge)
    if pem_is_declared:
        pem_row_val = (
            f'<div style="display:flex;align-items:center;gap:8px;">'
            f'<span style="{SVP}">{pem_s}</span>'
            f'<span style="{_FM};font-size:9px;font-weight:600;background:#dbeafe;'
            f'color:#1e40af;border-radius:4px;padding:2px 6px;letter-spacing:.04em;">✓ BOCM</span>'
            f'</div>'
        )
        all_row_data.append(("PEM Declarado", pem_row_val))
    elif pem_is_estimated:
        # Show the raw text exactly as stored in the sheet (e.g. "Estimación PEM: €800K–€2.5M · ...")
        _est_display = _html_esc.escape(pem_est_text[:220])
        _is_confirmed = "✅" in pem_est_text or "confirmado" in pem_est_text.lower()
        _badge_style = (
            f"{_FM};font-size:9px;font-weight:600;background:#dbeafe;color:#1e40af;"
            f"border-radius:4px;padding:2px 6px;letter-spacing:.04em;flex-shrink:0;"
        ) if _is_confirmed else (
            f"{_FM};font-size:9px;font-weight:600;background:#fffbeb;color:#b45309;"
            f"border-radius:4px;border:1px solid #fde68a;padding:2px 6px;letter-spacing:.04em;flex-shrink:0;"
        )
        _badge_label = "✓ Confirmado" if _is_confirmed else "⚡ Est. IA"
        pem_row_val = (
            f'<div style="display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap;">'
            f'<span style="{_F};font-size:12px;color:#b45309;line-height:1.55;flex:1;">{_est_display}</span>'
            f'<span style="{_badge_style}">{_badge_label}</span>'
            f'</div>'
        )
        all_row_data.append(("Estimación PEM", pem_row_val))

    # Etapas from description
    etapa_m = re.findall(r'[Ee]tapa\s*(\d+)[^€\d]*?(\d[\d.,]+\s*(?:[MmKk€])?)', desc)
    if etapa_m:
        etags = "".join(f'<span style="{STA2}">Etapa {n}: {v}</span>' for n, v in etapa_m[:3])
        all_row_data.append(("Etapas", f'<div style="{STG}">{etags}</div>'))

    if prom:
        all_row_data.append(("Promotor", f'<span style="{SVA}">{prom}</span>'))
    if expd:
        all_row_data.append(("Expediente", f'<span style="{SVA};{_FM};font-size:11px;">{expd}</span>'))
    if conf in ("high", "medium", "low"):
        cm = {"high": (STG2, "Alta"), "medium": (STA2, "Media"), "low": (STA2, "Baja")}
        cs, ct = cm[conf]
        all_row_data.append(("Fiabilidad", f'<span style="{cs}">{ct}</span>'))

    for i, (key, val_html) in enumerate(all_row_data):
        row_s = SRL if i == len(all_row_data) - 1 else SRB
        table_rows.append(
            f'<div style="{row_s}">'
            f'<span style="{SKE}">{key}</span>'
            f'{val_html}'
            f'</div>'
        )

    table_html = (
        f'<div style="{STA}">{"".join(table_rows)}</div>'
        if table_rows else ""
    )

    # ─ FOOTER LINKS ─
    links = []
    if bocm:
        _is_boe = bocm.lower().startswith("https://www.boe.es") or bocm.lower().startswith("https://boe.es")
        _ver_label = "↗ Ver en el BOE" if _is_boe else "↗ Ver en el BOCM"
        links.append(f'<a href="{bocm}" target="_blank" rel="noopener" style="{SBP}">{_ver_label}</a>')
    if maps:
        links.append(f'<a href="{maps}" target="_blank" rel="noopener" style="{SBT}">📍 Mapa</a>')
    if pdf:
        links.append(f'<a href="{pdf}" target="_blank" rel="noopener" style="{SBT}">📑 PDF</a>')
    if prom:
        q = html_lib.unescape(prom).replace(" ", "+")
        links.append(f'<a href="https://www.linkedin.com/search/results/all/?keywords={html_lib.escape(q)}" target="_blank" rel="noopener" style="{SBT}">🔍 Promotor</a>')

    footer = (
        f'<div style="{SFO}">'
        + "".join(links)
        + f'<span style="{SNO}">Datos públicos · {"BOE" if bocm and (bocm.lower().startswith("https://www.boe.es") or bocm.lower().startswith("https://boe.es")) else "BOCM"}</span>'
        + f'<a href="mailto:info@planningscout.com?subject={html_lib.escape("Consulta%20lead%3A%20" + muni + "%20%E2%80%94%20" + (expd or ref_str[:40]))}&body={html_lib.escape("Hola,%0A%0AMe%20interesa%20obtener%20más%20información%20sobre%20este%20proyecto%3A%0A%0AMunicipio%3A%20" + muni + "%0ADirección%3A%20" + addr + "%0AExpediente%3A%20" + expd + "%0AURL%3A%20" + bocm + "%0A%0A[Describe%20tu%20consulta%20aquí]")}" style="{_F};display:inline-flex;align-items:center;gap:4px;font-size:11px;font-weight:500;color:#64748b;background:#f8fafc;border:1px solid #e2e8f0;padding:4px 10px;border-radius:7px;text-decoration:none;white-space:nowrap;margin-left:4px;" title="Reportar un error o solicitar más información sobre este proyecto">✉️ Reportar error / Más info</a>'
        + '</div>'
    )


    # ── AI Evaluation — dropdown, same style as old Descripción dropdown ──
    # Phase (col Q) shown as a tag row below.
    _SUM = (
        "cursor:pointer;padding:10px 20px;font-size:12.5px;font-weight:600;"
        "color:#334155;display:flex;align-items:center;gap:8px;"
        "outline:none;user-select:none;-webkit-user-select:none;"
        "list-style:none;border-top:1px solid #f1f5f9;background:#fff;"
    )
    _DIV = "padding:4px 20px 16px 20px;"
    extras_html = ""

    ai_val = str(row.get("ai_evaluation", "") or row.get("AI Evaluation", "") or "").strip()
    if ai_val and ai_val.lower() not in ("nan", "none", ""):
        ai_e = _html_esc.escape(ai_val[:600])
        extras_html += (
            "<details><summary style='" + _SUM + "'>"
            "<span style='font-size:12px'>📋</span>"
            "<span style='color:#64748b;font-weight:500;'>Análisis del proyecto</span>"
            "<span style='margin-left:auto;font-size:10px;color:#94a3b8;'>▼</span>"
            "</summary><div style='" + _DIV + "'>"
            "<div style='font-size:13px;color:#374151;line-height:1.65;background:#f8fafc;"
            "border-radius:10px;padding:14px 16px;'>" + ai_e + "</div>"
            "</div></details>"
        )

    # Phase (col Q) — shown as a tag row
    fase_val = str(row.get("fase", "") or "").strip()
    _FASE_LABELS = {
        "definitivo":        ("🟢", "Aprobación definitiva",  "#f0fdf4", "#16a34a", "#bbf7d0"),
        "inicial":           ("🟡", "Aprobación inicial",     "#fffbeb", "#b45309", "#fde68a"),
        "licitacion":        ("🔵", "Licitación activa",      "#eff4fb", "#1e3a5f", "#bfdbfe"),
        "primera_ocupacion": ("⚪", "1ª Ocupación",           "#f8fafc", "#64748b", "#e2e8f0"),
        "en_tramite":        ("🟠", "En trámite",             "#fff7ed", "#c2410c", "#fed7aa"),
    }
    if fase_val and fase_val in _FASE_LABELS:
        fi, ft, fb, fc, fbd = _FASE_LABELS[fase_val]
        extras_html += (
            f"<div style='padding:10px 20px 14px 20px;display:flex;align-items:center;gap:8px;'>"
            f"<span style='font-size:10px;font-weight:700;text-transform:uppercase;"
            f"letter-spacing:.07em;color:#94a3b8;'>Fase:</span>"
            f"<span style='font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px;"
            f"background:{fb};color:{fc};border:1px solid {fbd};'>{fi} {ft}</span>"
            f"</div>"
        )

    # Supplies Needed — dropdown, col T, with AI disclaimer note
    sup_val = str(row.get("supplies_needed", "") or row.get("Supplies Needed", "") or "").strip()
    if sup_val and sup_val.lower() not in ("nan", "none", ""):
        sup_e = _html_esc.escape(sup_val[:500])
        extras_html += (
            "<details><summary style='" + _SUM + "'>"
            "<span style='font-size:12px'>🛒</span>"
            "<span style='color:#64748b;font-weight:500;'>Materiales y suministros estimados</span>"
            "<span style='margin-left:auto;font-size:10px;color:#94a3b8;'>▼</span>"
            "</summary><div style='" + _DIV + "'>"
            "<div style='font-size:12.5px;color:#374151;line-height:1.7;background:#f8fafc;"
            "border-radius:10px;padding:14px 16px;'>" + sup_e + "</div>"
            "<div style='margin-top:8px;font-size:10.5px;color:#94a3b8;font-style:italic;"
            "font-family:\"JetBrains Mono\",monospace;padding:0 2px;'>"
            "⚠️ Estimación generada por IA. Puede no ser 100% precisa. "
            "Verificar siempre con el proyecto técnico original."
            "</div>"
            "</div></details>"
        )

    return (
        f'<div style="{SC}">'
        f'{head}'
        f'<div style="{SBO}">'
        f'{ref_html}'
        f'{title_html}'
        f'{addr_html}'
        f'{desc_preview_html}'
        f'{table_html}'
        f'</div>'
        f'{extras_html}'
        f'{footer}'
        f'</div>'
    )

# ════════════════════════════════════════════════════════════
# DATA LOADING
# ════════════════════════════════════════════════════════════
COL_MAP = {
    "Date Granted": "fecha", "Municipality": "municipio",
    "Full Address": "direccion", "Applicant": "promotor",
    "Permit Type": "tipo", "Declared Value PEM (€)": "pem_raw",
    "Est. Build Value (€)": "est_raw", "Maps Link": "maps",
    "Description": "descripcion", "Source URL": "bocm_url",
    "PDF URL": "pdf_url", "Mode": "modo", "Confidence": "confianza",
    "Date Found": "fecha_encontrado", "Lead Score": "score_raw",
    "Expediente": "expediente", "Phase": "fase",
    "AI Evaluation": "ai_evaluation", "Supplies Needed": "supplies_needed",
    "Estimated PEM": "pem_est_raw",
}

@st.cache_data(ttl=300)
def load_data():
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", SHEET_ID)).worksheet("Leads")
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as ex:
        st.error(f"Error conectando a Google Sheets: {ex}")
        return pd.DataFrame()

with st.spinner("Cargando proyectos…"):
    df_raw = load_data()

if df_raw.empty:
    st.markdown(f"""
    <div style="min-height:60vh;display:flex;align-items:center;justify-content:center;">
    <div style="text-align:center;padding:48px 32px;background:#fff;border-radius:14px;
         border:1.5px solid #e2e8f0;box-shadow:0 2px 8px rgba(0,0,0,.05);">
      {LOGO_HTML}
      <div style="font-size:40px;margin:20px 0 14px;">📡</div>
      <h3 style="font-family:'Fraunces',Georgia,serif;font-size:20px;color:#0d1a2b;margin:0 0 8px;">Sin datos todavía</h3>
      <p style="font-size:14px;color:#64748b;line-height:1.6;margin:0;">
        El scraper no ha procesado proyectos aún.<br>
        Ejecuta <strong>--weeks 8</strong> en GitHub Actions para el backfill inicial.
      </p>
    </div></div>""", unsafe_allow_html=True)
    st.stop()

df = df_raw.rename(columns={k: v for k, v in COL_MAP.items() if k in df_raw.columns})
df["pem"]          = df["pem_raw"].apply(parse_val)                  if "pem_raw"     in df.columns else pd.Series(0.0, index=df.index)
df["pem_est"]      = df["pem_est_raw"].apply(parse_est_pem_numeric)  if "pem_est_raw" in df.columns else pd.Series(0.0, index=df.index)
# pem_combined: declared PEM if > 0, else AI-estimated PEM
# pem_est was computed above from pem_est_raw column
if "pem" in df.columns and "pem_est" in df.columns:
    df["pem_combined"] = df.apply(lambda r: r["pem"] if r["pem"] > 0 else r["pem_est"], axis=1)
elif "pem" in df.columns:
    df["pem_combined"] = df["pem"]
else:
    df["pem_combined"] = 0

# 3. LIMPIEZA DE SCORE: Función robusta para evitar errores de formato
def parse_sc(val):
    if pd.isna(val) or str(val).strip() == "": 
        return 0
    try: 
        # Esta línea limpia puntos de miles y comas decimales
        clean_val = str(val).replace(".", "").replace(",", ".").strip()
        return int(float(clean_val))
    except Exception: 
        return 0

# 4. APLICACIÓN DEL SCORE: Busca la columna original 'score_raw'
if "score_raw" in df.columns:
    df["score"] = df["score_raw"].apply(parse_sc)
else:
    df["score"] = 0

def _best_date(row):
    """Use the most recent of Date Found and Date Granted.
    Many leads have fecha_encontrado = fecha_granted (wrong value),
    so this prevents valid leads from being excluded by date filter."""
    best = None
    for col in ["fecha_encontrado", "fecha"]:
        v = str(row.get(col, "") or "").strip()[:10]
        if len(v) == 10:
            try:
                dt = pd.to_datetime(v)
                if best is None or dt > best:
                    best = dt
            except Exception:
                pass
    return best if best is not None else pd.NaT

df["fecha_dt"] = df.apply(_best_date, axis=1)

all_munis = sorted([
    m for m in (df["municipio"].dropna().unique().tolist() if "municipio" in df.columns else [])
    if str(m).strip() and str(m) not in ("nan", "")
])

profile_names = list(PROFILES.keys())
default_idx   = len(profile_names) - 1  # Vista General fallback
is_locked     = False

if forced_profile_key:
    matched = next(
        (n for n, p in PROFILES.items() if p["key"] == forced_profile_key),
        next(
            (n for n in profile_names if n == forced_profile_key),
            profile_names[-1]   # final fallback → Vista General
        )
    )
    default_idx = profile_names.index(matched)

# Email users are ALWAYS locked — they cannot switch profiles.
# Token URL users (Inga's admin link ?token=inga_admin) see the full selector.
is_locked = _is_email_user
# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:

    # Crisp logo — base64 embedded, no resizing blur
    st.markdown(LOGO_HTML, unsafe_allow_html=True)
    st.markdown('<div style="height:1px;background:#e2e8f0;margin:14px 0 16px;"></div>', unsafe_allow_html=True)

    # Profile selector
    st.markdown(
        '<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;'
        'color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin:0 0 10px;">Perfil</p>',
        unsafe_allow_html=True
    )

    if is_locked:
        st.markdown(f"""
        <div style="background:#eff4fb;border:1.5px solid rgba(30,58,95,.2);border-radius:10px;
             padding:10px 14px;font-size:13px;font-weight:600;color:#1e3a5f;margin-bottom:14px;
             font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
          {profile_names[default_idx]}
        </div>""", unsafe_allow_html=True)
        selected_profile = profile_names[default_idx]
    else:
        selected_profile = st.radio(
            "Perfil",
            profile_names,
            index=default_idx,
            label_visibility="collapsed",
        )

    prof = PROFILES[selected_profile]

    st.markdown('<div style="height:1px;background:#e2e8f0;margin:14px 0 16px;"></div>', unsafe_allow_html=True)
    st.markdown(
        '<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;'
        'color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin:0 0 12px;">Filtros</p>',
        unsafe_allow_html=True
    )

    days_back = st.selectbox(
        "Período",
        [7, 14, 30, 60, 365],
        index=([7, 14, 30, 60, 365].index(prof["days"]) if prof["days"] in [7, 14, 30, 60, 365] else 0),
        format_func=lambda x: "Todo el historial" if x >= 365 else f"Últimos {x} días",
    )
    min_pem   = st.number_input("PEM mínimo (€)", value=prof["min_value"], min_value=0, step=50_000, format="%d")
    min_score = st.slider("Puntuación mínima", 0, 100, value=prof["min_score"], step=5)
    muni_sel  = st.multiselect("Municipio", options=all_munis, placeholder="Todos")

    st.markdown('<div style="height:1px;background:#e2e8f0;margin:14px 0 16px;"></div>', unsafe_allow_html=True)

    if st.button("🔄 Actualizar datos"):
        st.cache_data.clear()
        st.rerun()

    if not is_locked:
        with st.expander("🔗 Compartir con cliente"):
            pk = prof["key"]
            st.code(f"planningscout.streamlit.app?perfil={pk}", language=None)
            st.caption("El cliente abre este enlace en su navegador — sin cuenta, sin login.")

    # Last update info
    last_dt  = df["fecha_dt"].max() if "fecha_dt" in df.columns else None
    last_str = last_dt.strftime("%d %b %Y") if pd.notna(last_dt) else "—"
    st.markdown(f"""
    <div style="margin-top:16px;padding:12px 14px;background:#f7f8fa;border-radius:8px;border:1px solid #e2e8f0;">
      <p style="font-family:'JetBrains Mono',monospace;font-size:9.5px;color:#94a3b8;
         text-transform:uppercase;letter-spacing:.07em;margin:0 0 3px;">Última actualización</p>
      <p style="font-size:13px;font-weight:600;color:#334155;margin:0;">{last_str}</p>
      <p style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;margin:4px 0 0;">
        BOCM · Comunidad de Madrid</p>
    </div>""", unsafe_allow_html=True)

    # ── Session info + logout + password change (bottom of sidebar) ──────────
    _umail = st.session_state.get("user_email", "")
    if _umail and not _umail.startswith("token:"):
        st.markdown('<div style="height:1px;background:#e2e8f0;margin:20px 0 14px;"></div>', unsafe_allow_html=True)
        _udisplay = _umail if len(_umail) <= 30 else _umail[:27] + "\u2026"
        st.markdown(f"""
<div style="background:#eff4fb;border:1px solid rgba(30,58,95,.15);border-radius:8px;
     padding:8px 12px;margin-bottom:6px;">
  <p style="font-family:'JetBrains Mono',monospace;font-size:9px;color:#94a3b8;
     text-transform:uppercase;letter-spacing:.07em;margin:0 0 2px;">Sesi\u00f3n activa</p>
  <p style="font-size:12px;font-weight:600;color:#1e3a5f;margin:0;
     overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_udisplay}</p>
</div>""", unsafe_allow_html=True)
        if st.button("\u21a9 Cerrar sesi\u00f3n", key="logout_btn"):
            st.session_state["authenticated"] = False
            st.session_state["user_email"]    = ""
            st.session_state["login_error"]   = ""
            st.session_state["user_perfil"]   = ""
            st.rerun()

        # ── Password change ──
        with st.expander("\U0001f511 Cambiar contrase\u00f1a"):
            _cp_cur = st.text_input("Contraseña actual",    type="password", key="cp_cur", placeholder="••••••••")
            _cp_new = st.text_input("Nueva contraseña",     type="password", key="cp_new", placeholder="••••••••")
            _cp_cnf = st.text_input("Confirmar contraseña", type="password", key="cp_cnf", placeholder="••••••••")
            if st.button("Guardar nueva contraseña", key="cp_save"):
                _cur_ok = _all_pw.get(_umail) == _cp_cur
                if not _cp_cur or not _cur_ok:
                    st.error("Contraseña actual incorrecta.")
                elif len(_cp_new) < 6:
                    st.error("La nueva contraseña debe tener al menos 6 caracteres.")
                elif _cp_new != _cp_cnf:
                    st.error("Las contraseñas nuevas no coinciden.")
                else:
                    if update_password_in_sheet(_umail, _cp_new):
                        load_users_from_sheet.clear()
                        st.success("\u2713 Contrase\u00f1a actualizada correctamente.")
                    else:
                        st.warning("No se encontr\u00f3 tu cuenta. Contacta con soporte.")
            st.caption("La nueva contrase\u00f1a se activa de inmediato.")

# ════════════════════════════════════════════════════════════
# MAIN CONTENT
# ════════════════════════════════════════════════════════════
emoji_part = selected_profile.split()[0]
name_part  = " ".join(selected_profile.split()[1:])

st.markdown(f"""
<div style="margin-bottom:24px;padding-bottom:18px;border-bottom:1px solid #e2e8f0;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
    <span style="font-size:24px;">{emoji_part}</span>
    <h1 style="font-family:'Fraunces',Georgia,serif;font-size:26px;font-weight:700;
         color:#0d1a2b;margin:0;line-height:1.2;">{name_part}</h1>
  </div>
  <p style="font-size:13px;color:#64748b;margin:0;font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
    {"Todo el historial disponible" if days_back >= 365 else f"Últimas {days_back // 7} semanas" if days_back >= 14 else f"Últimos {days_back} días"} &nbsp;·&nbsp; Proyectos detectados del BOCM (Comunidad de Madrid)
  </p>
</div>""", unsafe_allow_html=True)

# ── Filter data ──
cutoff = datetime.now() - timedelta(days=days_back)
df_f   = df[df["fecha_dt"] >= cutoff].copy() if "fecha_dt" in df.columns else df.copy()

if min_score > 0:
    df_f = df_f[(df_f["score"] >= min_score) | (df_f["score"] == 0)]
# ── PEM filter: show if PEM meets threshold OR PEM is 0 (not declared in text)
# Most urbanización/plan especial BOCM texts do NOT include the PEM value —
# it is only in the PDF annex. Excluding pem=0 removes ~80% of valid leads.
# PEM filter uses pem_combined — declared (col F) OR estimated (col R) treated equally
if min_pem > 0:
    df_f = df_f[df_f["pem_combined"] >= min_pem]

if prof["types"] and "tipo" in df_f.columns:
    pat  = "|".join(re.escape(t) for t in prof["types"])
    df_f = df_f[df_f["tipo"].str.contains(pat, case=False, na=False)]

if muni_sel and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(muni_sel)]

df_f = df_f.sort_values(["score", "pem_combined"], ascending=[False, False]).reset_index(drop=True)

# ── Metrics ──
total_pem  = df_f["pem_combined"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])
avg_score  = int(df_f["score"].mean()) if count > 0 else 0

c1, c2 = st.columns(2)
for col, (val, lbl, clr) in zip(
    [c1, c2],
    [
        (str(count),      "Proyectos detectados", "#1e3a5f"),
        (str(high_leads), "🟢 Prioritarios",      "#16a34a"),
    ]
):
    with col:
        st.markdown(f"""
        <div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;
             padding:16px 20px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,.04);">
          <span style="font-family:'Fraunces',Georgia,serif;font-size:26px;font-weight:700;
                color:{clr};line-height:1;display:block;margin-bottom:5px;">{val}</span>
          <span style="font-family:'JetBrains Mono',monospace;font-size:9.5px;
                color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;">{lbl}</span>
        </div>""", unsafe_allow_html=True)

# ── Tip ──
st.markdown(
    f'<div style="background:#fffbeb;border-left:3px solid #c8860a;border-radius:0 8px 8px 0;'
    f'padding:12px 16px;font-size:13px;color:#64748b;line-height:1.6;margin:18px 0;'
    f'font-family:\'Plus Jakarta Sans\',system-ui,sans-serif;">{prof["tip"]}</div>',
    unsafe_allow_html=True
)

# ── Export ──
if not df_f.empty:
    exp_cols = [c for c in ["fecha","municipio","direccion","promotor","tipo","pem_raw","descripcion","expediente","bocm_url"] if c in df_f.columns]
    csv = df_f[exp_cols].to_csv(index=False).encode("utf-8")
    col_dl, _ = st.columns([1, 3])
    with col_dl:
        st.download_button(
            f"⬇️ Exportar {count} leads CSV",
            data=csv,
            file_name=f"planningscout_{prof['key']}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── Leads ──
if df_f.empty:
    st.markdown(f"""
    <div style="text-align:center;padding:56px 24px;background:#fff;
         border:1.5px solid #e2e8f0;border-radius:14px;
         font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
      <div style="font-size:40px;">🔍</div>
      <h3 style="font-family:'Fraunces',Georgia,serif;font-size:19px;
          color:#0d1a2b;margin:14px 0 8px;">Sin proyectos con estos filtros</h3>
      <p style="font-size:13px;color:#64748b;line-height:1.6;margin:0;">
        Amplía el período (ahora: {days_back} días),<br>
        reduce el PEM mínimo ({fmt(min_pem)}),<br>
        o cambia el perfil en el panel izquierdo.
      </p>
    </div>""", unsafe_allow_html=True)
else:
    st.markdown(
        f'<div style="display:flex;align-items:center;justify-content:space-between;'
        f'margin:0 0 14px;padding-bottom:12px;border-bottom:1px solid #e2e8f0;">'
        f'<h2 style="font-family:\'Fraunces\',Georgia,serif;font-size:19px;font-weight:700;'
        f'color:#0d1a2b;margin:0;">Proyectos detectados</h2>'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:11px;'
        f'background:#1e3a5f;color:#fff;padding:4px 12px;border-radius:100px;">'
        f'{count} resultado{"s" if count != 1 else ""}</span>'
        f'</div>',
        unsafe_allow_html=True
    )

    for _, row in df_f.iterrows():
        st.markdown(build_card(row.to_dict()), unsafe_allow_html=True)

# ── Footer ──
st.markdown(f"""
<div style="text-align:center;padding:28px 0 8px;margin-top:28px;border-top:1px solid #e2e8f0;
     font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;line-height:1.9;">
  <strong style="color:#5a5a78;font-size:11px;">PlanningScout Madrid</strong><br>
  Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales<br>
  PEM = Presupuesto de Ejecución Material · {count} proyectos · {last_str}
</div>
""", unsafe_allow_html=True)
