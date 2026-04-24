import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import re
import hashlib
import hmac as _hmac_mod
import secrets as _secrets_mod
from urllib.parse import unquote
import html as html_lib
import html as _html_esc  # alias used in card builder
import os
import base64
import urllib.parse   # used in geocoding

# ════════════════════════════════════════════════════════════
# SESSION PERSISTENCE — stateless signed tokens (12 hours)
#
# Token format:  base64(email|perfil|expires_unix).HMAC_SHA256
# Verified with a secret key stored in st.secrets["SESSION_SECRET"].
# Works across process restarts (no in-memory dict needed).
# ════════════════════════════════════════════════════════════
_SESSION_HOURS = 12


def _session_secret() -> str:
    try: return str(st.secrets.get("SESSION_SECRET", "ps-session-2026-default"))
    except Exception: return "ps-session-2026-default"


def _make_session_token(email: str, perfil: str) -> str:
    """Create a signed session token valid for SESSION_HOURS hours."""
    expires = int((datetime.now() + timedelta(hours=_SESSION_HOURS)).timestamp())
    payload = f"{email}|{perfil}|{expires}"
    sig = _hmac_mod.new(
        _session_secret().encode(), payload.encode(), hashlib.sha256
    ).hexdigest()[:32]
    raw = f"{payload}.{sig}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _verify_session_token(token: str):
    """Verify token. Returns (email, perfil) or (None, None)."""
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
        payload, sig = raw.rsplit(".", 1)
        expected = _hmac_mod.new(
            _session_secret().encode(), payload.encode(), hashlib.sha256
        ).hexdigest()[:32]
        if not _hmac_mod.compare_digest(sig, expected):
            return None, None
        parts = payload.split("|")
        if len(parts) != 3: return None, None
        email, perfil, expires_str = parts
        if datetime.now().timestamp() > float(expires_str):
            return None, None   # expired
        return email, perfil
    except Exception:
        return None, None

# ── Auto-install folium if not present ──────────────────────
try:
    import folium
    from streamlit_folium import st_folium
    _FOLIUM_OK = True
except ImportError:
    try:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "folium", "streamlit-folium", "-q"])
        import folium
        from streamlit_folium import st_folium
        _FOLIUM_OK = True
    except Exception:
        _FOLIUM_OK = False

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
url_profile = unquote(qp.get("perfil", ""))
url_session = qp.get("s", "")   # 12-hour signed session token

client_tokens = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

# ── Initialise session state ──
for _k, _v in [("authenticated", False), ("user_email", ""), ("login_error", ""),
               ("user_perfil", ""), ("_transitioning", False), ("_session_tok", "")]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── Restore session from signed URL token (survives page refresh + process restart) ──
if not st.session_state["authenticated"] and url_session:
    _tok_email, _tok_perfil = _verify_session_token(url_session)
    if _tok_email:
        st.session_state["authenticated"] = True
        st.session_state["user_email"]    = _tok_email
        st.session_state["user_perfil"]   = _tok_perfil
        st.session_state["_session_tok"]  = url_session

# ── Process in-card action links (toggle follow / set priority) ───────────────
# These use ?s=SESSION&ps_action=toggle|setprio&exp=...&ue=...&tok=...
# The ?s= is preserved so authentication survives the click.
def _process_card_action():
    _act    = st.query_params.get("ps_action","")
    _a_exp  = st.query_params.get("exp","")
    _a_ue   = st.query_params.get("ue","")
    _a_tok  = st.query_params.get("tok","")
    _a_pv   = st.query_params.get("pv","")
    if not (_act and _a_exp and _a_ue and _a_tok):
        return
    try:
        _email = base64.urlsafe_b64decode((_a_ue + "==").encode()).decode()
        _secret = "ps-seguir-2026"
        try: _secret = str(st.secrets.get("SEGUIR_SECRET","ps-seguir-2026"))
        except: pass
        _sig_data = f"{_email}:{_a_exp}:{_act}:{_a_pv}:{_secret}"
        _expected = hashlib.sha256(_sig_data.encode()).hexdigest()[:20]
        if _expected != _a_tok:
            pass  # silently ignore bad tokens
        elif _act == "toggle":
            # Toggle follow state
            _wl = load_watchlist(_email)
            if _a_exp in _wl:
                remove_from_watchlist(_email, _a_exp)
                st.session_state.setdefault("just_removed", set()).add(_a_exp)
                st.session_state.get("just_saved", set()).discard(_a_exp)
            else:
                add_to_watchlist(_email, {"expediente": _a_exp, "bocm_url": ""})
                st.session_state.setdefault("just_saved", set()).add(_a_exp)
                st.session_state.get("just_removed", set()).discard(_a_exp)
            load_watchlist.clear()
        elif _act == "setprio" and _a_pv in ("0","1","2","3"):
            update_watchlist_row(_email, _a_exp, priority=int(_a_pv))
        # Pop action params, keep ?s= session token intact
        for _k in ("ps_action","exp","ue","tok","pv"):
            st.query_params.pop(_k, None)
        st.rerun()
    except Exception:
        for _k in ("ps_action","exp","ue","tok","pv"):
            st.query_params.pop(_k, None)

_process_card_action()

# ── Transition intercept ── Must be FIRST content check after state init.
# When _transitioning=True the previous cycle just authenticated the user.
# Render ONLY a full-page spinner for this cycle, clear the flag, then rerun
# into the dashboard. This guarantees zero flash: the browser never sees the
# login card and dashboard content in the same render cycle.
if st.session_state.get("_transitioning"):
    st.session_state["_transitioning"] = False
    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600&display=swap');
.stApp {{ background: #f0f2f5 !important; }}
[data-testid="stSidebar"], header[data-testid="stHeader"] {{ display: none !important; }}
.block-container {{ background: transparent !important; border: none !important;
    box-shadow: none !important; padding: 0 !important; max-width: 100% !important; }}
@keyframes _spin {{ to {{ transform: rotate(360deg); }} }}

/* Seguir / Siguiendo — compact pill-style */
[data-testid="stButton"] button[kind="secondary"] {
    font-size: 11px !important;
    padding: 2px 10px !important;
    height: 28px !important;
    line-height: 1 !important;
    border-radius: 100px !important;
}
</style>
<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;
     background:#f0f2f5;">
  <div style="text-align:center;">
    <div style="margin:0 auto 20px;">
      {LOGO_HTML}
    </div>
    <div style="width:32px;height:32px;border:3px solid #e2e8f0;border-top-color:#1e3a5f;
         border-radius:50%;animation:_spin .7s linear infinite;margin:0 auto 16px;"></div>
    <p style="font-family:'Plus Jakarta Sans',system-ui,sans-serif;font-size:14px;
       color:#64748b;margin:0;">Cargando tu radar&hellip;</p>
  </div>
</div>""", unsafe_allow_html=True)
    st.rerun()
    st.stop()

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
            # Create stateless signed token — works across process restarts
            _new_tok = _make_session_token(_e, _assigned)
            st.session_state["authenticated"]  = True
            st.session_state["user_email"]     = _e
            st.session_state["login_error"]    = ""
            st.session_state["user_perfil"]    = _assigned
            st.session_state["_session_tok"]   = _new_tok
            st.session_state["_transitioning"] = True
            st.query_params["s"] = _new_tok
            log_activity(_e, "login")
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

/* Watchlist / follow button — small, subtle, attached to card */
button[kind="secondary"]:has(> div > p:contains("🔖")) {
    font-size: 11px !important;
    padding: 4px 10px !important;
    background: #f8fafc !important;
    color: #64748b !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 0 0 8px 8px !important;
    margin-top: -2px !important;
}

/* Mobile */
@media (max-width: 768px) {
    .block-container {
        padding-left: 16px !important;
        padding-right: 16px !important;
        padding-top: 14px !important;
    }
}

/* Hide Streamlit "Press Enter to apply" hint */
[data-testid="InputInstructions"] { display: none !important; }

/* Seguir / Siguiendo st.button — sits just below the card, small and clean */
/* Only targets the specific narrow column (1 of 8 split) used in the render loop */
.seguir-col button {
    font-size: 12px !important;
    font-weight: 600 !important;
    padding: 5px 14px !important;
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
    "🏗️ Gran Infraestructura": {
        "key": "infrastructura",
        "tip": "💡 <strong>Aprobación definitiva = licitación en 12-18 meses.</strong> Las Tablas Oeste €106M, Los Cerros, Tres Cantos UE.5 — las Juntas de Compensación activas son tu señal de máxima prioridad.",
        "min_score": 35, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial", "plan especial / parcial", "plan parcial",
                  "licitación de obras", "contribuciones especiales"],
    },
    "🏢 Gran Constructora": {
        "key": "constructora",
        "tip": "💡 <strong>Aprobación definitiva = licitación en 12-18 meses.</strong> Prepara dossier técnico y alianzas antes que la competencia.",
        "min_score": 30, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial", "plan especial / parcial", "plan parcial",
                  "obra mayor industrial", "obra mayor nueva construcción", "licitación de obras",
                  "contribuciones especiales", "demolición y nueva planta"],
    },
    "🏪 Expansión Retail": {
        "key": "expansion",
        "tip": "💡 <strong>Una urbanización aprobada = nuevo barrio en 2-3 años.</strong> Identifica tu próxima apertura antes de que suba el precio del suelo y la competencia ocupe los locales.",
        "min_score": 0, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan especial", "plan especial / parcial", "plan parcial",
                  "cambio de uso", "licencia de actividad", "obra mayor nueva construcción",
                  "licitación de obras"],
    },
    "📐 Promotores / RE": {
        "key": "promotores",
        "tip": "💡 <strong>Reparcelación aprobada = suelo a precio de coste.</strong> Contacta a la Junta de Compensación antes de que la operación salga al mercado.",
        "min_score": 15, "min_value": 0, "days": 365,
        "types": ["urbanización", "plan parcial", "plan especial", "plan especial / parcial",
                  "obra mayor nueva construcción", "cambio de uso"],
    },
    "🔧 Instaladores MEP": {
        "key": "instaladores",
        "tip": "💡 <strong>Obra mayor = instalaciones eléctricas, HVAC, PCI y ascensores.</strong> Contacta al promotor en fase definitiva antes de que el constructor adjudique instalaciones.",
        "min_score": 0, "min_value": 80_000, "days": 365,
        "types": ["obra mayor nueva construcción", "obra mayor rehabilitación",
                  "declaración responsable", "declaración responsable obra mayor",
                  "licencia primera ocupación", "urbanización", "demolición y nueva planta"],
    },
    "🏭 Industrial / Log.": {
        "key": "industrial",
        "tip": "💡 <strong>Corredor logístico sur (Valdemoro, Getafe) y este (Coslada, Alcalá).</strong> Licencia de nave = obra en 3-6 meses. Sé el primero en llamar al promotor.",
        "min_score": 0, "min_value": 200_000, "days": 365,
        "types": ["obra mayor industrial", "urbanización", "obra mayor nueva construcción",
                  "cambio de uso", "licitación de obras"],
    },
    "🚧 Alquiler Maquinaria": {
        "key": "alquiler",
        "tip": "💡 <strong>Llega al constructor 30-60 días antes que tu competencia.</strong> Licitación adjudicada = llama al ganador hoy para excavadoras y plataformas.",
        "min_score": 0, "min_value": 200_000, "days": 60,
        "types": ["urbanización", "obra mayor nueva construcción", "obra mayor industrial",
                  "licitación de obras", "demolición y nueva planta", "obra mayor rehabilitación",
                  "declaración responsable", "declaración responsable obra mayor",
                  "plan especial / parcial", "plan especial", "contribuciones especiales"],
    },
    "🛒 Compras / Materiales": {
        "key": "compras",
        "tip": "💡 <strong>Cada urbanización = kilómetros de tubería, hormigón y áridos.</strong> Preséntate al promotor antes de que la constructora adjudique suministros.",
        "min_score": 0, "min_value": 150_000, "days": 365,
        "types": [],
    },
    "💼 Contract & Oficinas": {
        "key": "actiu",
        "tip": "💡 <strong>Primera ocupación = el edificio está terminado.</strong> Contacta al promotor antes de que cierre el contrato de mobiliario y equipamiento.",
        "min_score": 0, "min_value": 200_000, "days": 365,
        "types": ["obra mayor nueva construcción", "obra mayor rehabilitación",
                  "cambio de uso", "declaración responsable", "declaración responsable obra mayor",
                  "licencia primera ocupación", "urbanización"],
        "profile_fit_filter": "actiu",
    },
    "🏠 Flexliving & Hostelería": {
        "key": "hospe",
        "tip": "💡 <strong>Cambio de uso = señal de máxima prioridad.</strong> Primera ocupación = llama al promotor HOY — el edificio está listo y necesita operador.",
        "min_score": 0, "min_value": 0, "days": 365,
        "types": ["cambio de uso", "licencia primera ocupación", "declaración responsable",
                  "declaración responsable obra mayor", "obra mayor rehabilitación",
                  "obra mayor nueva construcción"],
        "profile_fit_filter": "hospe",
    },
    "🏙️ Vista General": {
        "key": "general",
        "tip": "Vista completa de todos los proyectos del BOCM. Selecciona un perfil para filtrar por sector.",
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


def _card_action_url(action: str, email: str, exp: str, sess_tok: str, pv: str = "") -> str:
    """
    Build a signed URL for an in-card action link.
    Includes the current ?s= session token so auth is preserved on click.
    Signature: HMAC over email:exp:action:pv:secret
    """
    _secret = "ps-seguir-2026"
    try: _secret = str(st.secrets.get("SEGUIR_SECRET","ps-seguir-2026"))
    except: pass
    _sig_data = f"{email}:{exp}:{action}:{pv}:{_secret}"
    _tok = hashlib.sha256(_sig_data.encode()).hexdigest()[:20]
    _ue  = base64.urlsafe_b64encode(email.encode()).decode().rstrip("=")
    _exp_q = urllib.parse.quote(exp)
    base = f"?s={urllib.parse.quote(sess_tok)}&ps_action={action}&exp={_exp_q}&ue={_ue}&tok={_tok}"
    if pv:
        base += f"&pv={pv}"
    return base


# ── Compact score circle for list rows ───────────────────────
_SC_COL = {True: "#15803d", False: "#b45309"}   # high / mid
def _score_circle(sc: int) -> str:
    """Small filled circle with score — matches website mini-card look."""
    if sc >= 65:   bg, txt = "#15803d", "#fff"
    elif sc >= 40: bg, txt = "#b45309", "#fff"
    elif sc >= 20: bg, txt = "#1e3a5f", "#fff"
    else:          bg, txt = "#e2e8f0", "#94a3b8"
    return (
        f'<div style="min-width:46px;width:46px;height:46px;border-radius:50%;'
        f'background:{bg};display:flex;flex-direction:column;align-items:center;'
        f'justify-content:center;flex-shrink:0;">'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:13px;'
        f'font-weight:700;color:{txt};line-height:1;">{sc}</span>'
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:8px;'
        f'font-weight:400;color:{txt};opacity:.75;line-height:1;">pts</span>'
        f'</div>'
    )


def build_compact_row(row: dict, full_card_html: str) -> str:
    """
    Compact clickable summary row (like the website screenshot) wrapping the full card.
    Uses native HTML <details>/<summary> — zero JS, works in all browsers,
    safe within Streamlit's markdown renderer.

    Visual: [score circle] [BOCM ref · Municipality] [title] [tipo / fase / PEM tags]
    Click anywhere on the row → expands to show full card below.
    """
    sc    = parse_sc(row.get("score_raw", 0))
    muni  = html_lib.escape(str(row.get("municipio","") or "Madrid").strip())
    addr  = html_lib.escape(str(row.get("direccion","") or "").strip())
    desc  = html_lib.escape(str(row.get("descripcion","") or "").strip())
    tipo  = html_lib.escape(str(row.get("tipo","") or "").strip())
    bocm  = str(row.get("bocm_url","") or "").strip()
    # Date: always use col A "Date Granted" (fecha) — not "Date Found"
    fecha_granted = str(row.get("fecha","") or "").strip()
    pem_est_text = str(row.get("pem_est_raw","") or "").strip()
    pem_raw_v    = parse_val(row.get("pem_raw",""))
    pem_est_v    = parse_est_pem_numeric(pem_est_text)
    pem_display  = pem_raw_v if pem_raw_v > 0 else pem_est_v

    # BOCM ref — date from col A only
    ref = ""
    m_bocm = re.search(r'BOCM[-_](\d{8})', bocm, re.I)
    if m_bocm: ref = f"BOCM-{m_bocm.group(1)}"
    if fecha_granted:
        try:
            dt = datetime.strptime(fecha_granted[:10], "%Y-%m-%d")
            ref += f" · {dt.strftime('%-d %b %Y')}" if ref else dt.strftime("%-d %b %Y")
        except Exception:
            pass
    if muni: ref += f" · {muni}" if ref else muni

    title = addr or desc[:80] or tipo or "Proyecto"
    if len(title) > 72: title = title[:72] + "…"

    # Tags
    tl_lower = tipo.lower() + " " + desc.lower()
    tags = []
    if tipo:
        tags.append(f'<span style="{SSBN}">{tipo[:24]}</span>')
    if "definitiv" in tl_lower:
        tags.append(f'<span style="{SSBG}">Aprobación definitiva</span>')
    elif "inicial" in tl_lower:
        tags.append(f'<span style="{SSBA}">Aprobación inicial</span>')
    if pem_display > 0:
        pem_s = fmt(pem_display)
        is_est = pem_raw_v == 0
        _pem_style = (f"{_FM};font-size:10px;font-weight:600;padding:4px 10px;border-radius:100px;"
                      f"white-space:nowrap;background:#fffbeb;color:#b45309;border:1px solid #fde68a;")
        _pem_suffix = " Est." if is_est else ""
        tags.append(f'<span style="{_pem_style}">{pem_s}{_pem_suffix}</span>')

    tags_html = "".join(tags)

    circle_html = _score_circle(sc)

    # Summary row — styled to match website mini-card
    _sum_style = (
        "list-style:none;display:flex;align-items:center;gap:12px;padding:12px 16px;"
        "cursor:pointer;background:#fff;user-select:none;-webkit-user-select:none;"
        "outline:none;"
    )
    _ref_s = (
        f"font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;"
        f"letter-spacing:.03em;margin-bottom:3px;"
    )
    _title_s = (
        f"font-family:'Fraunces',Georgia,serif;font-size:14px;font-weight:600;"
        f"color:#0d1a2b;line-height:1.35;margin-bottom:5px;"
    )

    summary_html = (
        f'<summary style="{_sum_style}">'
        f'{circle_html}'
        f'<div style="flex:1;min-width:0;">'
        f'<div style="{_ref_s}">{ref}</div>'
        f'<div style="{_title_s}">{title}</div>'
        f'<div style="display:flex;gap:5px;flex-wrap:wrap;">{tags_html}</div>'
        f'</div>'
        f'<span style="font-size:11px;color:#94a3b8;flex-shrink:0;transition:transform .15s;" '
        f'class="toggle-arrow">▼</span>'
        f'</summary>'
    )

    # Wrapper: card border, subtle hover, open state highlights border
    return (
        f'<details style="background:#fff;border:1.5px solid #e2e8f0;border-radius:12px;'
        f'margin-bottom:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.04);">'
        f'{summary_html}'
        f'<div style="border-top:1px solid #f1f5f9;">'
        f'{full_card_html}'
        f'</div>'
        f'</details>'
    )

def build_card(row, is_watched=False, inside_details=False):
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

    # BOCM / BOE reference — date from col A "Date Granted" (fecha), never fecha_encontrado
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
    # Use col A (Date Granted = fecha) for the date shown in the card
    _date_granted = str(row.get("fecha","") or "").strip()
    if _date_granted:
        try:
            dt = datetime.strptime(_date_granted[:10], "%Y-%m-%d")
            ref_parts.append(f"Concedido: {dt.strftime('%-d %b %Y')}")
        except Exception:
            ref_parts.append(_date_granted[:10])
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

    # ── Urgency badge: days since publication ──
    _new_badge  = ""
    _days_badge = ""
    _pub_for_new = fnd[:10] if fnd else fecha
    if _pub_for_new:
        try:
            _pub_dt  = datetime.strptime(_pub_for_new, "%Y-%m-%d")
            _days_old = (datetime.now() - _pub_dt).days
            if _days_old <= 3:
                _new_badge = (
                    "<span style='font-family:\"JetBrains Mono\",monospace;font-size:9px;"
                    "font-weight:700;letter-spacing:.08em;text-transform:uppercase;"
                    "background:#dc2626;color:#fff;border-radius:4px;padding:2px 7px;"
                    "margin-right:4px;'>Nuevo</span>"
                )
            elif _days_old <= 7:
                _days_badge = (
                    f"<span style='font-family:\"JetBrains Mono\",monospace;font-size:9px;"
                    f"font-weight:600;background:#fef3c7;color:#b45309;border-radius:4px;"
                    f"padding:2px 7px;margin-right:4px;'>⏱ {_days_old}d</span>"
                )
            elif _days_old <= 14:
                _days_badge = (
                    f"<span style='font-family:\"JetBrains Mono\",monospace;font-size:9px;"
                    f"font-weight:600;background:#f1f5f9;color:#64748b;border-radius:4px;"
                    f"padding:2px 7px;margin-right:4px;'>{_days_old}d</span>"
                )
        except Exception:
            pass

    # ── Source badge (BOCM / BOE) ──
    _fuente = str(row.get("fuente", "") or "").strip()
    _fuente_badge = ""
    if _fuente == "BOE":
        _fuente_badge = (
            "<span style='font-family:\"JetBrains Mono\",monospace;font-size:9px;"
            "font-weight:600;background:#eff4fb;color:#1e3a5f;border-radius:4px;"
            "padding:2px 6px;margin-right:4px;'>BOE</span>"
        )

    # ─ HEADER — suppressed inside_details (already shown in the summary row) ─
    head = "" if inside_details else (
        f'<div style="{SH}">'
        f'  <div style="{SLO}">'
        f'    <div style="{SDO}"></div>'
        f'    <span style="{SMU}">{muni}</span>'
        f'  </div>'
        f'  <div style="{SBD}">{_new_badge}{_days_badge}{_fuente_badge}{sbadge}{sc_pill(sc)}</div>'
        f'</div>'
    )

    # ─ BODY ─
    # ref, title, desc_preview suppressed inside_details (already in summary row)
    ref_html   = "" if inside_details else (f'<div style="{SRF}">{ref_str}</div>' if ref_str else "")
    title_html = "" if inside_details else f'<div style="{STI}">{title}</div>'

    # ── Project size (new col W) ──
    _proj_size = esc(row.get("project_size", "") or "")
    _size_html = ""
    if _proj_size and _proj_size.lower() not in ("nan", "none", ""):
        _size_html = (
            f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:10.5px;'
            f'color:#64748b;margin-bottom:10px;padding:5px 8px;background:#f8fafc;'
            f'border-radius:6px;border-left:2px solid #e2e8f0;">'
            f'📐 {_proj_size}</div>'
        )
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

    # ── Key Contacts + Action Window — removed from card display (noise).
    # These fields were generating NameError when extras_html was used before definition.
    # extras_html defined here, before ANY extras_html += below.
    extras_html = ""

    _raw_kc = str(row.get("key_contacts", "") or "").strip()
    _raw_aw = str(row.get("action_window", "") or "").strip()
    _raw_ot = str(row.get("obra_timeline", "") or "").strip()
    # key_contacts and action_window are NOT rendered — removed per UX review.
    # The information exists in the data but clutters the card. Promotor button
    # covers contact discovery; urgency is handled by per-user Mis Alertas priorities.

    # ── Footer buttons: BOCM · Mapa · PDF · Promotor · [Seguir/Siguiendo] · source · Reportar
    # Seguir and Reportar live exclusively here — nothing outside the card.
    _src_label = "BOE" if bocm and (bocm.lower().startswith("https://www.boe.es") or bocm.lower().startswith("https://boe.es")) else "BOCM"
    _mailto = (
        f'mailto:info@planningscout.com'
        f'?subject={html_lib.escape("Lead: " + muni + " — " + (expd or ref_str[:30]))}'
        f'&body={html_lib.escape("Municipio: " + muni + chr(10) + "Dirección: " + addr + chr(10) + "Expediente: " + expd + chr(10) + "URL: " + bocm)}'
    )

    _seguir_el = ""
    _prio_group = ""

    _reportar_el = (
        f'<a href="{_mailto}" style="{_F};display:inline-flex;align-items:center;gap:3px;'
        f'font-size:11px;font-weight:500;color:#94a3b8;background:#f8fafc;border:1px solid #e2e8f0;'
        f'padding:4px 9px;border-radius:7px;text-decoration:none;white-space:nowrap;" '
        f'title="Reportar error o pedir más info">✉️ Reportar</a>'
    )

    footer = (
        f'<div style="{SFO}">'
        + "".join(links)
        + f'<span style="{SNO}">{_src_label}</span>'
        + " " + _reportar_el
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
        "solicitud":         ("⚡", "Pre-lead · En solicitud","#fffbeb", "#b45309", "#fde68a"),
        "adjudicacion":      ("🏆", "Adjudicación",           "#f0f9ff", "#0369a1", "#bae6fd"),
        "en_obra":           ("🏗️", "En obra",               "#eff4fb", "#1e3a5f", "#bfdbfe"),
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
        f'{_size_html}'
        f'{desc_preview_html}'
        f'{table_html}'
        f'</div>'
        f'{extras_html}'
        f'{footer}'
        f'</div>'
    )

# ════════════════════════════════════════════════════════════
# MAP HELPERS — coordinate extraction + geocoding
# ════════════════════════════════════════════════════════════
# Madrid region bounding box for sanity-checking coordinates
_MAD_LAT_MIN, _MAD_LAT_MAX = 39.8, 41.2
_MAD_LON_MIN, _MAD_LON_MAX = -4.6, -3.0

def _extract_coords_from_maps_url(maps_url):
    """Try to extract lat/lon from a Google Maps URL. Returns (lat, lon) or (None, None)."""
    if not maps_url:
        return None, None
    # Pattern 1: @lat,lon,zoom  (e.g. @40.4165,-3.7026,15z)
    m = re.search(r'@(-?\d{1,3}\.\d+),(-?\d{1,3}\.\d+)', maps_url)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        if _MAD_LAT_MIN <= lat <= _MAD_LAT_MAX and _MAD_LON_MIN <= lon <= _MAD_LON_MAX:
            return lat, lon
    # Pattern 2: ?q=lat,lon or &q=lat,lon
    m = re.search(r'[?&]q=(-?\d{1,3}\.\d+),(-?\d{1,3}\.\d+)', maps_url)
    if m:
        lat, lon = float(m.group(1)), float(m.group(2))
        if _MAD_LAT_MIN <= lat <= _MAD_LAT_MAX and _MAD_LON_MIN <= lon <= _MAD_LON_MAX:
            return lat, lon
    return None, None

def _extract_search_query_from_maps_url(maps_url):
    """Extract the search query text from a Google Maps search URL."""
    if not maps_url:
        return ""
    m = re.search(r'/search/([^?#]+)', maps_url)
    if m:
        return m.group(1).replace('+', ' ').strip()
    return ""

@st.cache_data(ttl=86400, show_spinner=False)
def _geocode_nominatim(query):
    """
    Geocode a free-text query using Nominatim (OpenStreetMap).
    Returns (lat, lon) or (None, None).
    Always appends ', Comunidad de Madrid, España' to bias results.
    Caches for 24h — no repeated calls for same address.
    """
    try:
        import urllib.request, json, time
        q = query.strip()
        if not q or len(q) < 4:
            return None, None
        # Strip urbanismo codes (UE.5, S-02, ZO.8, APE.08.24) that confuse geocoders
        q_clean = re.sub(r'\b(UE|ZO|S|UA|PE|PP|APE|ARE|SUS|SUB)[\.\-]?\d+(?:[\.\-]\d+)?\b', '', q, flags=re.I)
        q_clean = re.sub(r'\bUnidad de Ejecuci[oó]n\b', '', q_clean, flags=re.I)
        q_clean = re.sub(r'\bSector Urbanizable\b', '', q_clean, flags=re.I)
        q_clean = re.sub(r'\bPlan Especial\b', '', q_clean, flags=re.I)
        q_clean = re.sub(r'\s+', ' ', q_clean).strip(' ,.')
        if not q_clean or len(q_clean) < 3:
            return None, None
        # Add Madrid context
        full = q_clean + ", Comunidad de Madrid, España"
        url = (f"https://nominatim.openstreetmap.org/search"
               f"?q={urllib.parse.quote(full)}&format=json&limit=1&countrycodes=es")
        req = urllib.request.Request(url, headers={"User-Agent": "PlanningScout/1.0"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        if data:
            lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
            if _MAD_LAT_MIN <= lat <= _MAD_LAT_MAX and _MAD_LON_MIN <= lon <= _MAD_LON_MAX:
                return lat, lon
        return None, None
    except Exception:
        return None, None

# Madrid municipality centroids — instant fallback when address geocoding fails.
# Covers the most common BOCM municipalities for retail/expansion profiles.
_MUNI_CENTROIDS = {
    "madrid": (40.4168, -3.7038),
    "alcalá de henares": (40.4818, -3.3647),
    "alcobendas": (40.5499, -3.6414),
    "alcorcón": (40.3490, -3.8242),
    "algete": (40.5956, -3.4965),
    "arganda del rey": (40.3015, -3.4422),
    "aranjuez": (40.0332, -3.6019),
    "boadilla del monte": (40.4071, -3.8759),
    "brunete": (40.4014, -3.9976),
    "collado villalba": (40.6330, -4.0046),
    "coslada": (40.4227, -3.5650),
    "fuenlabrada": (40.2839, -3.7982),
    "galapagar": (40.5761, -4.0048),
    "getafe": (40.3053, -3.7326),
    "humanes de madrid": (40.2593, -3.8270),
    "las rozas de madrid": (40.4933, -3.8728),
    "leganés": (40.3283, -3.7640),
    "majadahonda": (40.4734, -3.8718),
    "mejorada del campo": (40.3961, -3.4920),
    "móstoles": (40.3220, -3.8642),
    "navalcarnero": (40.2851, -4.0127),
    "paracuellos de jarama": (40.5065, -3.5271),
    "parla": (40.2381, -3.7760),
    "pinto": (40.2427, -3.6974),
    "pozuelo de alarcón": (40.4349, -3.8131),
    "rivas-vaciamadrid": (40.3556, -3.5218),
    "san fernando de henares": (40.4245, -3.5368),
    "san sebastián de los reyes": (40.5534, -3.6281),
    "torrejón de ardoz": (40.4586, -3.4795),
    "tres cantos": (40.5951, -3.7078),
    "valdemoro": (40.1910, -3.6747),
    "velilla de san antonio": (40.3774, -3.5115),
    "villanueva de la cañada": (40.4521, -3.9849),
    "villanueva del pardillo": (40.4748, -3.9354),
    "ajalvir": (40.5415, -3.4632),
    "becerril de la sierra": (40.7188, -3.8906),
    "buitrago del lozoya": (40.9988, -3.6352),
    "casarrubuelos": (40.2020, -3.8890),
    "ciempozuelos": (40.1600, -3.6215),
    "collado mediano": (40.6972, -3.8844),
    "cubas de la sagra": (40.2158, -3.8384),
    "el boalo": (40.7019, -3.9027),
    "el molar": (40.7158, -3.5879),
    "fuente el saz de jarama": (40.6235, -3.4856),
    "griñón": (40.2125, -3.8684),
    "meco": (40.5530, -3.3350),
    "quijorna": (40.4168, -3.9900),
    "robledo de chavela": (40.5068, -4.2424),
    "san agustín del guadalix": (40.7107, -3.6171),
    "san martín de la vega": (40.2078, -3.5680),
    "sevilla la nueva": (40.3556, -3.9711),
    "soto del real": (40.7666, -3.7813),
    "torres de la alameda": (40.4284, -3.3774),
    "valdilecha": (40.3468, -3.2897),
    "villa del prado": (40.2762, -4.2777),
    "villalbilla": (40.4284, -3.3017),
    "villaviciosa de odón": (40.3556, -3.9003),

}

def _get_coords(row):
    """
    Get (lat, lon) for a row using a 3-tier fallback chain:
    1. Extract directly from the Maps Link URL (instant, most accurate)
    2. Geocode the address + municipality via Nominatim (cached 24h)
    3. Fall back to municipality centroid (always works, zone-level precision)
    Returns (lat, lon, precision) where precision is 'exact'|'geocoded'|'municipality'|None
    """
    maps = str(row.get("maps", "") or "").strip()
    muni = str(row.get("municipio", "") or "Madrid").strip()
    addr = str(row.get("direccion", "") or "").strip()

    # Tier 1: direct coords from Maps URL
    lat, lon = _extract_coords_from_maps_url(maps)
    if lat:
        return lat, lon, "exact"

    # Tier 2: geocode the search query embedded in the Maps URL
    query = _extract_search_query_from_maps_url(maps)
    if query:
        lat, lon = _geocode_nominatim(query)
        if lat:
            return lat, lon, "geocoded"

    # Tier 3: geocode address + municipality
    if addr and muni and len(addr) > 4:
        lat, lon = _geocode_nominatim(f"{addr}, {muni}")
        if lat:
            return lat, lon, "geocoded"

    # Tier 4: municipality centroid
    key = muni.lower().strip()
    if key in _MUNI_CENTROIDS:
        lat, lon = _MUNI_CENTROIDS[key]
        return lat, lon, "municipality"

    # Last resort: Madrid centre
    return 40.4168, -3.7038, "municipality"


# Colour palette for map pins — matches dashboard design system
_PIN_COLOURS = {
    # By score range
    "high":         "#16a34a",   # green — score ≥ 65
    "mid":          "#c8860a",   # amber — score 40-64
    "low":          "#64748b",   # grey  — score < 40
    # By profile (used in tooltip badge)
    "expansion":    "#0ea5e9",
    "promotores":   "#8b5cf6",
    "constructora": "#ef4444",
    "instaladores": "#f97316",
    "industrial":   "#6b7280",
    "infrastructura":"#1e3a5f",
    "alquiler":     "#b45309",
    "compras":      "#059669",
    "general":      "#64748b",
}

def _score_colour(score):
    if score >= 65: return _PIN_COLOURS["high"]
    if score >= 40: return _PIN_COLOURS["mid"]
    return _PIN_COLOURS["low"]

def _make_pin_icon(score, fase=""):
    """Create a styled DivIcon for the folium marker."""
    colour = _score_colour(score)
    # Special icon for pre-leads (solicitud)
    symbol = "⚡" if fase == "solicitud" else ("★" if score >= 65 else "●")
    return folium.DivIcon(
        html=f"""<div style="
            width:28px;height:28px;border-radius:50%;
            background:{colour};border:2px solid #fff;
            box-shadow:0 2px 6px rgba(0,0,0,.3);
            display:flex;align-items:center;justify-content:center;
            font-size:12px;color:#fff;font-weight:700;
            cursor:pointer;">
            {symbol}
        </div>""",
        icon_size=(28, 28),
        icon_anchor=(14, 14),
    )


def build_map(df_map, profile_key="general"):
    """
    Build and return a Folium map for the given filtered dataframe.
    Each lead becomes a marker with a popup showing key info and a link.
    """
    if not _FOLIUM_OK:
        return None

    # Filter to leads with mappable locations
    rows_with_loc = []
    with st.spinner("Cargando mapa…"):
        for _, row in df_map.iterrows():
            r = row.to_dict()
            # Fast path: skip Nominatim if we can get municipality centroid directly.
            # This makes map load in <2s instead of 30s+ for 50 rows.
            maps_url = str(r.get("maps", "") or "").strip()
            lat, lon = _extract_coords_from_maps_url(maps_url)
            if lat:
                rows_with_loc.append((row, lat, lon, "exact"))
                continue
            # Municipality centroid — instant
            muni_key = str(r.get("municipio", "") or "Madrid").lower().strip()
            if muni_key in _MUNI_CENTROIDS:
                lat, lon = _MUNI_CENTROIDS[muni_key]
                rows_with_loc.append((row, lat, lon, "municipality"))
                continue
            # Only fall back to Nominatim as last resort
            lat, lon, prec = _get_coords(r)
            rows_with_loc.append((row, lat, lon, prec))

    if not rows_with_loc:
        return None

    # Centre map on the mean of all points
    lats = [r[1] for r in rows_with_loc]
    lons = [r[2] for r in rows_with_loc]
    centre_lat = sum(lats) / len(lats)
    centre_lon = sum(lons) / len(lons)

    # Choose zoom based on spread
    lat_range = max(lats) - min(lats)
    zoom = 10 if lat_range > 0.5 else (11 if lat_range > 0.2 else 12)

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=zoom,
        tiles=None,
    )

    # Light CartoDB tile — clean, no clutter
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        attr="© OpenStreetMap contributors © CARTO",
        name="CartoDB Light",
        max_zoom=19,
    ).add_to(m)

    # Add markers
    for row, lat, lon, prec in rows_with_loc:
        r = row.to_dict()
        score   = int(r.get("score", 0) or 0)
        muni    = r.get("municipio", "Madrid") or "Madrid"
        addr    = r.get("direccion", "") or ""
        tipo    = r.get("tipo", "") or ""
        fase    = r.get("fase", "") or ""
        pem     = r.get("pem_combined", 0) or 0
        bocm    = r.get("bocm_url", "") or ""
        maps_u  = r.get("maps", "") or ""
        desc    = r.get("descripcion", "") or ""
        fecha   = r.get("fecha", "") or ""
        pz      = r.get("pem_est_raw", "") or ""   # Estimated PEM text

        # PEM display
        if pem >= 1_000_000:
            pem_s = f"€{pem/1_000_000:.1f}M"
        elif pem >= 1000:
            pem_s = f"€{int(pem/1000)}K"
        elif pz and "⚪" not in pz and pz.strip():
            pem_s = pz[:30]
        else:
            pem_s = "PEM no declarado"

        # Score badge colour
        sc_bg = "#dcfce7" if score >= 65 else ("#fef3c7" if score >= 40 else "#f1f5f9")
        sc_fg = "#16a34a" if score >= 65 else ("#b45309" if score >= 40 else "#64748b")

        # Precision indicator (only show when approximate)
        prec_note = ""
        if prec == "municipality":
            prec_note = f"<div style='font-size:10px;color:#94a3b8;margin-top:4px;'>📍 Ubicación aproximada ({muni})</div>"
        elif prec == "geocoded":
            prec_note = f"<div style='font-size:10px;color:#94a3b8;margin-top:4px;'>📍 Zona estimada</div>"

        # Pre-lead badge
        prelead_badge = ""
        if fase == "solicitud":
            prelead_badge = "<span style='background:#fef3c7;color:#b45309;font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;margin-right:4px;'>⚡ PRE-LEAD</span>"

        # Links
        link_bocm  = f'<a href="{bocm}" target="_blank" style="color:#1e3a5f;font-weight:600;font-size:12px;text-decoration:none;">↗ Ver BOCM</a>' if bocm else ""
        link_maps  = f'<a href="{maps_u}" target="_blank" style="color:#1e3a5f;font-weight:600;font-size:12px;text-decoration:none;margin-left:10px;">🗺️ Maps</a>' if maps_u else ""

        popup_html = f"""
<div style="font-family:'Plus Jakarta Sans',system-ui,sans-serif;min-width:260px;max-width:320px;">
  <div style="background:#f7f8fa;border-radius:8px 8px 0 0;padding:10px 12px;border-bottom:1px solid #e2e8f0;">
    <div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;">{muni}</div>
    <div style="font-weight:700;color:#0d1a2b;font-size:13px;margin-top:2px;line-height:1.3;">{addr[:60] or tipo[:60]}</div>
  </div>
  <div style="padding:10px 12px;">
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:8px;">
      {prelead_badge}
      <span style="background:{sc_bg};color:{sc_fg};font-size:11px;font-weight:700;padding:2px 8px;border-radius:100px;">{score} pts</span>
      <span style="background:#eff4fb;color:#1e3a5f;font-size:10px;padding:2px 8px;border-radius:100px;">{tipo[:28]}</span>
    </div>
    <div style="font-size:13px;font-weight:700;color:#1e3a5f;margin-bottom:6px;">{pem_s}</div>
    {prec_note}
  </div>
  {'<details style="border-top:1px solid #f1f5f9;"><summary style="padding:8px 12px;font-size:11px;font-weight:600;color:#334155;cursor:pointer;list-style:none;display:flex;align-items:center;gap:6px;"><span>📋</span><span>Descripción</span><span style="margin-left:auto;font-size:9px;color:#94a3b8;">▼</span></summary><div style="padding:4px 12px 10px;font-size:11px;color:#64748b;line-height:1.5;">' + desc[:300] + ('…' if len(desc) > 300 else '') + '</div></details>' if desc else ''}
  {'<details style="border-top:1px solid #f1f5f9;"><summary style="padding:8px 12px;font-size:11px;font-weight:600;color:#334155;cursor:pointer;list-style:none;display:flex;align-items:center;gap:6px;"><span>🤖</span><span>Análisis IA</span><span style="margin-left:auto;font-size:9px;color:#94a3b8;">▼</span></summary><div style="padding:4px 12px 10px;font-size:11px;color:#374151;line-height:1.5;background:#f8fafc;margin:0 8px 8px;border-radius:6px;">' + r.get("ai_evaluation","")[:350] + ('…' if len(r.get("ai_evaluation","")) > 350 else '') + '</div></details>' if r.get("ai_evaluation","") and str(r.get("ai_evaluation","")).lower() not in ("nan","none","") else ''}
  <div style="padding:10px 12px;border-top:1px solid #f1f5f9;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
    {link_bocm}{link_maps}
    <span style="font-size:10px;color:#94a3b8;margin-left:auto;">↩ Vuelve a 📋 Lista para la ficha completa</span>
  </div>
  <div style="padding:4px 12px 10px;font-size:10px;color:#94a3b8;">{fecha}</div>
</div>"""

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=310),
            tooltip=f"{muni} · {pem_s} · {score}pts",
            icon=_make_pin_icon(score, fase),
        ).add_to(m)

    return m, len(rows_with_loc)


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
    "Profile Fit": "profile_fit", "Fuente": "fuente",
    "Project Size":        "project_size",
    "Action Window":       "action_window",
    "Key Contacts":        "key_contacts",
    "Obra Timeline":       "obra_timeline",
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

    # ── Phase filter ──
    _FASE_OPTIONS = {
        "definitivo":        "🟢 Aprobación definitiva",
        "licitacion":        "🔵 Licitación activa",
        "adjudicacion":      "🏆 Adjudicación",
        "en_obra":           "🏗️ En obra",
        "primera_ocupacion": "🏠 1ª Ocupación",
        "inicial":           "🟡 Aprobación inicial",
        "en_tramite":        "🟠 En trámite",
        "solicitud":         "⚡ Pre-lead (solicitud)",
    }
    fase_sel = st.multiselect(
        "Fase del proyecto",
        options=list(_FASE_OPTIONS.keys()),
        format_func=lambda k: _FASE_OPTIONS[k],
        placeholder="Todas las fases",
    )

    muni_sel  = st.multiselect("Municipio", options=all_munis, placeholder="Todos")
    st.caption(f"📍 {len(all_munis)} municipios con datos en el período seleccionado")
    aw_sel = []  # Urgencia removed — field data not reliable enough yet

    # ── Keyword search ──
    st.markdown(
        '<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;'
        'color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin:8px 0 4px;">Buscar</p>',
        unsafe_allow_html=True
    )
    kw_search = st.text_input(
        "Buscar",
        placeholder="promotor, calle, tipo…",
        label_visibility="collapsed",
        key="kw_search_input",
    ).strip().lower()

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
            st.session_state["_session_tok"]  = ""
            # Removing the ?s= param invalidates the session token in the URL
            st.query_params.pop("s", None)
            st.rerun()



# ════════════════════════════════════════════════════════════
# WATCHLIST — Save alert for a project
# ════════════════════════════════════════════════════════════
def _get_sheet_connection():
    """Get gspread spreadsheet object for watchlist write-back."""
    try:
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        return gc.open_by_key(st.secrets.get("SHEET_ID", SHEET_ID))
    except Exception:
        return None


@st.cache_data(ttl=60)
def load_watchlist(user_email: str) -> list:
    """Load deduplicated watchlist expediente list for this user."""
    try:
        ss = _get_sheet_connection()
        if not ss: return []
        try: ws = ss.worksheet("Watchlist")
        except Exception: return []
        rows = ws.get_all_records()
        seen, result = set(), []
        for r in rows:
            if r.get("email","").lower() != user_email.lower(): continue
            exp = str(r.get("expediente","") or "").strip()
            if exp and exp not in seen:
                seen.add(exp); result.append(exp)
        return result
    except Exception:
        return []


# NO @st.cache_data here — this function must always return fresh data.
# Caching it (even with .clear()) causes a same-rerun stale-read bug in Streamlit Cloud:
# after add_to_watchlist writes to sheet and calls .clear(), a subsequent call in the
# SAME rerun cycle can still return the pre-write cached value. For a small per-user
# list this uncached call costs ~0.3s which is acceptable.
def load_watchlist_full(user_email: str) -> list:
    """Load full watchlist rows (notes + priority) for Mis Alertas tab. Always fresh."""
    try:
        ss = _get_sheet_connection()
        if not ss: return []
        try: ws = ss.worksheet("Watchlist")
        except Exception: return []
        rows = ws.get_all_records()
        seen, result = set(), []
        for r in rows:
            if r.get("email","").lower() != user_email.lower(): continue
            exp = str(r.get("expediente","") or "").strip()
            if exp and exp not in seen:
                seen.add(exp); result.append(r)
        return result
    except Exception:
        return []


def add_to_watchlist(user_email: str, row: dict) -> bool:
    """Add a project to watchlist. Uses stable key (expediente or BOCM slug)."""
    try:
        ss = _get_sheet_connection()
        if not ss: return False
        try: ws = ss.worksheet("Watchlist")
        except Exception:
            ws = ss.add_worksheet("Watchlist", rows=500, cols=10)
            ws.append_row(["email","source_url","expediente","fecha_added",
                           "phase_at_add","last_alerted","muni","description","notes","priority"])
        bocm  = str(row.get("bocm_url","") or "").strip()
        fase  = str(row.get("fase","") or "").strip()
        muni  = str(row.get("municipio","") or "").strip()
        desc  = str(row.get("descripcion","") or "")[:150]
        today = datetime.now().strftime("%Y-%m-%d")

        # Stable unique key: expediente → BOCM slug → hash.
        # Guarantees every card can be saved, not just those with a formal expediente.
        exp = str(row.get("expediente","") or "").strip()
        if not exp or exp.lower() in ("nan","none"):
            _slug_m = re.search(r'BOCM[-_](\d{8}[-_]?\d*)', bocm, re.I)
            if _slug_m:
                exp = f"BOCM-{_slug_m.group(1).replace('_','-')}"
            elif bocm:
                exp = f"BOCM-{abs(hash(bocm)) % 10**10}"
        if not exp:
            return False

        # Idempotency — don't add same user+exp twice
        try:
            existing = ws.get_all_records()
            for r in existing:
                if (r.get("email","").lower() == user_email.lower() and
                        str(r.get("expediente","")).strip() == exp):
                    load_watchlist.clear(); return True
        except Exception:
            pass  # if check fails, proceed to write (may create duplicate — acceptable)

        ws.append_row([user_email, bocm, exp, today, fase, "", muni, desc, "", "0"])
        load_watchlist.clear()
        return True
    except Exception as _err:
        import sys; print(f"[add_to_watchlist] {_err}", file=sys.stderr)
        return False


def update_watchlist_row(user_email: str, expediente: str,
                         notes: str = None, priority: int = None) -> bool:
    """Persist notes and/or priority to the Watchlist sheet row."""
    try:
        ss = _get_sheet_connection()
        if not ss: return False
        try: ws = ss.worksheet("Watchlist")
        except Exception: return False
        headers = ws.row_values(1)
        try:
            ecol = headers.index("email")      + 1
            xcol = headers.index("expediente") + 1
        except ValueError: return False
        # Ensure notes/priority columns exist
        try: ncol = headers.index("notes")    + 1
        except ValueError:
            ws.update_cell(1, len(headers)+1, "notes"); ncol = len(headers)+1
        try: pcol = headers.index("priority") + 1
        except ValueError:
            ws.update_cell(1, len(headers)+2, "priority"); pcol = len(headers)+2
        all_rows = ws.get_all_values()
        for i, row in enumerate(all_rows[1:], start=2):
            if (len(row) >= max(ecol,xcol) and
                    row[ecol-1].lower() == user_email.lower() and
                    row[xcol-1].strip() == expediente.strip()):
                if notes    is not None: ws.update_cell(i, ncol, notes)
                if priority is not None: ws.update_cell(i, pcol, str(priority))
                return True  # _full is uncached so next call always fresh
        return False
    except Exception:
        return False


def remove_from_watchlist(user_email: str, expediente: str) -> bool:
    """Remove all rows for this user+expediente."""
    try:
        ss = _get_sheet_connection()
        if not ss: return False
        try: ws = ss.worksheet("Watchlist")
        except Exception: return False
        rows = ws.get_all_values()
        if len(rows) < 2: return True
        headers = rows[0]
        try: ecol = headers.index("email")+1; xcol = headers.index("expediente")+1
        except ValueError: return False
        to_del = [i for i, row in enumerate(rows[1:], start=2)
                  if len(row) >= max(ecol,xcol)
                  and row[ecol-1].lower() == user_email.lower()
                  and row[xcol-1].strip() == expediente.strip()]
        for idx in reversed(to_del): ws.delete_rows(idx)
        load_watchlist.clear()  # _full is uncached
        return True
    except Exception:
        return False

# ════════════════════════════════════════════════════════════
# NOTE: Seguir/Remove use st.button() only — no query params.
# <a href="?..."> in st.markdown causes full browser navigation
# = new Streamlit session = session_state wiped = logout bug.
# ════════════════════════════════════════════════════════════

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

# ── NEW: Profile Fit Filter (Secondary filter for overlapping profiles) ──
_pff = prof.get("profile_fit_filter", "")
if _pff and "profile_fit" in df_f.columns:
    # Use astype(str) to prevent crashes on empty/NaN cells
    _pff_mask = df_f["profile_fit"].astype(str).str.lower().str.contains(_pff.lower(), na=False)
    # Only apply if it would keep at least 3 rows — otherwise show everything
    if _pff_mask.sum() >= 3:
        df_f = df_f[_pff_mask]

# Action window filter — applies when profile uses action_filter
if prof.get("action_filter") and "action_window" in df_f.columns:
    _aw_pat = prof["action_filter"]
    df_f = df_f[df_f["action_window"].astype(str).str.contains(_aw_pat, na=False) | (df_f["action_window"].astype(str) == "")]

if muni_sel and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(muni_sel)]

# ── Phase filter ──
if fase_sel and "fase" in df_f.columns:
    df_f = df_f[df_f["fase"].isin(fase_sel)]

# ── Action Window filter ──
if aw_sel and "action_window" in df_f.columns:
    _aw_mask = pd.Series([False] * len(df_f), index=df_f.index)
    for _aw_key in aw_sel:
        _aw_mask = _aw_mask | df_f["action_window"].astype(str).str.contains(
            _aw_key, na=False, case=False)
    df_f = df_f[_aw_mask]

# ── Keyword search across key text fields ──
if kw_search:
    _search_cols = ["municipio", "direccion", "promotor", "tipo", "descripcion", "expediente", "ai_evaluation", "key_contacts", "supplies_needed", "fuente"]
    _mask = pd.Series([False] * len(df_f), index=df_f.index)
    for _col in _search_cols:
        if _col in df_f.columns:
            _mask = _mask | df_f[_col].astype(str).str.lower().str.contains(
                re.escape(kw_search), na=False)
    df_f = df_f[_mask]

# Deduplicate by expediente — when the same project has multiple phases,
# keep only the most advanced/recent entry. Show all as "historial" on the card.
if "expediente" in df_f.columns:
    _exp_col = df_f["expediente"].astype(str).str.strip()
    _has_exp = (_exp_col != "") & (_exp_col != "nan") & (_exp_col != "None")

    # Phase priority — higher = more advanced = shown first
    _PHASE_PRIORITY = {
        "en_obra": 7, "adjudicacion": 6, "primera_ocupacion": 5,
        "definitivo": 4, "licitacion": 3, "en_tramite": 2, "inicial": 1, "solicitud": 0,
    }
    if "fase" in df_f.columns:
        df_f["_phase_priority"] = df_f["fase"].map(_PHASE_PRIORITY).fillna(2)
    else:
        df_f["_phase_priority"] = 2

    df_f = df_f.sort_values(
        ["_phase_priority", "score", "pem_combined"],
        ascending=[False, False, False]
    )
    # For rows with a real expediente, keep only the most advanced phase
    # (the sort above ensures the first occurrence is the most advanced)
    _dupes_mask = _has_exp & df_f.duplicated(subset=["expediente"], keep="first")
    df_dup_history = df_f[_dupes_mask].copy()   # save for potential "historial" display
    df_f = df_f[~_dupes_mask].reset_index(drop=True)
else:
    df_f = df_f.sort_values(["score", "pem_combined"], ascending=[False, False]).reset_index(drop=True)
# ── Metrics ──
total_pem  = df_f["pem_combined"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])
avg_score  = int(df_f["score"].mean()) if count > 0 else 0

# Format total PEM
if total_pem >= 1_000_000_000:
    total_pem_s = f"€{total_pem/1_000_000_000:.1f}B"
elif total_pem >= 1_000_000:
    total_pem_s = f"€{total_pem/1_000_000:.0f}M"
elif total_pem >= 1_000:
    total_pem_s = f"€{int(total_pem/1_000)}K"
else:
    total_pem_s = "—"

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

# ════════════════════════════════════════════════════════════
# TABS — Lista de leads  |  Mapa interactivo
# ════════════════════════════════════════════════════════════
_tab_leads, _tab_mapa, _tab_alertas = st.tabs([
    "📋  Lista de proyectos",
    "🗺️  Mapa interactivo",
    "🔖  Mis alertas",
])

# ── TAB 1: LEADS LIST ────────────────────────────────────────
with _tab_leads:
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
        # ── Session state ─────────────────────────────────────────────────────
        for _sk in ("just_saved", "just_removed"):
            if _sk not in st.session_state: st.session_state[_sk] = set()

        _u = st.session_state.get("user_email","")
        _is_real_user  = bool(_u and not _u.startswith("token:"))
        _sheet_watched = set(load_watchlist(_u)) if _is_real_user else set()
        _watched_set   = (_sheet_watched | st.session_state["just_saved"]) - st.session_state["just_removed"]

        for _, row in df_f.iterrows():
            # ── Stable unique key ──────────────────────────────────────────
            _exp = str(row.get("expediente","") or "").strip()
            if not _exp or _exp.lower() in ("nan","none"):
                _bocm_raw = str(row.get("bocm_url","") or "")
                _slug_m   = re.search(r'BOCM[-_](\d{8}[-_]?\d*)', _bocm_raw, re.I)
                if _slug_m:
                    _exp = f"BOCM-{_slug_m.group(1).replace('_','-')}"
                elif _bocm_raw:
                    _exp = f"BOCM-{abs(hash(_bocm_raw)) % 10**10}"
            _already = (_exp in _watched_set) if _exp else False

            # ── Card HTML (pure display, no action links) ───────────────────
            _full_html = build_card(row.to_dict(), is_watched=_already, inside_details=True)
            st.markdown(build_compact_row(row.to_dict(), _full_html), unsafe_allow_html=True)

            # ── Seguir / Siguiendo — small native button, right-aligned ─────
            # st.button() = Python rerun only. Never opens new tab. Never loses state.
            # Key prefix "sv_" — unique to main list (Mis alertas uses "al_" prefix).
            if _exp and _is_real_user:
                _safe_k = re.sub(r'[^a-zA-Z0-9_]', '_', _exp)
                _pad, _btn_col = st.columns([10, 2])
                with _btn_col:
                    if _already:
                        if st.button("🔔 Siguiendo ✕", key=f"sv_{_safe_k}",
                                     help="Dejar de seguir",
                                     use_container_width=True):
                            remove_from_watchlist(_u, _exp)
                            st.session_state.setdefault("just_removed", set()).add(_exp)
                            st.session_state.get("just_saved", set()).discard(_exp)
                            load_watchlist.clear()
                            st.rerun()
                    else:
                        if st.button("🔔 Seguir", key=f"sv_{_safe_k}",
                                     help="Guardar en Mis alertas",
                                     use_container_width=True):
                            add_to_watchlist(_u, row.to_dict())
                            st.session_state.setdefault("just_saved", set()).add(_exp)
                            st.session_state.get("just_removed", set()).discard(_exp)
                            load_watchlist.clear()
                            st.rerun()

# ── TAB 2: INTERACTIVE MAP ───────────────────────────────────
with _tab_mapa:
    if not _FOLIUM_OK:
        st.warning("Instala `folium` y `streamlit-folium` para activar el mapa.")
    elif df_f.empty:
        st.info("Sin proyectos con los filtros actuales. Ajusta el período o el perfil.")
    else:
        # Legend
        st.markdown("""
<div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;
     padding:10px 16px;background:#fff;border:1px solid #e2e8f0;
     border-radius:10px;margin-bottom:14px;font-family:'Plus Jakarta Sans',system-ui,sans-serif;font-size:12px;">
  <strong style="color:#0d1a2b;font-size:12px;">Leyenda:</strong>
  <span><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#16a34a;margin-right:4px;vertical-align:middle;"></span>Alta prioridad (≥65 pts)</span>
  <span><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#c8860a;margin-right:4px;vertical-align:middle;"></span>Media (40–64 pts)</span>
  <span><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#64748b;margin-right:4px;vertical-align:middle;"></span>Baja (&lt;40 pts)</span>
  <span>⚡ = Pre-lead (solicitud)</span>
  <span style="color:#94a3b8;font-size:11px;">📍 Ubicación aprox. para proyectos sin dirección exacta</span>
</div>""", unsafe_allow_html=True)

        # Map size control — Expansion directors want a large overview
        # Cap at 50 pins — Nominatim geocoding is ~0.3s/row.
        # 50 pins = ~15s max. 200 pins = >60s = user gives up.
        # Show highest-scored leads first (already sorted).
        _map_rows = min(len(df_f), 50)
        df_map = df_f.head(_map_rows)
        if len(df_f) > _map_rows:
            st.caption(f"ℹ️ Mapa muestra los {_map_rows} proyectos con mayor puntuación. Usa filtros para ver zonas concretas.")
        result = build_map(df_map, profile_key=prof["key"])
        if result:
            folium_map, n_plotted = result
            st_folium(
                folium_map,
                use_container_width=True,
                height=580,
                returned_objects=[],   # don't return click data (faster)
            )
            # Summary below map
            _prec_note = ""
            if n_plotted < count:
                _prec_note = f" · primeros {n_plotted} mostrados"
            st.caption(
                f"📍 {n_plotted} proyectos mapeados{_prec_note} · "
                f"Haz clic en un pin para ver los detalles · "
                f"Zoom con rueda del ratón o pellizco · "
                f"Los pines con ≈ indican ubicación a nivel de municipio"
            )
        else:
            st.info("No se pudo generar el mapa. Comprueba la conexión.")

# ── TAB 3: MIS ALERTAS ───────────────────────────────────────
with _tab_alertas:
    _ua = st.session_state.get("user_email","")
    if not _ua or _ua.startswith("token:"):
        st.markdown("""
<div style="text-align:center;padding:64px 24px;background:#fff;border:1.5px solid #e2e8f0;border-radius:14px;margin-top:8px;">
  <div style="font-size:40px;margin-bottom:12px;">🔒</div>
  <p style="font-size:14px;color:#64748b;margin:0;">Inicia sesión con tu email para gestionar tus alertas.</p>
</div>""", unsafe_allow_html=True)
    else:
        if "alert_notes_local"    not in st.session_state: st.session_state["alert_notes_local"]    = {}
        if "alert_notes_saved_ok" not in st.session_state: st.session_state["alert_notes_saved_ok"] = set()

        _wl_full = load_watchlist_full(_ua)
        _removed = st.session_state.get("just_removed", set())
        _wl_full = [r for r in _wl_full
                    if str(r.get("expediente","")).strip() not in _removed]
        _seen = {str(r.get("expediente","")).strip() for r in _wl_full}
        for _je in st.session_state.get("just_saved", set()):
            _je = str(_je).strip()
            if _je and _je not in _seen:
                _wl_full.append({"expediente":_je, "notes":"", "priority":"0"})
                _seen.add(_je)

        _PRIO = {
            "1": ("🔴", "Prioridad 1", "#dc2626"),
            "2": ("🟠", "Prioridad 2", "#c2410c"),
            "3": ("🟡", "Prioridad 3", "#a16207"),
            "0": ("",   "Sin prioridad","#94a3b8"),
        }
        def _gprio(r): return str(r.get("priority","0") or "0").strip() or "0"
        _wl_full.sort(key=lambda r: int(_gprio(r)) if _gprio(r) != "0" else 999)

        # Accept any non-empty key ≤ 60 chars.
        # NOTE: .isupper() removed — incorrectly rejected BOCM-20260414 type slugs.
        _wl_valid = [r for r in _wl_full
                     if str(r.get("expediente","") or "").strip()
                     and len(str(r.get("expediente","") or "").strip()) <= 60]

        if not _wl_valid:
            st.markdown("""
<div style="text-align:center;padding:64px 24px;background:#fff;border:1.5px solid #e2e8f0;border-radius:14px;margin-top:8px;">
  <div style="font-size:40px;margin-bottom:12px;">🔔</div>
  <h3 style="font-family:'Fraunces',Georgia,serif;font-size:18px;color:#0d1a2b;margin:0 0 8px;">Sin alertas guardadas</h3>
  <p style="font-size:13px;color:#64748b;line-height:1.6;margin:0;">
    Pulsa <strong>🔔 Seguir</strong> en cualquier proyecto para recibir alertas cuando avance de fase.
  </p>
</div>""", unsafe_allow_html=True)
        else:
            st.markdown(
                f'<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;color:#94a3b8;'
                f'text-transform:uppercase;letter-spacing:.08em;margin:0 0 16px;">'
                f'{len(_wl_valid)} proyecto{"s" if len(_wl_valid)!=1 else ""} en seguimiento</p>',
                unsafe_allow_html=True)

            # Build lookup from current sheet data
            _df_idx = {}
            if "expediente" in df.columns:
                for _, _r in df[df["expediente"].astype(str).str.strip().isin(_seen)].iterrows():
                    _k = str(_r.get("expediente","")).strip()
                    if _k: _df_idx[_k] = _r

            for _wr in _wl_valid:
                _exp_s  = str(_wr.get("expediente","") or "").strip()
                if not _exp_s: continue
                _safe_k = re.sub(r'[^a-zA-Z0-9_]', '_', _exp_s)
                _pv     = _gprio(_wr)
                _, _pl, _pfc = _PRIO.get(_pv, _PRIO["0"])

                _note_sheet   = str(_wr.get("notes","") or "")
                _note_session = st.session_state["alert_notes_local"].get(_exp_s)
                _note_display = _note_session if _note_session is not None else _note_sheet
                _note_saved_ok = _exp_s in st.session_state["alert_notes_saved_ok"]

                _row = _df_idx.get(_exp_s)

                # ── Priority border strip above each alert card ───────────────
                if _pv != "0":
                    st.markdown(
                        f'<div style="height:3px;background:{_pfc};border-radius:3px 3px 0 0;'
                        f'margin-bottom:-2px;"></div>',
                        unsafe_allow_html=True)

                # ── Real lead card (identical to main list) ───────────────────
                if _row is not None:
                    _card_row = _row.to_dict()
                else:
                    # Minimal row so card still renders cleanly when not in current filter
                    _card_row = {
                        "municipio": _wr.get("muni",""),
                        "descripcion": _wr.get("description",""),
                        "expediente": _exp_s,
                        "bocm_url": _wr.get("source_url",""),
                        "fase": _wr.get("phase_at_add",""),
                        "tipo": "",
                    }
                # ── Card (pure display) ──────────────────────────────────────
                st.markdown(build_card(_card_row, is_watched=True, inside_details=False),
                            unsafe_allow_html=True)

                # ── Notes + actions below each alert card ─────────────────
                # Layout (two rows):
                #   Row 1: full-width notes expander
                #   Row 2: [spacer] [priority selectbox] [Dejar de seguir]
                # Full-width expander avoids column height mismatch.
                # All keys prefixed "al_" — never collide with main list "sv_" keys.
                _PRIO_OPTS = ["—", "🔴 P1", "🟡 P2", "🔵 P3"]
                _PRIO_VAL  = {"—":"0","🔴 P1":"1","🟡 P2":"2","🔵 P3":"3"}
                _PRIO_BACK = {"0":"—","1":"🔴 P1","2":"🟡 P2","3":"🔵 P3"}
                _cur_label = _PRIO_BACK.get(_pv, "—")

                # ── Row 1: Full-width notes expander ─────────────────────────
                _note_lbl = (
                    f"📝 {_note_display[:50]}{'…' if len(_note_display)>50 else ''}"
                    if _note_display else "📝 Añadir nota privada…"
                )
                with st.expander(_note_lbl, expanded=False):
                    _typed = st.text_area(
                        "Nota", value=_note_display,
                        placeholder="Contactos, próximos pasos, contexto…",
                        key=f"al_note_{_safe_k}", label_visibility="collapsed",
                        height=80,
                    )
                    _sn_l, _sn_r = st.columns([7, 2])
                    with _sn_r:
                        if st.button("💾 Guardar nota", key=f"al_save_{_safe_k}",
                                     use_container_width=True):
                            st.session_state["alert_notes_local"][_exp_s] = _typed
                            ok = update_watchlist_row(_ua, _exp_s, notes=_typed)
                            if ok:
                                st.session_state["alert_notes_saved_ok"].add(_exp_s)
                                st.toast("✅ Nota guardada", icon="💾")
                            else:
                                st.toast("⚠️ No se pudo guardar.")
                            st.rerun()
                    with _sn_l:
                        if _note_saved_ok and _note_display:
                            st.caption("✓ Guardada")

                # ── Row 2: Priority selectbox + Dejar de seguir ───────────────
                # Spacer pushes both controls flush-right, perfectly aligned.
                _r2_sp, _r2_prio, _r2_rm = st.columns([6, 2, 2])

                with _r2_prio:
                    _sel = st.selectbox(
                        "Prioridad",
                        options=_PRIO_OPTS,
                        index=_PRIO_OPTS.index(_cur_label) if _cur_label in _PRIO_OPTS else 0,
                        key=f"al_prio_{_safe_k}",
                        label_visibility="collapsed",
                        help="Asignar prioridad a este proyecto",
                    )
                    if _PRIO_VAL[_sel] != _pv:
                        update_watchlist_row(_ua, _exp_s, priority=int(_PRIO_VAL[_sel]))
                        load_watchlist.clear()
                        st.rerun()

                with _r2_rm:
                    if st.button("✕ Dejar de seguir", key=f"al_rm_{_safe_k}",
                                 help="Eliminar de Mis alertas",
                                 use_container_width=True):
                        remove_from_watchlist(_ua, _exp_s)
                        st.session_state.setdefault("just_removed", set()).add(_exp_s)
                        st.session_state.get("just_saved", set()).discard(_exp_s)
                        load_watchlist.clear()
                        st.rerun()

                st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── Footer ──
st.markdown(f"""
<div style="text-align:center;padding:28px 0 8px;margin-top:28px;border-top:1px solid #e2e8f0;
     font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;line-height:1.9;">
  <strong style="color:#5a5a78;font-size:11px;">PlanningScout Madrid</strong><br>
  Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales<br>
  PEM = Presupuesto de Ejecución Material · {count} proyectos · {last_str}
</div>
""", unsafe_allow_html=True)
