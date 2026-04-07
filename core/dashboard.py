import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import re
import base64

# ════════════════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout — Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="collapsed",  # sidebar hidden — we don't use it
)

# ════════════════════════════════════════════════════════════
# AUTH — URL tokens for paid clients, open ?perfil= for trials
# ════════════════════════════════════════════════════════════
qp              = st.query_params
url_token       = qp.get("token", "")
url_profile     = qp.get("perfil", "")
trial_days_left = None

client_tokens = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

require_token = str(st.secrets.get("REQUIRE_TOKEN", "false")).lower() == "true"

forced_profile_key = None
if url_token and url_token in client_tokens:
    forced_profile_key = client_tokens[url_token]
elif url_profile:
    forced_profile_key = url_profile.lower().replace(" ", "_")

if require_token and not forced_profile_key:
    st.markdown("""
    <div style="min-height:80vh;display:flex;align-items:center;justify-content:center;">
    <div style="text-align:center;max-width:400px;padding:48px 32px;background:white;border-radius:16px;box-shadow:0 8px 32px rgba(0,0,0,.1);border:1px solid #e2e8f0;">
      <div style="font-size:48px;margin-bottom:20px;">🔒</div>
      <h2 style="font-family:Georgia,serif;font-size:24px;color:#0d1a2b;margin-bottom:12px;">Acceso restringido</h2>
      <p style="color:#64748b;font-size:15px;line-height:1.6;margin-bottom:24px;">Accede a través del enlace personalizado que te hemos enviado, o regístrate en planningscout.com para obtener tu mes gratuito.</p>
      <a href="https://planningscout.com" style="display:inline-block;background:#1e3a5f;color:white;padding:12px 28px;border-radius:10px;font-weight:600;font-size:14px;text-decoration:none;">Ir a planningscout.com →</a>
    </div></div>
    """, unsafe_allow_html=True)
    st.stop()

# ════════════════════════════════════════════════════════════
# DESIGN SYSTEM — same tokens as the website
# ════════════════════════════════════════════════════════════
SHEET_ID = st.secrets.get("SHEET_ID", "")

CSS = """
<style>
/* ── Fonts (same as website) ── */
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,600;0,9..144,700;1,9..144,400&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Reset Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 !important; max-width: 100% !important; }
.stApp { background: #f7f8fa; }
[data-testid="collapsedControl"] { display: none; }

/* ── Global ── */
* { box-sizing: border-box; }

/* ── Topbar ── */
.ps-topbar {
  background: #ffffff;
  border-bottom: 1px solid #e2e8f0;
  padding: 14px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 100;
  box-shadow: 0 1px 3px rgba(0,0,0,.05);
}
.ps-logo {
  font-family: 'Fraunces', Georgia, serif;
  font-size: 18px;
  font-weight: 600;
  color: #0d1a2b;
  letter-spacing: -0.3px;
}
.ps-logo span { color: #c8860a; }
.ps-last-update {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: #94a3b8;
}

/* ── Profile pills ── */
.profile-bar {
  background: white;
  border-bottom: 1px solid #e2e8f0;
  padding: 12px 20px;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  white-space: nowrap;
  scrollbar-width: none;
}
.profile-bar::-webkit-scrollbar { display: none; }

/* ── Content wrapper ── */
.ps-content {
  max-width: 900px;
  margin: 0 auto;
  padding: 20px 16px 40px;
}

/* ── Metrics row ── */
.metrics-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
  margin: 0 0 20px;
}
@media (max-width: 640px) {
  .metrics-row { grid-template-columns: repeat(2, 1fr); }
}
.metric-card {
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 14px 16px;
  text-align: center;
}
.metric-card .val {
  font-family: 'Fraunces', Georgia, serif;
  font-size: 26px;
  font-weight: 600;
  color: #1e3a5f;
  line-height: 1;
}
.metric-card .lbl {
  font-family: 'JetBrains Mono', monospace;
  font-size: 10px;
  color: #94a3b8;
  text-transform: uppercase;
  letter-spacing: .08em;
  margin-top: 4px;
}

/* ── Filter expander ── */
.filter-section {
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  margin-bottom: 16px;
  overflow: hidden;
}
.filter-header {
  padding: 14px 18px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  cursor: pointer;
  user-select: none;
}
.filter-header-text {
  font-family: 'Plus Jakarta Sans', sans-serif;
  font-size: 14px;
  font-weight: 600;
  color: #334155;
}

/* ── LEAD CARD — matches website exactly ── */
.lead-card {
  background: white;
  border: 1.5px solid #e2e8f0;
  border-radius: 14px;
  overflow: hidden;
  margin-bottom: 14px;
  box-shadow: 0 2px 8px rgba(0,0,0,.05);
  transition: box-shadow .2s, border-color .2s;
}
.lead-card:hover {
  box-shadow: 0 6px 20px rgba(0,0,0,.09);
  border-color: #cbd5e1;
}

/* Card header — dark navy like website */
.lc-head {
  background: #1e3a5f;
  padding: 14px 18px;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
}
.lc-location {
  display: flex;
  align-items: center;
  gap: 8px;
  flex: 1;
  min-width: 0;
}
.lc-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #34c759;
  flex-shrink: 0;
}
.lc-muni {
  font-family: 'Fraunces', Georgia, serif;
  font-size: 15px;
  font-weight: 600;
  color: white;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.lc-badges {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-shrink: 0;
  flex-wrap: wrap;
  justify-content: flex-end;
}
.score-badge {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  padding: 4px 10px;
  border-radius: 10px;
  white-space: nowrap;
}
.score-gold   { background: #16a34a; color: white; }
.score-orange { background: #c8860a; color: white; }
.score-yellow { background: rgba(255,255,255,.15); color: #fde68a; border: 1px solid rgba(253,230,138,.3); }
.score-dim    { background: rgba(255,255,255,.1);  color: rgba(255,255,255,.6); }

/* Card body */
.lc-body {
  padding: 16px 18px;
}
.lc-title {
  font-family: 'Plus Jakarta Sans', sans-serif;
  font-size: 14px;
  font-weight: 600;
  color: #0d1a2b;
  margin-bottom: 14px;
  line-height: 1.4;
}
.lc-chip {
  display: inline-block;
  background: #eff4fb;
  color: #1e3a5f;
  font-family: 'JetBrains Mono', monospace;
  font-size: 10px;
  font-weight: 500;
  padding: 3px 9px;
  border-radius: 6px;
  border: 1px solid rgba(30,58,95,.15);
  margin-right: 6px;
  margin-bottom: 10px;
}

/* Info table inside card */
.lc-table {
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  overflow: hidden;
  margin-bottom: 12px;
}
.lc-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 9px 14px;
  border-bottom: 1px solid #f1f5f9;
  gap: 12px;
  flex-wrap: wrap;
}
.lc-row:last-child { border-bottom: none; }
.lc-key {
  font-family: 'JetBrains Mono', monospace;
  font-size: 10px;
  color: #94a3b8;
  text-transform: uppercase;
  letter-spacing: .08em;
  flex-shrink: 0;
  min-width: 70px;
}
.lc-val {
  font-family: 'Plus Jakarta Sans', sans-serif;
  font-size: 13px;
  color: #334155;
  text-align: right;
  flex: 1;
}
.lc-val.pem {
  font-weight: 700;
  color: #1e3a5f;
  font-size: 15px;
}
.lc-val.verde {
  background: #f0fdf4;
  color: #16a34a;
  border: 1px solid rgba(22,163,74,.2);
  padding: 2px 8px;
  border-radius: 6px;
  font-size: 11px;
  font-weight: 600;
}

/* Description */
.lc-desc {
  font-size: 13px;
  color: #64748b;
  line-height: 1.6;
  padding: 0 0 4px;
}

/* Card footer — links */
.lc-footer {
  background: #f8f9fb;
  border-top: 1px solid #e2e8f0;
  padding: 10px 18px;
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.lc-link {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-family: 'Plus Jakarta Sans', sans-serif;
  font-size: 12px;
  font-weight: 600;
  color: #1e3a5f;
  background: #eff4fb;
  border: 1px solid rgba(30,58,95,.15);
  padding: 5px 12px;
  border-radius: 7px;
  text-decoration: none;
  transition: background .15s;
}
.lc-link:hover { background: #dce8f5; }
.lc-link.contact {
  color: #0369a1;
  background: #f0f9ff;
  border-color: rgba(3,105,161,.15);
}
.lc-date {
  font-family: 'JetBrains Mono', monospace;
  font-size: 10px;
  color: #94a3b8;
  margin-left: auto;
}

/* ── Empty state ── */
.empty-state {
  text-align: center;
  padding: 60px 24px;
  background: white;
  border: 1.5px solid #e2e8f0;
  border-radius: 14px;
}
.empty-state .icon { font-size: 48px; margin-bottom: 16px; }
.empty-state h3 {
  font-family: 'Fraunces', Georgia, serif;
  font-size: 20px;
  color: #0d1a2b;
  margin-bottom: 8px;
}
.empty-state p { font-size: 14px; color: #64748b; line-height: 1.6; }

/* ── Tip box ── */
.tip-box {
  background: #fdf3e3;
  border-left: 4px solid #c8860a;
  border-radius: 8px;
  padding: 12px 16px;
  font-size: 13px;
  color: #64748b;
  margin-bottom: 16px;
  line-height: 1.55;
}
.tip-box strong { color: #9a6200; }

/* ── Section header ── */
.section-title {
  font-family: 'Fraunces', Georgia, serif;
  font-size: 16px;
  font-weight: 600;
  color: #0d1a2b;
  margin-bottom: 14px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-count {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  background: #1e3a5f;
  color: white;
  padding: 3px 10px;
  border-radius: 100px;
}

/* ── Download button style override ── */
.stDownloadButton button {
  background: white !important;
  color: #1e3a5f !important;
  border: 1.5px solid #cbd5e1 !important;
  border-radius: 8px !important;
  font-size: 13px !important;
  font-weight: 600 !important;
  padding: 8px 16px !important;
  width: 100% !important;
}

/* ── Streamlit expander tweaks ── */
[data-testid="stExpander"] {
  background: white !important;
  border: 1px solid #e2e8f0 !important;
  border-radius: 12px !important;
  overflow: hidden;
  margin-bottom: 14px;
}
[data-testid="stExpander"] > div:first-child {
  padding: 12px 18px !important;
}

/* ── Slider / selectbox tweaks ── */
.stSlider > div > div { padding: 0 !important; }

/* ── Mobile adjustments ── */
@media (max-width: 640px) {
  .ps-topbar { padding: 12px 16px; }
  .ps-content { padding: 16px 12px 40px; }
  .lc-head { padding: 12px 14px; }
  .lc-muni { font-size: 14px; }
  .lc-body { padding: 14px 14px; }
  .lc-footer { padding: 10px 14px; gap: 8px; }
  .lc-table { margin-bottom: 10px; }
  .lc-row { padding: 8px 12px; }
  .metrics-row .val { font-size: 22px; }
  .lead-card { margin-bottom: 12px; }
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════════════════════════
PROFILES = {
    "🔧 Instaladores MEP": {
        "key": "instaladores",
        "tip": "💡 Contacta al promotor 6-12 meses antes de la obra para entrar en las especificaciones técnicas de ascensores, HVAC y PCI.",
        "min_score": 0, "min_value": 80_000, "days_default": 30,
        "permit_types": ["obra mayor", "cambio de uso", "declaración responsable", "licencia primera ocupación", "urbanización"],
        "color": "#00838f",
    },
    "🏪 Expansión Retail": {
        "key": "expansion",
        "tip": "💡 Las urbanizaciones aprobadas = nuevos barrios en 2-3 años. Negocia el local ahora antes de que suba el precio.",
        "min_score": 0, "min_value": 0, "days_default": 60,
        "permit_types": ["urbanización", "plan especial", "plan parcial", "cambio de uso", "licencia de actividad", "obra mayor nueva construcción"],
        "color": "#e65100",
    },
    "📐 Promotores / RE": {
        "key": "promotores",
        "tip": "💡 Una reparcelación aprobada = suelo urbanizable. Contacta a la Junta antes de que salga al mercado.",
        "min_score": 20, "min_value": 300_000, "days_default": 60,
        "permit_types": ["urbanización", "plan parcial", "plan especial", "obra mayor nueva construcción", "cambio de uso", "declaración responsable obra mayor"],
        "color": "#6a1b9a",
    },
    "🏢 Gran Constructora": {
        "key": "constructora",
        "tip": "💡 La aprobación definitiva de un plan = licitación en 12-18 meses. Empieza a preparar el dossier técnico ya.",
        "min_score": 35, "min_value": 2_000_000, "days_default": 90,
        "permit_types": ["urbanización", "plan especial", "plan parcial", "obra mayor industrial", "obra mayor nueva construcción"],
        "color": "#b71c1c",
    },
    "🏭 Industrial / Log.": {
        "key": "industrial",
        "tip": "💡 Una licencia de nave = obra en 3-6 meses. Contacta al promotor para demolición previa o ejecución.",
        "min_score": 0, "min_value": 200_000, "days_default": 60,
        "permit_types": ["obra mayor industrial", "urbanización", "obra mayor nueva construcción", "cambio de uso"],
        "color": "#37474f",
    },
    "🛒 Compras / Materiales": {
        "key": "compras",
        "tip": "💡 Con el nombre del promotor y el expediente puedes presentar tus materiales antes de que la constructora adjudique suministros.",
        "min_score": 0, "min_value": 150_000, "days_default": 30,
        "permit_types": [],
        "color": "#4527a0",
    },
    "🏙️ Vista General": {
        "key": "general",
        "tip": "Vista completa de todos los proyectos detectados. Selecciona un perfil específico para filtrar por sector.",
        "min_score": 0, "min_value": 0, "days_default": 14,
        "permit_types": [],
        "color": "#1e3a5f",
    },
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def parse_value(v):
    if not v or str(v).strip() in ("", "—", "N/A", "nan"):
        return 0.0
    s = re.sub(r'[^\d,.]', '', str(v))
    if s.count(',') == 1 and s.count('.') >= 1:
        s = s.replace('.', '').replace(',', '.')
    elif s.count(',') == 1 and s.count('.') == 0:
        s = s.replace(',', '.')
    elif s.count('.') > 1:
        s = s.replace('.', '')
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_score(v):
    try:
        return int(float(str(v).strip())) if str(v).strip() else 0
    except Exception:
        return 0

def fmt_eur(v):
    if v == 0:
        return "—"
    if v >= 1_000_000:
        return f"€{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"€{int(v/1000)}K"
    return f"€{int(v):,}"

def score_class(sc):
    if sc >= 65:
        return "score-gold"
    if sc >= 40:
        return "score-orange"
    if sc >= 20:
        return "score-yellow"
    return "score-dim"

def score_emoji(sc):
    if sc >= 65:
        return "🟢"
    if sc >= 40:
        return "🟠"
    if sc >= 20:
        return "🟡"
    return "⚪"

def clean(v, fallback=""):
    s = str(v or fallback).strip()
    return "" if s in ("nan", "None", "—") else s

def make_lead_card(row):
    sc    = int(row.get("score", 0))
    pem   = row.get("pem", 0)
    muni  = clean(row.get("municipio"), "Madrid")
    addr  = clean(row.get("direccion"))
    prom  = clean(row.get("promotor"))
    tipo  = clean(row.get("tipo"))
    desc  = clean(row.get("descripcion"))
    fecha = clean(row.get("fecha"))
    maps  = clean(row.get("maps"))
    bocm  = clean(row.get("bocm_url"))
    pdf   = clean(row.get("pdf_url"))
    expd  = clean(row.get("expediente"))
    conf  = clean(row.get("confianza"))

    pem_str  = fmt_eur(pem)
    sc_class = score_class(sc)
    sc_emoji = score_emoji(sc)

    title = addr if addr else (desc[:80] if desc else tipo)

    # Build links
    links_html = ""
    if maps:
        links_html += f'<a href="{maps}" target="_blank" class="lc-link">📍 Mapa</a>'
    if bocm:
        links_html += f'<a href="{bocm}" target="_blank" class="lc-link">📄 BOCM</a>'
    if pdf:
        links_html += f'<a href="{pdf}" target="_blank" class="lc-link">📑 PDF</a>'
    if prom:
        q = prom.replace(" ", "+")
        links_html += f'<a href="https://www.linkedin.com/search/results/all/?keywords={q}" target="_blank" class="lc-link contact">🔍 LinkedIn</a>'

    date_html = f'<span class="lc-date">{fecha}</span>' if fecha else ""

    # Build table rows
    table_rows = ""
    if tipo:
        table_rows += f'<div class="lc-row"><span class="lc-key">Tipo</span><span class="lc-val">{tipo}</span></div>'
    if pem > 0:
        table_rows += f'<div class="lc-row"><span class="lc-key">PEM</span><span class="lc-val pem">{pem_str}</span></div>'
    if prom:
        table_rows += f'<div class="lc-row"><span class="lc-key">Promotor</span><span class="lc-val">{prom[:60]}</span></div>'
    if expd:
        table_rows += f'<div class="lc-row"><span class="lc-key">Expediente</span><span class="lc-val">{expd}</span></div>'
    if conf and conf in ("high", "medium", "low"):
        conf_es = {"high": "alta", "medium": "media", "low": "baja"}.get(conf, conf)
        conf_class = "verde" if conf == "high" else ""
        table_rows += f'<div class="lc-row"><span class="lc-key">Fiabilidad</span><span class="lc-val {conf_class}">{conf_es}</span></div>'

    # Description (truncated)
    desc_html = f'<div class="lc-desc">{desc[:220]}</div>' if desc else ""

    html = f"""
<div class="lead-card">
  <div class="lc-head">
    <div class="lc-location">
      <div class="lc-dot"></div>
      <span class="lc-muni">{muni}</span>
    </div>
    <div class="lc-badges">
      <span class="score-badge {sc_class}">{sc_emoji} {sc} pts</span>
    </div>
  </div>
  <div class="lc-body">
    <div class="lc-title">{title}</div>
    {"<div class='lc-table'>" + table_rows + "</div>" if table_rows else ""}
    {desc_html}
  </div>
  <div class="lc-footer">
    {links_html}
    {date_html}
  </div>
</div>"""
    return html

# ════════════════════════════════════════════════════════════
# LOAD DATA
# ════════════════════════════════════════════════════════════
COL_MAP = {
    "Date Granted": "fecha", "Municipality": "municipio",
    "Full Address": "direccion", "Applicant": "promotor",
    "Permit Type": "tipo", "Declared Value PEM (€)": "pem_raw",
    "Est. Build Value (€)": "est_raw", "Maps Link": "maps",
    "Description": "descripcion", "Source URL": "bocm_url",
    "PDF URL": "pdf_url", "Mode": "modo", "Confidence": "confianza",
    "Date Found": "fecha_encontrado", "Lead Score": "score_raw",
    "Expediente": "expediente",
}

@st.cache_data(ttl=300)
def load_data():
    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", SHEET_ID)).worksheet("Permits")
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error conectando a Google Sheets: {e}")
        return pd.DataFrame()

# Load first (so municipios are available before rendering sidebar)
with st.spinner(""):
    df_raw = load_data()

if df_raw.empty:
    st.markdown("""
    <div class="ps-content">
    <div class="empty-state">
      <div class="icon">📡</div>
      <h3>Sin datos todavía</h3>
      <p>El scraper aún no ha procesado ningún proyecto.<br>
      Ejecuta <code>--weeks 8</code> en GitHub Actions para hacer un backfill.</p>
    </div></div>""", unsafe_allow_html=True)
    st.stop()

df = df_raw.rename(columns={k: v for k, v in COL_MAP.items() if k in df_raw.columns})
df["pem"]      = df["pem_raw"].apply(parse_value)   if "pem_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["est"]      = df["est_raw"].apply(parse_value)   if "est_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["score"]    = df["score_raw"].apply(parse_score) if "score_raw"        in df.columns else pd.Series(0, index=df.index)
df["fecha_dt"] = pd.to_datetime(
    df["fecha_encontrado"].str[:10], errors="coerce"
) if "fecha_encontrado" in df.columns else pd.NaT

all_munis = sorted(df["municipio"].dropna().unique().tolist()) if "municipio" in df.columns else []
all_munis = [m for m in all_munis if str(m).strip() and str(m) != "nan"]

# ════════════════════════════════════════════════════════════
# TOPBAR
# ════════════════════════════════════════════════════════════
last_update = df["fecha_dt"].max()
last_str = last_update.strftime("%d %b %Y") if pd.notna(last_update) else "—"

st.markdown(f"""
<div class="ps-topbar">
  <span class="ps-logo">🏗️ Planning<span>Scout</span></span>
  <span class="ps-last-update">Actualizado: {last_str}</span>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# PROFILE SELECTOR — horizontal pills, scrollable on mobile
# ════════════════════════════════════════════════════════════
profile_names = list(PROFILES.keys())

# Determine active profile
if forced_profile_key:
    matched = next(
        (n for n, p in PROFILES.items() if p["key"] == forced_profile_key),
        profile_names[-1]  # Vista General
    )
    default_idx = profile_names.index(matched)
    is_locked = True
else:
    default_idx = len(profile_names) - 1  # Vista General default
    is_locked = False

# Use st.radio as the profile selector (rendered via custom CSS)
# We put it in main content so it works on mobile
st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

if is_locked:
    selected_profile = profile_names[default_idx]
    st.markdown(f"""
    <div style="background:#eff4fb;border:1px solid rgba(30,58,95,.15);border-radius:10px;
         padding:10px 16px;margin:0 0 16px;display:flex;align-items:center;gap:10px;">
      <span style="font-size:18px">{selected_profile.split()[0]}</span>
      <div>
        <div style="font-family:'Plus Jakarta Sans',sans-serif;font-size:13px;font-weight:600;color:#1e3a5f;">{selected_profile}</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;">Vista personalizada para tu perfil</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    # Render profile pills using Streamlit columns for mobile-friendly layout
    st.markdown("""
    <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;
         text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">
    Tu perfil de cliente
    </div>""", unsafe_allow_html=True)

    selected_profile = st.radio(
        "Perfil",
        profile_names,
        index=default_idx,
        horizontal=True,
        label_visibility="collapsed",
    )

prof = PROFILES[selected_profile]

# ════════════════════════════════════════════════════════════
# FILTERS — collapsible expander
# ════════════════════════════════════════════════════════════
with st.expander("⚙️ Filtros", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        days_back = st.selectbox(
            "Período",
            [7, 14, 30, 60, 90],
            index=[7,14,30,60,90].index(prof["days_default"]) if prof["days_default"] in [7,14,30,60,90] else 1,
            format_func=lambda x: f"Últimos {x} días",
        )
        min_pem = st.number_input(
            "PEM mínimo (€)", value=prof["min_value"],
            min_value=0, step=50_000, format="%d",
        )
    with col2:
        min_score = st.slider("Puntuación mínima", 0, 100, value=prof["min_score"], step=5)
        muni_sel = st.multiselect(
            "Municipio", options=all_munis, placeholder="Todos",
        )

    st.caption("💡 Para backfill de 8 semanas: Actions → Run workflow → weeks: 8")

# ════════════════════════════════════════════════════════════
# FILTER DATA
# ════════════════════════════════════════════════════════════
cutoff = datetime.now() - timedelta(days=days_back)
df_f   = df[df["fecha_dt"] >= cutoff].copy() if "fecha_dt" in df.columns else df.copy()

if min_score > 0:
    df_f = df_f[(df_f["score"] >= min_score) | (df_f["score"] == 0)]

df_f = df_f[df_f["pem"] >= min_pem]

if prof["permit_types"] and "tipo" in df_f.columns:
    pattern = "|".join(re.escape(t) for t in prof["permit_types"])
    df_f = df_f[df_f["tipo"].str.contains(pattern, case=False, na=False)]

if muni_sel and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(muni_sel)]

df_f = df_f.sort_values(["score", "pem"], ascending=[False, False]).reset_index(drop=True)

# ════════════════════════════════════════════════════════════
# METRICS
# ════════════════════════════════════════════════════════════
total_pem  = df_f["pem"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])
avg_score  = int(df_f["score"].mean()) if count > 0 else 0

pem_str  = fmt_eur(total_pem)
est_str  = fmt_eur(total_pem / 0.03) if total_pem > 0 else "—"

st.markdown(f"""
<div class="metrics-row">
  <div class="metric-card">
    <div class="val">{count}</div>
    <div class="lbl">Proyectos</div>
  </div>
  <div class="metric-card">
    <div class="val">{pem_str}</div>
    <div class="lbl">PEM total</div>
  </div>
  <div class="metric-card">
    <div class="val" style="color:#16a34a">{high_leads}</div>
    <div class="lbl">🟢 Prioritarios</div>
  </div>
  <div class="metric-card">
    <div class="val">{avg_score}</div>
    <div class="lbl">Score medio</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# PROFILE TIP
# ════════════════════════════════════════════════════════════
st.markdown(f'<div class="tip-box">{prof["tip"]}</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# EXPORT
# ════════════════════════════════════════════════════════════
if not df_f.empty:
    export_cols = [c for c in ["fecha","municipio","direccion","promotor","tipo","pem_raw","est_raw","descripcion","expediente","bocm_url"] if c in df_f.columns]
    csv = df_f[export_cols].to_csv(index=False).encode("utf-8")
    col_dl, col_info = st.columns([1, 2])
    with col_dl:
        st.download_button(
            label=f"⬇️ Exportar {count} leads (CSV)",
            data=csv,
            file_name=f"planningscout_{prof['key']}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    with col_info:
        st.caption(f"Período: últimos {days_back} días · Perfil: {selected_profile}")

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# LEAD CARDS
# ════════════════════════════════════════════════════════════
if df_f.empty:
    st.markdown(f"""
    <div class="empty-state">
      <div class="icon">🔍</div>
      <h3>Sin proyectos con estos filtros</h3>
      <p>
        Prueba a ampliar el período (actualmente {days_back} días),<br>
        reducir el PEM mínimo ({fmt_eur(min_pem)})<br>
        o cambiar el perfil de cliente.
      </p>
    </div>""", unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="section-title">
      Proyectos detectados
      <span class="section-count">{count}</span>
    </div>""", unsafe_allow_html=True)

    for _, row in df_f.iterrows():
        st.markdown(make_lead_card(row), unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# FOOTER
# ════════════════════════════════════════════════════════════
st.markdown(f"""
<div style="text-align:center;padding:32px 20px;font-family:'JetBrains Mono',monospace;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;margin-top:24px;">
  <strong style="color:#334155;">PlanningScout</strong> — Datos extraídos del BOCM (registros públicos oficiales CM Madrid)<br>
  PEM = Presupuesto de Ejecución Material · Est. Proyecto = PEM / 0.03 · {count} proyectos en esta vista
</div>
""", unsafe_allow_html=True)
