import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import re

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
# AUTH
# ════════════════════════════════════════════════════════════
qp             = st.query_params
url_token      = qp.get("token", "")
url_profile    = qp.get("perfil", "")
client_tokens  = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

require_token     = str(st.secrets.get("REQUIRE_TOKEN", "false")).lower() == "true"
forced_profile_key = None
if url_token and url_token in client_tokens:
    forced_profile_key = client_tokens[url_token]
elif url_profile:
    forced_profile_key = url_profile.lower().replace(" ", "_")

if require_token and not forced_profile_key:
    st.markdown("""
    <div style="min-height:80vh;display:flex;align-items:center;justify-content:center;font-family:system-ui,sans-serif;">
    <div style="text-align:center;max-width:380px;padding:48px 32px;background:white;
         border-radius:16px;box-shadow:0 8px 40px rgba(0,0,0,.1);border:1px solid #e2e8f0;">
      <div style="font-size:44px;margin-bottom:20px;">🔒</div>
      <h2 style="font-size:22px;color:#0d1a2b;margin-bottom:10px;font-weight:700;">Acceso restringido</h2>
      <p style="color:#64748b;font-size:14px;line-height:1.6;margin-bottom:28px;">
        Accede a través del enlace personalizado que te enviamos,<br>
        o regístrate para obtener tu mes gratuito.
      </p>
      <a href="https://planningscout.com" style="display:inline-block;background:#1e3a5f;
         color:white;padding:12px 28px;border-radius:10px;font-weight:600;
         font-size:14px;text-decoration:none;">Ir a planningscout.com →</a>
    </div></div>""", unsafe_allow_html=True)
    st.stop()

# ════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# Colors: navy #1e3a5f | amber #c8860a | green #16a34a
# Logo slate: #5a5a78  | text: #0d1a2b | muted: #64748b
# ════════════════════════════════════════════════════════════
SHEET_ID = st.secrets.get("SHEET_ID", "")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,600;0,9..144,700;1,9..144,400&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ─ Streamlit chrome cleanup ─ */
#MainMenu, footer { visibility: hidden; }
header { visibility: hidden; }

/* ─ Sidebar: clean white with subtle left border ─ */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
    box-shadow: 2px 0 8px rgba(0,0,0,.03) !important;
}
[data-testid="stSidebar"] > div:first-child {
    padding: 0 !important;
}
[data-testid="stSidebarContent"] {
    padding: 0 !important;
}

/* ─ Main content: constrained width, proper padding ─ */
.block-container {
    padding: 28px 40px 40px !important;
    max-width: 1060px !important;
}

/* ─ App background ─ */
.stApp { background: #f7f8fa !important; }

/* ─ Remove Streamlit's default radio button ugliness ─ */
.stRadio > label { display: none !important; }
.stRadio > div { gap: 6px !important; }

/* ─ Slider label color fix ─ */
.stSlider label { color: #334155 !important; font-size: 13px !important; font-weight: 600 !important; }
.stSlider [data-testid="stWidgetLabel"] p { color: #334155 !important; }

/* ─ Selectbox / multiselect label ─ */
.stSelectbox label, .stMultiSelect label, .stNumberInput label {
    color: #334155 !important;
    font-size: 13px !important;
    font-weight: 600 !important;
}

/* ─ Expander: clean, not fighting with sidebar ─ */
[data-testid="stExpander"] {
    background: white !important;
    border: 1.5px solid #e2e8f0 !important;
    border-radius: 12px !important;
    margin-bottom: 0 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,.04) !important;
}
[data-testid="stExpander"] summary {
    padding: 12px 16px !important;
}
[data-testid="stExpander"] summary p {
    color: #334155 !important;
    font-size: 13px !important;
    font-weight: 600 !important;
}

/* ─ Download button ─ */
.stDownloadButton button {
    background: white !important;
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

/* ─ Button (refresh) ─ */
.stButton button {
    background: white !important;
    color: #334155 !important;
    border: 1.5px solid #e2e8f0 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
}
.stButton button:hover {
    border-color: #1e3a5f !important;
    color: #1e3a5f !important;
}

/* ─ Metric card ─ */
.ps-metric {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
}
.ps-metric .val {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 28px;
    font-weight: 600;
    color: #1e3a5f;
    line-height: 1;
    display: block;
}
.ps-metric .lbl {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: .07em;
    margin-top: 5px;
    display: block;
}

/* ─ LEAD CARD — matches mockup exactly ─ */
.lead-card {
    background: white;
    border: 1.5px solid #e2e8f0;
    border-radius: 14px;
    overflow: hidden;
    margin-bottom: 14px;
    box-shadow: 0 2px 8px rgba(0,0,0,.05);
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    transition: box-shadow .2s, border-color .2s;
}
.lead-card:hover {
    box-shadow: 0 6px 24px rgba(0,0,0,.09);
    border-color: #cbd5e1;
}

/* Card header: light grey like the mockup */
.lc-head {
    background: #f7f8fa;
    border-bottom: 1px solid #e2e8f0;
    padding: 14px 20px;
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
}
.lc-loc {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    flex: 1;
}
.lc-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #16a34a;
    flex-shrink: 0;
    margin-top: 2px;
}
.lc-muni {
    font-size: 14px;
    font-weight: 700;
    color: #0d1a2b;
    line-height: 1.3;
}
.lc-badges {
    display: flex;
    align-items: center;
    gap: 7px;
    flex-shrink: 0;
    flex-wrap: wrap;
    justify-content: flex-end;
}
/* Score pill — dark navy like mockup */
.score-pill {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    font-weight: 500;
    background: #1e3a5f;
    color: white;
    padding: 5px 12px;
    border-radius: 100px;
    white-space: nowrap;
}
.score-pill.gold   { background: #15803d; }
.score-pill.orange { background: #b45309; }
.score-pill.navy   { background: #1e3a5f; }
.score-pill.dim    { background: #94a3b8; }
/* Status badge — outlined like mockup */
.status-badge {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    padding: 4px 10px;
    border-radius: 100px;
    white-space: nowrap;
}
.status-verde    { background: #f0fdf4; color: #16a34a; border: 1px solid #bbf7d0; }
.status-amber    { background: #fffbeb; color: #b45309; border: 1px solid #fde68a; }
.status-navy     { background: #eff4fb; color: #1e3a5f; border: 1px solid #bfdbfe; }

/* Card body */
.lc-body { padding: 18px 20px; }
.lc-ref {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    color: #94a3b8;
    margin-bottom: 6px;
    letter-spacing: .03em;
}
.lc-title {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 17px;
    font-weight: 600;
    color: #0d1a2b;
    margin-bottom: 6px;
    line-height: 1.3;
}
.lc-addr {
    font-size: 13px;
    color: #64748b;
    display: flex;
    align-items: flex-start;
    gap: 5px;
    margin-bottom: 16px;
    line-height: 1.4;
}
.lc-addr-icon { flex-shrink: 0; margin-top: 1px; }

/* Data table inside card — matches mockup */
.lc-table {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 14px;
}
.lc-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 14px;
    border-bottom: 1px solid #f1f5f9;
    gap: 12px;
}
.lc-row:last-child { border-bottom: none; }
.lc-key {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: .08em;
    flex-shrink: 0;
    min-width: 80px;
}
.lc-val {
    font-size: 13px;
    color: #334155;
    text-align: right;
    line-height: 1.4;
}
.lc-val-pem {
    font-size: 16px;
    font-weight: 700;
    color: #1e3a5f;
    font-family: 'Fraunces', Georgia, serif;
}
.lc-tags { display: flex; gap: 5px; justify-content: flex-end; flex-wrap: wrap; }
.lc-tag-amber {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    background: #fffbeb;
    color: #b45309;
    border: 1px solid #fde68a;
    padding: 3px 9px;
    border-radius: 6px;
}
.lc-tag-navy {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    background: #eff4fb;
    color: #1e3a5f;
    border: 1px solid #bfdbfe;
    padding: 3px 9px;
    border-radius: 6px;
}

/* Card footer */
.lc-footer {
    background: #f7f8fa;
    border-top: 1px solid #e2e8f0;
    padding: 11px 20px;
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}
.lc-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    font-size: 12px;
    font-weight: 600;
    color: #334155;
    background: white;
    border: 1px solid #cbd5e1;
    padding: 6px 13px;
    border-radius: 8px;
    text-decoration: none;
    transition: border-color .15s, color .15s;
    white-space: nowrap;
}
.lc-btn:hover { border-color: #1e3a5f; color: #1e3a5f; text-decoration: none; }
.lc-btn.primary { background: #1e3a5f; color: white; border-color: #1e3a5f; }
.lc-btn.primary:hover { background: #162e4d; }
.lc-note {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #94a3b8;
    margin-left: auto;
}

/* ─ Section header ─ */
.ps-section-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
    padding-bottom: 12px;
    border-bottom: 1px solid #e2e8f0;
}
.ps-section-title {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 18px;
    font-weight: 600;
    color: #0d1a2b;
}
.ps-section-count {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    background: #1e3a5f;
    color: white;
    padding: 4px 12px;
    border-radius: 100px;
}

/* ─ Tip box ─ */
.ps-tip {
    background: #fffbeb;
    border-left: 3px solid #c8860a;
    border-radius: 0 8px 8px 0;
    padding: 11px 16px;
    font-size: 13px;
    color: #64748b;
    margin-bottom: 18px;
    line-height: 1.55;
}
.ps-tip strong { color: #9a6200; }

/* ─ Empty state ─ */
.ps-empty {
    text-align: center;
    padding: 60px 24px;
    background: white;
    border: 1.5px solid #e2e8f0;
    border-radius: 14px;
}
.ps-empty .icon { font-size: 40px; margin-bottom: 14px; }
.ps-empty h3 { font-family: 'Fraunces',Georgia,serif; font-size:19px; color:#0d1a2b; margin-bottom:8px; }
.ps-empty p { font-size:13px; color:#64748b; line-height:1.6; }

/* ─ Sidebar interior ─ */
.sb-logo {
    padding: 20px 20px 14px;
    border-bottom: 1px solid #e2e8f0;
    display: flex;
    align-items: center;
    gap: 10px;
}
.sb-logo-icon {
    width: 32px; height: 32px;
    background: #1e3a5f;
    border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 16px;
}
.sb-logo-text {
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    font-size: 15px;
    font-weight: 700;
    color: #0d1a2b;
    letter-spacing: -.2px;
}
.sb-logo-text em { color: #5a5a78; font-style: normal; }
.sb-section {
    padding: 16px 16px 0;
}
.sb-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: .1em;
    margin-bottom: 10px;
    display: block;
}
.sb-divider {
    height: 1px;
    background: #e2e8f0;
    margin: 16px 0 0;
}

/* ─ Mobile: profile pills ─ */
@media (max-width: 768px) {
    .block-container { padding: 16px 14px 40px !important; }
    .lc-head { padding: 12px 14px; }
    .lc-body { padding: 14px 14px; }
    .lc-footer { padding: 10px 14px; }
    .ps-metric .val { font-size: 22px; }
    .lc-title { font-size: 15px; }
    .lc-val-pem { font-size: 14px; }
}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════════════════════════
PROFILES = {
    "🔧 Instaladores MEP": {
        "key": "instaladores", "color": "#0e7490",
        "tip": "💡 <strong>Contacta al promotor 6-12 meses antes de la obra</strong> para entrar en las especificaciones técnicas de ascensores, HVAC y PCI. La licencia concedida es tu señal de arranque.",
        "min_score": 0, "min_value": 80_000, "days_default": 30,
        "permit_types": ["obra mayor", "cambio de uso", "declaración responsable", "licencia primera ocupación", "urbanización"],
    },
    "🏪 Expansión Retail": {
        "key": "expansion", "color": "#c2410c",
        "tip": "💡 <strong>Las urbanizaciones aprobadas = nuevos barrios en 2-3 años.</strong> Identifica la ubicación de tu próxima apertura antes de que el suelo suba de precio.",
        "min_score": 0, "min_value": 0, "days_default": 60,
        "permit_types": ["urbanización", "plan especial", "plan parcial", "cambio de uso", "licencia de actividad", "obra mayor nueva construcción"],
    },
    "📐 Promotores / RE": {
        "key": "promotores", "color": "#7c3aed",
        "tip": "💡 <strong>Una reparcelación aprobada = suelo urbanizable ahora.</strong> Contacta a la Junta de Compensación antes de que la operación salga al mercado.",
        "min_score": 20, "min_value": 300_000, "days_default": 60,
        "permit_types": ["urbanización", "plan parcial", "plan especial", "obra mayor nueva construcción", "cambio de uso"],
    },
    "🏢 Gran Constructora": {
        "key": "constructora", "color": "#be123c",
        "tip": "💡 <strong>Aprobación definitiva = licitación en 12-18 meses.</strong> Empieza a preparar el dossier técnico y las alianzas antes que la competencia.",
        "min_score": 35, "min_value": 2_000_000, "days_default": 90,
        "permit_types": ["urbanización", "plan especial", "plan parcial", "obra mayor industrial", "obra mayor nueva construcción"],
    },
    "🏭 Industrial / Log.": {
        "key": "industrial", "color": "#374151",
        "tip": "💡 <strong>Una licencia de nave = obra en 3-6 meses.</strong> Contacta al promotor ahora para la demolición previa o la ejecución completa.",
        "min_score": 0, "min_value": 200_000, "days_default": 60,
        "permit_types": ["obra mayor industrial", "urbanización", "obra mayor nueva construcción", "cambio de uso"],
    },
    "🛒 Compras / Materiales": {
        "key": "compras", "color": "#5a5a78",
        "tip": "💡 <strong>Todos los proyectos grandes son tu oportunidad.</strong> Con el nombre del promotor puedes presentar materiales antes de que la constructora adjudique suministros.",
        "min_score": 0, "min_value": 150_000, "days_default": 30,
        "permit_types": [],
    },
    "🏙️ Vista General": {
        "key": "general", "color": "#1e3a5f",
        "tip": "Vista completa de todos los proyectos. Selecciona un perfil para ver solo los leads de tu sector.",
        "min_score": 0, "min_value": 0, "days_default": 14,
        "permit_types": [],
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
    elif s.count(',') == 1:
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

def clean(v, fallback=""):
    s = str(v or fallback).strip()
    return "" if s in ("nan", "None", "—", "none") else s

def score_pill_class(sc):
    if sc >= 65: return "gold"
    if sc >= 40: return "orange"
    if sc >= 20: return "navy"
    return "dim"

def score_label(sc):
    if sc >= 65: return "🟢"
    if sc >= 40: return "🟠"
    if sc >= 20: return "🟡"
    return "⚪"

def build_lead_card(row):
    """Build the lead card HTML matching the mockup design."""
    sc    = int(row.get("score", 0))
    pem   = row.get("pem", 0)
    muni  = clean(row.get("municipio"), "Madrid")
    addr  = clean(row.get("direccion"))
    prom  = clean(row.get("promotor"))
    tipo  = clean(row.get("tipo"))
    desc  = clean(row.get("descripcion"))
    fecha = clean(row.get("fecha"))
    fecha_found = clean(row.get("fecha_encontrado"))
    maps  = clean(row.get("maps"))
    bocm  = clean(row.get("bocm_url"))
    pdf   = clean(row.get("pdf_url"))
    expd  = clean(row.get("expediente"))
    conf  = clean(row.get("confianza"))

    pem_str   = fmt_eur(pem)
    sp_class  = score_pill_class(sc)
    sp_emoji  = score_label(sc)

    # Publication date from URL or fecha field
    pub_date = fecha if fecha else ""
    if fecha_found:
        pub_date = fecha_found[:10]

    # Reference from BOCM URL
    bocm_ref = ""
    if bocm:
        m = re.search(r'BOCM[-_](\d{8})', bocm, re.I)
        if m:
            d = m.group(1)
            bocm_ref = f"BOCM-{d}"

    ref_str = bocm_ref if bocm_ref else ""
    if pub_date:
        try:
            dt = datetime.strptime(pub_date, "%Y-%m-%d")
            pub_fmt = dt.strftime("%-d %b %Y") if hasattr(datetime, 'strptime') else pub_date
        except Exception:
            pub_fmt = pub_date
        ref_str = f"{ref_str} · Publicado: {pub_fmt}" if ref_str else f"Publicado: {pub_date}"

    # Title: prefer address-based title
    if addr:
        title_text = addr
    elif desc:
        title_text = desc[:90]
    else:
        title_text = tipo or muni

    # Status badge based on tipo/desc
    status_text = ""
    status_class = "status-verde"
    t_low = tipo.lower() + desc.lower()
    if "definitiv" in t_low:
        status_text = "Aprobación definitiva"
        status_class = "status-verde"
    elif "inicial" in t_low or "provisional" in t_low:
        status_text = "Aprobación inicial"
        status_class = "status-amber"
    elif "concede" in t_low or "otorga" in t_low or "obra mayor" in t_low:
        status_text = "Licencia concedida"
        status_class = "status-verde"
    elif "urbanización" in t_low:
        status_text = "Urbanización"
        status_class = "status-navy"

    # ── CARD HEADER ──
    badge_html = f'<span class="score-pill {sp_class}">{sp_emoji} {sc} / 100</span>'
    if status_text:
        badge_html = f'<span class="status-badge {status_class}">{status_text}</span>' + badge_html

    head_html = f"""
    <div class="lc-head">
      <div class="lc-loc">
        <div class="lc-dot"></div>
        <span class="lc-muni">{muni}</span>
      </div>
      <div class="lc-badges">{badge_html}</div>
    </div>"""

    # ── CARD BODY ──
    ref_html   = f'<div class="lc-ref">{ref_str}</div>' if ref_str else ""
    title_html = f'<div class="lc-title">{title_text}</div>'
    addr_html  = ""
    if addr and addr != title_text:
        addr_html = f'''<div class="lc-addr">
          <span class="lc-addr-icon">📍</span>{addr}
        </div>'''

    # ── DATA TABLE ──
    table_rows = ""
    if tipo:
        table_rows += f'<div class="lc-row"><span class="lc-key">Tipo</span><span class="lc-val">{tipo}</span></div>'

    if pem > 0:
        table_rows += f'<div class="lc-row"><span class="lc-key">PEM Total</span><span class="lc-val lc-val-pem">{pem_str}</span></div>'

    # Check for etapas in description
    etapa_matches = re.findall(r'[Ee]tapa\s+(\d+)[^\d€]*€?([\d.,]+[MmKk]?)', desc)
    if etapa_matches:
        etapa_tags = ""
        for num, val in etapa_matches[:4]:
            etapa_tags += f'<span class="lc-tag-amber">Etapa {num}: {val}</span>'
        table_rows += f'<div class="lc-row"><span class="lc-key">Etapas</span><div class="lc-tags">{etapa_tags}</div></div>'

    if prom:
        table_rows += f'<div class="lc-row"><span class="lc-key">Promotor</span><span class="lc-val">{prom[:70]}</span></div>'

    if expd:
        table_rows += f'<div class="lc-row"><span class="lc-key">Expediente</span><span class="lc-val" style="font-family:\'JetBrains Mono\',monospace;font-size:11px;">{expd}</span></div>'

    if conf in ("high", "medium", "low") and table_rows:
        conf_map = {"high": ("Alta", "status-verde"), "medium": ("Media", "status-amber"), "low": ("Baja", "status-amber")}
        conf_txt, conf_cls = conf_map[conf]
        table_rows += f'<div class="lc-row"><span class="lc-key">Estado</span><div class="lc-tags"><span class="status-badge {conf_cls}">{conf_txt} fiabilidad</span></div></div>'

    table_html = f'<div class="lc-table">{table_rows}</div>' if table_rows else ""

    # ── FOOTER LINKS ──
    footer_links = ""
    if bocm:
        footer_links += f'<a href="{bocm}" target="_blank" class="lc-btn primary">↗ Ver en el BOCM</a>'
    if maps:
        footer_links += f'<a href="{maps}" target="_blank" class="lc-btn">📍 Mapa</a>'
    if pdf:
        footer_links += f'<a href="{pdf}" target="_blank" class="lc-btn">📑 PDF</a>'
    if prom:
        q = prom.replace(" ", "+")
        footer_links += f'<a href="https://www.linkedin.com/search/results/all/?keywords={q}" target="_blank" class="lc-btn">🔍 Promotor</a>'
    footer_note = '<span class="lc-note">Datos públicos oficiales · BOCM</span>'

    return f"""
<div class="lead-card">
  {head_html}
  <div class="lc-body">
    {ref_html}
    {title_html}
    {addr_html}
    {table_html}
  </div>
  <div class="lc-footer">
    {footer_links}
    {footer_note}
  </div>
</div>"""

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
            sa_info, scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", SHEET_ID)).worksheet("Permits")
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error conectando a Google Sheets: {e}")
        return pd.DataFrame()

# Load data first
with st.spinner("Cargando proyectos…"):
    df_raw = load_data()

if df_raw.empty:
    st.markdown("""
    <div class="ps-empty" style="margin:40px auto;max-width:500px;">
      <div class="icon">📡</div>
      <h3>Sin datos todavía</h3>
      <p>El scraper aún no ha procesado proyectos.<br>
      Ejecuta <strong>--weeks 8</strong> en GitHub Actions para hacer el backfill inicial.</p>
    </div>""", unsafe_allow_html=True)
    st.stop()

df = df_raw.rename(columns={k: v for k, v in COL_MAP.items() if k in df_raw.columns})
df["pem"]      = df["pem_raw"].apply(parse_value)   if "pem_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["est"]      = df["est_raw"].apply(parse_value)   if "est_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["score"]    = df["score_raw"].apply(parse_score) if "score_raw"        in df.columns else pd.Series(0, index=df.index)
df["fecha_dt"] = pd.to_datetime(
    df["fecha_encontrado"].str[:10], errors="coerce"
) if "fecha_encontrado" in df.columns else pd.NaT

all_munis = sorted([
    m for m in (df["municipio"].dropna().unique().tolist() if "municipio" in df.columns else [])
    if str(m).strip() and str(m) != "nan"
])

profile_names = list(PROFILES.keys())
if forced_profile_key:
    matched     = next((n for n, p in PROFILES.items() if p["key"] == forced_profile_key), profile_names[-1])
    default_idx = profile_names.index(matched)
    is_locked   = True
else:
    default_idx = len(profile_names) - 1
    is_locked   = False

# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:
    # Logo
    st.markdown("""
    <div class="sb-logo">
      <div class="sb-logo-icon">🏗️</div>
      <div class="sb-logo-text">Planning<em>Scout</em></div>
    </div>""", unsafe_allow_html=True)

    # Profile selector
    st.markdown('<div class="sb-section"><span class="sb-label">Perfil de cliente</span></div>', unsafe_allow_html=True)

    if is_locked:
        matched_prof = profile_names[default_idx]
        st.markdown(f"""
        <div style="margin:0 16px 0;padding:10px 14px;background:#eff4fb;
             border:1.5px solid rgba(30,58,95,.2);border-radius:10px;
             font-size:13px;font-weight:600;color:#1e3a5f;">
          {matched_prof}
        </div>""", unsafe_allow_html=True)
        selected_profile = matched_prof
    else:
        with st.container():
            selected_profile = st.radio(
                "Perfil",
                profile_names,
                index=default_idx,
                label_visibility="collapsed",
            )

    prof = PROFILES[selected_profile]

    st.markdown('<div class="sb-divider"></div><div class="sb-section" style="padding-top:14px;"><span class="sb-label">Filtros</span></div>', unsafe_allow_html=True)

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
    min_score = st.slider("Puntuación mínima", 0, 100, value=prof["min_score"], step=5)

    if all_munis:
        muni_sel = st.multiselect("Municipio", options=all_munis, placeholder="Todos")
    else:
        muni_sel = []

    st.markdown('<div class="sb-divider"></div>', unsafe_allow_html=True)

    if st.button("🔄 Actualizar datos"):
        st.cache_data.clear()
        st.rerun()

    # Sharing URL
    if not is_locked:
        with st.expander("🔗 Compartir con cliente"):
            prof_key = prof["key"]
            st.code(f"planningscout.streamlit.app?perfil={prof_key}", language=None)
            st.caption("El cliente accede directo a su perfil.")

    # Last update info
    last_dt = df["fecha_dt"].max()
    last_str = last_dt.strftime("%d %b %Y") if pd.notna(last_dt) else "—"
    st.markdown(f"""
    <div style="padding:16px 16px 20px;margin-top:8px;">
      <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.07em;margin-bottom:4px;">Última actualización</div>
      <div style="font-size:13px;color:#334155;font-weight:500;">{last_str}</div>
      <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;margin-top:8px;">Fuente: BOCM · CM Madrid</div>
    </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# MAIN CONTENT
# ════════════════════════════════════════════════════════════

# ── Page title ──
prof_color = prof["color"]
st.markdown(f"""
<div style="margin-bottom:24px;padding-bottom:20px;border-bottom:1px solid #e2e8f0;">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">
    <span style="font-size:22px;">{selected_profile.split()[0]}</span>
    <h1 style="font-family:'Fraunces',Georgia,serif;font-size:24px;font-weight:700;
         color:#0d1a2b;margin:0;line-height:1.2;">{' '.join(selected_profile.split()[1:])}</h1>
  </div>
  <p style="font-size:13px;color:#64748b;margin:0;font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
    Últimos {days_back} días · Datos del BOCM (Comunidad de Madrid)
  </p>
</div>
""", unsafe_allow_html=True)

# ── Filter data ──
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

# ── Metrics ──
total_pem  = df_f["pem"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])
avg_score  = int(df_f["score"].mean()) if count > 0 else 0

c1, c2, c3, c4 = st.columns(4)
metrics = [
    (str(count), "Proyectos"),
    (fmt_eur(total_pem), "PEM total"),
    (str(high_leads), "🟢 Prioritarios"),
    (f"{avg_score}", "Score medio"),
]
for col, (val, lbl) in zip([c1, c2, c3, c4], metrics):
    with col:
        color = "#16a34a" if lbl == "🟢 Prioritarios" else "#1e3a5f"
        st.markdown(f"""
        <div class="ps-metric">
          <span class="val" style="color:{color};">{val}</span>
          <span class="lbl">{lbl}</span>
        </div>""", unsafe_allow_html=True)

st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

# ── Profile tip ──
st.markdown(f'<div class="ps-tip">{prof["tip"]}</div>', unsafe_allow_html=True)

# ── Export ──
if not df_f.empty:
    export_cols = [c for c in ["fecha","municipio","direccion","promotor","tipo","pem_raw","est_raw","descripcion","expediente","bocm_url"] if c in df_f.columns]
    csv = df_f[export_cols].to_csv(index=False).encode("utf-8")
    col_dl, col_sp = st.columns([1, 3])
    with col_dl:
        st.download_button(
            f"⬇️ Exportar {count} leads (CSV)",
            data=csv,
            file_name=f"planningscout_{prof['key']}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── Leads ──
if df_f.empty:
    st.markdown(f"""
    <div class="ps-empty">
      <div class="icon">🔍</div>
      <h3>Sin proyectos con estos filtros</h3>
      <p>
        Prueba ampliando el período (ahora: {days_back} días),<br>
        reduciendo el PEM mínimo ({fmt_eur(min_pem)})<br>
        o cambiando el perfil de cliente.
      </p>
    </div>""", unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="ps-section-head">
      <span class="ps-section-title">Proyectos detectados</span>
      <span class="ps-section-count">{count} resultados</span>
    </div>""", unsafe_allow_html=True)

    for _, row in df_f.iterrows():
        row_d = row.to_dict()
        st.markdown(build_lead_card(row_d), unsafe_allow_html=True)

# ── Footer ──
st.markdown(f"""
<div style="text-align:center;padding:32px 0 16px;border-top:1px solid #e2e8f0;margin-top:32px;
     font-family:'JetBrains Mono',monospace;font-size:10.5px;color:#94a3b8;line-height:1.8;">
  <strong style="color:#5a5a78;">PlanningScout</strong> &nbsp;·&nbsp;
  Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) &nbsp;·&nbsp; Registros públicos oficiales<br>
  PEM = Presupuesto de Ejecución Material &nbsp;·&nbsp; {count} proyectos en esta vista &nbsp;·&nbsp; Actualizado {last_str}
</div>
""", unsafe_allow_html=True)
