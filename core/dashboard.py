import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import re
import html as html_lib
import os

# ════════════════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout — Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",  # always starts expanded
)

# ════════════════════════════════════════════════════════════
# AUTH
# ════════════════════════════════════════════════════════════
qp              = st.query_params
url_token       = qp.get("token", "")
url_profile     = qp.get("perfil", "")
client_tokens   = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

require_token      = str(st.secrets.get("REQUIRE_TOKEN", "false")).lower() == "true"
forced_profile_key = None
if url_token and url_token in client_tokens:
    forced_profile_key = client_tokens[url_token]
elif url_profile:
    forced_profile_key = url_profile.lower().replace(" ", "_")

if require_token and not forced_profile_key:
    st.markdown("""
    <div style="min-height:80vh;display:flex;align-items:center;justify-content:center;">
    <div style="text-align:center;max-width:380px;padding:48px 32px;background:#fff;
         border-radius:16px;box-shadow:0 8px 40px rgba(0,0,0,.1);border:1px solid #e2e8f0;
         font-family:system-ui,sans-serif;">
      <div style="font-size:44px;margin-bottom:20px;">🔒</div>
      <h2 style="font-size:22px;color:#0d1a2b;margin:0 0 12px;font-weight:700;">Acceso restringido</h2>
      <p style="color:#64748b;font-size:14px;line-height:1.6;margin:0 0 28px;">
        Accede mediante el enlace personalizado que te enviamos,
        o regístrate para tu mes gratuito.
      </p>
      <a href="https://planningscout.com" style="display:inline-block;background:#1e3a5f;
         color:#fff;padding:12px 28px;border-radius:10px;font-weight:600;
         font-size:14px;text-decoration:none;">Ir a planningscout.com →</a>
    </div></div>""", unsafe_allow_html=True)
    st.stop()

# ════════════════════════════════════════════════════════════
# LOGO PATH  
# navbar.png lives in the same folder as dashboard.py (core/)
# ════════════════════════════════════════════════════════════
LOGO_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "navbar.png")
SHEET_ID  = st.secrets.get("SHEET_ID", "")

# ════════════════════════════════════════════════════════════
# CSS — surgical, no conflicts, sidebar reopen button kept
# Key rules:
#   - block-container: proper padding + max-width
#   - sidebar: white bg, padding inside
#   - NO hiding of collapsedControl (that's the reopen button)
#   - All lead card classes defined here
# ════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,600;0,9..144,700;1,9..144,400&family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Streamlit chrome ── */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* ── App background ── */
.stApp { background: #f0f2f5 !important; }

/* ── Main content: breathing room on both sides ── */
.block-container {
    padding-top: 32px !important;
    padding-bottom: 48px !important;
    padding-left: 48px !important;
    padding-right: 48px !important;
    max-width: 1100px !important;
}

/* ── Sidebar: white, clean, padded ── */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
}
[data-testid="stSidebarContent"] {
    padding: 0 20px 32px 20px !important;
}

/* ── Sidebar labels: dark text, good contrast ── */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stRadio label,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
    color: #334155 !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif !important;
}

/* ── Sidebar radio options: readable ── */
[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label span {
    color: #0d1a2b !important;
    font-size: 13px !important;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif !important;
}

/* ── Sidebar selectbox / number input ── */
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stNumberInput label,
[data-testid="stSidebar"] .stMultiSelect label,
[data-testid="stSidebar"] .stSlider label {
    color: #334155 !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}

/* ── Sidebar slider value text ── */
[data-testid="stSidebar"] .stSlider [data-testid="stTickBar"] span,
[data-testid="stSidebar"] .stSlider p {
    color: #334155 !important;
}

/* ── Sidebar expander ── */
[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: #f7f8fa !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary p {
    color: #334155 !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}

/* ── Download button ── */
.stDownloadButton button {
    background: #ffffff !important;
    color: #1e3a5f !important;
    border: 1.5px solid #cbd5e1 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    padding: 6px 16px !important;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif !important;
}
.stDownloadButton button:hover {
    border-color: #1e3a5f !important;
    background: #eff4fb !important;
}

/* ── Refresh button ── */
.stButton button {
    background: #ffffff !important;
    color: #334155 !important;
    border: 1.5px solid #e2e8f0 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif !important;
}
.stButton button:hover {
    border-color: #1e3a5f !important;
    color: #1e3a5f !important;
}

/* ── Metric cards ── */
.ps-m {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
}
.ps-m .v {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 26px;
    font-weight: 700;
    color: #1e3a5f;
    line-height: 1;
    display: block;
    margin-bottom: 5px;
}
.ps-m .l {
    font-family: 'JetBrains Mono', monospace;
    font-size: 9.5px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: .08em;
}

/* ── Tip box ── */
.ps-tip {
    background: #fffbeb;
    border-left: 3px solid #c8860a;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px;
    font-size: 13px;
    color: #64748b;
    line-height: 1.6;
    margin: 18px 0;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
}
.ps-tip strong { color: #9a6200; }

/* ── Section header ── */
.ps-sh {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin: 0 0 14px;
    padding-bottom: 12px;
    border-bottom: 1px solid #e2e8f0;
}
.ps-sh h2 {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 19px;
    font-weight: 700;
    color: #0d1a2b;
    margin: 0;
}
.ps-sh .cnt {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    background: #1e3a5f;
    color: #fff;
    padding: 4px 12px;
    border-radius: 100px;
}

/* ═══════════════════════════════════════════
   LEAD CARDS — matching the mockup exactly
   Light grey header, white body, grey footer
═══════════════════════════════════════════ */
.lcard {
    background: #ffffff;
    border: 1.5px solid #e2e8f0;
    border-radius: 14px;
    overflow: hidden;
    margin-bottom: 14px;
    box-shadow: 0 2px 8px rgba(0,0,0,.05);
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    transition: box-shadow .2s, border-color .2s;
}
.lcard:hover {
    box-shadow: 0 6px 24px rgba(0,0,0,.09);
    border-color: #cbd5e1;
}

/* Card header — light grey bg (matches mockup) */
.lcard-h {
    background: #f7f8fa;
    border-bottom: 1px solid #e2e8f0;
    padding: 13px 20px;
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
}
.lcard-loc {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    min-width: 0;
    flex: 1;
}
.lcard-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #16a34a;
    flex-shrink: 0;
    margin-top: 4px;
}
.lcard-muni {
    font-size: 14px;
    font-weight: 700;
    color: #0d1a2b;
    line-height: 1.3;
    word-break: break-word;
}
.lcard-badges {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-shrink: 0;
    flex-wrap: wrap;
    justify-content: flex-end;
}
/* Score pill — dark navy like mockup */
.sp {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    font-weight: 500;
    padding: 4px 11px;
    border-radius: 100px;
    white-space: nowrap;
    color: #fff;
}
.sp-g { background: #15803d; }   /* 65+ green */
.sp-o { background: #b45309; }   /* 40+ amber */
.sp-n { background: #1e3a5f; }   /* 20+ navy */
.sp-d { background: #94a3b8; }   /* <20 dim   */

/* Status badge — outlined (matches mockup) */
.sb {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    padding: 4px 10px;
    border-radius: 100px;
    white-space: nowrap;
}
.sb-g { background: #f0fdf4; color: #16a34a; border: 1px solid #bbf7d0; }
.sb-a { background: #fffbeb; color: #b45309; border: 1px solid #fde68a; }
.sb-n { background: #eff4fb; color: #1e3a5f; border: 1px solid #bfdbfe; }

/* Card body */
.lcard-b { padding: 16px 20px; }

.lcard-ref {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    color: #94a3b8;
    margin-bottom: 5px;
    letter-spacing: .03em;
}
.lcard-title {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 17px;
    font-weight: 600;
    color: #0d1a2b;
    margin-bottom: 5px;
    line-height: 1.3;
}
.lcard-addr {
    font-size: 13px;
    color: #64748b;
    display: flex;
    align-items: flex-start;
    gap: 5px;
    margin-bottom: 14px;
    line-height: 1.4;
}

/* Data table inside card */
.lcard-t {
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 12px;
}
.lcard-r {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 9px 14px;
    border-bottom: 1px solid #f1f5f9;
    gap: 12px;
}
.lcard-r:last-child { border-bottom: none; }
.lcard-k {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: .08em;
    flex-shrink: 0;
    min-width: 75px;
}
.lcard-v {
    font-size: 13px;
    color: #334155;
    text-align: right;
    line-height: 1.4;
    word-break: break-word;
}
.lcard-v-pem {
    font-size: 17px;
    font-weight: 700;
    color: #1e3a5f;
    font-family: 'Fraunces', Georgia, serif;
}
.lcard-tags { display: flex; gap: 5px; justify-content: flex-end; flex-wrap: wrap; }
.tag-a {
    font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
    background: #fffbeb; color: #b45309; border: 1px solid #fde68a;
    padding: 3px 8px; border-radius: 5px;
}
.tag-n {
    font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
    background: #eff4fb; color: #1e3a5f; border: 1px solid #bfdbfe;
    padding: 3px 8px; border-radius: 5px;
}
.tag-g {
    font-family: 'JetBrains Mono', monospace; font-size: 10.5px;
    background: #f0fdf4; color: #16a34a; border: 1px solid #bbf7d0;
    padding: 3px 8px; border-radius: 5px;
}

/* Card footer */
.lcard-f {
    background: #f7f8fa;
    border-top: 1px solid #e2e8f0;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 7px;
    flex-wrap: wrap;
}
.lcard-btn {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    font-size: 12px;
    font-weight: 600;
    color: #334155;
    background: #ffffff;
    border: 1px solid #cbd5e1;
    padding: 5px 12px;
    border-radius: 7px;
    text-decoration: none !important;
    white-space: nowrap;
    transition: border-color .15s, color .15s;
}
.lcard-btn:hover {
    border-color: #1e3a5f;
    color: #1e3a5f;
    text-decoration: none !important;
}
.lcard-btn-p {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
    font-size: 12px;
    font-weight: 600;
    color: #ffffff;
    background: #1e3a5f;
    border: 1px solid #1e3a5f;
    padding: 5px 12px;
    border-radius: 7px;
    text-decoration: none !important;
    white-space: nowrap;
    transition: background .15s;
}
.lcard-btn-p:hover { background: #162e4d; text-decoration: none !important; }
.lcard-note {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #94a3b8;
    margin-left: auto;
}

/* ── Empty state ── */
.ps-empty {
    text-align: center;
    padding: 56px 24px;
    background: #fff;
    border: 1.5px solid #e2e8f0;
    border-radius: 14px;
    font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
}
.ps-empty h3 {
    font-family: 'Fraunces', Georgia, serif;
    font-size: 19px; color: #0d1a2b; margin: 14px 0 8px;
}
.ps-empty p { font-size: 13px; color: #64748b; line-height: 1.6; margin: 0; }

/* ── Mobile ── */
@media (max-width: 768px) {
    .block-container {
        padding-left: 16px !important;
        padding-right: 16px !important;
        padding-top: 16px !important;
    }
    .lcard-h { padding: 11px 14px; }
    .lcard-b { padding: 13px 14px; }
    .lcard-f { padding: 9px 14px; }
    .lcard-title { font-size: 15px; }
    .lcard-v-pem { font-size: 14px; }
    .ps-m .v { font-size: 22px; }
}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════════════════════════
PROFILES = {
    "🔧 Instaladores MEP": {
        "key": "instaladores",
        "tip": "💡 <strong>Contacta al promotor 6-12 meses antes de la obra</strong> — antes de que cierre contratos con tus competidores. La licencia concedida es tu señal de arranque.",
        "min_score": 0, "min_value": 80_000, "days": 30,
        "types": ["obra mayor", "cambio de uso", "declaración responsable", "licencia primera ocupación", "urbanización"],
    },
    "🏪 Expansión Retail": {
        "key": "expansion",
        "tip": "💡 <strong>Urbanización aprobada = nuevo barrio en 2-3 años.</strong> Negocia el local comercial ahora antes de que suba el precio del suelo.",
        "min_score": 0, "min_value": 0, "days": 60,
        "types": ["urbanización", "plan especial", "plan parcial", "cambio de uso", "licencia de actividad", "obra mayor nueva construcción"],
    },
    "📐 Promotores / RE": {
        "key": "promotores",
        "tip": "💡 <strong>Reparcelación aprobada = suelo urbanizable.</strong> Contacta a la Junta de Compensación antes de que la operación salga al mercado.",
        "min_score": 20, "min_value": 300_000, "days": 60,
        "types": ["urbanización", "plan parcial", "plan especial", "obra mayor nueva construcción", "cambio de uso"],
    },
    "🏢 Gran Constructora": {
        "key": "constructora",
        "tip": "💡 <strong>Aprobación definitiva = licitación en 12-18 meses.</strong> Empieza ya a preparar el dossier técnico y alianzas.",
        "min_score": 35, "min_value": 2_000_000, "days": 90,
        "types": ["urbanización", "plan especial", "plan parcial", "obra mayor industrial", "obra mayor nueva construcción"],
    },
    "🏭 Industrial / Log.": {
        "key": "industrial",
        "tip": "💡 <strong>Licencia de nave = obra en 3-6 meses.</strong> Contacta al promotor para la demolición previa o ejecución completa.",
        "min_score": 0, "min_value": 200_000, "days": 60,
        "types": ["obra mayor industrial", "urbanización", "obra mayor nueva construcción", "cambio de uso"],
    },
    "🛒 Compras / Materiales": {
        "key": "compras",
        "tip": "💡 <strong>Todo proyecto grande = oportunidad de suministro.</strong> Preséntate antes de que la constructora adjudique materiales.",
        "min_score": 0, "min_value": 150_000, "days": 30,
        "types": [],
    },
    "🏙️ Vista General": {
        "key": "general",
        "tip": "Vista completa de todos los proyectos. Selecciona un perfil para ver solo los leads relevantes para tu sector.",
        "min_score": 0, "min_value": 0, "days": 14,
        "types": [],
    },
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def e(v):
    """HTML-escape a value. Prevents raw data from breaking HTML."""
    if not v or str(v).strip() in ("", "nan", "None", "—"):
        return ""
    return html_lib.escape(str(v).strip())

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

def parse_sc(v):
    try:
        return int(float(str(v).strip())) if str(v).strip() else 0
    except Exception:
        return 0

def fmt(v):
    if v == 0: return "—"
    if v >= 1_000_000: return f"€{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"€{int(v/1000)}K"
    return f"€{int(v):,}"

def sc_class(sc):
    if sc >= 65: return "sp-g"
    if sc >= 40: return "sp-o"
    if sc >= 20: return "sp-n"
    return "sp-d"

def sc_emoji(sc):
    if sc >= 65: return "🟢"
    if sc >= 40: return "🟠"
    if sc >= 20: return "🟡"
    return "⚪"

def lead_card(row):
    """
    Build a lead card HTML that matches the mockup.
    ALL data values are html.escape()'d to prevent broken HTML.
    NO inline styles with escaped quotes — only CSS classes.
    """
    sc      = parse_sc(row.get("score_raw", 0))
    pem     = parse_val(row.get("pem_raw", ""))
    muni    = e(row.get("municipio", "Madrid")) or "Madrid"
    addr    = e(row.get("direccion", ""))
    prom    = e(row.get("promotor", ""))
    tipo    = e(row.get("tipo", ""))
    desc    = e(row.get("descripcion", ""))
    fecha   = e(row.get("fecha", ""))
    fnd     = e(row.get("fecha_encontrado", ""))
    maps    = str(row.get("maps", "") or "").strip()
    bocm    = str(row.get("bocm_url", "") or "").strip()
    pdf     = str(row.get("pdf_url", "") or "").strip()
    expd    = e(row.get("expediente", ""))
    conf    = str(row.get("confianza", "") or "").strip()

    pem_str = fmt(pem)
    spc     = sc_class(sc)
    spe     = sc_emoji(sc)

    # Format BOCM reference
    bocm_ref = ""
    if bocm:
        m = re.search(r'BOCM[-_](\d{8})', bocm, re.I)
        if m:
            d = m.group(1)
            bocm_ref = f"BOCM-{d}"

    # Format publication date
    pub_date = fnd[:10] if fnd else fecha
    try:
        dt = datetime.strptime(pub_date, "%Y-%m-%d")
        pub_fmt = dt.strftime("%-d %b %Y")
    except Exception:
        pub_fmt = pub_date

    ref_parts = [p for p in [bocm_ref, f"Publicado: {pub_fmt}" if pub_fmt else ""] if p]
    ref_str   = " · ".join(ref_parts)

    # Choose title
    title = addr if addr else (desc[:90] if desc else tipo)

    # Status badge from tipo
    status_html = ""
    tl = tipo.lower() + " " + desc.lower()
    if "definitiv" in tl:
        status_html = '<span class="sb sb-g">Aprobación definitiva</span>'
    elif "inicial" in tl:
        status_html = '<span class="sb sb-a">Aprobación inicial</span>'
    elif "concede" in tl or "otorga" in tl:
        status_html = '<span class="sb sb-g">Licencia concedida</span>'
    elif tipo:
        status_html = f'<span class="sb sb-n">{tipo[:30]}</span>'

    # ── HEADER ──
    head = f"""
<div class="lcard-h">
  <div class="lcard-loc">
    <div class="lcard-dot"></div>
    <span class="lcard-muni">{muni}</span>
  </div>
  <div class="lcard-badges">
    {status_html}
    <span class="sp {spc}">{spe} {sc} / 100</span>
  </div>
</div>"""

    # ── BODY ──
    ref_html   = f'<div class="lcard-ref">{ref_str}</div>' if ref_str else ""
    title_html = f'<div class="lcard-title">{title}</div>'
    addr_html  = ""
    if addr and addr != title:
        addr_html = f'<div class="lcard-addr"><span>📍</span><span>{addr}</span></div>'

    # ── TABLE ROWS ──
    rows = ""
    if tipo:
        rows += f'<div class="lcard-r"><span class="lcard-k">Tipo</span><span class="lcard-v">{tipo}</span></div>'
    if pem > 0:
        rows += f'<div class="lcard-r"><span class="lcard-k">PEM Total</span><span class="lcard-v-pem">{pem_str}</span></div>'

    # Detect etapas from description
    etapa_m = re.findall(r'[Ee]tapa\s*(\d+)[^€\d]*?(\d[\d.,]+\s*(?:M|K|€)?)', e(row.get("descripcion", "")))
    if etapa_m:
        etag = "".join(f'<span class="tag-a">Etapa {n}: {v}</span>' for n, v in etapa_m[:3])
        rows += f'<div class="lcard-r"><span class="lcard-k">Etapas</span><div class="lcard-tags">{etag}</div></div>'

    if prom:
        rows += f'<div class="lcard-r"><span class="lcard-k">Promotor</span><span class="lcard-v">{prom}</span></div>'
    if expd:
        rows += f'<div class="lcard-r"><span class="lcard-k">Expediente</span><span class="lcard-v">{expd}</span></div>'
    if conf in ("high", "medium", "low"):
        conf_map = {"high": ("Alta fiabilidad", "tag-g"), "medium": ("Media fiabilidad", "tag-a"), "low": ("Baja fiabilidad", "tag-a")}
        ct, cc = conf_map[conf]
        rows += f'<div class="lcard-r"><span class="lcard-k">Fiabilidad</span><div class="lcard-tags"><span class="{cc}">{ct}</span></div></div>'

    table_html = f'<div class="lcard-t">{rows}</div>' if rows else ""

    # ── FOOTER ──
    links = ""
    if bocm:
        links += f'<a href="{bocm}" target="_blank" rel="noopener" class="lcard-btn-p">↗ Ver en el BOCM</a>'
    if maps:
        links += f'<a href="{maps}" target="_blank" rel="noopener" class="lcard-btn">📍 Mapa</a>'
    if pdf:
        links += f'<a href="{pdf}" target="_blank" rel="noopener" class="lcard-btn">📑 PDF</a>'
    if prom:
        q = html_lib.escape(prom).replace("&amp;", "%26").replace(" ", "+")
        links += f'<a href="https://www.linkedin.com/search/results/all/?keywords={q}" target="_blank" rel="noopener" class="lcard-btn">🔍 Promotor</a>'

    return f"""
<div class="lcard">
  {head}
  <div class="lcard-b">
    {ref_html}
    {title_html}
    {addr_html}
    {table_html}
  </div>
  <div class="lcard-f">
    {links}
    <span class="lcard-note">Datos públicos oficiales · BOCM</span>
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
        sa = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(st.secrets.get("SHEET_ID", SHEET_ID)).worksheet("Permits")
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as ex:
        st.error(f"Error conectando a Google Sheets: {ex}")
        return pd.DataFrame()

with st.spinner("Cargando proyectos…"):
    df_raw = load_data()

if df_raw.empty:
    st.markdown("""
    <div class="ps-empty" style="margin:40px auto;max-width:500px;">
      <div style="font-size:40px;">📡</div>
      <h3>Sin datos todavía</h3>
      <p>El scraper no ha procesado ningún proyecto aún.<br>
      Ejecuta <strong>--weeks 8</strong> en GitHub Actions para el backfill inicial.</p>
    </div>""", unsafe_allow_html=True)
    st.stop()

df = df_raw.rename(columns={k: v for k, v in COL_MAP.items() if k in df_raw.columns})
df["pem"]      = df["pem_raw"].apply(parse_val)  if "pem_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["score"]    = df["score_raw"].apply(parse_sc) if "score_raw"        in df.columns else pd.Series(0,   index=df.index)
df["fecha_dt"] = pd.to_datetime(
    df["fecha_encontrado"].str[:10], errors="coerce"
) if "fecha_encontrado" in df.columns else pd.NaT

all_munis = sorted([
    m for m in (df["municipio"].dropna().unique().tolist() if "municipio" in df.columns else [])
    if str(m).strip() and str(m) not in ("nan","")
])

profile_names = list(PROFILES.keys())
if forced_profile_key:
    matched     = next((n for n, p in PROFILES.items() if p["key"] == forced_profile_key), profile_names[-1])
    default_idx = profile_names.index(matched)
    is_locked   = True
else:
    default_idx = len(profile_names) - 1   # Vista General
    is_locked   = False

# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:

    # Logo image — loaded from same folder as this file
    try:
        st.image(LOGO_PATH, width=190)
    except Exception:
        st.markdown("### 🏗️ PlanningScout")

    st.markdown('<div style="height:4px;border-bottom:1px solid #e2e8f0;margin:0 0 16px;"></div>', unsafe_allow_html=True)

    # Profile selector
    st.markdown('<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin:0 0 10px;">Perfil de cliente</p>', unsafe_allow_html=True)

    if is_locked:
        st.markdown(f"""
        <div style="background:#eff4fb;border:1.5px solid rgba(30,58,95,.2);border-radius:10px;
             padding:10px 14px;font-size:13px;font-weight:600;color:#1e3a5f;margin-bottom:16px;
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

    st.markdown('<div style="border-top:1px solid #e2e8f0;margin:14px 0 16px;"></div>', unsafe_allow_html=True)
    st.markdown('<p style="font-family:\'JetBrains Mono\',monospace;font-size:10px;color:#94a3b8;text-transform:uppercase;letter-spacing:.08em;margin:0 0 12px;">Filtros</p>', unsafe_allow_html=True)

    days_back = st.selectbox(
        "Período",
        [7, 14, 30, 60, 90],
        index=[7,14,30,60,90].index(prof["days"]) if prof["days"] in [7,14,30,60,90] else 1,
        format_func=lambda x: f"Últimos {x} días",
    )
    min_pem   = st.number_input("PEM mínimo (€)", value=prof["min_value"], min_value=0, step=50_000, format="%d")
    min_score = st.slider("Puntuación mínima", 0, 100, value=prof["min_score"], step=5)
    muni_sel  = st.multiselect("Municipio", options=all_munis, placeholder="Todos los municipios")

    st.markdown('<div style="border-top:1px solid #e2e8f0;margin:16px 0;"></div>', unsafe_allow_html=True)

    if st.button("🔄 Actualizar datos"):
        st.cache_data.clear()
        st.rerun()

    if not is_locked:
        with st.expander("🔗 Compartir vista con cliente"):
            st.code(f"planningscout.streamlit.app?perfil={prof['key']}", language=None)
            st.caption("El cliente accede directamente a su perfil filtrado.")

    # Last update
    last_dt  = df["fecha_dt"].max() if "fecha_dt" in df.columns else None
    last_str = last_dt.strftime("%d %b %Y") if pd.notna(last_dt) else "—"
    st.markdown(f"""
    <div style="margin-top:20px;padding:12px 14px;background:#f7f8fa;border-radius:8px;
         border:1px solid #e2e8f0;font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
      <p style="font-family:'JetBrains Mono',monospace;font-size:9.5px;color:#94a3b8;
         text-transform:uppercase;letter-spacing:.07em;margin:0 0 3px;">Última actualización</p>
      <p style="font-size:13px;font-weight:600;color:#334155;margin:0;">{last_str}</p>
      <p style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;
         margin:4px 0 0;">BOCM · Comunidad de Madrid</p>
    </div>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# MAIN CONTENT
# ════════════════════════════════════════════════════════════

# Page title
emoji = selected_profile.split()[0]
name  = " ".join(selected_profile.split()[1:])
st.markdown(f"""
<div style="margin-bottom:24px;padding-bottom:18px;border-bottom:1px solid #e2e8f0;">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
    <span style="font-size:24px;">{emoji}</span>
    <h1 style="font-family:'Fraunces',Georgia,serif;font-size:26px;font-weight:700;
         color:#0d1a2b;margin:0;line-height:1.2;">{name}</h1>
  </div>
  <p style="font-size:13px;color:#64748b;margin:0;
     font-family:'Plus Jakarta Sans',system-ui,sans-serif;">
    Últimos {days_back} días &nbsp;·&nbsp; Proyectos detectados del BOCM (Comunidad de Madrid)
  </p>
</div>
""", unsafe_allow_html=True)

# ── Filter ──
cutoff = datetime.now() - timedelta(days=days_back)
df_f   = df[df["fecha_dt"] >= cutoff].copy() if "fecha_dt" in df.columns else df.copy()

if min_score > 0:
    df_f = df_f[(df_f["score"] >= min_score) | (df_f["score"] == 0)]
df_f = df_f[df_f["pem"] >= min_pem]

if prof["types"] and "tipo" in df_f.columns:
    pat  = "|".join(re.escape(t) for t in prof["types"])
    df_f = df_f[df_f["tipo"].str.contains(pat, case=False, na=False)]

if muni_sel and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(muni_sel)]

df_f = df_f.sort_values(["score", "pem"], ascending=[False, False]).reset_index(drop=True)

# ── Metrics ──
total_pem  = df_f["pem"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])
avg_score  = int(df_f["score"].mean()) if count > 0 else 0

c1, c2, c3, c4 = st.columns(4)
for col, (val, lbl, color) in zip(
    [c1, c2, c3, c4],
    [
        (str(count),         "Proyectos",       "#1e3a5f"),
        (fmt(total_pem),     "PEM total",        "#1e3a5f"),
        (str(high_leads),    "🟢 Prioritarios",  "#16a34a"),
        (f"{avg_score} pts", "Score medio",      "#5a5a78"),
    ]
):
    with col:
        st.markdown(f"""
        <div class="ps-m">
          <span class="v" style="color:{color};">{val}</span>
          <span class="l">{lbl}</span>
        </div>""", unsafe_allow_html=True)

# ── Tip ──
st.markdown(f'<div class="ps-tip">{prof["tip"]}</div>', unsafe_allow_html=True)

# ── Export ──
if not df_f.empty:
    exp_cols = [c for c in ["fecha","municipio","direccion","promotor","tipo","pem_raw","descripcion","expediente","bocm_url"] if c in df_f.columns]
    csv = df_f[exp_cols].to_csv(index=False).encode("utf-8")
    col_btn, col_inf = st.columns([1, 3])
    with col_btn:
        st.download_button(
            f"⬇️ Exportar {count} leads CSV",
            data=csv,
            file_name=f"planningscout_{prof['key']}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

# ── Leads ──
if df_f.empty:
    st.markdown(f"""
    <div class="ps-empty">
      <div style="font-size:40px;">🔍</div>
      <h3>Sin proyectos con estos filtros</h3>
      <p>
        Amplía el período (ahora: {days_back} días),<br>
        reduce el PEM mínimo ({fmt(min_pem)}),<br>
        o cambia el perfil en el panel izquierdo.
      </p>
    </div>""", unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="ps-sh">
      <h2>Proyectos detectados</h2>
      <span class="cnt">{count} resultado{"s" if count != 1 else ""}</span>
    </div>""", unsafe_allow_html=True)

    for _, row in df_f.iterrows():
        st.markdown(lead_card(row.to_dict()), unsafe_allow_html=True)

# ── Footer ──
st.markdown(f"""
<div style="text-align:center;padding:28px 0 8px;margin-top:28px;border-top:1px solid #e2e8f0;
     font-family:'JetBrains Mono',monospace;font-size:10px;color:#94a3b8;line-height:1.9;">
  <strong style="color:#5a5a78;font-size:11px;">PlanningScout Madrid</strong><br>
  Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales<br>
  PEM = Presupuesto de Ejecución Material · {count} proyectos · Actualizado {last_str}
</div>
""", unsafe_allow_html=True)
