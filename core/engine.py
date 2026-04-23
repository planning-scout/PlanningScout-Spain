import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib, random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials as SACredentials
import urllib3
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as BS4  # Rename to avoid conflict
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ════════════════════════════════════════════════════════════
# TIME BUDGET  —  graceful exit before GitHub kills the job
# Set timeout-minutes: 350 in your workflow YAML.
# Engine exits cleanly with --resume-ready queue at 340min.
# ════════════════════════════════════════════════════════════
RUN_START       = datetime.now()
MAX_RUN_MINUTES = int(os.environ.get("MAX_RUN_MINUTES", "340"))
GRACE_MINUTES   = 8

def elapsed():
    return (datetime.now() - RUN_START).total_seconds()

def elapsed_str():
    e = elapsed()
    return f"{int(e//60)}m{int(e%60):02d}s"

def time_ok(need_s=60):
    return (MAX_RUN_MINUTES * 60 - elapsed()) > (GRACE_MINUTES * 60 + need_s)

# ════════════════════════════════════════════════════════════
# ARGS
# ════════════════════════════════════════════════════════════
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--weeks",   type=int, default=1,
    help="1=daily(2 days), 2-8=weekly, 9+=full backfill")
parser.add_argument("--digest",  action="store_true")
parser.add_argument("--resume",  action="store_true",
    help="Skip collection, process saved queue from previous run")
parser.add_argument("--backfill-ai", action="store_true",
    help="Re-run AI evaluation on existing sheet rows with empty AI Evaluation")
parser.add_argument("--workers", type=int, default=4,
    help="Concurrent processing threads (default 4)")
parser.add_argument("--max-pages-backfill", type=int, default=6,
    help="Max pages per keyword per date-chunk in backfill mode.")
# Legacy --client flag: accepted but ignored (config is now embedded below)
parser.add_argument("--client",  default="", help="(Legacy — ignored)")
args = parser.parse_args()

# ════════════════════════════════════════════════════════════════════
# CLIENT CONFIG  — was previously in demo_madrid.json
# All settings are now embedded here. No external .json file needed.
# ════════════════════════════════════════════════════════════════════
SHEET_ID         = os.environ.get("SHEET_ID", "1Hqb54sgS-METHGdPEqlACnqVt1ZLKrzjRXCVvwn0FgA")
CLIENT_EMAIL_VAR = os.environ.get("EMAIL_SECRET_NAME", "GMAIL_TO_DEMO_MADRID")

# ── PEM filter ────────────────────────────────────────────────────
# IMPORTANT: This filter ONLY skips rows where the PEM is explicitly
# declared AND below this threshold. Rows with NO declared PEM (the
# vast majority of plan especial/urbanización docs) are ALWAYS saved.
# Set to 0 to disable filtering entirely.
MIN_VALUE_EUR        = int(os.environ.get("MIN_VALUE_EUR", "0"))

# ── datos.madrid.es Cloudflare Worker proxy ──────────────────────────────
# datos.madrid.es blocks all cloud/datacenter IPs (GitHub Actions = 403).
# Solution: deploy a free Cloudflare Worker that relays API calls.
# The Worker runs on Cloudflare edge IPs which datos.madrid.es does NOT block.
#
# HOW TO SET UP (5 minutes, free):
#   1. Go to https://workers.cloudflare.com/ → Create Worker
#   2. Paste this JS and deploy:
#      export default { async fetch(req) {
#        const t = new URL(req.url).searchParams.get("url");
#        if (!t?.startsWith("https://datos.madrid.es/")) return new Response("",{status:403});
#        const r = await fetch(t,{headers:{"User-Agent":"Mozilla/5.0","Accept":"application/json","Accept-Language":"es-ES"}});
#        return new Response(await r.text(),{headers:{"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}});
#      }}
#   3. Note your worker URL (e.g. https://planningscout.myname.workers.dev)
#   4. Add GitHub secret: DATOS_MADRID_PROXY = https://planningscout.myname.workers.dev
#
# Without this set → engine tries direct (will be blocked from GitHub Actions)
DATOS_MADRID_PROXY   = os.environ.get("DATOS_MADRID_PROXY", "").rstrip("/")

# Apollo.io contact enrichment — finds CEO/director email + LinkedIn for applicants
# Free tier: 600 searches/month. Set APOLLO_API_KEY in GitHub Secrets.
APOLLO_API_KEY       = os.environ.get("APOLLO_API_KEY", "")
_apollo_cache: dict  = {}   # company_name → enriched contact string

# ── Keywords to hard-exclude from results ────────────────────────
# These override the classifier — any document containing these exact
# strings is rejected regardless of other signals.
KEYWORDS_EXCLUDE = [
    "menor", "vallado", "señalización", "vado", "tala de arbolado",
    "terraza", "veladores", "zanjas", "piscina individual", "rótulo",
    "nombramiento", "festejos", "teatro", "eurotaxi", "comisión",
    "modificación presupuestaria", "matrícula del impuesto",
    "organización y funcionamiento", "delegación de competencias",
    "subvenciones para", "juez de paz",
]

WEEKS_BACK       = args.weeks
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "").strip()
USE_AI           = bool(OPENAI_API_KEY)
QUEUE_FILE       = "/tmp/bocm_queue.json"
N_WORKERS        = max(1, min(args.workers, 8))
# Max pages per keyword per date-chunk in backfill mode:
# 6 pages × 10 results/page = up to 60 unique results per week-chunk per keyword.
# The global dedup set means diminishing returns beyond page 4-6 anyway.
MAX_PAGES_BACKFILL = args.max_pages_backfill

# ── Run mode ──────────────────────────────────────────────────────────────────
# DAILY  (--weeks 1)       : scan last 2 working days only. ~30-45 min.
# WEEKLY (--weeks 2-8)     : scan all days in window + 81 focused keywords. ~2-4 hrs.
# FULL   (--weeks 9+)      : everything, 152 keywords. Use only with --resume.
MODE = "daily" if WEEKS_BACK <= 1 else ("weekly" if WEEKS_BACK <= 8 else "full")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}|{elapsed_str()}] {msg}", flush=True)

# ════════════════════════════════════════════════════════════
# HTTP — thread-local sessions for concurrent processing
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"
BOE_BASE  = "https://www.boe.es"
BOE_RSS   = "https://www.boe.es/rss/anuncios.php?cl=45000000&s=B"  # CPV 45 = obras construccion

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

def make_headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer: h["Referer"] = referer
    return h

# Main session (collection phase — single thread)
_main_session   = None
_consecutive_bad = 0
MAX_BAD = 5

def make_session():
    s = requests.Session()
    s.headers.update(make_headers())
    for name in ["cookies-agreed","cookie-agreed","has_js","bocm_cookies","cookie_accepted"]:
        s.cookies.set(name, "1", domain="www.bocm.es")
    return s

def get_session():
    global _main_session
    if _main_session is None: _main_session = make_session()
    return _main_session

def rotate_session():
    global _main_session, _consecutive_bad
    log("  🔄 Rotating session…")
    _main_session = make_session(); _consecutive_bad = 0; time.sleep(12)

# Thread-local sessions for concurrent processing
_tl = threading.local()

def get_thread_session():
    if not hasattr(_tl, "session") or _tl.session is None:
        _tl.session = make_session()
    return _tl.session

def safe_get(url, timeout=30, retries=3, backoff_base=8, referer=None, thread_local=False):
    global _consecutive_bad
    sess = get_thread_session() if thread_local else get_session()
    if referer: sess.headers.update({"Referer": referer})
    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code == 200:
                if not thread_local: _consecutive_bad = 0
                return r
            if r.status_code in (502, 503, 429):
                if not thread_local: _consecutive_bad += 1
                wait = backoff_base * (2 ** attempt)
                log(f"  ⚠️  HTTP {r.status_code} — wait {wait}s")
                time.sleep(wait)
                if not thread_local and _consecutive_bad >= MAX_BAD: rotate_session()
                continue
            return r
        except requests.exceptions.Timeout:
            time.sleep(backoff_base * (2 ** attempt))
        except Exception as e:
            log(f"  ❌ {type(e).__name__}: {str(e)[:60]}")
            if attempt < retries - 1: time.sleep(backoff_base)
    return None

# ════════════════════════════════════════════════════════════
# BOCM ID + URL normalisation
# ════════════════════════════════════════════════════════════
def extract_bocm_id(url):
    m = re.search(r'(BOCM-\d{8}-\d+)', str(url), re.I)
    if m: return m.group(1).upper()
    # BOE IDs: BOE-B-YYYY-NNNNN
    m = re.search(r'(BOE-[ABCS]-\d{4}-\d+)', str(url), re.I)
    return m.group(1).upper() if m else None

def normalise_url(url):
    """Any PDF/JSON URL → HTML entry page (has JSON-LD with full text)."""
    m = re.search(r'(bocm-\d{8}-\d+)', url, re.I)
    if m: return f"{BOCM_BASE}/{m.group(1).lower()}"
    # BOE URLs: keep as-is (they have their own structure)
    if "boe.es" in url.lower(): return url
    return url

def derive_pdf_url(url):
    """
    Derive the BOCM individual-announcement PDF URL from any BOCM URL.
    Pattern (confirmed from live BOCM structure):
      HTML:  https://www.bocm.es/bocm-20260401-96
      PDF:   https://www.bocm.es/boletin/CM_Orden_BOCM/2026/04/01/BOCM-20260401-96.PDF
    Returns None if the URL doesn't match the expected pattern.
    """
    if "boe.es" in url.lower(): return None
    m = re.search(r'[Bb][Oo][Cc][Mm]-(\d{4})(\d{2})(\d{2})-(\d+)', url)
    if m:
        yyyy, mm, dd, num = m.group(1), m.group(2), m.group(3), m.group(4)
        return (f"{BOCM_BASE}/boletin/CM_Orden_BOCM/{yyyy}/{mm}/{dd}"
                f"/BOCM-{yyyy}{mm}{dd}-{num}.PDF")
    return None

# ════════════════════════════════════════════════════════════
# BOCM SECTIONS
# III = Ayuntamientos (licencias, urbanismo, contratos)
# II  = Comunidad de Madrid (grandes planes, DIR, Ayto Madrid)
# V   = Anuncios (ICIO, notificaciones tributarias)
# ════════════════════════════════════════════════════════════
SECTION_II   = "8386"
SECTION_III  = "8387"
SECTION_V    = "8389"
BOCM_RSS     = "https://www.bocm.es/boletines.rss"

def build_search_url(kw, d_from, d_to, sec=SECTION_III):
    df = d_from.strftime("%d-%m-%Y")
    dt = d_to.strftime("%d-%m-%Y")
    return (f"{BOCM_BASE}/advanced-search"
            f"?search_api_views_fulltext_1={quote(kw)}"
            f"&field_bulletin_field_date%5Bdate%5D={df}"
            f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
            f"&field_orden_seccion={sec}"
            f"&field_orden_apartado_1=All&field_orden_tipo_disposicin_1=All"
            f"&field_orden_organo_y_organismo_1_1=All&field_orden_organo_y_organismo_1=All"
            f"&field_orden_organo_y_organismo_2=All&field_orden_apartado_adm_local_3=All"
            f"&field_orden_organo_y_organismo_3=All&field_orden_apartado_y_organo_4=All"
            f"&field_orden_organo_5=All")

def build_page_url(kw, d_from, d_to, page, sec=SECTION_III):
    df = d_from.strftime("%d-%m-%Y"); dt = d_to.strftime("%d-%m-%Y"); kw_q = quote(kw)
    return (f"{BOCM_BASE}/advanced-search/p"
            f"/field_bulletin_field_date/date__{df}"
            f"/field_bulletin_field_date_1/date__{dt}"
            f"/field_orden_organo_y_organismo_1_1/All/field_orden_organo_y_organismo_1/All"
            f"/field_orden_organo_y_organismo_2/All/field_orden_organo_y_organismo_3/All"
            f"/field_orden_apartado_y_organo_4/All"
            f"/busqueda/{kw_q}/seccion/{sec}"
            f"/apartado/All/disposicion/All/administracion_local/All/organo_5/All"
            f"/search_api_aggregation_2/{kw_q}/page/{page}")

# ════════════════════════════════════════════════════════════
# SEARCH KEYWORDS — tiered by mode
#
# KEY INSIGHT: BOCM's Solr tokenises "licencia de obra mayor" as
# [licencia, obra, mayor] and "licencia urbanística" as [licencia, urbanistica].
# These hit overlapping index buckets.
#
# To avoid redundant searches, keywords are chosen to hit DISTINCT buckets:
# - "obra mayor" ≠ "urbanización" ≠ "reparcelación" ≠ "licitación" etc.
# - Verified against each other: <10% URL overlap between any two.
#
# DAILY keywords: Not used in daily mode (day scan is comprehensive enough).
# WEEKLY keywords (25): Most productive distinct-bucket terms.
# FULL keywords (60): Add more specific variants and niche terms.
# ════════════════════════════════════════════════════════════

# (keyword, section, max_pages_per_chunk, profile_tag)
# ── DESIGN PRINCIPLE ────────────────────────────────────────────────────────
# Every keyword here must earn its place. Ask: "Does this term appear primarily
# in BOCM documents worth capturing, or does it mostly pull noise?"
# The Day Scan (SOURCE 1) already captures EVERYTHING published each day in
# Section III. These keywords are supplementary — they catch documents that
# use a specific term in a way the day scan might miss due to pagination.
# RULE: if a keyword mostly pulls small commercial licences → remove it.
#        if it pulls large-scale urbanismo/obra mayor → keep it.
# ─────────────────────────────────────────────────────────────────────────────
KW_WEEKLY = [
    # ── CORE LICENCIAS — highest signal, every profile benefits ──────────────
    # "obra mayor" appears in every significant permit: new builds, rehab, industrial.
    ("obra mayor",              SECTION_III, 12, "ALL"),
    # "licencia urbanística" is the formal BOCM term — plans, urbanizaciones, big builds.
    ("licencia urbanística",    SECTION_III, 10, "ALL"),
    # "declaración responsable" is the fast-track licence (Ley 1/2020) for obra mayor.
    ("declaración responsable", SECTION_III,  8, "ALL"),

    # ── URBANISMO / PLANEAMIENTO — the core of the old database good results ──
    # These are the keywords that produced all the big urbanización, plan especial,
    # Las Tablas €106M, PAU-5 €108M, Tres Cantos €17M results.
    ("proyecto de urbanización", SECTION_III, 12, "PRO+CON"),
    ("proyecto de urbanización", SECTION_II,   8, "PRO+CON"),  # CM-level plans
    ("reparcelación",            SECTION_III, 10, "PRO+CON"),
    ("junta de compensación",    SECTION_III, 10, "PRO+CON"),
    ("plan parcial",             SECTION_III, 10, "PRO+CON"),
    ("plan especial",            SECTION_III, 12, "PRO+CON+RET+HOSPE"),  # also catches cambio de uso plans
    ("plan especial",            SECTION_II,   8, "PRO+CON"),
    ("aprobación definitiva",    SECTION_III, 12, "ALL"),
    ("modificación puntual",     SECTION_III,  8, "PRO+CON"),
    ("convenio urbanístico",     SECTION_III,  8, "PRO"),
    ("estudio de detalle",       SECTION_III,  8, "PRO+CON"),

    # ── INDUSTRIAL / LOGISTICS ────────────────────────────────────────────────
    ("nave industrial",          SECTION_III, 10, "IND+MAT"),
    ("plataforma logística",     SECTION_III,  8, "IND+MAT"),
    ("parque empresarial",       SECTION_III,  8, "IND+CON+MAT"),
    ("actividades productivas",  SECTION_III,  8, "IND+MAT"),

    # ── LICITACIONES / CONTRACTS ──────────────────────────────────────────────
    ("licitación de obras",      SECTION_III, 10, "CON+MAT+INFRA"),
    ("licitación de obras",      SECTION_II,  10, "CON+MAT+INFRA"),
    ("adjudicación de obras",    SECTION_III,  8, "CON+MAT"),

    # ── ICIO — confirmed construction, PEM confirmed by law ───────────────────
    ("base imponible",           SECTION_III, 10, "ALL"),
    ("base imponible",           SECTION_V,    8, "ALL"),
    ("liquidación icio",         SECTION_V,    8, "ALL"),

    # ── CONTRIBUCIONES ESPECIALES — live obra with confirmed budget + address ─
    ("contribuciones especiales", SECTION_III, 10, "MEP+MAT+CON"),

    # ── PRIMERA OCUPACIÓN — building finished, MEP + hospitality window ───────
    ("primera ocupación",         SECTION_III, 10, "MEP+HOSPE"),

    # ── OBRA COMPLETIONS — contract awarded or complete, call NOW ────────────
    ("liquidación de obras",     SECTION_III,  8, "CON+MAT"),
    ("resolución de adjudicación",SECTION_III, 8, "CON+MAT"),

    # ── MEP / EU NEXT GENERATION REHABILITATION ───────────────────────────────
    # EU-funded retrofits have confirmed budget. High quality for MEP installers.
    ("rehabilitación energética",      SECTION_III,  8, "MEP+MAT"),
    ("rehabilitación energética",      SECTION_II,   6, "MEP+MAT"),
    ("eficiencia energética edificio", SECTION_III,  6, "MEP+MAT"),
    ("programa de rehabilitación",     SECTION_III,  6, "MEP+MAT"),

    # ── REHABILITACIÓN INTEGRAL — whole building rehab (quality signal) ───────
    # "rehabilitación integral" is highly specific → almost always a real project.
    ("rehabilitación integral",        SECTION_III,  8, "MEP+MAT+HOSPE+CON"),
    ("reforma integral",               SECTION_III,  8, "MEP+MAT+HOSPE+CON"),
    ("obras de rehabilitación",        SECTION_III,  8, "MEP+MAT+HOSPE"),
    # Additional rehabilitation phrasings BOCM uses — catches what the above miss
    ("renovación integral",            SECTION_III,  5, "MEP+MAT+HOSPE"),     # EU Next Gen term
    ("actuación de regeneración",      SECTION_III,  5, "MEP+MAT+HOSPE+PRO"), # urban regeneration
    ("regeneración urbana",            SECTION_III,  5, "PRO+CON+MEP"),        # city-level plans

    # ── CAMBIO DE USO — all BOCM legal phrasings (hospe, retail, residential) ──
    # Comprehensive coverage: BOCM uses many synonyms for use change permits.
    # max_pages=5 to avoid small shop conversions; day scan gets the big ones.
    ("cambio de uso",                  SECTION_III,  5, "MEP+RET+HOSPE"),
    ("cambio de destino",              SECTION_III,  5, "RET+HOSPE"),          # most common synonym
    ("modificación de uso",            SECTION_III,  5, "RET+HOSPE"),          # with renovation

    # ── FCC / GRAN CONSTRUCTORA / MOLECOR — infrastructure & civil works ──────
    # FCC Construcción: awarded €950M+ in Madrid in 5 years (Ayuntamiento 46 contracts).
    # They need: urbanizaciones, obra civil, licitaciones municipales, colectores.
    # Molecor (PVC pipes): sells to any urbanización or saneamiento project.
    # Adding tube-specific keywords to catch more Molecor-relevant projects:
    ("saneamiento de aguas",           SECTION_III,  5, "INFRA+CON+MAT"),      # sewer works
    ("colector de saneamiento",        SECTION_III,  5, "INFRA+CON+MAT"),      # sewer collector
    ("red de abastecimiento",          SECTION_III,  5, "INFRA+CON+MAT"),      # water supply network
    ("infraestructura de saneamiento", SECTION_III,  5, "INFRA+CON+MAT"),      # sanitation infrastructure
    ("obras de urbanización",          SECTION_III,  6, "CON+MAT+INFRA"),      # urbanisation works

    # ── GRUPO SAONA / KINÉPOLIS / MALVÓN — retail & food expansion signals ────
    # These companies need: new centros comerciales, plan especial de actividad,
    # new high-footfall zones (transport hubs, retail parks), licencia de apertura
    # for large food/retail establishments, and uso dotacional recreational.
    ("zona comercial",                 SECTION_III,  5, "RET+CON"),            # commercial zone development
    ("parque comercial",               SECTION_III,  5, "RET+CON"),            # retail park
    ("actividad de restauración",      SECTION_III,  6, "RET"),                # restaurant activity licence

    # ── KILOUTOU — machinery rental needs ANY obra mayor to fire ─────────────
    # Kiloutou needs leads early — before construction starts. Key signals:
    # any obra de demolición, vaciado, movimiento de tierras, cimentación.
    ("demolición",                     SECTION_III,  5, "ALQUILER+CON+IND"),   # demolition = maquinaria needed

    # ── CBRE / MUPPY / UVESCO — Real Estate Investment (Promotores/RE) ────────
    ("segregación de finca",           SECTION_III,  5, "PRO"),                # land division signal
    ("declaración de interés regional",SECTION_II,   8, "PRO+INFRA"),

    # ── PROMOTORES/RE — land development instruments ─────────────────────────

    # ── GRAN INFRAESTRUCTURA ──────────────────────────────────────────────────
    ("obra civil",                     SECTION_II,   8, "INFRA+CON"),
    ("infraestructura hidráulica",     SECTION_II,   6, "INFRA"),
    ("saneamiento colector",           SECTION_III,  6, "INFRA+CON+MAT"),

    # ── KINÉPOLIS — cinema/multiplex/large leisure venue signals ─────────────
    # Kinépolis needs 2,000–5,000m². Missing: cinema-specific vocabulary.
    ("gran superficie comercial",      SECTION_III,  5, "RET+CON"),
    ("equipamiento de ocio",           SECTION_III,  5, "RET+CON"),
    ("uso recreativo",                 SECTION_III,  3, "RET"),
    ("sala de espectáculos",           SECTION_III,  3, "RET"),
    # NEW: cinema-specific terms Kinépolis actually searches for

    # ── ACTIU / MOBILIARIO OFICINA — office + coworking + hospitality ─────────
    ("campus empresarial",             SECTION_III,  5, "ACTIU+RET"),
    ("zona terciaria",                 SECTION_III,  5, "ACTIU+RET"),
    ("edificio de oficinas",           SECTION_III,  5, "ACTIU+RET"),
    ("rehabilitación de oficinas",     SECTION_III,  5, "ACTIU+MEP"),
    ("acondicionamiento de local",     SECTION_III,  4, "ACTIU+RET"),
    ("adecuación de edificio",         SECTION_III,  4, "ACTIU+MEP"),
    # NEW: corporate office and public building keywords ACTIU needs
    ("sede corporativa",               SECTION_III,  4, "ACTIU+RET"),   # corporate HQ = full furniture
    ("reforma interior",               SECTION_III,  4, "ACTIU+MEP"),   # interior refurb = furniture
    ("acondicionamiento de planta",    SECTION_III,  4, "ACTIU+MEP"),   # floor fit-out = furniture
    ("centro sanitario",               SECTION_III,  4, "ACTIU+MEP"),   # health centre = furniture

    # ── MOLECOR — PVC pipe-specific infrastructure signals ───────────────────
    ("red de pluviales",               SECTION_III,  5, "INFRA+CON+MAT"),
    ("tubería de abastecimiento",      SECTION_III,  5, "INFRA+CON+MAT"),
    ("depuradora de aguas",            SECTION_III,  4, "INFRA+CON+MAT"),
    ("estación de bombeo",             SECTION_III,  4, "INFRA+CON+MAT"),
    ("red de saneamiento",             SECTION_III,  5, "INFRA+CON+MAT"),
    # NEW: Molecor-specific pipe vocabulary
    ("emisario",                       SECTION_III,  4, "INFRA+MAT"),   # sewerage outfall = large PVC
    ("colector general",               SECTION_III,  5, "INFRA+CON+MAT"),# main sewer = key Molecor signal
    ("abastecimiento de aguas",        SECTION_III,  5, "INFRA+MAT"),   # water supply = Molecor

    # ── FCC CONSTRUCCIÓN — licitación + obra civil Madrid signals ─────────────
    ("licitación de obras públicas",   SECTION_II,   6, "INFRA+CON"),
    ("obras de reforma",               SECTION_III,  5, "CON+MEP"),
    ("acondicionamiento viario",       SECTION_III,  4, "INFRA+CON"),
    ("renovación de infraestructuras", SECTION_II,   4, "INFRA+CON"),
    # NEW: additional FCC-relevant signals

    # ── KILOUTOU / ALQUILER MAQUINARIA — early earthwork signals ─────────────
    ("vaciado de solar",               SECTION_III,  5, "ALQUILER+CON"),
    ("excavación en roca",             SECTION_III,  4, "ALQUILER+CON"),
    ("demolición de edificio",         SECTION_III,  5, "ALQUILER+CON"),
    ("movimiento de tierras",          SECTION_III,  4, "ALQUILER+CON"),
    # NEW: Kiloutou's specific machinery signals — foundation work
    ("cimentación",                    SECTION_III,  5, "ALQUILER+CON"), # foundation = early obra signal
    ("pilotaje",                       SECTION_III,  4, "ALQUILER+CON"), # piling = heavy machinery
    ("muro pantalla",                  SECTION_III,  4, "ALQUILER+CON"), # retaining wall = major machinery

    # ── GRUPO SAONA / MALVÓN / RESTAURACIÓN — restaurant expansion signals ────
    # Saona (Guillermo Suárez): 68 restaurants, opening 80+ over 5yr.
    # Malvón (Andrés Ibáñez): 97 stores, 20 new/yr, 15-120m² format.
    # These are restaurant-opening licencia de actividad signals the engine NEVER catches.
    ("local de restauración",          SECTION_III,  5, "RET"),         # restaurant premises
    ("actividad hostelera",            SECTION_III,  3, "RET"),         # hospitality activity
    ("uso hostelero",                  SECTION_III,  3, "RET"),         # hotel/catering use
    ("implantación de restaurante",    SECTION_III,  4, "RET"),         # restaurant implantation

    # ── SHARING CO / ROOM00 / HOSPE — use-change and VUT signals ────────────
    # Jaime Bello (Sharing Co): cambio de uso = holy grail.
    # Primera ocupación = building complete, needs operator NOW.
    ("división horizontal",            SECTION_III,  4, "HOSPE+PRO"),
    ("segregación de vivienda",        SECTION_III,  3, "HOSPE+PRO"),
    ("licencia de primera ocupación",  SECTION_III, 10, "HOSPE+MEP"),
    # NEW: VUT/coliving-specific vocabulary
    ("vivienda de uso turístico",      SECTION_III,  5, "HOSPE"),       # VUT licence = core Sharing Co signal
    ("apartamento turístico",          SECTION_III,  5, "HOSPE"),       # tourist apartment
    ("viviendas turísticas",           SECTION_III,  5, "HOSPE"),       # tourist homes

    # ── PROMOTORES/RE (CBRE / Muppy / Uvesco) — land intelligence ────────────
    ("proyecto de actuación",          SECTION_III,  5, "PRO+CON"),

    # ── INDUSTRIAL/LOGÍSTICA — new warehousing + logistics Madrid corridor ──
    ("parque logístico",               SECTION_III,  5, "IND+MAT"),
    ("centro de distribución",         SECTION_III,  5, "IND+MAT"),
    # NEW: last-mile logistics vocabulary
    # ── 5 new high-value keywords ────────────────────────────────────────────
    # Acta de replanteo = obra STARTED — Kiloutou: equipment needed this week
    ("acta de comprobación del replanteo", SECTION_III,  3, "CON+ALQUILER"),
    # Hotels in BOCM — ACTIU + MEP + Sharing Co cambio de uso
    ("hotel",                              SECTION_III,  3, "ACTIU+MEP+HOSPE"),
    # Senior housing — fastest-growing Madrid asset class 2025-2030
    ("residencia de mayores",              SECTION_III,  3, "ACTIU+CON+MEP"),
    # Retail opening licences — location intelligence for warm leads
    ("licencia de apertura comercial",     SECTION_III,  3, "RET"),
    # Environmental clearance → major infra obra in 12-18 months
    ("declaración de impacto ambiental",   SECTION_III,  3, "INFRA+CON"),
        ("nave logística",                 SECTION_III,  5, "IND+MAT"),     # logistics warehouse
]

KW_EXTRA_FULL = [
    # Additional keywords for full backfill only (--weeks 9+).
    # RULE: terms already in KW_WEEKLY (any section) are NOT repeated here.
    # These are either lower-signal, more niche, or too noisy for weekly scanning.
    # ── Core licencias (additional phrasings) ──────────────────────────────────
    ("licencia de edificación",  SECTION_III,  8, "ALL"),
    ("autorización de obras",    SECTION_III,  8, "ALL"),
    ("se expide licencia",       SECTION_III,  8, "ALL"),
    ("nueva planta",             SECTION_III,  8, "MEP+CON+MAT"),
    # demolición → moved to KW_WEEKLY; not repeated here
    # ── Hospitality niche (lower volume, room00/sharing co) ───────────────────
    ("hostal",                   SECTION_III,  5, "HOSPE"),
    ("pensión",                  SECTION_III,  4, "HOSPE"),
    ("residencia de estudiantes",SECTION_III,  5, "HOSPE+MEP"),
    ("residencia de mayores",    SECTION_III,  5, "MEP+HOSPE"),
    ("apartamentos turísticos",  SECTION_III,  5, "HOSPE"),
    ("viviendas de uso turístico",SECTION_III, 5, "HOSPE"),
    ("rehabilitación de edificio",SECTION_III, 6, "HOSPE+MEP+MAT"),
    # ── Offices / ACTIU (less frequent in BOCM) ───────────────────────────────
    ("coworking",                SECTION_III,  5, "ACTIU+RET"),
    ("edificio terciario",       SECTION_III,  5, "ACTIU+RET"),
    ("uso oficinas",             SECTION_III,  5, "ACTIU+RET"),
    # ── MEP — specific installations (low frequency but high precision) ────────
    ("instalación de ascensor",  SECTION_III,  5, "MEP"),
    ("centro de salud",          SECTION_III,  5, "MEP"),
    # ── Industrial / logistics (niche terms) ─────────────────────────────────
    ("almacén",                  SECTION_III,  8, "IND+MAT"),
    ("polígono industrial",      SECTION_III,  8, "IND+MAT"),
    ("zona logística",           SECTION_III,  5, "IND+MAT"),
    # ── Urbanismo (Section II complements for weekly Section III searches) ─────
    ("plan parcial",             SECTION_II,   8, "PRO+CON"),
    ("junta de compensación",    SECTION_II,   8, "PRO+CON"),
    # obras de urbanización → moved to KW_WEEKLY; not repeated here
    ("contrato de obras",        SECTION_III,  8, "CON+MAT"),
    ("contrato de obras",        SECTION_II,   8, "INFRA+CON"),
    ("valor estimado",           SECTION_III,  8, "CON+MAT"),
    ("impuesto construcciones",  SECTION_V,    5, "ALL"),
    ("notificación tributaria",  SECTION_V,    5, "ALL"),
    ("sector de suelo",          SECTION_III,  5, "PRO+CON"),
    ("suelo urbanizable",        SECTION_III,  5, "PRO+CON"),
    ("modificación del plan",    SECTION_II,   5, "PRO+CON"),
    ("obras de infraestructura", SECTION_III,  6, "INFRA+CON"),
    ("concesión de obra",        SECTION_III,  6, "INFRA+CON"),
    ("aprobación definitiva",    SECTION_II,   8, "INFRA+CON"),
    # ── Alquiler Maquinaria (earthworks signals) ───────────────────────────────
    ("obras de adecuación",      SECTION_III,  5, "ALQUILER"),
    ("obras de ampliación",      SECTION_III,  5, "ALQUILER+MEP"),
    # ── MEP specific installations ─────────────────────────────────────────────
    ("instalación eléctrica",    SECTION_III,  5, "MEP"),
    ("instalación fontanería",   SECTION_III,  4, "MEP"),
    ("instalación climatización",SECTION_III,  4, "MEP"),
    ("sala de máquinas",         SECTION_III,  4, "MEP"),
    ("vivienda protegida",       SECTION_III,  6, "MEP+CON"),
    ("residencia universitaria", SECTION_III,  5, "MEP+CON+HOSPE"),
    ("viviendas de protección",  SECTION_III,  6, "MEP+CON"),
    # ── Retail / activity (with surface threshold in classifier) ──────────────
    # "apertura de establecimiento" is kept in EXTRA_FULL only — too noisy weekly.
    ("apertura de establecimiento",SECTION_III, 5, "RET"),
    ("implantación de actividad", SECTION_III, 5, "RET+MEP"),
    ("superficie útil",           SECTION_III, 4, "RET"),
    # ── Industrial extras ─────────────────────────────────────────────────────
    ("centro de datos",             SECTION_III,  5, "IND+MEP+MAT"),
    ("instalación fotovoltaica",    SECTION_III,  5, "IND+MEP"),
    ("zona industrial",             SECTION_III,  5, "IND+MAT"),
    ("obras de accesibilidad",      SECTION_III,  5, "ALQUILER+MEP"),
    ("vivienda de protección oficial",SECTION_III, 6, "MEP+CON"),
    ("acta de recepción",           SECTION_III,  6, "CON+MAT"),
    ("contribuciones especiales obras",SECTION_V,  5, "ALL"),
    ("hub logístico",               SECTION_III,  4, "IND+MAT"),
    # ── Promotores/RE extras ─────────────────────────────────────────────────
    # segregación de finca → moved to KW_WEEKLY; not repeated here
    ("normalización de fincas",     SECTION_III,  5, "PRO"),
    ("agrupación de fincas",        SECTION_III,  4, "PRO"),
    # ── MEP — EU retrofits extras ─────────────────────────────────────────────
    ("aislamiento térmico",         SECTION_III,  5, "MEP+MAT"),
    ("aerotermia",                  SECTION_III,  4, "MEP"),
    ("bomba de calor",              SECTION_III,  4, "MEP"),
    # ── BOE state-level infrastructure ────────────────────────────────────────
    ("obra de infraestructura",     SECTION_II,   8, "INFRA+CON"),
    ("concesión administrativa",    SECTION_II,   5, "INFRA+PRO"),
    # ── Alquiler Maquinaria earthworks ────────────────────────────────────────
    ("desescombro",                 SECTION_III,  4, "ALQUILER"),
    ("explanación",                 SECTION_III,  4, "ALQUILER+CON"),
    # ── Kinépolis / gran formato: full-mode extra terms ───────────────────────
    ("parque de ocio",              SECTION_III,  4, "RET+CON"),
    ("centro de ocio",              SECTION_III,  4, "RET+CON"),
    ("equipamiento recreativo",     SECTION_III,  4, "RET+CON"),
    ("uso dotacional recreativo",   SECTION_III,  4, "RET"),
    # ── ACTIU / contract furniture: additional full-mode terms ───────────────
    ("hotel de negocios",           SECTION_III,  4, "ACTIU+HOSPE"),
    ("residencia corporativa",      SECTION_III,  4, "ACTIU+HOSPE"),
    ("edificio multifuncional",     SECTION_III,  4, "ACTIU+RET"),
    # ── Molecor: maximum hydraulic infrastructure coverage ───────────────────
    ("colector principal",          SECTION_III,  4, "INFRA+MAT"),
    ("red de saneamiento separativa",SECTION_III, 4, "INFRA+MAT"),
    ("infraestructura de agua",     SECTION_III,  4, "INFRA+MAT"),
    ("suministro de agua potable",  SECTION_III,  4, "INFRA+MAT"),
    ("planta potabilizadora",       SECTION_III,  4, "INFRA+CON+MAT"),
]

# Logistics corridor municipalities for targeted bonus search in full mode
LOGISTICS_MUNICIPALITIES = [
    "Valdemoro", "Getafe", "Pinto", "Parla",
    "Torrejón de Ardoz", "Coslada", "San Fernando de Henares",
    "Mejorada del Campo", "Rivas-Vaciamadrid", "Arganda del Rey",
    "Alcalá de Henares", "Alcobendas", "Tres Cantos",
    "San Sebastián de los Reyes", "Móstoles", "Leganés",
    "Fuenlabrada", "Alcorcón", "Paracuellos de Jarama",
    "Majadahonda", "Las Rozas de Madrid", "Pozuelo de Alarcón",
]

# ── Day-scan broad keywords ────────────────────────────────────────────────────
DAY_SCAN_KWS   = [
    "licencia", "urbanización", "licitación",
    "reparcelación", "aprobación definitiva", "plan especial", "plan parcial",
    "contribuciones especiales",  # confirmed obra with budget + address
    "declaración responsable",    # fast-track licences replacing obra mayor
    "adjudicación",               # confirmed contracts
    "declaración de interés regional",  # DIRs = major land development
    # ── CRITICAL additions: without these, daily mode NEVER finds these docs ──
    # Daily mode (--weeks 1) relies 100% on DAY_SCAN_KWS — keyword search only
    # runs in weekly/full mode. These were previously missing, causing:
    # • Sharing Co / Room00: zero cambio de uso leads on daily runs
    # • MEP installers: zero obra mayor / rehabilitación on daily runs
    # • Retail (Saona/Malvón/Kinépolis): zero refurbishment leads daily
    "cambio de uso",        # Sharing Co / hospe: use conversions
    "cambio de destino",    # BOCM synonym — very common in Ayuntamiento licencias
    "rehabilitación",       # catches rehabilitación integral, de edificio, energética
    "obra mayor",           # individual building permits — MEP + constructora
    "reforma integral",     # whole-building renovation
    "nueva construcción",   # new builds — AEDAS/Vía Célere type projects
]
DAY_SCAN_KWS_V = ["base imponible", "icio", "notificación", "liquidación provisional"]

# ── Profile trigger words (used in scoring and PDF analysis) ────────────────
# Presence of these in the document text boosts score for the matching profile.
PROFILE_TRIGGERS = {
    "infrastructura": [
        "proyecto de urbanización", "obras de urbanización", "junta de compensación",
        "aprobación definitiva", "plan parcial", "reparcelación",
        "licitación de obras", "contrato de obras", "obra civil",
        "infraestructura viaria", "plazo de ejecución", "presupuesto de ejecución material",
        "sector de suelo urbanizable", "plan de sectorización",
    ],
    "constructora": [
        "proyecto de urbanización", "junta de compensación", "aprobación definitiva",
        "licitación de obras", "obra mayor", "nueva construcción",
        "edificio plurifamiliar", "plan especial", "reparcelación",
    ],
    "mep": [
        "edificio plurifamiliar", "nueva construcción", "rehabilitación integral",
        "primera ocupación", "declaración responsable de obra mayor",
        "instalación de ascensor", "viviendas", "bloque de viviendas",
        "licencia de obras mayor", "reforma integral",
    ],
    "industrial": [
        "nave industrial", "almacén", "centro logístico", "polígono industrial",
        "plataforma logística", "uso industrial", "actividades productivas",
        "parque empresarial", "distribución logística",
    ],
    "retail": [
        "local comercial", "gran superficie", "centro comercial", "cambio de uso",
        "uso terciario", "superficie comercial", "actividad comercial",
    ],
    "alquiler": [
        "obra mayor", "nueva construcción", "rehabilitación", "demolición",
        "licitación de obras", "nave industrial", "urbanización",
        "movimiento de tierras", "pavimentación", "vaciado", "explanación",
        "derribo", "desescombro", "cimentación",
    ],
    # ── Promotores/RE — land development and planning signals ─────────────────
    "promotores": [
        "reparcelación", "junta de compensación", "suelo urbanizable",
        "plan parcial", "plan de sectorización", "declaración de interés regional",
        "agente urbanizador", "segregación de finca", "normalización de fincas",
        "actuación de dotación", "convenio urbanístico", "agrupación de fincas",
        "aprovechamiento urbanístico", "coeficiente de edificabilidad",
        "proyecto de actuación especial",
    ],
    # ── Materiales/Compras — Molecor (PVC pipes) + cement, steel, all materials ─
    # Molecor (Javier González): manufactures PVC pipes in Loeches/Getafe Madrid.
    # EVERY urbanización = saneamiento network = direct pipe sales opportunity.
    # Label: "materiales" (consistent with AI prompt and _enhance_profile_fit)
    "materiales": [
        "proyecto de urbanización", "nueva construcción", "rehabilitación integral",
        "nave industrial", "licitación de obras", "hormigón", "tubería",
        "acero", "áridos", "pavimentación", "cerramiento", "cubierta",
        "carpintería", "aislamiento", "estructura metálica", "panel sándwich",
        # Molecor-specific: sewer/water infrastructure = direct PVC pipe sales
        "saneamiento", "colector", "red de abastecimiento",
        "conducción de agua", "abastecimiento de agua", "red de saneamiento",
        "pluviales", "drenaje", "fecales",
        # Urbanización quantities — every project has these
        "zahorra", "pavimentación asfáltica", "bordillo", "encintado",
        # Confirmed project signals
        "presupuesto de ejecución material", "base imponible",
        "unidades de obra", "mediciones", "partidas de obra",
    ],
    # ── Hospitality / Flexliving / Residencial Investment (Sharing Co / Room00) ──
    # ALL cambio de uso synonyms that BOCM uses — previously missing 5 of them.
    # Jaime Bello (Sharing Co): cambio de uso = holy grail.
    # Primera ocupación = building complete, needs operator NOW.
    "hospe": [
        # Cambio de uso — ALL BOCM phrasings (was missing 5 of these)
        "cambio de uso", "cambio de destino",
        "modificación de uso",        # ← was MISSING
        "cambio de actividad",        # ← was MISSING
        "variación de uso",           # ← was MISSING
        "alteración de uso",          # ← was MISSING
        "implantación de nuevo uso",  # ← was MISSING
        "reconversión",               # ← was MISSING
        "actuación de regeneración",  # ← was MISSING
        "renovación integral",        # ← was MISSING (EU Next Gen term)
        # Rehabilitación — all phrasings
        "rehabilitación integral", "rehabilitación de edificio",
        "reforma integral", "rehabilitación de viviendas",
        "restauración integral", "reforma general del edificio",
        "gran rehabilitación",
        # Use types and residential
        "uso residencial", "uso hospedaje", "uso hotelero", "uso turístico",
        "edificio plurifamiliar", "edificio de viviendas", "nueva construcción",
        "viviendas de uso turístico", "apartamentos turísticos",
        "residencia de estudiantes", "residencia de mayores",
        "licencia de obras mayor", "obra mayor", "primera ocupación",
        "número de habitaciones", "número de viviendas",
        "licencia de apertura", "actividad de hospedaje",
        "alojamiento temporal",
        "hostal", "hotel", "pensión", "establecimiento hotelero",
        "reforma de edificio",
    ],
    # ── ACTIU — contract/office furniture: new builds, fit-outs, public buildings ─
    # Jonatan Molina (Director Comercial Madrid): every new edificio de oficinas,
    # coworking, hotel, hospital, university = direct furniture contract sale.
    "actiu": [
        "edificio de oficinas", "uso oficinas", "edificio terciario",
        "coworking", "espacio de trabajo", "centro de negocios",
        "reforma de oficinas", "adecuación de local",
        "obra mayor", "rehabilitación integral", "reforma integral",
        "nueva construcción", "edificio plurifamiliar",
        "edificio de uso mixto", "uso terciario",
        "licitación de obras", "contrato de obras",
        "hotel", "establecimiento hotelero", "residencia de estudiantes",
        "centro educativo", "colegio", "universidad",
        # Public buildings with large furniture procurement
        "hospital", "centro de salud", "biblioteca", "sede municipal",
        "edificio administrativo", "equipamiento público",
    ],
}

def is_bad_url(url):
    if not url: return True
    if "bocm.es" not in url and "boe.es" not in url: return True
    low = url.lower()
    bad_exts  = (".xml",".css",".js",".png",".jpg",".gif",".ico",".woff",".svg",".zip",".epub")
    bad_paths = ("/advanced-search","/login","/user","/admin","/sites/","/modules/","#","javascript:","/CM_Boletin_BOCM/")
    return any(low.endswith(x) for x in bad_exts) or any(x in low for x in bad_paths)

def url_date_ok(url, date_from):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))) >= date_from - timedelta(days=1)
        except ValueError: pass
    return True

def extract_result_links(soup):
    links = []
    for sel in ["a[href*='/boletin/']","a[href*='/anuncio/']","a[href*='/bocm-']",
                ".view-content .views-row a",".view-content a","article h3 a",
                "article h2 a",".field--name-title a","h3.field-content a"]:
        found = soup.select(sel)
        if found:
            for a in found:
                href = a.get("href","")
                if href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    links.append(full)
            if links: break
    if not links:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            if "bocm.es" in full and any(s in full for s in ["/boletin/","/anuncio/","/bocm-"]):
                links.append(full)
    return links

# ════════════════════════════════════════════════════════════
# SEARCH — with global dedup (THE main speed fix)
#
# Passing global_seen into the search function means pagination
# stops the moment a page returns 0 GLOBALLY new URLs — not just
# locally new. This prevents the 15-page grind on keywords that
# return the same 1,200 URLs already collected by earlier keywords.
# ════════════════════════════════════════════════════════════
def search_one_window(kw, d_from, d_to, global_seen, sec=SECTION_III, max_pages=15):
    """
    Search one date window for one keyword.
    Stops paginating when no globally-new URLs found on a page.
    Returns list of newly-found normalised URLs.
    """
    local_urls = []
    local_seen = set()
    page = 0
    consecutive_empty = 0

    while page < max_pages:
        url = (build_search_url(kw, d_from, d_to, sec) if page == 0
               else build_page_url(kw, d_from, d_to, page, sec))
        r = safe_get(url, timeout=20, backoff_base=5,
                     referer=f"{BOCM_BASE}/advanced-search")
        if not r or r.status_code != 200: break

        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)

        global_new = 0
        for link in links:
            if is_bad_url(link): continue
            if not url_date_ok(link, d_from): continue
            norm = normalise_url(link)
            bid  = extract_bocm_id(norm)
            key  = bid if bid else norm
            if key in local_seen: continue
            local_seen.add(key)
            if key not in global_seen:
                local_urls.append(norm)
                global_new += 1

        if global_new == 0:
            consecutive_empty += 1
            if consecutive_empty >= 2: break  # 2 pages with no globally-new → done
        else:
            consecutive_empty = 0

        has_next = bool(
            soup.select_one("li.pager-next a") or
            soup.select_one(".pager__item--next a") or
            soup.find("a", string=re.compile(r"Siguiente|siguiente|Next|»", re.I))
        )
        if not has_next: break
        page += 1
        time.sleep(0.8)

    return local_urls

def search_keyword_chunked(kw, d_from, d_to, global_seen, sec=SECTION_III, max_pages=15, chunk_days=7):
    """
    Date-chunked search. Splits range into weekly windows.
    Each window can return up to 250 results (25 pages × 10).
    Passes global_seen to stop early when all results already collected.
    """
    all_urls = []
    current  = d_from
    while current < d_to:
        if not time_ok(need_s=30): break  # time budget check
        chunk_end = min(current + timedelta(days=chunk_days-1), d_to)
        new_urls  = search_one_window(kw, current, chunk_end,
                                      global_seen | set(extract_bocm_id(u) or u for u in all_urls),
                                      sec=sec, max_pages=max_pages)
        if new_urls:
            log(f"    {current.strftime('%d/%m')}-{chunk_end.strftime('%d/%m')}: "
                f"+{len(new_urls)}")
        all_urls.extend(new_urls)
        current = chunk_end + timedelta(days=1)
        time.sleep(0.4)
    return all_urls

def scrape_day_section(date, sec=SECTION_III, global_seen=None):
    """
    Collect all document URLs published on a specific day in a section.
    Uses 3 broad keywords that together cover virtually every relevant document type.
    Much faster than 6 keywords: 3 instead of 6 HTTP calls per day.
    """
    if global_seen is None: global_seen = set()
    urls      = []
    seen      = set()
    d_compact = date.strftime("%Y%m%d")
    kws       = DAY_SCAN_KWS if sec != SECTION_V else DAY_SCAN_KWS_V

    for kw in kws:
        r = safe_get(build_search_url(kw, date, date, sec), timeout=20, backoff_base=4)
        if not r or r.status_code != 200: continue
        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)
        added = 0
        for link in links:
            if is_bad_url(link): continue
            if d_compact not in link and d_compact not in normalise_url(link): continue
            norm = normalise_url(link)
            bid  = extract_bocm_id(norm)
            key  = bid if bid else norm
            if key in seen: continue
            seen.add(key)
            if key not in global_seen:
                urls.append(norm); added += 1
        # Paginate if there were results
        page = 1
        while added > 0 and page < 6:
            r2 = safe_get(build_page_url(kw, date, date, page, sec),
                          timeout=20, backoff_base=4)
            if not r2 or r2.status_code != 200: break
            soup2 = BeautifulSoup(r2.text, "html.parser")
            links2 = extract_result_links(soup2)
            added2 = 0
            for link in links2:
                if is_bad_url(link): continue
                if d_compact not in link and d_compact not in normalise_url(link): continue
                norm = normalise_url(link)
                bid  = extract_bocm_id(norm)
                key  = bid if bid else norm
                if key in seen: continue
                seen.add(key)
                if key not in global_seen:
                    urls.append(norm); added2 += 1
            if added2 == 0: break
            if not soup2.select_one("li.pager-next a"): break
            page += 1; added = added2; time.sleep(0.8)
        time.sleep(0.6)

    return urls

def get_rss_links(date_from, date_to, global_seen):
    log("📡 RSS…")
    urls = []; seen = set()
    r = safe_get(BOCM_RSS, timeout=20)
    if not r: return urls
    try:
        import xml.etree.ElementTree as ET
        root  = ET.fromstring(r.content)
        items = root.findall(".//item") or root.findall(".//entry")
        for item in items:
            pub = ""
            for tag in ["pubDate","published","updated","date"]:
                el = item.find(tag)
                if el is not None and el.text: pub = el.text; break
            pub_date = None
            for fmt in ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S +0000",
                        "%Y-%m-%dT%H:%M:%S%z"]:
                try: pub_date = datetime.strptime(pub[:30], fmt).replace(tzinfo=None); break
                except ValueError: pass
            if not pub_date:
                try:
                    from dateutil import parser as dp
                    pub_date = dp.parse(pub).replace(tzinfo=None)
                except: pass
            if pub_date and (pub_date < date_from or pub_date > date_to): continue
            link_el = item.find("link")
            burl = link_el.text if link_el is not None else ""
            if not burl: continue
            br = safe_get(burl, timeout=20)
            if not br: continue
            bsoup = BeautifulSoup(br.text, "html.parser")
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                if "CM_Orden_BOCM" in full and ".PDF" in full.upper():
                    norm = normalise_url(full)
                    bid  = extract_bocm_id(norm)
                    key  = bid if bid else norm
                    if key not in seen and key not in global_seen:
                        seen.add(key); urls.append(norm)
            time.sleep(0.5)
    except Exception as e:
        log(f"  ⚠️  RSS: {e}")
    log(f"  📡 RSS: {len(urls)}")
    return urls

# ════════════════════════════════════════════════════════════
# FETCH — HTML JSON-LD first, enhanced PDF fallback
# ════════════════════════════════════════════════════════════
def extract_date_from_url(url):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return ""

def extract_jsonld(soup):
    for script in soup.find_all("script", {"type":"application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if data.get("text"):
                pdf_url = None
                for enc in data.get("encoding",[]):
                    cu = enc.get("contentUrl","")
                    if cu.upper().endswith(".PDF"): pdf_url = cu; break
                return (data["text"],
                        (data.get("datePublished","") or "").replace("/","-")[:10],
                        data.get("name",""),
                        pdf_url)
        except: continue
    return None, None, None, None

def extract_pdf_text_enhanced(url):
    """
    METICULOUS full-read PDF extraction.
    Extracts ALL pages without cap, and pulls every type of structured data:
      - Financial tables (PEM, ETAPA, presupuesto, ICIO, base imponible)
      - Parcel tables (FINCA, superficie, referencia catastral, coeficiente)
      - Owner/promotor data (empresa, CIF, domicilio, representante)
      - Surface/area figures (m², ha, unidades, viviendas)
      - Phase/timeline data (plazo, meses, etapas)
      - Cadastral references (referencia catastral)
      - Land use classification (uso, clasificación)
    All extracted data is clearly tagged so the AI/keyword extractor can use it.
    """
    try:
        r = (get_thread_session() if threading.current_thread().name != "MainThread"
             else get_session()).get(
            url, timeout=60, verify=False, allow_redirects=True,
            headers={**make_headers(referer=BOCM_BASE), "Accept":"application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 400: return ""
        if r.content[:4] != b"%PDF": return ""

        text_parts       = []
        financial_rows   = []
        parcel_rows      = []
        surface_rows     = []
        owner_rows       = []
        timeline_rows    = []

        FINANCIAL_KWS  = ["ETAPA","PEM","IMPORTE","PRESUPUESTO","ICIO",
                          "BASE IMPONIBLE","TOTAL","LICITACIÓN","VALOR","EUROS","€"]
        PARCEL_KWS     = ["FINCA","PARCELA","REFERENCIA CATASTRAL","SUPERFICIE",
                          "COEFICIENTE","M2","CUOTA","APROVECHAMIENTO","DESIGNACIÓN"]
        OWNER_KWS      = ["CIF","NIF","DNI","REPRESENTAD","DOMICILIO","PROPIETARIO",
                          "PROMOTOR","S.L.","S.A.","SLU","JUNTA","ADJUDICATARIO"]
        TIMELINE_KWS   = ["PLAZO","MESES","ETAPA","FASE","EJECUCIÓN","INICIO","FIN",
                          "CALENDARIO","AÑO","TRIMESTRE"]
        SURFACE_KWS    = ["M²","M2","HA","HECTÁREAS","METROS","VIVIENDAS","UNIDADES",
                          "PLANTAS","ALTURA","EDIFICABILIDAD","TECHO"]

        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg_num, pg in enumerate(pdf.pages, 1):
                # --- Full text of every page ---
                t = pg.extract_text()
                if t:
                    text_parts.append(f"[PÁG.{pg_num}]\n{t}")

                # --- All tables: categorize each row by content type ---
                for table in (pg.extract_tables() or []):
                    if not table: continue
                    for row in table:
                        if not row: continue
                        rt = " | ".join(str(c or "").strip() for c in row if c is not None)
                        if not rt.strip(): continue
                        ru = rt.upper()
                        if any(kw in ru for kw in FINANCIAL_KWS):
                            financial_rows.append(rt)
                        if any(kw in ru for kw in PARCEL_KWS):
                            parcel_rows.append(rt)
                        if any(kw in ru for kw in OWNER_KWS):
                            owner_rows.append(rt)
                        if any(kw in ru for kw in TIMELINE_KWS):
                            timeline_rows.append(rt)
                        if any(kw in ru for kw in SURFACE_KWS):
                            surface_rows.append(rt)

        # --- Assemble with clear section tags ---
        full = "\n\n".join(text_parts)

        if financial_rows:
            full += "\n\n[TABLA_DATOS_FINANCIEROS]\n" + "\n".join(financial_rows[:60])
        if parcel_rows:
            full += "\n\n[TABLA_PARCELAS]\n" + "\n".join(parcel_rows[:80])
        if surface_rows:
            full += "\n\n[DATOS_SUPERFICIES]\n" + "\n".join(surface_rows[:40])
        if owner_rows:
            full += "\n\n[DATOS_PROMOTORES_PROPIETARIOS]\n" + "\n".join(owner_rows[:40])
        if timeline_rows:
            full += "\n\n[DATOS_PLAZOS_FASES]\n" + "\n".join(timeline_rows[:20])

        # Also keep legacy tag for backward-compat with extract_pem_value()
        if financial_rows:
            full += "\n\nTABLA_DATOS:\n" + "\n".join(financial_rows[:60])

        return full[:35000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""


def _fetch_pem_only_from_pdf(pdf_url):
    """
    Lightweight PDF scan targeting PEM/ICIO values only.
    Reads ALL pages (BOCM PDFs often have financial tables at the end).
    Extracts:
      - Explicit PEM / presupuesto de ejecución material lines
      - ICIO base imponible (= PEM exactly, per TRLCI art.101)
      - Etapa budget lines (sum = total PEM for multi-phase projects)
      - Any € figure near financial keywords
    Returns a short tagged string or "".
    """
    try:
        sess = (get_thread_session() if threading.current_thread().name != "MainThread"
                else get_session())
        r = sess.get(pdf_url, timeout=40, verify=False,
                     headers={**make_headers(referer=BOCM_BASE), "Accept":"application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 500: return ""
        if r.content[:4] != b"%PDF": return ""

        parts    = []
        fin_keys = ["PEM", "PRESUPUESTO DE EJECUCIÓN", "P.E.M", "BASE IMPONIBLE",
                    "ICIO", "IMPORTE TOTAL", "PRESUPUESTO TOTAL", "ETAPA",
                    "VALOR ESTIMADO", "IMPORTE DE LICITACIÓN", "€", "EUROS"]

        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages:          # ALL pages — no limit
                # Table rows first (most reliable for PEM figures)
                for tbl in (pg.extract_tables() or []):
                    for row in (tbl or []):
                        if not row: continue
                        row_s = " | ".join(str(c or "") for c in row)
                        if any(k in row_s.upper() for k in fin_keys):
                            parts.append(row_s)

                # Full page text — scan every line for financial clues
                t = pg.extract_text() or ""
                for line in t.split("\n"):
                    lu = line.upper()
                    if any(k in lu for k in fin_keys) and re.search(r'[\d]', line):
                        parts.append(line.strip())

        if not parts:
            return ""

        # Deduplicate while preserving order
        seen_p = set()
        unique = []
        for p in parts:
            pk = re.sub(r'\s+', ' ', p).strip()
            if pk and pk not in seen_p:
                seen_p.add(pk)
                unique.append(pk)

        return "TABLA_DATOS:\n" + "\n".join(unique[:50])
    except Exception:
        return ""

def _parse_pem_from_estimated_string(s):
    """Extract a numeric PEM from an estimated PEM string like 'Estimación IA: €1.2M–€2.1M'.
    Returns the midpoint as a float, or None."""
    if not s or "⚪" in s: return None
    nums = []
    for m in re.finditer(r'€\s*([\d]+(?:[.,]\d+)?)\s*([MmKk]?)', s):
        try:
            v = float(m.group(1).replace(',', '.'))
            suf = m.group(2).upper()
            if suf == 'M': v *= 1_000_000
            elif suf == 'K': v *= 1_000
            if 10_000 < v < 5_000_000_000: nums.append(v)
        except Exception: pass
    if not nums: return None
    return sum(nums) / len(nums)   # midpoint of range

def _extract_project_size(text):
    """
    Extract or infer the physical size of the project from BOCM/PDF text.
    Returns a compact string for the 'Project Size' column, e.g.:
      '48 viviendas · 4.200m² const. · Sótano 2 plantas'
      '85.000m² sector · 32.000m² techo'
      '8.500m² nave industrial · 2 plantas'
      '12ha sector · 450 viviendas previstas'
    Returns "" if nothing useful found.
    """
    t   = text.lower()
    out = []

    def _pn(s):
        """Parse a number string like '4.200' or '4,200' or '4200'."""
        try:
            s = s.replace(".", "").replace(",", ".")
            return float(s)
        except Exception:
            return None

    # --- Viviendas / dwelling units ---
    for pat in [
        r'(\d{1,4})\s*(?:viviendas?|viv\.)',
        r'(\d{1,4})\s*unidades?\s+(?:de\s+)?(?:vivienda|residencial)',
        r'(\d{1,4})\s*(?:vpo|vppl|vpg|vpba)',
    ]:
        m = re.search(pat, t)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 5000:
                out.append(f"{n} viviendas")
            break

    # --- Built m² ---
    for pat in [
        r'superficie\s+(?:total\s+)?constru[íi]da[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*m[2²]',
        r'(?:techo\s+)?edificabilidad[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*m[2²]',
        r'([0-9]{2,3}(?:[.,][0-9]{3})+)\s*m[2²]\s+(?:de\s+)?(?:construcci[oó]n|edificaci[oó]n|techo)',
        r'construidos?[^\d]*([0-9]{2,3}(?:[.,][0-9]{3})*)\s*m[2²]',
    ]:
        m = re.search(pat, t)
        if m:
            v = _pn(m.group(1))
            if v and 50 < v < 500_000:
                out.append(f"{int(v):,}m² const.".replace(",", "."))
            break

    # --- Plot / sector m² ---
    for pat in [
        r'superficie\s+(?:neta\s+)?(?:de\s+)?(?:actuaci[oó]n|sector|parcela|suelo)[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*m[2²]',
        r'([0-9]{1,3}(?:[.,][0-9]{3})+)\s*m[2²]\s+(?:de\s+)?(?:superficie|suelo|sector|actuaci)',
        r'(?:suelo|sector)\s+de\s+([0-9]{1,3}(?:[.,][0-9]{3})*)\s*m[2²]',
    ]:
        m = re.search(pat, t)
        if m:
            v = _pn(m.group(1))
            if v and 500 < v < 20_000_000:
                label = "sector" if "sector" in t[max(0, m.start()-30):m.start()+30] else "parcela"
                out.append(f"{int(v):,}m² {label}".replace(",", "."))
            break

    # --- Hectáreas ---
    m = re.search(r'([0-9]+(?:[.,][0-9]+)?)\s*(?:ha|hect[aá]reas?)', t)
    if m:
        v = _pn(m.group(1))
        if v and 0.01 < v < 5000:
            if v >= 1: out.append(f"{v:.1f} ha")
            else:      out.append(f"{v*10000:.0f}m² ({v:.2f}ha)")

    # --- Plantas / floors ---
    for pat in [
        r'(\d{1,2})\s*plantas?\s+(?:sobre\s+rasante|sobre\s+suelo|s\.r\.|sr)',
        r'(?:planta\s+)?b\s*\+\s*(\d{1,2})',
        r'altura\s+(?:de\s+)?(\d{1,2})\s*plantas?',
    ]:
        m = re.search(pat, t)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 50:
                out.append(f"B+{n}")
            break

    # --- Garaje / parking ---
    m = re.search(r'(\d{1,4})\s*plazas?\s+(?:de\s+)?(?:garaje|aparcamiento|parking)', t)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 10000:
            out.append(f"{n} plz. garaje")

    # --- Industrial / nave m² ---
    for pat in [
        r'nave\s+(?:industrial|logística|almacén)[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*)\s*m[2²]',
        r'([0-9]{1,3}(?:[.,][0-9]{3})+)\s*m[2²]\s+(?:de\s+)?nave',
    ]:
        m = re.search(pat, t)
        if m and not any("vivienda" in x for x in out):
            v = _pn(m.group(1))
            if v and 100 < v < 500_000:
                out.append(f"{int(v):,}m² nave".replace(",", "."))
            break

    return " · ".join(out) if out else ""

def _ai_extract_project_size(text, permit_type="", description=""):
    """Ask GPT-4o-mini to extract project size when regex finds nothing."""
    if not USE_AI: return ""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        t_len = len(text)
        sample = text[:2500] + ("\n...\n" + text[-1500:] if t_len > 4000 else "")
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content":
                f"Proyecto: {permit_type}. Descripción: {description[:300]}\n\nTexto:\n{sample[:4000]}\n\n"
                "Extrae en UNA línea compacta los datos físicos del proyecto: m² construidos, "
                "m² parcela/sector, número de viviendas, plantas, plazas de garaje, longitud de viales. "
                "Ejemplo: '48 viviendas · 4.200m² const. · B+5 · 48 plz. garaje'. "
                "Si no hay datos físicos útiles, responde exactamente: ninguno"}],
            temperature=0, max_tokens=80)
        r = resp.choices[0].message.content.strip()
        return "" if r.lower() in ("ninguno", "none", "") else r[:200]
    except Exception:
        return ""

def _ai_estimate_pem(text, permit_type="", municipality="Madrid", description=""):
    """
    Ask GPT-4o-mini to estimate PEM when the heuristic extractor finds nothing.
    ALWAYS produces a range — never returns ⚪ unless the document is genuinely a
    non-construction announcement (budget note, administrative resolution, etc.).

    Key improvements vs. v1:
    - Uses first 2000 + middle 2000 + last 2000 chars of text (captures financial
      tables that are usually at the end of BOCM PDFs)
    - Madrid COAM 2026 reference rates (Módulo M=1.050€, updated IPC Oct-2025)
    - ICIO base imponible = PEM exactly (Ley TRLCI art.101) — extracted if present
    - Explicit reasoning chain: AI must state what data it found before estimating
    - Output format: just "€X.XM – €Y.YM" (as requested) for the column cell
    """
    if not USE_AI: return "⚪ Sin datos PEM en BOCM"
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        # Extract the most information-dense sections of the text:
        # beginning (project description), middle (tables), end (financial summary)
        t_len = len(text)
        if t_len <= 6000:
            text_sample = text
        else:
            text_sample = (text[:2500] + "\n...\n"
                           + text[t_len//2 - 1000: t_len//2 + 1000] + "\n...\n"
                           + text[-2500:])

        prompt = f"""Eres un aparejador colegiado en Madrid con 20 años de experiencia valorando obras.
Debes estimar el Presupuesto de Ejecución Material (PEM) de este proyecto.

DATOS DEL PROYECTO:
- Tipo: {permit_type}
- Municipio: {municipality}
- Descripción: {description[:600]}

TEXTO DEL BOCM / PDF:
{text_sample[:5500]}

INSTRUCCIONES DE VALORACIÓN (sigue ESTE orden):

PASO 1 — BUSCAR PEM EXPLÍCITO:
Busca en el texto cualquiera de estas expresiones:
- "base imponible" + número = PEM EXACTO (por ley TRLCI art.101, base ICIO = PEM)
- "presupuesto de ejecución material", "p.e.m", "PEM:"
- "presupuesto total de las obras", "importe de licitación", "valor estimado del contrato"
- Etapas numeradas con importes: "Etapa 1: X€, Etapa 2: Y€" → suma = PEM total
Si encuentras cualquiera de estos, úsalo directamente como PEM y pon confidence: "high".

PASO 2 — EXTRAER INDICADORES CUANTITATIVOS:
Anota cualquier número útil: m² de parcela, m² construidos, número de viviendas,
número de plantas, plazas de garaje, hectáreas, unidades de actuación, longitud de viales.

PASO 3 — CALCULAR CON MÓDULOS COAM MADRID 2026 (Módulo base M = 1.050€/m²):
Aplica el coeficiente Cm según tipo:
- Vivienda libre nueva (plurifamiliar): Cm = 1,15 → ~€1.210/m² construido
- VPO / vivienda protegida: Cm = 0,90 → ~€945/m²
- Rehabilitación integral: Cm = 0,75–0,95 → €790–€1.000/m²
- Oficinas / terciario: Cm = 1,10 → ~€1.155/m²
- Local comercial / supermercado: Cm = 0,80 → ~€840/m²
- Hotel: Cm = 1,20 → ~€1.260/m²
- Nave industrial nueva: Cm = 0,40 → ~€420/m²
- Nave logística / almacén: Cm = 0,35 → ~€370/m²
- Garaje sótano: Cm = 0,45 → ~€472/m² útil
- Urbanización (viales, redes servicios): €85–€180/m² de superficie neta de actuación
- Demolición: €15–€25/m³ de volumen edificado
- Movimiento de tierras: €8–€20/m³ excavado
Ajuste por municipio: Madrid capital +10%; Pozuelo/Las Rozas/Majadahonda +15%; Municipios <50.000 hab. −10%.

PASO 4 — SI NO HAY DATOS NUMÉRICOS:
Estima por analogía con proyectos típicos del tipo en la zona:
- Plan especial en municipio medio Madrid: €2M–€8M
- Urbanización sector residencial 50.000m²: €4M–€12M
- Edificio plurifamiliar 20 viviendas Madrid: €2.5M–€4M
- Nave industrial 5.000m²: €1.5M–€3M
- Rehabilitación integral edificio: €0.8M–€2.5M
NUNCA devuelvas rango >4× el valor bajo (si bajo = €1M, alto máx €4M).

RESPONDE ÚNICAMENTE con este JSON (sin texto adicional ni backticks):
{{
  "pem_range": "€X.XM – €Y.YM",
  "midpoint_eur": <entero>,
  "basis": "<qué dato usaste: 'base imponible €X', '85 viv × 90m² × €1.210', '50.000m² urb × €120', 'analogía tipo proyecto', etc.>",
  "confidence": "high|medium|low"
}}
Notas: usa M para millones (€1.2M), K para miles (€850K). Si PEM es <€100K devuelve midpoint_eur:0.
"""

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=250,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content.strip()
        d   = json.loads(raw)
        mid = d.get("midpoint_eur", 0)
        if mid and isinstance(mid, (int, float)) and mid >= 100_000:
            rng   = d.get("pem_range", "")
            basis = d.get("basis", "")
            conf  = d.get("confidence", "low")
            # Return ONLY the clean range for the cell — matches requested format
            out = rng if rng else (
                f"€{mid/1_000_000:.1f}M" if mid >= 1_000_000 else f"€{mid/1000:.0f}K"
            )
            # Append confidence tag compactly
            conf_tag = " 🟢" if conf == "high" else " 🟡" if conf == "medium" else " 🔴"
            return out + conf_tag
        else:
            return "⚪ Sin datos PEM en BOCM"
    except Exception as ex:
        log(f"    AI PEM estimate error: {ex}")
        return "⚪ Sin datos PEM en BOCM"

def _estimate_pem_from_pdf(text):
    """
    Estimate PEM range from PDF/BOCM text using extracted m², floors, units, use type,
    and 2024-2025 Spanish construction reference rates (COAM / Colegio Aparejadores Madrid).

    Returns a dict with:
      estimated_pem       — midpoint for scoring/filtering (unchanged API)
      estimated_pem_low   — low end of range
      estimated_pem_high  — high end of range
      method, basis, confidence
    """
    t = text.lower()

    # ── Helpers ──────────────────────────────────────────────────────────────
    def _fmtp(v):
        if v >= 1_000_000: return f"€{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"€{int(v/1000)}K"
        return f"€{int(v):,}"

    result = {
        "estimated_pem": None, "estimated_pem_low": None, "estimated_pem_high": None,
        "method": "", "basis": "", "confidence": "low",
    }

    total_m2  = 0.0   # plot / sector surface
    built_m2  = 0.0   # above-ground constructed surface (preferred for building calc)
    garage_m2 = 0.0   # underground parking / sótano
    num_units = 0     # viviendas
    n_plantas = 0     # floors above ground
    plazas_garaje = 0 # parking spaces

    # ── STEP 1: Extract surface areas ─────────────────────────────────────────

    # From structured table markers (most reliable)
    for marker in ["tabla_superficies:", "tabla_parcelas:", "[tabla_parcelas]"]:
        if marker in t:
            block = text.split(marker, 1)[1].split("\n\n")[0]
            tot = re.search(r"total[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*,[0-9]{2})", block, re.I)
            if tot:
                v = _parse_euro(tot.group(1))
                if v and 100 < v < 10_000_000: total_m2 = v
            if not total_m2:
                for vs in re.findall(
                        r"(?:suelo|parcela|finca)[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*,[0-9]{2})", block, re.I):
                    v = _parse_euro(vs)
                    if v and 50 < v < 200_000: total_m2 += v
            break

    # Built / constructed surface (most accurate for building PEM)
    for pat in [
        r"superficie\s+(?:total\s+)?constru[íi]da[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"superficie\s+edificable[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"edificabilidad[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"techo\s+edificable[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"metros\s+cuadrados?\s+(?:construidos?|de\s+construcci[oó]n)[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)",
    ]:
        mm = re.search(pat, t, re.I)
        if mm:
            v = _parse_euro(mm.group(1))
            if v and 50 < v < 500_000: built_m2 = v; break

    # Total surface / sector / plot
    if not total_m2:
        for pat, is_ha in [
            (r"superficie\s+total[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]", False),
            (r"([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]\s+(?:de\s+)?(?:superficie|suelo|sector)", False),
            (r"([0-9]+(?:[.,][0-9]+)?)\s*(?:ha|hect[aá]reas?)", True),
            (r"sector[^\d]{1,30}([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]", False),
            (r"([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]\s+de\s+(?:suelo|terreno|parcela)", False),
            (r"parcela\s+de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]", False),
        ]:
            mm = re.search(pat, t, re.I)
            if mm:
                v = _parse_euro(mm.group(1))
                if is_ha and v and v < 5000: v = v * 10_000
                if v and 100 < v < 20_000_000: total_m2 = v; break

    # Underground parking / garage surface
    for pat in [
        r"s[oó]tano[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"garaje[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
        r"aparcamiento[^\d]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*m[2²]",
    ]:
        mm = re.search(pat, t, re.I)
        if mm:
            v = _parse_euro(mm.group(1))
            if v and 100 < v < 50_000: garage_m2 = v; break

    # ── STEP 2: Extract floor count ───────────────────────────────────────────
    for pat in [
        r"([0-9]+)\s*plantas?\s+(?:sobre\s+rasante|sobre\s+la\s+rasante|de\s+altura)",
        r"([0-9]+)\s*(?:alturas?|pisos?|niveles?)\s+(?:sobre\s+rasante)",
        r"planta\s+baja\s+(?:m[aá]s\s+)?([0-9]+)",
        r"\bb\s*\+\s*([0-9]+)\b",
        r"([0-9]+)\s*pp\b",
    ]:
        mm = re.search(pat, t, re.I)
        if mm:
            try:
                v = int(mm.group(1))
                if 0 < v <= 40: n_plantas = v; break
            except: pass

    # ── STEP 3: Extract viviendas and parking spaces ──────────────────────────
    for pat in [
        r"([0-9]+)\s*viviendas",
        r"([0-9]+)\s*unidades?\s+(?:de\s+)?vivienda",
        r"([0-9]+)\s*(?:pisos|apartamentos|d[úu]plex)",
    ]:
        mm = re.search(pat, t, re.I)
        if mm:
            try:
                v = int(mm.group(1))
                if 0 < v < 5000: num_units = v; break
            except: pass

    for pat in [r"([0-9]+)\s*plazas?\s+(?:de\s+)?(?:garaje|aparcamiento|parking)"]:
        mm = re.search(pat, t, re.I)
        if mm:
            try: plazas_garaje = int(mm.group(1))
            except: pass

    # ── STEP 4: Classify use type ─────────────────────────────────────────────
    is_data_center    = any(k in t for k in ["data center","centro de datos","cpd ","procesamiento de datos"])
    is_hotel          = any(k in t for k in ["hotel ","hotelero","hospedaje","habitaciones hotel","establecimiento hotelero"])
    is_office         = any(k in t for k in ["edificio de oficinas","uso oficinas","edificio terciario","oficinas de"])
    is_retail_large   = any(k in t for k in ["gran superficie","centro comercial","hipermercado","galería comercial"])
    is_vpo            = any(k in t for k in ["vivienda protegida","vpo ","vpa ","viviendas de protección",
                                              "protección oficial","precio tasado","renta básica"])
    is_rehab_energ    = any(k in t for k in ["rehabilitación energética","eficiencia energética",
                                              "aislamiento térmico","fondos next generation",
                                              "plan de recuperación","actuación de rehabilitación"])
    is_rehab_integral = (any(k in t for k in ["rehabilitación integral","reforma integral","rehabilitación de edificio"])
                         and not is_rehab_energ)
    is_industrial_log = any(k in t for k in ["logístico","plataforma logística","centro de distribución",
                                              "almacén logístico","hub logístico","cross-docking"])
    is_industrial     = (any(k in t for k in ["industrial","nave industrial","almacén","polígono",
                                               "actividades productivas","uso industrial"])
                         and not is_industrial_log)
    is_cons_entity    = any(k in t for k in ["entidad de conservaci","conservación de obras","mantenimiento de viales"])
    is_reparc_only    = (any(k in t for k in ["reparcelación","junta de compensación"]) and
                         not any(k in t for k in ["proyecto de urbanización","obras de urbanización","obra mayor"]))
    is_urb_ind        = (any(k in t for k in ["proyecto de urbanización","obras de urbanización","urbanización"]) and
                         any(k in t for k in ["industrial","polígono","logístico"]))
    is_urb_res        = (any(k in t for k in ["proyecto de urbanización","obras de urbanización","urbanización"])
                         and not is_urb_ind)
    is_residencial    = any(k in t for k in ["viviendas","plurifamiliar","unifamiliar","residencial"])
    is_comercial      = any(k in t for k in ["comercial","terciario","local comercial","uso comercial"])

    # ── STEP 5: Determine calculation base ────────────────────────────────────
    # Prefer above-ground built_m2 for building PEM; use total_m2 for urbanisation
    calc_m2 = 0.0
    m2_label = ""

    if built_m2 > 100:
        calc_m2 = built_m2
        m2_label = f"{built_m2:,.0f}m² constru."
    elif total_m2 > 100 and n_plantas > 0:
        calc_m2 = total_m2 * n_plantas * 0.65   # 65% occupation ratio
        m2_label = f"{total_m2:,.0f}m²suelo×{n_plantas}pl"
    elif total_m2 > 100:
        calc_m2 = total_m2
        m2_label = f"{total_m2:,.0f}m²"
    elif num_units > 0:
        calc_m2 = num_units * 92.0              # avg 92m²/vivienda Spain
        m2_label = f"{num_units}viv×92m²"

    # Underground parking adds value at ~350€/m² (or ~25K€/plaza)
    garage_pem = 0.0
    if garage_m2 > 100:
        garage_pem = garage_m2 * 350.0
    elif plazas_garaje > 0:
        garage_pem = plazas_garaje * 22_000.0

    # ── STEP 6: Apply 2024-2025 reference rates (range) ──────────────────────
    # Sources: COAM, Colegio Aparejadores Madrid, Ministerio de Vivienda módulos autonómicos
    lo = hi = 0.0
    method = ""
    confidence = "low"

    if calc_m2 > 50:
        if is_data_center:
            lo, hi = calc_m2 * 3_500, calc_m2 * 6_000
            method = f"data center (3.500–6.000€/m²)"; confidence = "medium"
        elif is_hotel:
            lo, hi = calc_m2 * 1_400, calc_m2 * 2_500
            method = f"hotel (1.400–2.500€/m²)"; confidence = "medium"
        elif is_office:
            lo, hi = calc_m2 * 1_100, calc_m2 * 1_800
            method = f"oficinas (1.100–1.800€/m²)"; confidence = "medium"
        elif is_retail_large:
            lo, hi = calc_m2 * 850, calc_m2 * 1_200
            method = f"gran superficie (850–1.200€/m²)"; confidence = "medium"
        elif is_rehab_energ:
            lo, hi = calc_m2 * 120, calc_m2 * 420
            method = f"rehab. energética (120–420€/m²)"; confidence = "low"
        elif is_rehab_integral:
            lo, hi = calc_m2 * 550, calc_m2 * 1_050
            method = f"rehab. integral (550–1.050€/m²)"; confidence = "medium"
        elif is_industrial_log:
            lo, hi = calc_m2 * 480, calc_m2 * 720
            method = f"logístico (480–720€/m²)"; confidence = "medium"
        elif is_industrial:
            lo, hi = calc_m2 * 400, calc_m2 * 650
            method = f"industrial (400–650€/m²)"; confidence = "medium"
        elif is_urb_ind:
            lo, hi = calc_m2 * 75, calc_m2 * 145
            method = f"urb. industrial (75–145€/m²)"; confidence = "medium"
        elif is_urb_res:
            lo, hi = calc_m2 * 110, calc_m2 * 210
            method = f"urb. residencial (110–210€/m²)"; confidence = "medium"
        elif is_reparc_only:
            lo, hi = calc_m2 * 18, calc_m2 * 38
            method = f"reparcelación (18–38€/m²)"; confidence = "low"
        elif is_cons_entity:
            lo, hi = calc_m2 * 5, calc_m2 * 15
            method = f"conservación (5–15€/m²)"; confidence = "low"
        elif is_vpo:
            # VPO: Comunidad de Madrid módulo 2024 ≈ 980€/m² (capped by CM)
            lo, hi = calc_m2 * 880, calc_m2 * 1_050
            method = f"VPO Madrid (880–1.050€/m²)"; confidence = "medium"
        elif is_residencial:
            # Residential libre, Madrid metro area 2024-2025
            lo, hi = calc_m2 * 1_150, calc_m2 * 1_650
            method = f"residencial libre (1.150–1.650€/m²)"; confidence = "medium"
        elif is_comercial:
            lo, hi = calc_m2 * 780, calc_m2 * 1_150
            method = f"comercial (780–1.150€/m²)"; confidence = "low"
        else:
            # Generic — wide uncertainty
            lo, hi = calc_m2 * 850, calc_m2 * 1_450
            method = f"construcción genérica (850–1.450€/m²)"; confidence = "low"

        # Add underground parking contribution
        lo += garage_pem * 0.8
        hi += garage_pem * 1.2

        # Round to meaningful precision
        rnd = -4 if hi > 10_000_000 else -3
        lo = round(lo, rnd)
        hi = round(hi, rnd)

        if lo > 0 and hi > lo:
            midpoint = round((lo + hi) / 2, rnd)
            basis_str = m2_label
            if plazas_garaje: basis_str += f" +{plazas_garaje}pz garaje"
            result = {
                "estimated_pem":      midpoint,
                "estimated_pem_low":  lo,
                "estimated_pem_high": hi,
                "method":             method,
                "basis":              basis_str,
                "confidence":         confidence,
            }

    elif num_units > 0:
        # Unit-based fallback (no m² found anywhere)
        m2_unit = 92.0
        if is_vpo:
            lo = round(num_units * m2_unit * 880, -3)
            hi = round(num_units * m2_unit * 1_050, -3)
            method = f"VPO ({num_units} viv × {m2_unit:.0f}m²)"
        else:
            lo = round(num_units * m2_unit * 1_150, -3)
            hi = round(num_units * m2_unit * 1_650, -3)
            method = f"residencial libre ({num_units} viv × {m2_unit:.0f}m²)"
        midpoint = round((lo + hi) / 2, -3)
        result = {
            "estimated_pem":      midpoint,
            "estimated_pem_low":  lo,
            "estimated_pem_high": hi,
            "method":             method,
            "basis":              f"{num_units} viviendas",
            "confidence":         "low",
        }

    return result

def fetch_announcement(url):
    """Returns (text, pdf_url, pub_date, doc_title)."""
    url_low = url.lower()
    pdf_url = None

    # ── BOE documents: fetch directly (they have full HTML content) ──────────
    if "boe.es" in url_low and "diario_boe" in url_low:
        r = safe_get(url, timeout=25)
        if r and r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            # BOE page structure: main content in #cabeceraFichero + #textoBOE
            parts = []
            for sel in ["#textoBOE", ".dispo", "article", "main"]:
                el = soup.select_one(sel)
                if el: parts.append(el.get_text(separator=" ", strip=True)); break
            # Extract PDF link
            for a in soup.find_all("a", href=True):
                if ".pdf" in a["href"].lower() and "boe" in a["href"].lower():
                    pdf_url = urljoin(BOE_BASE, a["href"]) if a["href"].startswith("/") else a["href"]
                    break
            text_out = re.sub(r'\s+', ' ', " ".join(parts)).strip() if parts else ""
            pub_date = extract_date_from_url(url)
            if not pub_date:
                m = re.search(r'(\d{4}-\d{2}-\d{2})', url)
                if m: pub_date = m.group(1)
            if text_out and len(text_out) > 50:
                return text_out, pdf_url, pub_date, ""

    # Convert to HTML entry page
    html_url = url
    if url_low.endswith(".pdf") or url_low.endswith(".json"):
        html_candidate = normalise_url(url)
        if html_candidate and html_candidate != url:
            html_url = html_candidate
            if url_low.endswith(".pdf"): pdf_url = url

    # Try HTML + JSON-LD (fastest, cleanest, always has full text)
    r = safe_get(html_url, timeout=25, referer=f"{BOCM_BASE}/advanced-search",
                 thread_local=(threading.current_thread().name != "MainThread"))
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        jtext, jdate, jname, jpdf = extract_jsonld(soup)
        if jtext and len(jtext.strip()) > 100:
            return (re.sub(r'\s+', ' ', jtext).strip(),
                    pdf_url or jpdf, jdate or extract_date_from_url(html_url),
                    jname or "")

    # PDF fallback (enhanced)
    if url_low.endswith(".pdf"):
        text = extract_pdf_text_enhanced(url)
        if text and len(text.strip()) > 100:
            return text, url, extract_date_from_url(url), ""
        return "", None, extract_date_from_url(url), ""

    # HTML body fallback
    if not r or r.status_code != 200:
        r = safe_get(url, timeout=25, thread_local=(threading.current_thread().name != "MainThread"))
    if not r or r.status_code != 200: return "", None, "", ""

    soup = BeautifulSoup(r.text, "html.parser")
    parts = []
    for sel in [".field--name-body",".field-name-body",".contenido-boletin",
                ".anuncio-texto",".anuncio","article .content","article","main","#content"]:
        el = soup.select_one(sel)
        if el: parts.append(el.get_text(separator=" ", strip=True)); break
    if not parts:
        for tag in soup.find_all(["nav","header","footer","aside","script","style"]):
            tag.decompose()
        parts.append(soup.get_text(separator=" ", strip=True)[:10000])

    pub_date = extract_date_from_url(url)
    if not pub_date:
        m = re.search(r'\b(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})\b', " ".join(parts))
        if m: pub_date = m.group(0)

    for a in soup.find_all("a", href=True):
        h = a["href"]
        if ".pdf" in h.lower() or ".PDF" in h:
            pdf_url = urljoin(BOCM_BASE, h) if h.startswith("/") else h; break

    if pdf_url and not parts:
        ptext = extract_pdf_text_enhanced(pdf_url)
        if ptext: parts.append(ptext)

    text_out = re.sub(r'\s+', ' ', " ".join(parts)).strip()

    # ── PEM-only PDF fetch (when text found but no PEM in text) ────────────
    # Many BOCM urbanismo announcements contain the project description but
    # the PEM is only in the attached PDF financial summary table.
    # If we have text but no PEM, do a targeted PDF fetch to look for PEM.
    if text_out and len(text_out) > 100 and pdf_url:
        if not any(p in text_out.lower() for p in
                   ["presupuesto de ejecución","p.e.m","base imponible","€","euros"]):
            pem_text = _fetch_pem_only_from_pdf(pdf_url)
            if pem_text:
                text_out = text_out + " " + pem_text

    return text_out, pdf_url, pub_date, ""

# ════════════════════════════════════════════════════════════
# CLASSIFICATION — 5 stages
# ════════════════════════════════════════════════════════════
HARD_REJECT = [
    # ── Pure admin / budget / HR — no construction content ────────────────────
    "convocatoria de subvención", "bases reguladoras para la concesión de ayudas",
    "ayuda económica", "aportación dineraria",
    "modificación presupuestaria", "suplemento de crédito",
    "modificación del plan estratégico de subvenciones",
    "nombramiento funcionari", "convocatoria de proceso selectivo",
    "convocatoria de oposiciones", "oferta de empleo público",
    "bases de la convocatoria para la cobertura",
    "ordenanza fiscal reguladora",
    "impuesto sobre actividades económicas",
    "inicio del período voluntario de pago",
    "matrícula del impuesto",
    "festejos taurinos", "certamen de",
    "convocatoria de premios", "actividades deportivas",
    "acción social en el ámbito del deporte",
    "actividades educativas", "proyectos educativos",
    "juez de paz", "composición del pleno",
    "composición de las comisiones", "encomienda de gestión",
    "reglamento orgánico municipal",
    "eurotaxi", "autotaxi",
    "criterio interpretativo vinculante",
    "corrección de errores del bocm", "corrección de hipervínculo",
    "licitación de servicios de", "licitación de suministro de",
    "contrato de servicios de limpieza", "contrato de mantenimiento de",
    "servicio de limpieza", "servicio de recogida",
    # Finished projects (no opportunity remains)
    "disolución de la junta de compensación",
    "disolver la junta de compensación",
    # Pure tax / admin with no construction content
    "tasa de residuos", "tasa de basuras", "tasa por recogida",
    "contribuciones por prestación de servicios",
    "impuesto sobre vehículos", "padrón municipal de habitantes",
    "tasa por utilización del dominio público",
    "ordenanza de tráfico", "ordenanza de movilidad",
    "subasta de bienes municipales", "enajenación de bienes",
    "permuta de bienes", "cesión de uso",
    "servicios de limpieza integral", "servicios de vigilancia",
    "servicios de mantenimiento de jardines",
    "servicio de limpieza de",
    "retirada de residuos",
    "cabinas de almacenamiento",
    "repuestos de juntas",
    "anillos tóricos",
    "contactores para el mantenimiento de",
    "suministro de repuestos",
    "radiodiagnóstico",
    "armarios y puertas antivandálicas",
    # ── SMALL COMMERCIAL LICENCES — always noise, never worth capturing ────────
    # These activity types appear frequently in BOCM Section III as licencia de
    # actividad or apertura de establecimiento. They are NOT construction leads.
    # Any of these terms in the document text → reject immediately.
    # Car repair / workshops (extremely common BOCM noise)
    "taller de vehículos",
    "taller de reparación de automóviles",
    "taller de reparación de vehículos",
    "taller de automóviles",
    "taller de mecánica",
    "taller de motos",
    "taller de chapa",
    "taller de pintura de vehículos",
    # Printing / graphics (small commercial)
    "artes gráficas",
    "impresión gráfica",
    "centro de impresiones",
    "impresión digital",
    "imprenta",
    # Storage (trasteros are NOT warehouses)
    "actividad de trasteros",
    "licencia de trasteros",
    "apertura de trasteros",
    "uso de trasteros",
    # Pharmacies (common in BOCM, never construction leads)
    "apertura de farmacia",
    "instalación de farmacia",
    "licencia de farmacia",
    # Very small personal services (already partly in SMALL_ACTIVITY but catch early)
    "salón de tatuajes",
    "sala de tatuajes",
    # General admin noise
    "expediente sancionador",
    "resolución sancionadora",
    "publicación de la lista definitiva",
    "publicación de la lista provisional",
]

APPLICATION_SIGNALS = [
    "se ha solicitado licencia", "ha solicitado licencia",
    "se solicita licencia de",
    "lo que se hace público en cumplimiento de lo preceptuado",
    "a fin de que quienes se consideren afectados de algún modo",
    "quienes se consideren afectados puedan formular",
    "formular por escrito las observaciones pertinentes",
    "durante el plazo de veinte días",
    "durante el plazo de treinta días",
    "presentarán en el registro general del ayuntamiento",
]

DENIAL_SIGNALS = [
    "denegación de licencia", "se deniega la licencia",
    "desestimación de la solicitud", "se desestima",
    "resolución denegatoria", "no se concede",
    "caducidad de la licencia", "archivo del expediente",
]

GRANT_SIGNALS = [
    "se concede", "se otorga", "se autoriza",
    "concesión de licencia", "licencia concedida",
    "se resuelve favorablemente", "otorgamiento de licencia",
    "se acuerda conceder", "se acuerda otorgar",
    "resolución estimatoria", "expedición de licencia",
    "se expide licencia", "licencia municipal de obras",
    "aprobar definitivamente", "aprobación definitiva",
    "aprobación inicial", "aprobación provisional",
    "se aprueba definitivamente", "se aprueba provisionalmente",
    "aprobación del proyecto", "acuerdo de aprobación",
    "declaración responsable de obra mayor",
    "declaración responsable urbanística",
    "toma de conocimiento de la declaración responsable",
    "con un presupuesto",
    "promovido por la junta de compensación",
    "licitación de obras", "contrato de obras",
    "adjudicación del contrato de obras", "se convoca licitación",
    "obras de construcción", "obras de urbanización",
    "obras de rehabilitación", "convocatoria de licitación",
    "acuerdo de reparcelación", "aprobación del proyecto de reparcelación",
    "suscripción del convenio", "aprobación del convenio urbanístico",
    "se aprueba", "se acuerda aprobar",
    "modificación puntual",
    "aprobación del estudio de detalle",
    "base imponible del icio",  # ICIO = confirmed obra
    # Contribuciones especiales = obras confirmed active/complete
    "contribuciones especiales por la ejecución",
    "obras de pavimentación", "obras de urbanización de la calle",
    "se aprueba definitivamente la ordenanza fiscal",  # = obra approved and funded
    "adjudicado a", "contrato adjudicado",
    "resolución de adjudicación",
    "acta de comprobación del replanteo",  # = obra started on-site
    "acta de recepción de las obras",      # = obra complete
    "sometido a información pública",  # Planning phase
    "exposición pública",
    "tramitación del expediente",
    "inicio del expediente",
    "proyecto básico",
    "proyecto de ejecución",
    "memoria del proyecto",
    "pliego de condiciones",
    # Additional grant signals missing from original
    "se adjudica",                    # contract awarded
    "adjudicación definitiva",
    "adjudicación provisional",
    "declarada de interés regional",  # DIR approved = huge development signal
    "se autoriza la implantación",    # industrial/commercial activity authorised
    "licencia de apertura",           # activity licence granted
    "actividad autorizada",
    "acta de inicio de obras",        # construction literally started
    "certificado de eficiencia energética",  # rehab project confirmed done
    "se aprueba el proyecto de reparcelación",
    "aprobación del proyecto de actuación",
    "inscripción en el registro de entidades",
    # ── Cambio de uso / rehabilitación grant language ─────────────────────────
    # BOCM uses these phrases when authorising use-change or full rehab licences:
    "se autoriza el cambio de uso",
    "se autoriza el cambio de destino",
    "autorización de cambio de uso",
    "autorización del cambio de destino",
    "cambio de uso autorizado",
    "se concede la licencia de cambio",
    "licencia de cambio de uso",
    "obras de rehabilitación integral",
    "rehabilitación y cambio de uso",
    "actuación de rehabilitación",
    "proyecto de rehabilitación",
    "modificación de uso",
    "autorización de obras de rehabilitación",
    # ── Additional BOCM-specific plenario / agreement phrasings ──────────────
    "se acuerda la aprobación definitiva",  # Plenary agreement on final approval
    "resuelve aprobar definitivamente",     # Resolution to definitively approve
    "queda aprobado definitivamente",       # Definitive approval confirmed
    "acuerdo plenario de aprobación",       # Plenary board approval
    "resolución aprobatoria",              # Approving resolution
    "se declara definitivamente aprobado",  # Declared definitively approved
    "se aprueba el plan especial",         # Plan especial approval
    "aprobación definitiva del plan",      # Generic definitive plan approval
    "aprobación definitiva del estudio",   # Estudio de detalle approval
    "se aprueba el proyecto de urbanización",  # Direct urbanisation project
    "aprobación inicial y provisional",    # Initial + provisional in one document
    "se autoriza la actividad",            # Activity licence granted
    "se concede la autorización",          # Generic authorisation granted
    # ── CM Contratos / government contract language ───────────────────────────
    "obras de mantenimiento",
    "obras de conservación",
    "obras de reparación urgente",
    "ejecución de obras de",
    "formalización del contrato de obras",
]
CONSTRUCTION_SIGNALS = [
    "obra mayor", "obras mayores", "licencia de obras",
    "licencia urbanística", "licencia de edificación",
    "declaración responsable",
    "nueva construcción", "nueva planta", "obra nueva",
    "edificio de nueva", "viviendas de nueva",
    "edificio plurifamiliar", "complejo residencial",
    "viviendas unifamiliares",
    "proyecto de urbanización", "obras de urbanización",
    "unidad de ejecución", "área de planeamiento específico",
    "junta de compensación", "reparcelación",
    # ── Rehabilitación (all phrasings BOCM uses) ─────────────────────────────
    "rehabilitación integral", "rehabilitación de edificio",
    "reforma integral", "reforma estructural",
    "renovación integral",            # EU Next Gen terminology
    "actuación de regeneración",      # urban regeneration
    "regeneración urbana",            # city-level plans
    "restauración integral",          # historic buildings
    "reforma general del edificio",   # BOCM-specific
    "gran rehabilitación",            # ICIO/tax context
    "obras de rehabilitación",
    "rehabilitación y cambio de uso", # combo rehab + use change
    "rehabilitación completa",
    # ── Cambio de uso (all legal synonyms) ──────────────────────────────────
    "cambio de uso", "cambio de destino",
    "modificación de uso", "cambio de actividad",
    "variación de uso", "alteración de uso",
    "implantación de nuevo uso", "reconversión",
    "mutación de destino",
    # ── Other construction signals ──────────────────────────────────────────
    "demolición y construcción", "demolición y nueva planta",
    "ampliación de edificio",
    "nave industrial", "naves industriales",
    "almacén industrial", "almacén", "centro logístico",
    "plataforma logística", "parque empresarial",
    "instalación industrial", "actividades productivas",
    "edificio industrial", "uso industrial",
    "hotel", "bloque de viviendas", "demolición", "derribo",
    "primera ocupación",
    "plan especial", "plan parcial", "estudio de detalle",
    "proyecto urbanístico", "modificación puntual",
    "presupuesto de ejecución material", "p.e.m",
    "base imponible del icio", "base imponible icio",
    "licitación de obras", "contrato de obras",
    "impuesto sobre construcciones",
    "convenio urbanístico",
    "residencia de mayores", "centro de salud",
    "edificio de oficinas",
    "superficie comercial", "centro comercial", "gran superficie",
    "local comercial", "uso terciario",
    # Contribuciones especiales = confirmed active obra
    "contribuciones especiales", "obras de pavimentación",
    "acta de recepción", "acta de comprobación del replanteo",
    "resolución de adjudicación", "contrato de obras adjudicado",
    "centro de datos", "data center", "instalación fotovoltaica",
    "vivienda protegida", "viviendas de protección oficial",
    # MEP / retrofits
    "rehabilitación energética", "eficiencia energética",
    "aislamiento térmico", "instalación de placas solares",
    "aerotermia", "bomba de calor", "ventilación mecánica",
    # Retail / industrial activity
    "apertura de establecimiento", "actividad clasificada",
    "modificación sustancial de instalaciones",
    "licencia de apertura", "instalación de maquinaria",
    # Gran infraestructura
    "declaración de interés regional", "obra de infraestructura",
    "infraestructura hidráulica", "colector general",
    # Promotores — Molecor/saneamiento triggers
    "saneamiento de aguas", "red de abastecimiento",
    "colector de saneamiento", "infraestructura de saneamiento",
    "obras de urbanización",
    # Promotores
    "segregación de finca", "normalización de fincas",
    "proyecto de actuación especial",
]

SMALL_ACTIVITY = [
    # ── Personal care ─────────────────────────────────────────────────────────
    "peluquería", "barbería", "salón de belleza", "centro de estética",
    "spa ", "centro de bronceado", "clínica de fisioterapia",
    # ── Food retail (small) ───────────────────────────────────────────────────
    "pastelería", "panadería", "carnicería", "pescadería",
    "frutería", "estanco", "quiosco", "despacho de pan",
    "charcutería", "ultramarinos", "colmado",
    # ── Services (small) ─────────────────────────────────────────────────────
    "locutorio", "gestoría", "asesoría fiscal",
    "academia de idiomas", "academia de danza",
    "centro de yoga", "pilates", "boxeo",
    # ── Healthcare / wellness (small) ────────────────────────────────────────
    "clínica dental", "consulta médica", "consulta veterinaria",
    "óptica", "ortopedia",
    # ── Food service (small) — bar/café/restaurant ONLY if no declared PEM > 500K
    "bar ", "bar-", "café ", "cafetería", "pizzería", "kebab",
    "heladería", "hamburguesería", "bocadillería",
    # ── Retail (small) ────────────────────────────────────────────────────────
    "lavandería", "tintorería", "zapatería", "cerrajería",
    "papelería", "floristería", "bazar",
    # ── Workshops (small) ─────────────────────────────────────────────────────
    "taller de reparación de electrodomésticos",
    "servicio técnico de electrodomésticos",
    # ── Storage — vestuarios / trasteros (NOT warehouses) ─────────────────────
    # These must be rejected when they appear alone without obra mayor context
    "vestuarios y duchas",
    "instalación de vestuarios",
    # NOTE: "almacén" and "depósito" are NOT in SMALL_ACTIVITY because
    # industrial warehouses (naves almacén) are valid leads.
]

def _is_major_construction(text: str) -> bool:
    """Returns True if text contains clear major construction signals.
    Used to override SMALL_ACTIVITY rejection for large-scale projects."""
    t = text.lower()
    return any(p in t for p in [
        # New construction
        "obra mayor", "proyecto de ejecución", "presupuesto de ejecución material",
        "p.e.m", "base imponible del icio", "nueva construcción", "nueva planta",
        "demolición y construcción", "rehabilitación integral", "reforma integral",
        "edificio plurifamiliar", "bloque de viviendas", "viviendas",
        "nave industrial", "centro comercial", "gran superficie",
        "declaración responsable de obra mayor",
        # Industrial / warehouse adaptation (for TEC Container-type leads)
        "almacén", "nave", "polígono industrial", "uso industrial",
        "actividad industrial", "adecuación de nave", "adecuación de local",
        "instalación industrial", "maquinaria industrial",
        # Large PEM declared
        "presupuesto total", "valor estimado del contrato", "importe de licitación",
    ])

def classify_permit(text):
    """Returns (is_lead, reason, tier 1-5)."""
    t = text.lower()

    # ── Stage 0: Config-level keyword exclusions ─────────────────────────────
    # Applied before any other check — these come from KEYWORDS_EXCLUDE above.
    for kw in KEYWORDS_EXCLUDE:
        if kw in t: return False, f"Excluded keyword: '{kw}'", 0

    # ── Stage 1: Hard reject — admin noise ───────────────────────────────────
    for kw in HARD_REJECT:
        if kw in t: return False, f"Admin noise: '{kw}'", 0

    # ── Stage 1b: Additional small-activity hard reject ───────────────────────
    # These are too specific and always noise — check before the expensive signal pass.
    # (These differ from SMALL_ACTIVITY in that they never have legitimate large-scale use.)
    _noise_patterns = [
        "licencia para farmacia", "apertura para farmacia",
        "farmacia en calle", "farmacia en la calle", "farmacia en avenida",
        "farmacia en la avenida", "farmacia en plaza",
        "centro de impresiones", "impresión gráfica en",
        "taller de vehículos en", "taller de automóviles en",
        "licencia para pastelería", "pastelería en calle",
        "vestuarios y muelle",
    ]
    for pat in _noise_patterns:
        if pat in t: return False, f"Direct noise pattern: '{pat}'", 0

    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 3:
        # NOTE: "lo que se hace público", "a fin de que quienes se consideren afectados",
        # "durante el plazo de veinte días", and "presentarán en el registro" are STANDARD
        # BOCM boilerplate that appears in BOTH approval documents AND solicitudes.
        # We must therefore check for ANY approval language — not just the narrow list.
        # Without this fix, legitimate plan especial / reparcelación / obra mayor approvals
        # that use "se aprueba" instead of "aprobación definitiva" get falsely rejected.
        has_definitive = any(p in t for p in [
            # Formal approval phrases
            "aprobación definitiva", "aprobar definitivamente",
            "se aprueba definitivamente", "se aprueba provisionalmente",
            "acuerdo de aprobación definitiva", "acuerdo de aprobación",
            # Grant language
            "se concede", "se otorga", "licencia concedida",
            "se expide licencia", "licencia municipal de obras",
            "se resuelve favorablemente", "otorgamiento de licencia",
            "se acuerda conceder", "se acuerda otorgar",
            # General approval — covers plan especial, reparcelación, urbanización
            "se aprueba",            # "se aprueba el presente plan especial..."
            "se acuerda aprobar",    # "se acuerda aprobar el proyecto de reparcelación"
            "ha sido aprobado",
            "proyecto aprobado",
            "aprobación del proyecto",
            "se autoriza",           # "se autoriza el cambio de uso"
            "queda aprobado",
            "ha quedado aprobado",
            "aprobación provisional",
            "aprobación inicial",    # keep — initial approval IS actionable for promotores
            # Planning instruments — these are always approval documents
            "junta de compensación",  # reparcelación/urbanización always approval
            "proyecto de reparcelación",
            "proyecto de urbanización aprobado",
            "estudio de detalle aprobado",
            # Contract / licitación — always actionable
            "licitación de obras", "contrato de obras",
            "adjudicación de obras", "se convoca licitación",
            "adjudicado a",
            # ICIO — confirmed obra
            "base imponible del icio",
            "contribuciones especiales por la ejecución",
            # Declaración responsable — fast-track licence, always granted by definition
            "declaración responsable de obra mayor",
            "toma de conocimiento de la declaración responsable",
        ])
        if not has_definitive:
            # TIER-6: Application-phase PRE-LEAD — only for LARGE-SCALE activities
            # with a confirmed address. Explicitly exclude small shops and services.
            _is_large_scale_app = any(r in t for r in [
                "cambio de uso", "licencia ambiental",
                "gran superficie", "centro comercial",
                "actividad clasificada", "implantación de actividad",
                "nave industrial", "almacén", "uso industrial",
            ])
            # Reject if it's a small personal service even if it has an address
            _is_small_noise = any(r in t for r in [
                "farmacia", "pastelería", "panadería", "peluquería",
                "taller de vehículos", "taller de automóviles",
                "impresión", "vestuarios", "trasteros", "pizzería",
                "cafetería", "bar ", "kebab", "carnicería",
            ])
            if _is_large_scale_app and not _is_small_noise:
                if any(r in t for r in ["calle ", "avenida ", "plaza ", "vía ", "paseo "]):
                    return True, "Tier-6: Pre-lead solicitud (actividad grande)", 6
            return False, "Application phase (not granted or small activity)", 0
        # else: fall through — it's an approval that mentions past public period

    for kw in DENIAL_SIGNALS:
        if kw in t: return False, f"Denial: '{kw}'", 0

    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)
    
    if not has_grant:
        # Accept planning documents even without grant language
        if any(p in t for p in ["plan parcial", "plan especial", "modificación puntual",
                                 "reparcelación", "junta de compensación",
                                 "proyecto de urbanización", "estudio de detalle"]):
            return True, "Tier-2: Planning document (initial phase)", 2
        return False, "No grant language", 0
    if not has_construction: return False, "No construction content", 0

    has_major = any(p in t for p in [
        "obra mayor","nueva construcción","nueva planta","nave industrial",
        "proyecto de urbanización","rehabilitación integral","plan especial",
        "plan parcial","bloque de viviendas","junta de compensación",
        "licitación de obras","base imponible", "reforma integral",
        "edificio plurifamiliar", "edificio de viviendas"])
    if not has_major:
        # SMALL_ACTIVITY rejection: if the text contains a small-scale activity
        # keyword → reject UNLESS there is an explicit major construction signal
        # (obra mayor, new building, PEM declared, etc.).
        # NOTE: We no longer rely on surface area in m² because BOCM documents
        # frequently omit it. A bakery is a bakery regardless of stated m².
        for kw in SMALL_ACTIVITY:
            if kw in t and not _is_major_construction(t):
                return False, f"Small activity (no major construction signal): '{kw}'", 0
    # Tier classification
    if any(p in t for p in ["proyecto de urbanización","junta de compensación",
                             "reparcelación","plan parcial","aprobación definitiva del plan"]):
        if any(p in t for p in ["aprobar definitivamente","aprobación definitiva","presupuesto","pem"]):
            return True, "Tier-1: Urbanismo definitivo", 1

    if any(p in t for p in ["licitación de obras","contrato de obras","adjudicación de obras"]):
        return True, "Tier-1: Licitación/contrato obras", 1

    if any(p in t for p in ["plan especial","reforma interior","área de planeamiento","estudio de detalle"]):
        if any(p in t for p in ["definitiv","presupuesto","pem"]):
            return True, "Tier-2: Plan especial definitivo", 2

    if any(p in t for p in ["nueva construcción","nueva planta","nave industrial",
                             "bloque de viviendas","demolición y construcción",
                             "rehabilitación integral","parque empresarial","base imponible"]):
        return True, "Tier-3: Obra mayor / industrial", 3

    # Tier-3b: Cambio de uso / residential conversion / rehab — prime for hospe/flexliving
    # Comprehensive list covering ALL BOCM synonyms for use change and rehabilitation.
    _cambio_rehab_signals = [
        # Cambio de uso synonyms
        "cambio de uso", "cambio de destino", "modificación de uso", "cambio de actividad",
        "variación de uso", "alteración de uso", "implantación de nuevo uso",
        "reconversión", "mutación de destino", "transformación a uso residencial",
        # Rehabilitación synonyms
        "rehabilitación de edificio", "rehabilitación de vivienda",
        "rehabilitación y cambio", "renovación integral", "restauración integral",
        "reforma general del edificio", "gran rehabilitación",
        "actuación de regeneración", "regeneración urbana",
        # Residential construction signals
        "edificio plurifamiliar", "edificio de viviendas", "uso residencial",
    ]
    if any(p in t for p in _cambio_rehab_signals):
        return True, "Tier-3: Cambio de uso / rehab residencial", 3

    if any(p in t for p in ["obra mayor","reforma integral","cambio de uso",
                             "ampliación de edificio","declaración responsable"]):
        return True, "Tier-4: Obra mayor / cambio de uso", 4

    # Tier-4b: EU-funded retrofits — confirmed budget, MEP opportunity
    if any(p in t for p in ["rehabilitación energética","eficiencia energética edificio",
                             "programa de rehabilitación","fondos next generation",
                             "plan de recuperación"]):
        return True, "Tier-4: Rehabilitación energética (EU fondos)", 4

    # Tier-4c: Retail / industrial activity openings with size data
    if any(p in t for p in ["apertura de establecimiento","actividad clasificada",
                             "licencia ambiental","modificación sustancial de instalaciones"]):
        if any(p in t for p in ["m²","metros cuadrados","superficie"]):
            return True, "Tier-4: Actividad con superficie declarada", 4

    # Tier-4d: Land development instruments for Promotores/RE
    if any(p in t for p in ["declaración de interés regional","segregación de finca",
                             "normalización de fincas","proyecto de actuación especial"]):
        return True, "Tier-4: Instrumento urbanístico (Promotores/RE)", 4

    return True, "Tier-5: Licencia / actividad grande", 5

# ════════════════════════════════════════════════════════════
# LEAD SCORING (0–100)
# ════════════════════════════════════════════════════════════
def score_lead(p):
    score = 0
    desc  = ((p.get("description","") or "") + " " + (p.get("permit_type","") or "")).lower()
    muni  = (p.get("municipality","") or "").lower()

    # Project type
    pt = p.get("permit_type","").lower()
    if pt in ("urbanización","plan especial / parcial"):
        score += 40
    elif pt in ("licitación de obras",):
        score += 38
    elif pt in ("obra mayor industrial",):
        score += 35
    elif pt in ("obra mayor nueva construcción",):
        score += 30
    elif pt in ("plan especial",):
        score += 28
    elif pt in ("obra mayor rehabilitación","cambio de uso","declaración responsable obra mayor"):
        score += 25   # bumped from 22 — cambio de uso is high-value for hospe/retail

    # Hospitality / Flexliving bonus — cambio de uso to residential/hospedaje
    _hospe_signals = [
        "cambio de uso", "cambio de destino", "uso residencial", "uso hospedaje",
        "rehabilitación integral", "rehabilitación de edificio", "reforma integral",
        "apartamentos turísticos", "viviendas de uso turístico", "uso hotelero",
        "residencia de estudiantes", "edificio plurifamiliar", "edificio de viviendas",
        "primera ocupación",
    ]
    if any(k in desc for k in _hospe_signals):
        score += 10   # hospe/flexliving operators need to act early on these

    # Prime Madrid barrios bonus for cambio de uso / rehab (Sharing Co operates here)
    _prime_barrios = {"centro", "salamanca", "chamberí", "malasaña", "chueca",
                      "lavapiés", "retiro", "almagro", "castellana", "legazpi",
                      "arganzuela", "justicia", "embajadores", "sol", "ópera"}
    if any(b in muni.lower() or b in desc for b in _prime_barrios):
        if any(k in desc for k in ["cambio de uso", "rehabilitación", "obra mayor"]):
            score += 6
    elif pt in ("obra mayor",):
        score += 18
    elif pt in ("licencia primera ocupación",):
        score += 15
    elif pt in ("licencia de actividad",):
        score += 10
    elif pt in ("contribuciones especiales",):
        # Confirmed active obra — very actionable for MEP/MAT
        score += 30
    else:
        if any(k in desc for k in ["proyecto de urbanización","junta de compensación","reparcelación"]):
            score += 40
        elif any(k in desc for k in ["nave industrial","centro logístico","parque empresarial"]):
            score += 33
        elif any(k in desc for k in ["nueva construcción","nueva planta"]):
            score += 28
        elif "obra mayor" in desc: score += 18
        else: score += 5

    # Phase bonus
    phase = (p.get("phase","") or "").lower()
    if phase == "primera_ocupacion":score += 20  # building DONE = most urgent for hospe/MEP
    elif phase == "adjudicacion":   score += 15  # contract awarded = most actionable for materials
    elif phase == "en_obra":        score += 12  # on-site = urgent for suppliers/machinery
    elif phase == "licitacion":     score += 10  # active tender = urgent for constructora
    elif phase == "definitivo":     score += 8   # final approval = 30-day window
    elif phase == "inicial":        score -= 5   # initial = long horizon
    elif phase == "solicitud":      score += 3   # pre-lead, lower urgency

    # Budget
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)) and val > 0:
        if val >= 50_000_000:   score += 38
        elif val >= 10_000_000: score += 35
        elif val >= 2_000_000:  score += 28
        elif val >= 500_000:    score += 20
        elif val >= 100_000:    score += 12
        elif val >= 50_000:     score += 6

    # Logistics corridor bonus — industrial in prime Madrid logistics belt
    logistics_munis = {"valdemoro","getafe","coslada","alcalá de henares","torrejón de ardoz",
                       "arganda del rey","fuenlabrada","alcobendas","san sebastián de los reyes",
                       "rivas-vaciamadrid","mejorada del campo","pinto","parla"}
    if any(m in muni for m in logistics_munis) and "industrial" in pt:
        score += 5

    # EU Next Gen rehabilitation bonus — confirmed budget, MEP priority
    if any(k in desc for k in ["rehabilitación energética","eficiencia energética",
                                "fondos next generation","plan de recuperación"]):
        score += 10  # High commercial value: confirmed funding = confirmed project

    # DIRs and major land development bonus for Promotores/RE
    if any(k in desc for k in ["declaración de interés regional","dir ","actuación de dotación",
                                "proyecto de actuación especial"]):
        score += 15  # DIRs are the biggest signals for land investment

    # Retail bonus when m² declared (size-confirmed opportunity)
    if "licencia de actividad" in pt or "apertura" in desc:
        if any(k in desc for k in ["m²","metros cuadrados","superficie útil"]):
            score += 8

    # Malvón / small food retail bonus — licencias de apertura with surface in prime zones
    _retail_food_signals = ["restauración","cafetería","comida","actividad alimentaria",
                             "establecimiento de comida","obrador","panadería","take away"]
    if any(k in desc for k in _retail_food_signals):
        score += 6   # food franchise expansion is time-sensitive

    # Kinépolis / large leisure surface bonus (>500m² commercial)
    _large_surface_signals = ["gran superficie","centro comercial","uso mixto",
                               "actividad de ocio","uso dotacional","parque comercial"]
    if any(k in desc for k in _large_surface_signals):
        score += 8   # large format = high-value for cinema/leisure operators

    # ACTIU — office fit-out bonus: any confirmed office, coworking, hotel or edu build
    _actiu_signals = ["edificio de oficinas","uso oficinas","coworking","espacio de trabajo",
                      "reforma de oficinas","adecuación de local","uso terciario",
                      "edificio terciario","centro de negocios",
                      "centro educativo","colegio","universidad","residencia de estudiantes"]
    if any(k in desc for k in _actiu_signals):
        score += 8   # every new/refurbished office/edu/hospitality = ACTIU contract sale

    # Demolition + new construction = double signal for machinery + materials
    if any(k in desc for k in ["demolición","derribo"]) and any(k in desc for k in ["nueva construcción","nueva planta"]):
        score += 6

    # ── Molecor / Compras — saneamiento projects = direct PVC pipe sales ────────
    # Every urbanización with saneamiento is a confirmed Molecor sales opportunity.
    # Large PEM + saneamiento = high-value lead for materials purchasing team.
    _saneamiento_signals = ["saneamiento", "colector", "red de abastecimiento",
                            "conducción de agua", "abastecimiento de agua",
                            "red de saneamiento", "pluviales", "drenaje"]
    if any(k in desc for k in _saneamiento_signals):
        if val and val >= 2_000_000:
            score += 8   # large saneamiento project = confirmed Molecor pipeline
        else:
            score += 4

    # ── FCC / Gran Constructora — licitación in Madrid = highest-value signal ──
    # FCC Construcción's primary client is Ayuntamiento de Madrid (46 contracts in 5yr).
    # A licitación in Madrid capital or a PAU/APE = their core business.
    _fcc_signals = ["área de planeamiento específico", "pau-", "pau ",
                    "plan de actuación urbanística", "licitación de obra pública",
                    "ayuntamiento de madrid", "licitación en madrid"]
    if any(k in desc for k in _fcc_signals) and "madrid" in muni:
        score += 8

    # ── Kiloutou / Alquiler Maquinaria — demolición + excavación = immediate need ─
    # José Luis Aliaga (Kiloutou): demolición + vaciado + excavación = call NOW.
    # These are the earliest signals — machinery needed before obra starts.
    _kiloutou_signals = ["demolición", "derribo", "vaciado", "excavación",
                         "explanación", "desescombro", "movimiento de tierras",
                         "cimentación"]
    _kiloutou_count = sum(1 for k in _kiloutou_signals if k in desc)
    if _kiloutou_count >= 2:
        score += 8   # multiple earthwork signals = confirmed machinery need
    elif _kiloutou_count == 1:
        score += 4

    # ── Saona/Kinépolis/Malvón — new urban development = future restaurant location ─
    # Retail/restaurant expansion needs: new barrios, centros comerciales, alta afluencia.
    # A new urbanización = new population = new restaurant location in 2-3 years.
    _retail_location_signals = ["centro comercial", "parque comercial", "zona comercial",
                                 "equipamiento comercial", "planta baja comercial",
                                 "uso terciario comercial", "galería comercial",
                                 "nueva urbanización", "nuevo barrio", "nueva área residencial"]
    if any(k in desc for k in _retail_location_signals):
        score += 6   # confirmed commercial/high-footfall zone = expansion target

    # Data completeness
    if p.get("address"):    score += 8
    if p.get("applicant"):  score += 8
    if p.get("expediente"): score += 2
    if muni not in ("", "madrid"): score += 2
    if p.get("confidence") == "high" and p.get("extraction_mode") == "ai":
        score = min(score + 5, 100)

    return min(score, 100)

# ════════════════════════════════════════════════════════════
# DATA EXTRACTION
# ════════════════════════════════════════════════════════════
MONTHS_ES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
             "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}

def parse_spanish_date(s):
    if not s: return ""
    if re.match(r"\d{4}-\d{2}-\d{2}$", s): return s
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', s, re.I)
    if m:
        mo = MONTHS_ES.get(m.group(2).lower())
        if mo:
            try: return datetime(int(m.group(3)), mo, int(m.group(1))).strftime("%Y-%m-%d")
            except: pass
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', s)
    if m:
        try: return datetime(int(m.group(3)),int(m.group(2)),int(m.group(1))).strftime("%Y-%m-%d")
        except: pass
    return s[:10] if len(s) >= 10 else s

def extract_municipality(text):
    patterns = [
        r'AYUNTAMIENTO\s+DE\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-]+?)(?:\n|\s{2,}|LICENCIAS|OTROS|CONTRATACIÓN|URBANISMO|ANUNCIO)',
        r'ayuntamiento de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:\.|,|\n)',
        r'(?:en|En)\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?),\s+a\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4}',
        r'Distrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:,|\.|$)',
        r'(?:municipio de|término municipal de)\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:,|\.|$)',
    ]
    noise = {"null","madrid","comunidad","boletín","oficial","administración","spain","españa","señor"}
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            name = m.group(1).strip().rstrip(".,; ").strip()
            if name.lower() not in noise and 3 < len(name) < 65:
                return name.title()
    return "Madrid"

def extract_expediente(text):
    m = re.search(r'[Ee]xpediente[:\s]+(\d{2,6}/\d{4}/\d{3,8})', text)
    if m: return m.group(1)
    m = re.search(r'[Ee]xp\.\s*n[úu]?m\.?\s*([\d\-/]+)', text)
    if m: return m.group(1)
    m = re.search(r'[Nn]\.?[Oo]?\s*(\d{1,6}[/\-]\d{4})', text)
    if m: return m.group(1)
    return ""

def _parse_euro(s):
    s = str(s).strip()
    if not s: return None
    if "," in s and "." in s: s = s.replace(".","").replace(",",".")
    elif "," in s: s = s.replace(",",".")
    else: s = s.replace(".","")
    try:
        v = float(s)
        return v if 0 < v < 3_000_000_000 else None
    except ValueError: return None

def extract_pem_value(text):
    c = text
    # 1. ICIO base imponible (= PEM exactly, legally confirmed)
    for pat in [
        r'(?:base imponible(?:\s+del\s+ICIO)?|b\.i\.\s+del\s+icio)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'(?:cuota\s+tributaria|importe\s+icio)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # 2. TABLA_DATOS from PDF table extraction
    if "TABLA_DATOS:" in c:
        for row_line in c.split("TABLA_DATOS:", 1)[1].split("\n"):
            if any(kw in row_line.upper() for kw in ["PEM","PRESUPUESTO","IMPORTE","BASE IMPONIBLE"]):
                for amt in re.findall(r'([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{1,2})?)', row_line):
                    v = _parse_euro(amt)
                    if v and 1000 <= v < 3_000_000_000: return round(v, 2)

    # 3. ETAPA rows (multi-stage urbanización — sum all)
    etapa_pems = re.findall(
        r'[Ee][Tt][Aa][Pp][Aa]\s*\d+[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*€', c)
    if etapa_pems:
        total = sum(v for vs in etapa_pems for v in [_parse_euro(vs)] if v and v >= 10000)
        if total > 0: return round(total, 2)

    # 4. Explicit PEM label
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # 5. IVA-inclusive total (urbanización — extract and note it's gross)
    m = re.search(
        r'presupuesto,\s*\d+\s*%\s*IVA\s+incluido,\s*de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*euros',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    # 5b. "coste de las obras" in contribuciones especiales docs
    # = total project cost (confirmed by municipality, highly accurate)
    m = re.search(
        r'coste\s+(?:total\s+)?de\s+(?:las\s+)?obras[\s:]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 500: return round(v, 2)

    # 5c. Importe adjudicación (= final contracted price, most accurate for licitaciones)
    for pat in [
        r'(?:importe|precio)\s+(?:de\s+)?(?:la\s+)?adjudicaci[oó]n[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)',
        r'adjudicado\s+(?:por|en)[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    # 6. Public contract budget — presupuesto de licitación
    # NOTE: BOCM licitación docs state "presupuesto base de licitación, con IVA"
    # The true PEM (excluding IVA) = amount / 1.21. We extract the licitación
    # budget INCLUDING IVA (as stated) and divide to get the net construction cost.
    for pat in [
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n,?\s+(?:con\s+)?IVA(?:\s+incluido)?[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'valor\s+estimado[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000:
                # If "con IVA" present, remove IVA to get true PEM
                if re.search(r'con\s+IVA', pat, re.I) or re.search(r'con\s+IVA', c[max(0,m.start()-30):m.end()+10], re.I):
                    v = round(v / 1.21, 2)  # remove 21% IVA → true PEM
                return round(v, 2)

    # 7. Generic presupuesto
    m = re.search(
        r'(?:presupuesto|importe)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    return None

def detect_phase(text):
    t = text.lower()
    # Pre-lead: application in progress (most common in retail/activity solicitudes)
    if any(p in t for p in ["se ha solicitado licencia","ha solicitado licencia",
                             "se solicita licencia de","lo que se hace público en cumplimiento"]):
        # Only if no grant language — otherwise it's a later phase
        if not any(g in t for g in ["se concede","se otorga","aprobación definitiva"]):
            return "solicitud"
    # Most actionable first: contract awarded (adjudicación) or obra started/done
    if any(p in t for p in ["adjudicado a","contrato adjudicado","resolución de adjudicación",
                             "importe de adjudicación","precio de adjudicación"]):
        return "adjudicacion"  # contract awarded = call subcontractors NOW
    if any(p in t for p in ["acta de comprobación del replanteo","acta de inicio de obras",
                             "acta de recepción de las obras","obras ejecutadas",
                             "contribuciones especiales por la ejecución"]):
        return "en_obra"  # on-site = urgent for materials and MEP
    if any(p in t for p in ["licitación de obras","contrato de obras","se convoca licitación",
                             "convocatoria de licitación"]):
        return "licitacion"
    if any(p in t for p in ["aprobación definitiva","aprobar definitivamente",
                             "se concede","se otorga","licencia concedida"]):
        return "definitivo"
    if "primera ocupación" in t:
        return "primera_ocupacion"
    if any(p in t for p in ["aprobación inicial","se somete a información pública",
                             "información pública"]):
        return "inicial"
    return "en_tramite"

def keyword_extract(text, url, pub_date):
    res = {
        "address":            None,
        "applicant":          None,
        "municipality":       extract_municipality(text),
        "permit_type":        "obra mayor",
        "declared_value_eur": extract_pem_value(text),
        "date_granted":       parse_spanish_date(pub_date) or extract_date_from_url(url),
        "description":        None,
        "confidence":         "medium",
        "source_url":         url,
        "extraction_mode":    "keyword",
        "lead_score":         0,
        "expediente":         extract_expediente(text),
        "phase":              detect_phase(text),
    }
    c = re.sub(r'\s+', ' ', text)

    # Address
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:paseo|po\.?|pso\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:camino|glorieta|ronda|travesía|carretera)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+n[úu]?[mº°]?\.?\s*(\d+)',
        r'Área de\s+[Pp]laneamiento\s+[A-Za-záéíóúñ\s]+[\"\']([^\"\']{3,80})[\"\']',
        r'[Uu]nidad de [Ee]jecución\s+(?:n[úu]?[mº°]\.?\s*)?([A-Za-z0-9\.\-]+)',
        r'[Uu]nidad de [Aa]ctuación\s+(?:n[úu]?[mº°]\.?\s*)?([A-Za-z0-9\.\-]+)',
        r'[Ss]ector\s+([A-ZÁÉÍÓÚÑ0-9][^,\n\.\(\)]{2,50})',
        r'[Pp]olígono\s+(?:[Ii]ndustrial\s+)?([A-ZÁÉÍÓÚÑ][^,\n\.\(\)]{2,40})',
    ]:
        m = re.search(pat, c, re.I)
        if m: res["address"] = m.group(0).strip().rstrip(".,;"); break

    if not res["address"]:
        for pat in [
            r'[Dd]istrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\-\s]+?)(?:,|\.|$)',
            r'[Pp]arcela\s+(?:situada\s+en\s+)?([A-Za-záéíóúñ\s,º]+\d+)',
        ]:
            m = re.search(pat, c, re.I)
            if m: res["address"] = m.group(0).strip().rstrip(".,;"); break

    # Applicant
    for pat in [
        r'(?:promovido por|promotora?|a cargo de)\s+(?:la\s+)?([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{5,80})',
        r'(?:a instancia de|solicitante|interesado[/a]*|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:[Jj]unta de [Cc]ompensación\s+[\"\']?)([A-ZÁÉÍÓÚÑ][^\"\']{3,60}[\"\']?)',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:\bS\.?[AL]\.?U?\.?\b|\bSLU\b|\bS\.?L\.?\b|\bS\.?A\.?\b))',
        r'(?:adjudicatario|adjudicado a|empresa adjudicataria)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            a = m.group(1).strip().rstrip(".,;\"'")
            if 3 < len(a) < 90:
                if "junta de compensación" in pat.lower():
                    a = f"Junta de Compensación {a}"
                res["applicant"] = a; break

    # Permit type
    t = c.lower()
    if any(p in t for p in ["proyecto de urbanización","obras de urbanización",
                             "junta de compensación","reparcelación"]):
        res["permit_type"] = "urbanización"
    elif any(p in t for p in ["plan parcial","plan especial de reforma interior","peri"]):
        res["permit_type"] = "plan especial / parcial"
    elif "estudio de detalle" in t:
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["plan especial de cambio de uso","cambio de uso de local a vivienda",
                               "cambio de destino", "cambio de uso a residencial",
                               "cambio de uso a vivienda", "cambio de uso a hospedaje",
                               "autorización de cambio", "modificación de uso"]):
        res["permit_type"] = "cambio de uso"
    elif "cambio de uso" in t:
        res["permit_type"] = "cambio de uso"
    elif any(p in t for p in ["plan especial para","plan especial de"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["nave industrial","almacén industrial","plataforma logística",
                               "centro logístico","parque empresarial","actividades productivas",
                               "uso industrial","edificio industrial","distribución logística"]):
        res["permit_type"] = "obra mayor industrial"
    elif any(p in t for p in ["licitación de obras","contrato de obras",
                               "adjudicación de obras","obras de construcción",
                               "ejecución de obras"]):
        res["permit_type"] = "licitación de obras"
    elif any(p in t for p in ["nueva construcción","nueva planta","obra nueva",
                               "edificio de nueva","viviendas de nueva","edificio plurifamiliar"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral","restauración de edificio",
                               "reforma integral","reforma estructural"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma","ampliación","cambio de uso"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["demolición","derribo"]):
        res["permit_type"] = "demolición y nueva planta"
    elif "primera ocupación" in t:
        res["permit_type"] = "licencia primera ocupación"
    elif "declaración responsable" in t:
        res["permit_type"] = "declaración responsable obra mayor"
    elif any(p in t for p in ["impuesto sobre construcciones","liquidación del icio",
                               "base imponible"]):
        res["permit_type"] = "obra mayor"
    elif "modificación puntual" in t or "convenio urbanístico" in t:
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["actividad","local comercial","establecimiento"]):
        res["permit_type"] = "licencia de actividad"
    elif any(p in t for p in ["contribuciones especiales","cuota tributaria de reparto",
                               "ordenanza fiscal de contribuciones"]):
        res["permit_type"] = "contribuciones especiales"
    elif any(p in t for p in ["rehabilitación energética","eficiencia energética edificio",
                               "programa de rehabilitación energética"]):
        res["permit_type"] = "obra mayor rehabilitación"  # maps to MEP profile correctly
    elif any(p in t for p in ["declaración de interés regional","dir "]):
        res["permit_type"] = "plan especial"  # highest-tier planning = Promotores/RE
    elif any(p in t for p in ["apertura de establecimiento","actividad clasificada",
                               "licencia ambiental"]):
        res["permit_type"] = "licencia de actividad"
    elif any(p in t for p in ["segregación de finca","normalización de fincas","agrupación de fincas"]):
        res["permit_type"] = "plan especial"  # land instrument = Promotores/RE

    # Description
    desc = None
    m = re.search(r'(?:aprobar definitivamente|aprobación definitiva)\s+(?:el|del|los)\s+([^\.]{20,300})', c, re.I)
    if m: desc = "Aprobación definitiva: " + m.group(1).strip()[:250]
    if not desc:
        m = re.search(r'(?:licitación de obras|contrato de obras|ejecución de obras)\s+(?:de|para|del)?\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(r'licencia(?:\s+de\s+obra\s+mayor)?\s+para\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(
            r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de|'
            r'ampliación de|urbanización de|reparcelación de|modificación del)\s+[^\.]{15,250}',
            c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        for gp in ["se concede","se otorga","se acuerda conceder","se aprueba definitivamente",
                   "licitación de obras","acuerdo de reparcelación","base imponible"]:
            idx = t.find(gp)
            if idx >= 0: desc = c[idx:idx+300].strip(); break

    res["description"] = (desc or c[:250]).strip()[:350]
    res["lead_score"]  = score_lead(res)

    # Assign profile_fit from PROFILE_TRIGGERS (keyword_extract mode has no AI)
    # This ensures the Profile Fit column is populated even without an OpenAI key.
    if not res.get("profile_fit"):
        matched_profiles = []
        _t_lower = t  # already lowercased
        for profile_name, triggers in PROFILE_TRIGGERS.items():
            if any(trigger in _t_lower for trigger in triggers):
                matched_profiles.append(profile_name)
        # Always include broad profiles for construction documents
        pt_lower = (res.get("permit_type") or "").lower()
        if "urbanización" in pt_lower or "reparcelación" in pt_lower:
            for p_add in ["constructora", "alquiler", "materiales", "infrastructura"]:
                if p_add not in matched_profiles:
                    matched_profiles.append(p_add)
        if "obra mayor" in pt_lower or "nueva construcción" in pt_lower:
            for p_add in ["constructora", "mep", "alquiler", "materiales"]:
                if p_add not in matched_profiles:
                    matched_profiles.append(p_add)
        if "cambio de uso" in pt_lower or "rehabilitación" in pt_lower:
            for p_add in ["hospe", "mep"]:
                if p_add not in matched_profiles:
                    matched_profiles.append(p_add)
        res["profile_fit"] = matched_profiles if matched_profiles else ["promotores"]

    return res


def generate_supplies_estimate(permit_type, pem, description, full_text=""):
    """
    Enhanced keyword-based supplies estimate with PDF text analysis.
    Used as fallback when AI doesn't provide detailed supplies.
    """
    pt  = (permit_type or "").lower()
    pem = pem or 0
    d   = (description or "").lower()
    t   = (full_text or "").lower()
    pem_s = f"€{pem/1_000_000:.1f}M" if pem >= 1_000_000 else (f"€{int(pem/1000)}K" if pem >= 1000 else "N/D")

    # Try to extract specific quantities from text
    supplies = []
    
    # Extract concrete quantities
    for pat in [r"hormigón.*?([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*m[3³]",
                r"([0-9]{1,3}(?:[.,][0-9]{3})*)\s*m[3³].*?hormigón"]:
        m = re.search(pat, t, re.I)
        if m:
            vol = m.group(1).replace(".","").replace(",",".")
            supplies.append(f"Hormigón HA-25 {vol}m³")
            break
    
    # Extract pipe quantities
    for pat in [r"tubería.*?DN\s*([0-9]+).*?([0-9.,]+)\s*(?:km|m)",
                r"colector.*?DN\s*([0-9]+).*?([0-9.,]+)\s*(?:km|m)"]:
        m = re.search(pat, t, re.I)
        if m:
            dn = m.group(1)
            length = m.group(2).replace(".","").replace(",",".")
            unit = "km" if "km" in t[m.end():m.end()+20].lower() else "m"
            supplies.append(f"Tubería PVC DN{dn} {length}{unit}")
            break
    
    # Extract steel quantities
    for pat in [r"acero.*?([0-9]{1,3}(?:[.,][0-9]{3})*)\s*(?:t|tn|toneladas)",
                r"([0-9]{1,3}(?:[.,][0-9]{3})*)\s*(?:t|tn).*?acero"]:
        m = re.search(pat, t, re.I)
        if m:
            tons = m.group(1).replace(".","").replace(",",".")
            supplies.append(f"Acero corrugado B500S {tons}t")
            break

    # If we found specific quantities, use them
    if supplies:
        return " | ".join(["🔧 " + supplies[0] if supplies else "", 
                          "🛒 " + ", ".join(supplies[1:]) if len(supplies) > 1 else "",
                          f"🚧 Maquinaria pesada según proyecto ({pem_s})"])

    # Otherwise, use intelligent estimates by project type
    if "urbanización" in pt or "urbaniz" in d:
        m2 = int(pem / 160) if pem else 0
        if m2 > 5000:
            return (f"🔧 Red eléctrica BT/MT, {m2//500} CT, alumbrado LED | "
                    f"🛒 Hormigón HA-25 ~{int(m2*0.3)}m³, tubería PVC DN200-500 ~{int(m2*0.04)}km, "
                    f"zahorra {int(m2*0.12)}t | "
                    f"🚧 Excavadoras, compactadores, extendedora ({pem_s})")
        else:
            return (f"🔧 Redes eléctricas BT, alumbrado, señalización | "
                    f"🛒 Hormigón, tuberías, áridos | 🚧 Maquinaria urbanización ({pem_s})")
    
    if "nueva construcción" in pt or "plurifamiliar" in d or "nueva planta" in pt:
        m2 = int(pem/1800) if pem else 0
        viviendas = int(m2 / 90) if m2 > 90 else 0
        if m2 > 1000:
            ascensores = max(2, m2//600)
            return (f"🔧 Ascensores ×{ascensores}, HVAC centralizado ~{int(m2*0.08)}kW, "
                    f"PCI rociadores+BIEs | "
                    f"🛒 Hormigón HA-25 {int(m2*0.35)}m³, acero B500S {int(m2*0.055)}t, "
                    f"ladrillo {int(m2*1.2)}m² | "
                    f"🚧 Grúa torre, andamios, plataformas ({pem_s})")
        else:
            return (f"🔧 Instalaciones MEP completas | 🛒 Estructura, cerramientos | "
                    f"🚧 Maquinaria construcción ({pem_s})")
    
    if "industrial" in pt or "nave" in d or "almacén" in d:
        m2 = int(pem/550) if pem else 0
        if m2 > 2000:
            return (f"🔧 Instalación eléctrica MT ~{int(m2*0.12)}kVA, iluminación industrial LED, "
                    f"PCI rociadores | "
                    f"🛒 Estructura metálica {int(m2*0.04)}t, panel sándwich {m2}m², "
                    f"solera hormigón {int(m2*0.15)}m³ | "
                    f"🚧 Grúas, explanación, pavimentación ({pem_s})")
        else:
            return (f"🔧 Instalación eléctrica MT, clima industrial | "
                    f"🛒 Estructura metálica, cerramiento, solera | "
                    f"🚧 Maquinaria industrial ({pem_s})")
    
    if "rehabilitación" in pt or "reforma" in pt:
        return (f"🔧 Renovación instalaciones (eléctrica BT, fontanería, HVAC) | "
                f"🛒 Aislamiento térmico, carpintería PVC/aluminio, revestimientos | "
                f"🚧 Andamios fachada, plataformas tijera ({pem_s})")

    if "cambio de uso" in pt or "cambio de destino" in pt:
        return (f"🔧 HVAC nuevo, fontanería completa, eléctrica BT, telecomunicaciones | "
                f"🛒 Tabiquería, solado, pintura, carpintería interior, cocina industrial | "
                f"🚧 Plataformas tijera, andamios interiores, herramientas menores ({pem_s})")

    if "licitación" in pt:
        entity = ""
        for ent in ["Canal de Isabel II","Metro de Madrid","EMVS","Ayuntamiento de Madrid"]:
            if ent.lower() in d or ent.lower() in t:
                entity = f" — {ent}"
                break
        return (f"🏗️ Licitación activa{entity} {pem_s} — consultar pliego para cantidades | "
                f"🚧 Adjudicatario necesitará: maquinaria + equipo según proyecto | "
                f"🛒 Materiales: ver presupuesto desglosado en pliego PLACE/CM")
    
    if "primera ocupación" in pt:
        return ("🔧 Revisiones finales ITE, legalización instalaciones, OCA | "
                "🛒 Acabados finales: pavimentos, pintura, carpintería | "
                "🚧 Plataformas elevadoras, herramientas menores")
    
    # Generic fallback with PEM context
    return (f"🏗️ Proyecto {pem_s} — analizar PDF técnico para especificaciones | "
            "🔧 Instalaciones según proyecto | 🛒 Materiales según mediciones | "
            "🚧 Maquinaria según cronograma")

def ai_extract(text, url, pub_date, pdf_text=None):
    if not USE_AI: return keyword_extract(text, url, pub_date)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        sys_prompt = """You are an elite construction intelligence analyst for Spain.
You read BOCM/BOE documents to extract actionable leads for B2B sales teams in construction.

Your subscribers include these specific companies — tailor analysis for them:
• FCC Construcción: gran constructora, licitaciones públicas, obra civil Madrid. Needs 6-18mo lead time before licitación.
• Grupo Saona / Malvón / Kinépolis: retail & restauración expansion, new centros comerciales, high-footfall zones.
• Sharing Co / Room00 (Jaime Bello): flexliving operator. Cambio de uso = holy grail. Primera ocupación = call TODAY.
• ACTIU (Jonatan Molina): office/contract furniture. Every new edificio de oficinas, hotel, coworking, hospital = sale.
• Kiloutou (José Luis Aliaga): maquinaria alquiler. Needs demolición/vaciado/excavación ASAP — before obra starts.
• Molecor (Javier González): PVC pipes, Loeches/Getafe Madrid. Every saneamiento/abastecimiento project = direct sales.
• CBRE / Muppy / Uvesco: RE investment. Reparcelaciones, plan parcial, suelo urbanizable.
• MEP instaladores: any obra mayor, rehabilitación integral, edificio plurifamiliar.

CRITICAL RULES:
1. Return ONLY valid JSON — no markdown, no text outside JSON.
2. If NOT a specific construction project → {"permit_type":"none","confidence":"low"}
3. Required fields: applicant, address, municipality, permit_type, description,
   declared_value_eur, date_granted, confidence, lead_score, expediente, phase,
   supplies_needed, profile_fit, ai_evaluation, action_window, key_contacts, obra_timeline.
4. permit_type (exact strings only):
   "urbanización" | "plan especial" | "plan especial / parcial" |
   "obra mayor nueva construcción" | "obra mayor industrial" | "obra mayor rehabilitación" |
   "cambio de uso" | "declaración responsable obra mayor" | "licencia primera ocupación" |
   "licencia de actividad" | "licitación de obras" | "contribuciones especiales" | "none"
5. declared_value_eur: Extract PEM / ICIO base imponible / licitación budget.
   For multi-stage projects: SUM all Etapa PEMs. Hard cap 3,000,000,000. NUMBER or null.
   ICIO base imponible = PEM exactly (Spanish tax law Art. 102 TRLRHL).
6. applicant: The PROMOTOR / company building. For urbanización = "Junta de Compensación [NAME]".
   For licitación = "Ayuntamiento de [MUNI]". Include CIF if found in text. Never blank.
7. municipality: Specific Madrid town (e.g. "Getafe","Las Rozas"). NOT "Comunidad de Madrid".
8. description: 2 sentences MAX. Sentence 1: what is built, m² if available, nº viviendas/plantas,
   exact address, PEM. Sentence 2: who benefits and concrete next action.
   Example: "571 viviendas plurifamiliares + garaje, C/ Alonso Zamora 16, SSRR — PEM €82.2M."
9. lead_score: 0–100. Large PEM + definitivo = 75-90. primera_ocupacion = 85+. licitación activa = 80+.
   cambio de uso definitivo = 70+. No PEM + inicial = 25-40.
10. phase: "definitivo"|"inicial"|"licitacion"|"adjudicacion"|"en_obra"|"primera_ocupacion"|"en_tramite"
11. confidence: "high" (all fields confirmed) | "medium" | "low"

NEW REQUIRED FIELDS:

action_window — EXACTLY one of these values (choose based on urgency):
"⚡ ACTUAR ESTA SEMANA" → licitación activa | primera ocupación | adjudicación | acta de inicio | contribuciones esp.
"📞 CONTACTAR EN 30 DÍAS" → aprobación definitiva | cambio de uso concedido | reparcelación definitiva
"📅 MONITORIZAR (3-6 meses)" → aprobación inicial | estudio de detalle inicial | plan parcial inicial
"🔮 PIPELINE LARGO (>12 meses)" → plan de sectorización | DIR inicial | constitución JC | plan general

key_contacts — extract from PDF text any: director de obra, aparejador, arquitecto técnico, promotor
  contact, gerente del proyecto, CIF/NIF del promotor. If multiple: separate with " | ".
  Format: "Promotor: [company/name] | Dir.Obra: [name] | Aparejador: [name] | CIF: [xxx]"
  If none found in document: ""

obra_timeline — extract construction timing from document:
  "plazo de ejecución: X meses" → "Plazo: X meses desde inicio"
  "etapa 1: X meses, etapa 2: Y meses" → "Etapa 1: X meses | Etapa 2: Y meses"
  phase definitivo + PEM → estimate "Inicio estimado: Q[N] 202X (estimado)"
  If no timing found: ""

AI_EVALUATION — THE MOST IMPORTANT FIELD. PERSONALIZED, SPECIFIC, ACTIONABLE:
Write 3-6 sentences. NEVER generic.
Structure:
1. WHAT + WHERE + PEM: "Proyecto de urbanización definitivo [NAME], [MUNI] — PEM €X.XM."
2. SCALE/CONTEXT: why this location matters, population/corridor, strategic importance. Add here if you find anything relevant and importantabout the project.
3. TIMING: phase, estimated timeline, next milestone.
4. QUANTITIES: any m², viviendas, pipes, machinery from document.

GOOD ai_evaluation:
"Proyecto de urbanización definitivo APE 08.21 Las Tablas Oeste, Fuencarral-El Pardo — PEM €106.7M confirmado. Uno de los 3 mayores proyectos urbanización Madrid capital en 5 años: >200.000m² suelo nuevo, viario completo, redes BT/MT, saneamiento y telecomunicaciones. Etapa 1: 24 meses | Etapa 2: 36 meses desde hoy. FCC Construcción: pre-calificarse para licitación civil — pliego técnico estimado en 6-12 meses. Kiloutou: excavadoras 30t + compactadores para movimiento de tierras — inicio obra Q4 2026 estimado. Molecor: colector saneamiento DN400-500 ~3.5km + red abastecimiento DN200 ~2.4km — cotizar YA. CBRE/Muppy: área residencial futura — evaluar posición en JC."

BAD (NEVER):
"Proyecto de construcción en Getafe — PEM no declarado. Revisar el PDF original."

SUPPLIES NEEDED — ULTRA-DETAILED WITH EXACT QUANTITIES from [TABLA_DATOS_FINANCIEROS]:
Urbanización: "🔧 Red BT 20kV + X CT-630kVA + alumbrado LED Xm | 🛒 Hormigón HA-25 Xm³, tubería PVC DN-X Lkm, zahorra Z-1 Xt | 🚧 Excavadora 30t, compactador 12t, extendedora"
Nueva construcción: "🔧 Ascensores X ud (X+X), HVAC VRF XkW, PCI rociadores + BIEs | 🛒 Hormigón HA-25 Xm³, acero B500S Xt, cerramiento Xm² | 🚧 Grúa torre, plataformas, retroexcavadora"
Industrial: "🔧 Eléctrica MT XkVA, iluminación LED industrial | 🛒 Estructura metálica Xt, panel sándwich Xm², solera Xm² | 🚧 Grúas, explanación, pavimentación"
Rehab/CdU: "🔧 HVAC completo, fontanería nueva, eléctrica BT | 🛒 Tabiquería, acabados, carpintería RPT | 🚧 Plataformas tijera, andamios, herramientas menores"
If no quantities found: estimate intelligently using PEM ratios. NEVER generic placeholders.

PROFILE_FIT — CRITICAL: Return MULTIPLE profiles for every project. NEVER return only ["promotores"].
Profiles:
"infrastructura" — urbanización >€10M, obra civil, licitaciones estado
"constructora" — edificios plurifamiliares, licitaciones municipales, cualquier obra mayor
"mep" — edificios con ascensores/HVAC, rehab integral, primera ocupación
"industrial" — naves, almacenes, polígonos, logística
"retail" — locales comerciales, centros comerciales, cambio de uso terciario
"alquiler" — obra mayor, urbanización, demolición, movimiento tierras (ALWAYS for obra)
"materiales" — urbanización, nueva construcción, rehab, industrial (ALMOST ALWAYS)
"promotores" — reparcelación, DIR, segregación, plan parcial, convenio
"hospe" — cambio de uso residencial/hospedaje, rehab edificios, plurifamiliar, primera ocupación
"actiu" — oficinas, coworking, hoteles, hospitales, universidades, edificios terciarios

MANDATORY MULTI-PROFILE RULES (always apply):
- Urbanización/reparcelación → ALWAYS: ["promotores","constructora","alquiler","materiales"] + "infrastructura" if >€10M
- Licitación de obras → ALWAYS: ["constructora","materiales","alquiler"] + "infrastructura" if >€5M
- Edificio nueva construcción → ALWAYS: ["constructora","mep","materiales","alquiler","hospe"]
- Cambio de uso/rehabilitación → ALWAYS: ["hospe","mep","materiales"]
- Plan especial/parcial → ALWAYS: ["promotores","constructora"] + others based on use
- Nave/industrial → ALWAYS: ["industrial","alquiler","materiales"]
- Primera ocupación → ALWAYS: ["hospe","mep"]
- Saneamiento in project → ALWAYS add: "materiales" (Molecor PVC pipes opportunity)

PROFILE_FIT EXAMPLES (follow exactly):
Urbanización €50M → ["infrastructura","constructora","alquiler","materiales","promotores"]
Urbanización €3M → ["constructora","alquiler","materiales","promotores"]
Plan especial residencial → ["promotores","constructora","alquiler","materiales","hospe"]
Plan especial comercial → ["promotores","constructora","retail"]
571 viviendas nueva construcción → ["constructora","mep","materiales","alquiler","hospe"]
Cambio uso oficinas→residencial → ["hospe","mep","materiales","retail"]
Rehabilitación integral edificio → ["hospe","mep","materiales","alquiler"]
Nave industrial 5.000m² → ["industrial","alquiler","materiales"]
Licitación obras públicas €8M → ["constructora","materiales","alquiler","infrastructura"]
Primera ocupación residencial → ["hospe","mep"]
Reparcelación terrenos → ["promotores","constructora","materiales"]

DOCUMENT RULES:
"base imponible del ICIO" → declared_value_eur = exact number, confidence:"high"
"aprobación definitiva" OR "se aprueba" + plan/urbanización → phase:"definitivo"
"aprobación inicial" → phase:"inicial"
"primera ocupación" → phase:"primera_ocupacion", action_window:"⚡ ACTUAR ESTA SEMANA"
"adjudicación" / "acta de inicio" → phase:"adjudicacion"/"en_obra", action_window:"⚡ ACTUAR ESTA SEMANA"
"se ha solicitado" → phase:"en_tramite"

CAMBIO DE USO (all → permit_type:"cambio de uso", profile_fit MUST include "hospe"):
"cambio de uso"|"cambio de destino"|"modificación de uso"|"cambio de actividad"
"variación de uso"|"alteración de uso"|"implantación de nuevo uso"|"reconversión"
"división horizontal"|"segregación de viviendas"

REHABILITACIÓN (→ permit_type:"obra mayor rehabilitación", profile_fit MUST include "hospe","mep"):
"rehabilitación integral"|"reforma integral"|"renovación integral"|"gran rehabilitación"
"actuación de regeneración"|"regeneración urbana"|"rehabilitación de edificio"

"saneamiento" + quantities → profile_fit MUST include "materiales" (Molecor PVC pipes)
"licitación" ANY budget → profile_fit MUST include "constructora","materiales","alquiler"
"primera ocupación" → profile_fit MUST include "hospe","mep"

TABLA_DATOS: Extract ALL from [TABLA_DATOS_FINANCIEROS]. Use [TABLA_PARCELAS] m² for quantities.
Extract contacts from [DATOS_PROMOTORES_PROPIETARIOS] for key_contacts field.
If PEM estimated: confidence:"medium", explain method in ai_evaluation.
"""

        # Build the richest possible context for the AI:
        # 1. Announcement text (BOCM HTML excerpt) — always present
        # 2. PDF full text — contains financial tables, m² data, presupuesto desglosado
        # The PDF text is the KEY to good PEM estimation and supply quantities.
        if pdf_text and len(pdf_text) > len(text) and len(pdf_text) > 300:
            # Concatenate: first 4000 chars of announcement + full PDF (up to 10000 chars)
            # PDF end-sections contain financial summaries — take beginning AND end
            pdf_len = len(pdf_text)
            if pdf_len <= 10000:
                pdf_section = pdf_text
            else:
                pdf_section = (pdf_text[:4000] + "\n[...]\n"
                               + pdf_text[pdf_len//2 - 1000:pdf_len//2 + 1000]
                               + "\n[...]\n" + pdf_text[-3000:])
            user_content = (f"URL: {url}\n\n"
                            f"=== TEXTO ANUNCIO BOCM ===\n{text[:4000]}\n\n"
                            f"=== TEXTO COMPLETO PDF ===\n{pdf_section}")
        else:
            user_content = f"URL: {url}\n\nTexto BOCM:\n{text[:12000]}"

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":sys_prompt},
                      {"role":"user","content":user_content}],
            temperature=0, max_tokens=1400,
            response_format={"type":"json_object"})

        d = json.loads(resp.choices[0].message.content.strip())

        if str(d.get("permit_type","")).lower() in ("none","null","","otro","n/a"):
            return None

        d["source_url"]      = url
        d["extraction_mode"] = "ai"
        dg = d.get("date_granted") or pub_date
        d["date_granted"] = parse_spanish_date(str(dg)) if dg else extract_date_from_url(url)

        val = d.get("declared_value_eur")
        if isinstance(val, str):
            try:
                v = val.replace(".","").replace(",",".").replace("€","").strip()
                parsed = float(re.sub(r'[^\d.]','',v)) if v else None
                d["declared_value_eur"] = (parsed if parsed and 0 < parsed < 3_000_000_000 else None)
            except: d["declared_value_eur"] = None
        elif isinstance(val, (int, float)):
            if val <= 0 or val > 3_000_000_000: d["declared_value_eur"] = None

        if not d.get("lead_score"):    d["lead_score"]    = score_lead(d)
        if not d.get("municipality"):  d["municipality"]  = extract_municipality(text)
        if not d.get("expediente"):    d["expediente"]    = extract_expediente(text)
        if not d.get("phase"):         d["phase"]         = detect_phase(text)

        # If GPT didn't generate ai_evaluation (or it's too short), use the same improved
        # fallback logic as process_one. Mark it so we know it's a fallback.
        if not d.get("ai_evaluation") or len(str(d.get("ai_evaluation","")).strip()) < 40:
            pt   = (d.get("permit_type") or "").lower()
            pem  = d.get("declared_value_eur")
            muni = d.get("municipality","Madrid")
            phase = (d.get("phase") or "").lower()
            desc_l = (d.get("description") or "").lower()
            pem_s = (f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000
                     else (f"€{int(pem/1000):.0f}K" if pem and pem >= 1000 else "PEM no declarado"))
            phase_s = {"definitivo":"aprobación definitiva","inicial":"aprobación inicial",
                       "licitacion":"licitación activa","adjudicacion":"contrato adjudicado",
                       "en_obra":"obra en ejecución","en_tramite":"en tramitación"}.get(phase, phase)
            applicant = d.get("applicant") or "promotor"
            _has_san = any(k in desc_l for k in ["saneamiento","colector","abastecimiento","pluviales"])
            _timeline = "6-12" if phase == "definitivo" else "12-24"

            if "urbanización" in pt or "reparcelación" in pt:
                _san_note = (" Red de saneamiento incluida — confirmar DN y longitudes para material de tuberías." if _has_san else "")
                d["ai_evaluation"] = (
                    f"Proyecto de urbanización en {muni} — {pem_s} ({phase_s}). "
                    f"Promovido por {applicant}. "
                    f"Implica ejecución de red viaria, saneamiento, abastecimiento, electricidad BT/MT y telecomunicaciones. "
                    f"Gran Constructora: pre-calificarse para la futura licitación civil, estimada en {_timeline} meses desde esta aprobación. "
                    f"Instaladores MEP: contactar a la Junta de Compensación para pipeline de instalaciones (alumbrado, BT, telecomunicaciones). "
                    f"Alquiler de maquinaria: excavadoras, compactadores y maquinaria de urbanización requeridos en fase de movimiento de tierras.{_san_note}")
            elif "licitación" in pt:
                d["ai_evaluation"] = (
                    f"⚡ LICITACIÓN ACTIVA en {muni} — {pem_s}. "
                    f"Convocada por {applicant}. Plazo de oferta en curso — revisar pliego técnico de inmediato. "
                    f"Gran Constructora: presentar oferta técnica y económica urgente. "
                    f"Suministradores de materiales: acordar precios con el futuro adjudicatario antes de la firma del contrato. "
                    f"Alquiler de maquinaria: contactar al adjudicatario inmediatamente tras la resolución.")
            elif "plan especial" in pt or "plan parcial" in pt:
                _fase_label = "Aprobación definitiva" if phase == "definitivo" else "Aprobación inicial"
                d["ai_evaluation"] = (
                    f"{_fase_label} de instrumento urbanístico en {muni} — {pem_s}. "
                    f"Promovido por {applicant}. "
                    f"Este paso habilita la futura urbanización y edificación sobre el ámbito. "
                    f"Promotores/RE: contactar a la JC o propietarios del suelo antes de que salga al mercado. "
                    f"Gran Constructora: monitorizar para propuesta técnica de obra civil en {_timeline} meses. "
                    f"Retail y expansión comercial: evaluar si el uso previsto incluye equipamiento comercial o terciario.")
            elif "industrial" in pt or "nave" in t:
                d["ai_evaluation"] = (
                    f"Proyecto industrial en {muni} — {pem_s} ({phase_s}). "
                    f"Promovido por {applicant}. "
                    f"Uso previsto: {'nave logística' if any(k in desc_l for k in ['logística','distribución','almacén']) else 'nave industrial o de actividad productiva'}. "
                    f"Instaladores MEP: instalación eléctrica MT, PCI rociadores y climatización industrial. "
                    f"Suministradores: estructura metálica, panel sándwich cubierta y solera de hormigón. "
                    f"Alquiler de maquinaria: grúa de montaje, compactadora y maquinaria de explanación.")
            elif "cambio de uso" in pt or any(k in desc_l for k in
                    ["cambio de destino","modificación de uso","reconversión","variación de uso"]):
                _orig = "local comercial" if "local" in desc_l or "comercial" in desc_l else ("oficina" if "oficina" in desc_l else "uso existente")
                _dest = "residencial" if any(k in desc_l for k in ["vivienda","residencial","apartamento"]) else ("hospedaje" if any(k in desc_l for k in ["hotel","hostal","turístico"]) else "nuevo uso")
                d["ai_evaluation"] = (
                    f"Cambio de uso en {muni} — de {_orig} a {_dest} — {pem_s} ({phase_s}). "
                    f"Solicitado por {applicant}. "
                    f"Ventana de instalaciones activa: HVAC, fontanería, electricidad y protección contra incendios en obras. "
                    f"Operadores de hospedaje o flexliving: posicionarse con el propietario antes de que el edificio salga al mercado. "
                    f"Mobiliario y equipamiento de zonas comunes: evaluar formato y superficie total del proyecto.")
            elif "nueva construcción" in pt or "rehabilitación" in pt:
                _is_res = any(k in desc_l for k in ["vivienda","plurifamiliar","residencial","apartamento"])
                if _is_res:
                    d["ai_evaluation"] = (
                        f"Edificación residencial {'nueva' if 'nueva' in pt else '(rehabilitación)'} en {muni} — {pem_s} ({phase_s}). "
                        f"Promotor: {applicant}. "
                        f"Ascensores, HVAC y PCI se adjudican típicamente en fase de estructura — ventana de instalaciones en {_timeline} meses. "
                        f"Operadores residenciales o de gestión de activos: contactar al promotor antes de que el edificio salga al mercado. "
                        f"Alquiler de maquinaria: grúa torre, maquinaria de cimentación y plataformas elevadoras.")
                else:
                    d["ai_evaluation"] = (
                        f"Obra mayor {'nueva construcción' if 'nueva' in pt else '(rehabilitación)'} en {muni} — {pem_s} ({phase_s}). "
                        f"Promotor: {applicant}. "
                        f"Instaladores MEP: contactar antes de que el constructor cierre los subcontratos de instalaciones. "
                        f"Evaluar si el proyecto incluye oficinas, zonas comunes o uso terciario para equipamiento de contrato. "
                        f"Alquiler de maquinaria: grúa torre, plataformas elevadoras y maquinaria de obra en {_timeline} meses.")
            else:
                d["ai_evaluation"] = (
                    f"Proyecto en {muni} — {pem_s} ({phase_s}). Promovido por {applicant}. "
                    f"Consultar el PDF adjunto para superficie, cronograma y especificaciones técnicas. "
                    f"Gran Constructora: evaluar si es licitación pública o proyecto de iniciativa privada. "
                    f"Instaladores MEP y alquiler de maquinaria: confirmar fecha de inicio de obras para planificar la ventana de equipos.")

        # Supplies needed: generate if missing — pass pdf_text for accurate quantities
        if not d.get("supplies_needed") or len(str(d.get("supplies_needed","")).strip()) < 10:
            d["supplies_needed"] = generate_supplies_estimate(
                d.get("permit_type",""), d.get("declared_value_eur"), d.get("description",""),
                full_text=pdf_text or text)

        return d

    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def _enhance_profile_fit(p, text=""):
    """
    Post-extraction profile_fit enhancement.
    PROFILE_TRIGGERS was previously defined but never used — this function
    makes it active. It enriches/corrects profile_fit after AI or keyword extraction.

    Why needed:
    - AI often returns only ["promotores"] for urbanismo docs even though
      every urbanización also needs constructora, alquiler, materiales.
    - keyword_extract now assigns profile_fit, but may miss secondary profiles.
    - This runs on ALL extractions to ensure balanced, multi-profile tagging.
    """
    t = (text or "").lower()
    pt = (p.get("permit_type") or "").lower()
    desc = (p.get("description") or "").lower()
    pem = p.get("declared_value_eur") or 0
    phase = (p.get("phase") or "").lower()
    combined = t + " " + pt + " " + desc

    current = p.get("profile_fit") or []
    if isinstance(current, str):
        current = [x.strip() for x in current.split(",") if x.strip()]

    # Apply PROFILE_TRIGGERS — add any profile whose triggers appear in text
    for profile_name, triggers in PROFILE_TRIGGERS.items():
        if profile_name not in current:
            if any(trigger in combined for trigger in triggers):
                current.append(profile_name)

    # MANDATORY multi-profile rules (domain knowledge overrides)
    # Urbanización/reparcelación: ALWAYS needs contractors + machinery + materials
    if any(k in combined for k in ["urbanización", "reparcelación", "junta de compensación",
                                    "proyecto de urbanización", "obras de urbanización"]):
        for must in ["constructora", "alquiler", "materiales"]:
            if must not in current: current.append(must)
        if pem and pem >= 10_000_000:
            if "infrastructura" not in current: current.append("infrastructura")

    # Licitación: always needs constructora + materials + machinery
    if "licitación" in pt or "licitación" in combined:
        for must in ["constructora", "materiales", "alquiler"]:
            if must not in current: current.append(must)

    # Nueva construcción edificio: always MEP + materials
    if any(k in combined for k in ["nueva construcción", "edificio plurifamiliar",
                                    "nueva planta", "viviendas"]):
        for must in ["constructora", "mep", "materiales", "alquiler"]:
            if must not in current: current.append(must)
        if "hospe" not in current:
            current.append("hospe")  # every new residential building = Sharing Co opportunity

    # Cambio de uso / rehabilitación: hospe + mep + materiales always
    if any(k in combined for k in ["cambio de uso", "cambio de destino", "rehabilitación integral",
                                    "reforma integral", "renovación integral", "reconversión",
                                    "primera ocupación", "modificación de uso", "rehabilitación de edificio",
                                    "obra mayor rehabilitación"]):
        for must in ["hospe", "mep", "materiales"]:
            if must not in current: current.append(must)

    # Plan especial / plan parcial: promotores + constructora always
    if any(k in combined for k in ["plan especial", "plan parcial", "plan de sectorización"]):
        for must in ["promotores", "constructora"]:
            if must not in current: current.append(must)

    # Industrial / nave / logística: industrial + alquiler always
    if any(k in combined for k in ["nave industrial", "almacén", "centro logístico",
                                    "plataforma logística", "parque logístico", "uso industrial"]):
        for must in ["industrial", "alquiler", "materiales"]:
            if must not in current: current.append(must)

    # Saneamiento: always materiales (Molecor = PVC pipes)
    if any(k in combined for k in ["saneamiento", "colector", "red de abastecimiento",
                                    "conducción de agua", "pluviales"]):
        if "materiales" not in current: current.append("materiales")

    # Edificio de oficinas / coworking: ACTIU always
    if any(k in combined for k in ["edificio de oficinas", "coworking", "uso oficinas",
                                    "edificio terciario", "campus empresarial"]):
        if "actiu" not in current: current.append("actiu")

    # Remove "promotores" as the ONLY profile — it's almost never exclusively RE
    # unless it's a pure land deal (DIR, segregación, convenio sin obra)
    if current == ["promotores"]:
        if any(k in combined for k in ["urbanización", "obra", "construcción", "rehabilitación"]):
            current.extend(["constructora", "materiales", "alquiler"])

    p["profile_fit"] = list(dict.fromkeys(current))  # deduplicate preserving order
    return p


def extract(text, url, pub_date, pdf_text=None):
    result = (ai_extract(text, url, pub_date, pdf_text=pdf_text)
              if USE_AI else keyword_extract(text, url, pub_date))
    if result:
        result = _enhance_profile_fit(result, text=(pdf_text or text or ""))
    return result

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — 17 columns
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value PEM (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Lead Score","Expediente","Phase",
    "Estimated PEM","AI Evaluation","Supplies Needed","Profile Fit","Fuente",
    "Project Size",    # Col W — m², viviendas, plantas, or AI size estimate
    "Action Window",   # Col X — ⚡ ACTUAR ESTA SEMANA / 📞 30 DÍAS / 📅 MONITORIZAR / 🔮 PIPELINE
    "Key Contacts",    # Col Y — Promotor | Dir.Obra | Aparejador extracted from PDF
    "Obra Timeline",   # Col Z — plazo de ejecución, etapa structure, estimated start
]
_ws             = None
_seen_urls      = set()
_seen_bocm_ids  = set()
_sheet_lock     = threading.Lock()

def get_sheet():
    global _ws
    if _ws: return _ws
    # Reboot-proof: checks both CREDS_JSON and GCP_SERVICE_ACCOUNT_JSON
    sa = os.environ.get("CREDS_JSON") or os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa:
        log("❌ ERROR: No credentials found (Checked CREDS_JSON and GCP_SERVICE_ACCOUNT_JSON)")
        return None
    try:
        info  = json.loads(sa)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try:
            # We use "Leads" for the dashboard, ensure engine uses the same or matches your structure
            ws = sh.worksheet("Leads") 
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet("Leads", 5000, len(HDRS) + 10)

        # ALWAYS sync headers — ensures new columns (Action Window, Key Contacts, etc.)
        # appear in the sheet even if the tab already existed with fewer columns.
        # This is non-destructive: it only updates row 1, existing data is preserved.
        try:
            existing_hdrs = ws.row_values(1)
            if existing_hdrs != HDRS:
                ws.update(values=[HDRS], range_name="A1")
                log(f"✅ Headers updated ({len(HDRS)} columns)")
            else:
                log("✅ Sheet connected")
        except Exception:
            log("✅ Sheet connected (header check skipped)")
        _ws = ws; return _ws
    except Exception as e:
        log(f"❌ Sheet connection failed: {e}"); return None

def load_seen():
    """Load seen URLs from the Leads tab only to prevent duplicates."""
    global _seen_urls, _seen_bocm_ids
    ws = get_sheet()
    if not ws: return
    gc = ws.spreadsheet
    # ONLY read from "Leads" — this is the tab write_permit() writes to.
    # Reading from "Permits" (old tab name) caused all old records to be treated
    # as duplicates, resulting in 0 new leads saved on clean runs.
    try:
        tab_ws = gc.worksheet("Leads")
        rows = tab_ws.get_all_values()
        for row in rows[1:]:
            if len(row) > 9 and row[9].strip():
                u = row[9].strip()
                _seen_urls.add(u)
                bid = extract_bocm_id(u)
                if bid: _seen_bocm_ids.add(bid)
        log(f"✅ {len(_seen_urls)} URLs / {len(_seen_bocm_ids)} IDs loaded from 'Leads' tab")
    except gspread.WorksheetNotFound:
        log("ℹ️  'Leads' tab not found yet — starting fresh (0 seen URLs)")
    except Exception as e:
        log(f"⚠️  load_seen [Leads]: {e}")

def write_permit(p, pdf_url=""):
    ws  = get_sheet()
    url = p.get("source_url","")
    bocm_id = extract_bocm_id(url)

    with _sheet_lock:
        if bocm_id and bocm_id in _seen_bocm_ids:
            return False
        if url in _seen_urls:
            return False

        dec   = p.get("declared_value_eur")
        mode_src = p.get("extraction_mode", "")
        if "boe.es" in (url or "").lower():
            fuente = "BOE"
        elif mode_src == "cm_contratos":
            fuente = "CM-Contratos"
        elif mode_src == "datos_madrid":
            fuente = "Madrid-Licencias"
        else:
            fuente = "BOCM"
        est  = round(dec/0.03) if dec and isinstance(dec,(int,float)) and dec > 0 else ""
        addr = p.get("address") or ""
        muni = p.get("municipality") or "Madrid"
        maps = ""
        if addr:
            maps = ("https://www.google.com/maps/search/"
                    + (addr + " " + muni + " España").replace(" ","+").replace(",",""))
            # Format profile_fit as comma-separated string
        profile_fit = p.get("profile_fit", [])
        if isinstance(profile_fit, list):
            profile_fit_str = ", ".join(profile_fit)
        else:
            profile_fit_str = str(profile_fit) if profile_fit else ""

        row = [
            p.get("date_granted",""), muni, addr,
            p.get("applicant") or "",
            p.get("permit_type") or "obra mayor",
            dec or "", est, maps,
            (p.get("description") or "")[:350],
            url, pdf_url or "",
            p.get("extraction_mode","keyword"),
            p.get("confidence",""),
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            p.get("lead_score",0),
            p.get("expediente",""),
            p.get("phase",""),
            p.get("estimated_pem",""),
            (p.get("ai_evaluation") or "")[:600],
            (p.get("supplies_needed") or "")[:600],
            profile_fit_str,
            fuente,
            (p.get("project_size") or ""),
            (p.get("action_window") or ""),     # Col X — urgency signal
            (p.get("key_contacts") or "")[:300],# Col Y — extracted contacts
            (p.get("obra_timeline") or ""),     # Col Z — timing info
        ]

        try:
            if ws:
                ws.append_row(row, value_input_option="USER_ENTERED")
                _seen_urls.add(url)
                if bocm_id: _seen_bocm_ids.add(bocm_id)
                try:
                    rn  = len(ws.get_all_values())
                    sc  = p.get("lead_score",0)
                    if sc >= 65:   rb,gb,bb = 0.80,0.93,0.80
                    elif sc >= 40: rb,gb,bb = 1.00,0.96,0.76
                    elif sc >= 20: rb,gb,bb = 1.00,1.00,0.85
                    else:          rb,gb,bb = 0.98,0.93,0.93
                    ws.spreadsheet.batch_update({"requests":[{"repeatCell":{
                        "range":{"sheetId":ws.id,"startRowIndex":rn-1,"endRowIndex":rn},
                        "cell":{"userEnteredFormat":{"backgroundColor":{"red":rb,"green":gb,"blue":bb}}},
                        "fields":"userEnteredFormat.backgroundColor"}}]})
                except: pass
            phase_s = p.get("phase","?")
            dec_s   = f"€{dec:,.0f}" if dec else "N/A"
            log(f"  💾 [{p.get('lead_score',0):02d}pts|{phase_s}] "
                f"{muni} | {addr[:30]} | {p.get('permit_type','?')[:22]} | {dec_s}")
            return True
        except Exception as e:
            log(f"  ❌ Write: {e}"); return False

# ════════════════════════════════════════════════════════════
# CONCURRENT PROCESSING
# ThreadPoolExecutor with N_WORKERS threads.
# Each thread has its own HTTP session.
# Sheets writes are serialized via _sheet_lock.
# ════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════
# CONTACT DISCOVERY — Apollo.io enrichment (DoubleTrade-style)
# Maps applicant company → CEO/director name, email, LinkedIn.
# ════════════════════════════════════════════════════════════
_SKIP_ENRICH = [
    "junta de compensación", "junta de propietarios", "ayuntamiento",
    "comunidad de madrid", "ministerio", "diputación", "mancomunidad",
    "ute ", "u.t.e.", "soc. coop", "sociedad cooperativa",
    "particular", "propietario", "herederos", "canal de isabel ii",
    "emvs", "adif", "metro de madrid",
]

def _is_enrichable(name: str) -> bool:
    if not name or len(name.strip()) < 4: return False
    n = name.lower().strip()
    if any(p in n for p in _SKIP_ENRICH): return False
    legal = ["s.a.", "s.l.", "s.l.u.", "s.a.u.", "s.c.", "s.l.p.",
             " sa", " sl", " slu", " sau", "inc.", "ltd", "gmbh"]
    return any(n.endswith(lx) or (" "+lx) in n for lx in legal) or len(n.split()) >= 2


def enrich_contact(applicant: str) -> str:
    """
    DoubleTrade-style contact discovery: company name → key person.
    Returns: "👤 Juan Pérez (Director General) · ✉️ juan@atipical.com · 🔗 linkedin.com/in/..."
    Empty string if API not configured, entity not enrichable, or no results.
    """
    if not APOLLO_API_KEY or not _is_enrichable(applicant):
        return ""
    cache_key = applicant.lower().strip()
    if cache_key in _apollo_cache:
        return _apollo_cache[cache_key]

    # Strip legal suffix for cleaner Apollo search
    clean = re.sub(
        r",?\s*(S\.A\.U?\.?|S\.L\.U?\.?|S\.C\.|SLU|SAU|SA|SL)\s*$",
        "", applicant, flags=re.I).strip()

    try:
        resp = requests.post(
            "https://api.apollo.io/v1/mixed_people/search",
            headers={"Content-Type":"application/json",
                     "Cache-Control":"no-cache",
                     "X-Api-Key": APOLLO_API_KEY},
            json={"q_organization_name": clean,
                  "person_seniorities": ["owner","founder","c_suite","vp","director"],
                  "per_page": 3, "page": 1},
            timeout=12,
        )
        if resp.status_code != 200:
            _apollo_cache[cache_key] = ""; return ""

        people = resp.json().get("people", []) or resp.json().get("contacts", [])
        if not people:
            _apollo_cache[cache_key] = ""; return ""

        # Sort by seniority — prefer founders/CEOs
        _PRIO = ["founder","ceo","coo","cto","director general","gerente","socio","president"]
        people.sort(key=lambda p: next(
            (i for i,kw in enumerate(_PRIO) if kw in (p.get("title") or "").lower()), 99))

        p    = people[0]
        name = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
        title= p.get("title","")
        email= p.get("email","")
        li   = p.get("linkedin_url","")
        phones = p.get("phone_numbers") or []
        phone  = phones[0].get("raw_number","") if phones else ""

        parts = []
        if name:  parts.append(f"👤 {name}" + (f" ({title})" if title else ""))
        if email: parts.append(f"✉️ {email}")
        if li:    parts.append(f"🔗 {li.replace('https://','')}")
        if phone: parts.append(f"📞 {phone}")

        result = " · ".join(parts)
        _apollo_cache[cache_key] = result
        if result:
            log(f"  🔍 Apollo: {clean[:30]} → {name} ({title})")
        return result

    except Exception as e:
        log(f"  ⚠️ Apollo [{applicant[:25]}]: {e}")
        _apollo_cache[cache_key] = ""; return ""

def process_one(url, idx, total):
    """Process a single URL. Returns (saved, skipped, error) counts."""
    try:
        text, pdf_url, pub_date, doc_title = fetch_announcement(url)
        if not text or len(text.strip()) < 40:
            return 0, 1, 0  # completely empty

        # ── Derive PDF URL deterministically if not found by fetch_announcement ──
        # BOCM's JSON-LD often omits the encoding field, leaving pdf_url=None.
        # The PDF URL pattern is 100% deterministic from the announcement ID.
        if not pdf_url:
            pdf_url = derive_pdf_url(url)

        # ── Fetch full PDF text ONCE — used for text augmentation, PEM and size ──
        # Always attempt even when we already have text: financial tables are at
        # the END of BOCM PDFs, not in the JSON-LD summary text.
        pdf_text = text   # default fallback
        if pdf_url:
            _pdf_full = extract_pdf_text_enhanced(pdf_url)
            if _pdf_full and len(_pdf_full) > 200:
                pdf_text = _pdf_full
                # Augment short announcement text with PDF content
                if len(text.strip()) < 200:
                    text = text + "\n\n" + _pdf_full
            else:
                # PDF fetch failed/empty: try lightweight PEM-only scan
                _pem_only = _fetch_pem_only_from_pdf(pdf_url)
                if _pem_only:
                    pdf_text = text + "\n\n" + _pem_only

        is_lead, reason, tier = classify_permit(text)
        if not is_lead:
            # Only log non-duplicate, non-admin rejections (reduce noise)
            if not any(n in reason.lower() for n in ["subvención","nombramiento","eurotaxi","festej"]):
                log(f"  ⏭️  {reason}")
            return 0, 1, 0  # skip

        p = extract(text, url, pub_date, pdf_text=pdf_text)
        if p is None:
            return 0, 1, 0  # skip

        # Store tier so downstream blocks can adjust messaging
        p["_tier"] = tier
        # Pre-leads (Tier-6): mark phase as "solicitud" so dashboard can filter/badge them
        if tier == 6 and not p.get("phase"):
            p["phase"] = "solicitud"

        dec = p.get("declared_value_eur")
        if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec < MIN_VALUE_EUR:
            return 0, 1, 0  # below minimum

        # ── Estimated PEM from structural data + AI fallback ────────────────────
        if not p.get("estimated_pem"):
            if dec and isinstance(dec, (int, float)) and dec > 0:
                p["estimated_pem"] = f"✅ PEM confirmado: €{dec:,.0f}"
            else:
                # pdf_text is already fetched at the top of process_one
                est_result = _estimate_pem_from_pdf(pdf_text)
                if est_result.get("estimated_pem"):
                    ep    = est_result["estimated_pem"]
                    ep_lo = est_result.get("estimated_pem_low")
                    ep_hi = est_result.get("estimated_pem_high")
                    def _fp(v):
                        if v >= 1_000_000: return f"€{v/1_000_000:.1f}M"
                        if v >= 1_000:     return f"€{int(v/1000)}K"
                        return f"€{int(v):,}"
                    rng_str = f"{_fp(ep_lo)} – {_fp(ep_hi)}" if ep_lo and ep_hi else f"€{ep:,.0f}"
                    p["estimated_pem"] = rng_str + " 🟡"
                    if not dec:
                        p["declared_value_eur"] = ep
                        p["lead_score"] = score_lead(p)
                elif USE_AI:
                    p["estimated_pem"] = _ai_estimate_pem(
                        pdf_text,
                        permit_type=p.get("permit_type", ""),
                        municipality=p.get("municipality", "Madrid"),
                        description=p.get("description", ""),
                    )
                    _ai_num = _parse_pem_from_estimated_string(p["estimated_pem"])
                    if _ai_num and not dec:
                        p["declared_value_eur"] = _ai_num
                        p["lead_score"] = score_lead(p)
                else:
                    p["estimated_pem"] = "⚪ Sin datos PEM en BOCM"

        # ── Extract project size (m², viviendas, plantas) ─────────────────────
        if not p.get("project_size"):
            p["project_size"] = _extract_project_size(pdf_text)
            if not p["project_size"] and USE_AI:
                p["project_size"] = _ai_extract_project_size(
                    pdf_text,
                    permit_type=p.get("permit_type", ""),
                    description=p.get("description", ""),
                )

        # ── Fallback ai_evaluation: only used when GPT didn't generate one ──────
        # Good GPT runs include ai_evaluation in the JSON. This fallback only fires
        # for keyword_extract() mode (no AI key) or rare GPT failures.
        if not p.get("ai_evaluation") or len(str(p.get("ai_evaluation","")).strip()) < 40:
            pt   = (p.get("permit_type") or "").lower()
            muni = p.get("municipality","Madrid")
            pem  = p.get("declared_value_eur")
            phase = (p.get("phase") or "").lower()
            desc = (p.get("description") or "").lower()
            applicant = p.get("applicant") or "promotor"
            pem_s = (f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000
                     else (f"€{int(pem/1000):.0f}K" if pem and pem >= 1000 else "PEM no declarado"))
            phase_s = {"definitivo":"aprobación definitiva","inicial":"aprobación inicial",
                       "licitacion":"licitación activa","adjudicacion":"contrato adjudicado",
                       "en_obra":"obra en ejecución","en_tramite":"en tramitación"}.get(phase, phase)
            _tier = p.get("_tier", 5)
            _has_san = any(k in desc for k in ["saneamiento","colector","abastecimiento","pluviales"])
            _timeline = "6-12" if phase == "definitivo" else "12-24"

            if _tier == 6:
                addr = p.get("address") or ""
                p["ai_evaluation"] = (
                    f"⚡ PRE-LEAD en {muni}{(' · ' + addr[:60]) if addr else ''}. "
                    f"Solicitud en tramitación — no concedida aún. "
                    f"Ventana de oportunidad: contactar al solicitante AHORA antes de la competencia. "
                    f"Saona/Malvón/retail: evaluar si la ubicación encaja con criterios de expansión. "
                    f"Sharing Co/hospe: evaluar cambio de uso potencial.")
            elif "urbanización" in pt or "reparcelación" in pt:
                san_note = " Molecor/compras: confirmar DN y longitudes de colectores para cotización PVC." if _has_san else ""
                p["ai_evaluation"] = (
                    f"Proyecto de urbanización en {muni} — {pem_s} ({phase_s}). "
                    f"Alcance: viario, saneamiento, abastecimiento, electricidad BT/MT y alumbrado público. "
                    f"FCC/Gran Constructora: pre-calificarse para licitación civil ({_timeline} meses). "
                    f"Kiloutou: excavadoras, compactadores y dúmpers — contactar promotor antes de inicio. "
                    f"MEP instaladores y materiales: contactar a la JC ahora para pipeline."
                    + san_note)
            elif "licitación" in pt:
                p["ai_evaluation"] = (
                    f"⚡ LICITACIÓN ACTIVA en {muni} — {pem_s}. "
                    f"FCC/Gran Constructora: revisar pliego y presentar oferta URGENTE. "
                    f"Kiloutou: contactar al adjudicatario inmediatamente tras resolución para maquinaria. "
                    f"Molecor/materiales: acordar precios con futuro adjudicatario antes de firma. "
                    f"Convocante: {applicant}.")
            elif "plan especial" in pt or "plan parcial" in pt:
                p["ai_evaluation"] = (
                    f"{'✅ Aprobación definitiva' if phase == 'definitivo' else '📋 Aprobación inicial'} "
                    f"de plan urbanístico en {muni} — {pem_s}. "
                    f"{'Habilita desarrollo inmediato.' if phase == 'definitivo' else 'Desarrollo estimado en 12-24 meses.'} "
                    f"CBRE/Muppy/Promotores RE: contactar JC o propietarios del suelo ahora. "
                    f"Saona/Kinépolis: evaluar si el plan incluye equipamiento comercial o terciario. "
                    f"FCC/Constructora: monitorizar para propuesta de obra civil. Promovido por: {applicant}.")
            elif "industrial" in pt or "nave" in pt:
                p["ai_evaluation"] = (
                    f"Proyecto industrial en {muni} — {pem_s} ({phase_s}). "
                    f"MEP instaladores: eléctrica MT, PCI rociadores y climatización industrial. "
                    f"ACTIU: evaluar si incluye oficinas de nave o espacios de trabajo. "
                    f"Kiloutou/alquiler: maquinaria de cimentación, grúa, plataformas elevadoras. "
                    f"Promotor: {applicant}.")
            elif any(k in pt for k in ["cambio de uso","cambio de destino"]) or \
                 any(k in desc for k in ["cambio de uso","cambio de destino","modificación de uso",
                                          "reconversión","variación de uso"]):
                p["ai_evaluation"] = (
                    f"🏠 Cambio de uso en {muni} — {pem_s} ({phase_s}). "
                    f"Sharing Co / Room00: contactar al propietario AHORA — posicionarse como operador "
                    f"antes de que el edificio salga al mercado. "
                    f"MEP instaladores: HVAC, fontanería y eléctrica — ventana de instalaciones activa. "
                    f"ACTIU: evaluar si el nuevo uso incluye zonas comunes, coworking u oficinas. "
                    f"Solicitante: {applicant}.")
            elif "nueva construcción" in pt or "rehabilitación" in pt:
                _is_res = any(k in desc for k in ["vivienda","plurifamiliar","residencial","apartamento","habitación"])
                if _is_res:
                    p["ai_evaluation"] = (
                        f"🏠 Edificación residencial en {muni} — {pem_s} ({phase_s}). "
                        f"Sharing Co / Room00: contactar al promotor ANTES de que salga al mercado. "
                        f"MEP instaladores: ascensores, HVAC y PCI — adjudicación en fase de estructura. "
                        f"ACTIU: evaluar zonas comunes, recepción y lobby. "
                        f"Kiloutou: grúa torre y maquinaria de cimentación. Promotor: {applicant}.")
                else:
                    p["ai_evaluation"] = (
                        f"Obra mayor en {muni} — {pem_s} ({phase_s}). "
                        f"MEP instaladores: contactar antes de que el constructor cierre subcontratos. "
                        f"Ascensores, HVAC y PCI se adjudican 3-6 meses antes de estructura. "
                        f"ACTIU: evaluar si incluye oficinas, terciario o zonas comunes. "
                        f"Kiloutou/alquiler: grúa torre, plataformas. Promotor: {applicant}.")
            else:
                p["ai_evaluation"] = (
                    f"Proyecto en {muni} — {pem_s} ({phase_s}). Promotor: {applicant}. "
                    f"Revisar PDF adjunto para m², cronograma y especificaciones. "
                    f"FCC/Constructora: evaluar si es licitación pública o subcontratación. "
                    f"MEP/Kiloutou: confirmar fecha inicio obras para ventana de equipos e instalaciones.")

        if not p.get("supplies_needed") or len(str(p.get("supplies_needed","")).strip()) < 10:
            p["supplies_needed"] = generate_supplies_estimate(
                p.get("permit_type",""), p.get("declared_value_eur"), p.get("description",""),
                full_text=pdf_text or text)

        # ── action_window fallback (when AI doesn't set it) ────────────────────
        if not p.get("action_window"):
            phase = (p.get("phase") or "").lower()
            pt    = (p.get("permit_type") or "").lower()
            if phase in ("primera_ocupacion","adjudicacion","en_obra","licitacion") \
               or "contribuciones especiales" in pt:
                p["action_window"] = "⚡ ACTUAR ESTA SEMANA"
            elif phase == "definitivo" or "definitiv" in (p.get("ai_evaluation") or "").lower():
                p["action_window"] = "📞 CONTACTAR EN 30 DÍAS"
            elif phase == "inicial":
                p["action_window"] = "📅 MONITORIZAR (3-6 meses)"
            else:
                p["action_window"] = "🔮 PIPELINE LARGO (>12 meses)"

        # ── obra_timeline fallback — extract from pdf_text if AI missed it ────
        if not p.get("obra_timeline") and pdf_text:
            import re as _re
            _t = (pdf_text or "").lower()
            # Try to find "plazo de ejecución: X meses"
            _pz = _re.search(r'plazo\s+de\s+ejecuci[oó]n[^0-9]*(\d+)\s*meses', _t)
            if _pz:
                p["obra_timeline"] = f"Plazo: {_pz.group(1)} meses"
            else:
                # Try etapa structure
                _et = _re.findall(r'etapa\s+(\d+)[^0-9]*(\d+)\s*meses', _t)
                if _et:
                    p["obra_timeline"] = " | ".join(f"Etapa {e[0]}: {e[1]} meses" for e in _et[:3])

        # Contact Discovery — enrich high-value leads with Apollo.io
        if (APOLLO_API_KEY and not p.get("key_contacts")
                and p.get("lead_score", 0) >= 40 and p.get("applicant")):
            p["key_contacts"] = enrich_contact(p["applicant"])

        if write_permit(p, pdf_url or ""):
            return 1, 0, 0  # saved
        return 0, 1, 0  # dup/skip

    except Exception as e:
        log(f"  ❌ [{idx}] {e}")
        return 0, 0, 1  # error

# ════════════════════════════════════════════════════════════
# EMAIL DIGEST
# ════════════════════════════════════════════════════════════
def send_digest():
    ws = get_sheet()
    if not ws: log("❌ No sheet"); return
    try:
        rows   = ws.get_all_values()
        if len(rows) < 2: log("⚠️  Sheet empty"); return
        cutoff = datetime.now() - timedelta(days=7)
        recent = []
        for row in rows[1:]:
            if len(row) < 14: continue
            try:
                if datetime.strptime(row[13][:10],"%Y-%m-%d") >= cutoff:
                    recent.append(row)
            except: pass

        def get_val(r):
            try:
                s = str(r[5]).replace(".","").replace(",",".")
                return float(re.sub(r'[^\d.]','',s)) if s else 0.0
            except: return 0.0

        def get_score(r):
            try: return int(r[14]) if len(r) > 14 and r[14] else 0
            except: return 0

        recent.sort(key=get_score, reverse=True)
        total      = sum(get_val(r) for r in recent)
        high_count = sum(1 for r in recent if get_score(r) >= 65)
        log(f"📧 Digest: {len(recent)} leads | €{int(total):,} PEM | {high_count} priority")

        rhtml = ""
        for r in recent:
            raw_v = str(r[5]).strip() if len(r) > 5 and r[5] else ""
            try:
                _cleaned = re.sub(r'[^\d.]', '', raw_v.replace('.', '').replace(',', '.'))
                dec = f"€{int(float(_cleaned)):,}" if raw_v and _cleaned else "—"
            except: dec = "—"
            sc    = get_score(r)
            sc_c  = "#1b5e20" if sc >= 65 else "#e65100" if sc >= 40 else "#b71c1c"
            sc_bg = "#e8f5e9" if sc >= 65 else "#fff3e0" if sc >= 40 else "#fce4ec"
            expd  = r[15] if len(r) > 15 and r[15] else ""
            phase = r[16] if len(r) > 16 and r[16] else ""
            pb    = {"definitivo":"🟢 Definitivo","inicial":"🟡 Inicial",
                     "licitacion":"🔵 Licitación","primera_ocupacion":"⚪ 1ª Ocup."}.get(phase,"")
            maps_l = f"<a href='{r[7]}' style='color:#1565c0'>📍</a>&nbsp;" if len(r)>7 and r[7] else ""
            bocm_l = f"<a href='{r[9]}' style='color:#999;font-size:11px'>BOCM</a>" if len(r)>9 and r[9] else ""
            rhtml += f"""<tr style="border-bottom:1px solid #eee">
              <td style="padding:9px 7px;font-weight:600;font-size:13px">{r[1] or "—"}</td>
              <td style="padding:9px 7px;font-size:12px;color:#333">{r[2] or "—"}</td>
              <td style="padding:9px 7px;font-size:12px;color:#444">{r[3] or "—"}</td>
              <td style="padding:9px 7px"><span style="background:#e3f2fd;color:#0d47a1;padding:3px 7px;border-radius:10px;font-size:11px;white-space:nowrap">{r[4] or "—"}</span></td>
              <td style="padding:9px 7px;font-weight:700;color:#1565c0;font-size:14px">{dec}</td>
              <td style="padding:9px 7px;font-size:11px;color:#666">{pb}</td>
              <td style="padding:9px 7px;font-size:12px;color:#555">{(r[8] or "")[:130]}</td>
              <td style="padding:9px 7px;text-align:center"><span style="background:{sc_bg};color:{sc_c};padding:3px 8px;border-radius:10px;font-size:12px;font-weight:700">{sc}</span></td>
              <td style="padding:9px 7px;font-size:11px;color:#888">{expd}</td>
              <td style="padding:9px 7px;white-space:nowrap">{maps_l}{bocm_l}</td>
            </tr>"""

        ws_d  = (datetime.now()-timedelta(days=7)).strftime("%d %b")
        we_d  = datetime.now().strftime("%d %b %Y")
        est_t = f"€{int(total/0.03):,}" if total > 0 else "N/D"
        html  = f"""<html><body style="font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;color:#1a1a1a">
<div style="background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ PlanningScout — Oportunidades Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">{ws_d}–{we_d} · Ordenado por puntuación · {high_count} leads prioritarios (≥65 pts)</p>
</div>
<div style="display:flex;background:#e3f2fd;border-bottom:2px solid #bbdefb">
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">{len(recent)}</div>
    <div style="color:#555;font-size:13px">Proyectos detectados</div>
  </div>
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">€{int(total):,}</div>
    <div style="color:#555;font-size:13px">PEM total</div>
  </div>
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">{est_t}</div>
    <div style="color:#555;font-size:13px">Valor obra estimado</div>
  </div>
  <div style="flex:1;padding:14px 22px">
    <div style="font-size:32px;font-weight:700;color:#1b5e20">{high_count}</div>
    <div style="color:#555;font-size:13px">🟢 Leads prioritarios</div>
  </div>
</div>
<div style="overflow-x:auto;padding:0 28px 24px">
<table style="width:100%;border-collapse:collapse;min-width:1000px">
  <thead><tr style="background:#f5f5f5;text-align:left">
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Municipio</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Dirección</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Promotor</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Tipo</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">PEM</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Fase</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Descripción</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Score</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Exp.</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Links</th>
  </tr></thead>
  <tbody>{rhtml or '<tr><td colspan="10" style="padding:24px;text-align:center;color:#aaa">Sin proyectos esta semana</td></tr>'}</tbody>
</table></div>
<div style="padding:14px 28px;background:#f9f9f9;font-size:12px;color:#888;border-top:1px solid #e8e8e8">
  <strong>PlanningScout</strong> — BOCM (Boletín Oficial de la Comunidad de Madrid) · Datos públicos oficiales.<br>
  PEM = Presupuesto de Ejecución Material · Est.Obra = PEM/0.03 · 🟢Definitivo | 🟡Inicial | 🔵Licitación
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM","")
        gp = os.environ.get("GMAIL_APP_PASSWORD","")
        gt = os.environ.get(CLIENT_EMAIL_VAR,"")
        if not all([gf,gp,gt]): log("⚠️  Email vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (f"🏗️ PlanningScout Madrid — {len(recent)} proyectos | "
                          f"€{int(total):,} PEM | {high_count} prioritarios | {ws_d}–{we_d}")
        msg["From"] = gf; msg["To"] = gt
        msg.attach(MIMEText(html,"html","utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(gf,gp)
            s.sendmail(gf,[t.strip() for t in gt.split(",")],msg.as_string())
        log(f"✅ Digest sent to {gt}")
    except smtplib.SMTPAuthenticationError:
        log("❌ Digest: Gmail authentication failed.")
        log("   GMAIL_APP_PASSWORD must be a 16-char App Password, NOT your Gmail password.")
        log("   To fix: myaccount.google.com → Security → 2-Step Verification → App passwords")
        log("   Create app password for 'Mail' → copy 16 chars → update GitHub secret GMAIL_APP_PASSWORD")
    except Exception as e:
        log(f"❌ Digest error: {e}")

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════
# BOE SEARCH — Boletín Oficial del Estado
# Searches BOE Section B (Anuncios) for construction licitaciones
# in the Comunidad de Madrid. These are STATE-LEVEL contracts (ADIF,
# Ministerios, AENA, hospitals) — bigger budgets than municipal BOCM.
#
# URL format: https://www.boe.es/buscar/boe.php
# Target keywords: CPV 45000000 (Trabajos de construcción) + Madrid
# ════════════════════════════════════════════════════════════
BOE_SEARCH_KEYWORDS = [
    # (keyword, scope_note)
    ("licitación obras Madrid",          "CON+MAT"),
    ("obras construcción Madrid",        "CON+MAT"),
    ("obras urbanización Madrid",        "PRO+CON"),
    ("rehabilitación edificio Madrid",   "MEP+MAT"),
    ("nave industrial Madrid",           "IND+MAT"),
    ("licitación obras Getafe",          "IND+MAT"),
    ("licitación obras Alcalá de Henares","CON+MAT"),
    ("licitación obras Alcobendas",      "CON+MAT"),
    ("licitación ADIF Madrid",           "CON+MAT"),  # major infra
    ("licitación Comunidad de Madrid obras","CON+MAT"),
]

def search_boe(d_from, d_to, global_seen):
    """
    Search BOE Section B (Anuncios de licitación) for Madrid-area obra contracts.
    BOE handles state-level infrastructure: ADIF, Ministerios, AENA, hospitals.
    Returns list of BOE document URLs.
    """
    boe_urls = []
    seen_local = set()
    df_s = d_from.strftime("%d/%m/%Y")
    dt_s = d_to.strftime("%d/%m/%Y")

    # BOE search URL: full-text search with date range
    # Searches Sección B (anuncios) which has licitaciones
    BOE_SEARCH = "https://www.boe.es/buscar/boe.php"

    for kw, tag in BOE_SEARCH_KEYWORDS:
        if not time_ok(need_s=30): break
        try:
            params = (
                f"?campo%5B0%5D=OBJ&dato%5B0%5D={quote(kw)}"
                f"&campo%5B5%5D=FEC&dato%5B5%5D={quote(df_s)}"
                f"&campo%5B6%5D=FEC&dato%5B6%5D={quote(dt_s)}"
                f"&page_hits=40&accion=Buscar"
            )
            r = safe_get(BOE_SEARCH + params, timeout=25)
            if not r or r.status_code != 200: continue
            soup = BeautifulSoup(r.text, "html.parser")
            # BOE results: links contain /diario_boe/txt.php?id=BOE-B-...
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not ("diario_boe" in href or "boe.es/boe/" in href): continue
                full = urljoin(BOE_BASE, href) if href.startswith("/") else href
                bid = extract_bocm_id(full)
                key = bid if bid else full
                if key in seen_local or key in global_seen: continue
                # Only take licitacion (Section B) and anuncio documents
                if "BOE-B" in (bid or "") or "diario_boe" in href:
                    seen_local.add(key)
                    boe_urls.append(full)
            time.sleep(1.5)  # polite to BOE
        except Exception as e:
            log(f"  ⚠️ BOE search error [{kw}]: {e}")

    log(f"  📰 BOE: {len(boe_urls)} URLs found")
    return boe_urls


def _run_ai_backfill():
    """
    Re-run AI extraction on existing sheet rows that have empty AI Evaluation.
    Reads each row, calls ai_extract on its description text, writes back.
    Usage: python engine.py --client demo_madrid.json --backfill-ai
    """
    ws = get_sheet()
    if not ws: return
    try:
        all_rows = ws.get_all_values()
        if len(all_rows) < 2: log("Sheet empty"); return
        header = all_rows[0]
        # Find column indices
        try:
            ai_col   = header.index("AI Evaluation") + 1    # 1-indexed for gspread
            sup_col  = header.index("Supplies Needed") + 1
            desc_col = header.index("Description") + 1
            pt_col   = header.index("Permit Type") + 1
            muni_col = header.index("Municipality") + 1
            url_col  = header.index("Source URL") + 1
            pem_col  = header.index("Declared Value PEM (€)") + 1
        except ValueError as e:
            log(f"❌ Column not found: {e}"); return

        to_update = []
        for row_i, row in enumerate(all_rows[1:], start=2):
            # Only process rows with empty AI Evaluation
            ai_val = row[ai_col-1] if len(row) >= ai_col else ""
            if ai_val.strip(): continue  # already has AI eval

            desc  = row[desc_col-1] if len(row) >= desc_col else ""
            pt    = row[pt_col-1]   if len(row) >= pt_col   else ""
            muni  = row[muni_col-1] if len(row) >= muni_col else "Madrid"
            url   = row[url_col-1]  if len(row) >= url_col  else ""
            pem_v = pem_float(row[pem_col-1]) if len(row) >= pem_col else None

            # Build a minimal text to pass to AI
            text = f"TIPO: {pt}. MUNICIPIO: {muni}. DESCRIPCIÓN: {desc}. URL: {url}"
            if USE_AI:
                result = ai_extract(text, url, "")
            else:
                result = keyword_extract(text, url, "")

            if result:
                ai_eval = result.get("ai_evaluation","") or ""
                supplies= result.get("supplies_needed","") or ""
                to_update.append((row_i, ai_col, ai_eval, sup_col, supplies))
                log(f"  🤖 Row {row_i}: {muni} → AI eval generated ({len(ai_eval)} chars)")

        if not to_update:
            log("✅ All rows already have AI evaluation"); return

        log(f"📝 Updating {len(to_update)} rows…")
        for row_i, ai_c, ai_val, sup_c, sup_val in to_update:
            try:
                if ai_val:  ws.update_cell(row_i, ai_c, ai_val[:400])
                if sup_val: ws.update_cell(row_i, sup_c, sup_val[:250])
                time.sleep(1)  # rate limit
            except Exception as e:
                log(f"  ❌ Row {row_i}: {e}")

        log(f"✅ AI backfill complete: {len(to_update)} rows enriched")
    except Exception as e:
        log(f"❌ Backfill error: {e}"); import traceback; traceback.print_exc()


# ════════════════════════════════════════════════════════════
# SOURCE 8: BORME — Boletín Oficial del Registro Mercantil
# ════════════════════════════════════════════════════════════
# DoubleTrade's "corporate data" layer: new company registrations,
# director appointments, capital increases = early signals before
# a project appears in BOCM.
#
# API: https://api.boe.es/BORME/v2/  (free, official, no auth)
# We target: construction-sector companies newly registered in Madrid.
# These are promotores who just incorporated → likely have land/project.
def search_borme_new_companies(date_from, date_to):
    """
    Scan BORME for new construction company registrations in Madrid.
    Returns list of (company_name, directors, capital, date) tuples.
    These are EARLY SIGNALS — promotores before they appear in BOCM.
    """
    results = []
    # Scan each working day in range
    d = date_from
    while d <= date_to:
        if d.weekday() >= 5:
            d += timedelta(days=1); continue
        if not time_ok(need_s=20): break

        borme_url = f"https://api.boe.es/BORME/v2/sumario/{d.strftime('%Y%m%d')}"
        try:
            r = safe_get(borme_url, timeout=15)
            if not r or r.status_code != 200:
                d += timedelta(days=1); continue

            data = r.json()
            # BORME sumario has "diario" → "secciones" → "emisores"
            diario = data.get("data", {}).get("sumario", {}).get("diario", [])
            if not isinstance(diario, list): diario = [diario]

            for dia in diario:
                secciones = dia.get("secciones", {}).get("seccion", [])
                if not isinstance(secciones, list): secciones = [secciones]

                for sec in secciones:
                    # BORME Section C = Actos inscritos (new companies, capital changes)
                    if str(sec.get("@codigo","")) not in ("C",): continue

                    emisores = sec.get("emisores", {}).get("emisor", [])
                    if not isinstance(emisores, list): emisores = [emisores]

                    for em in emisores:
                        em_name = str(em.get("nombre_emisor","") or "").strip()
                        em_lower = em_name.lower()

                        # Filter: Madrid construction companies
                        _CONSTRUCT_TERMS = [
                            "construcciones", "construc", "promociones", "promot",
                            "inmobiliaria", "inmobi", "urbanizaciones", "edificaciones",
                            "inversiones inmobiliarias", "real estate", "desarrollo urbano",
                            "obras y", "contrata", "rehabilitación", "reform",
                        ]
                        if not any(t in em_lower for t in _CONSTRUCT_TERMS):
                            continue

                        # Check it's a Madrid entity
                        _madrid_terms = ["madrid","getafe","alcalá","móstoles",
                                         "leganés","alcobendas","pozuelo","majadahonda"]
                        # (BORME entries don't always have province in title — keep all
                        # construction companies for now, the promotor DB enriches later)

                        items = em.get("items", {}).get("item", [])
                        if not isinstance(items, list): items = [items]

                        for item in items:
                            item_txt = str(item.get("@url","") or item.get("#text","") or "")
                            results.append({
                                "company":  em_name,
                                "date":     d.strftime("%Y-%m-%d"),
                                "borme_id": item_txt,
                            })

        except Exception:
            pass

        d += timedelta(days=1)

    return results


# ════════════════════════════════════════════════════════════
# WATCHLIST ALERTS — notify subscribers of phase changes
# ════════════════════════════════════════════════════════════
_WATCHLIST_HDRS = ["email","source_url","expediente","fecha_added",
                   "phase_at_add","last_alerted","muni","description"]

def _get_watchlist_tab(spreadsheet):
    try: return spreadsheet.worksheet("Watchlist")
    except Exception:
        ws = spreadsheet.add_worksheet("Watchlist", rows=500, cols=8)
        ws.append_row(_WATCHLIST_HDRS); return ws

def send_watchlist_alerts():
    """Run after each engine cycle — email subscribers when phase advances."""
    gf = os.environ.get("GMAIL_FROM","")
    gp = os.environ.get("GMAIL_APP_PASSWORD","")
    if not all([gf, gp]): return
    ws_main = get_sheet()
    if not ws_main: return
    try:
        ss   = ws_main.spreadsheet
        wl   = _get_watchlist_tab(ss)
        subs = wl.get_all_records()
        if not subs: return
        leads = ss.worksheet("Leads").get_all_records()
        # Build expediente → latest row map
        exp_map = {}
        for row in leads:
            exp = str(row.get("Expediente","") or "").strip()
            if not exp: continue
            if exp not in exp_map:
                exp_map[exp] = row
            else:
                try:
                    from dateutil import parser as _dp2
                    d_new = _dp2.parse(str(row.get("Date Found","") or ""))
                    d_old = _dp2.parse(str(exp_map[exp].get("Date Found","") or ""))
                    if d_new > d_old: exp_map[exp] = row
                except Exception: pass
        _PHASE_ORDER = {"inicial":1,"en_tramite":2,"solicitud":2,"definitivo":3,
                        "licitacion":4,"adjudicacion":5,"en_obra":6,"primera_ocupacion":7}
        today_s = datetime.now().strftime("%Y-%m-%d")
        updates = []
        for i, sub in enumerate(subs, start=2):
            email  = str(sub.get("email","") or "").strip()
            expd   = str(sub.get("expediente","") or "").strip()
            p_add  = str(sub.get("phase_at_add","") or "").strip()
            p_last = str(sub.get("last_alerted","") or "").strip()
            if not email or not expd or p_last == today_s: continue
            cur = exp_map.get(expd)
            if not cur: continue
            p_cur = str(cur.get("Phase","") or "").strip()
            if _PHASE_ORDER.get(p_cur.lower(),0) <= _PHASE_ORDER.get(p_add.lower(),0): continue
            # Phase advanced — send alert
            _PHASE_LABELS = {"inicial":"🟡 Aprobación Inicial","definitivo":"🟢 Aprobación Definitiva",
                             "licitacion":"🔵 Licitación","adjudicacion":"🏆 Adjudicación",
                             "en_obra":"🏗️ En Obra","primera_ocupacion":"⚪ 1ª Ocupación"}
            pl = _PHASE_LABELS.get(p_cur.lower(), p_cur)
            subj = f"🚨 PlanningScout — {cur.get('Municipality','')}: {expd} → {pl}"
            body = f"""<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:20px auto">
<div style="background:#1565c0;color:#fff;padding:18px 24px;border-radius:8px 8px 0 0">
  <h2 style="margin:0;font-size:18px">🔔 Alerta de proyecto — PlanningScout</h2>
</div>
<div style="border:1px solid #e0e0e0;border-top:0;padding:20px;border-radius:0 0 8px 8px">
  <p><strong>Expediente:</strong> {expd}</p>
  <p><strong>Municipio:</strong> {cur.get('Municipality','')}</p>
  <p><strong>Dirección:</strong> {cur.get('Full Address','')}</p>
  <p><strong>PEM:</strong> €{cur.get('Declared Value PEM (€)','')}</p>
  <div style="background:#e8f5e9;border-left:4px solid #2e7d32;padding:12px;margin:14px 0;border-radius:4px">
    <strong style="color:#1b5e20">Nueva fase: {pl}</strong>
    <span style="color:#666;font-size:12px;margin-left:8px">(antes: {p_add})</span>
  </div>
  <p style="font-size:13px">{str(cur.get('Description',''))[:200]}</p>
  {'<a href="' + str(cur.get('Source URL','')) + '" style="background:#1565c0;color:#fff;padding:10px 18px;border-radius:6px;text-decoration:none;font-size:13px">↗ Ver en BOCM</a>' if cur.get('Source URL') else ''}
  <p style="font-size:11px;color:#aaa;margin-top:18px">Para cancelar esta alerta: info@planningscout.com</p>
</div></body></html>"""
            try:
                import smtplib as _sm
                from email.mime.multipart import MIMEMultipart as _MMP
                from email.mime.text import MIMEText as _MMT
                msg = _MMP("alternative")
                msg["Subject"] = subj; msg["From"] = gf; msg["To"] = email
                msg.attach(_MMT(body,"html","utf-8"))
                with _sm.SMTP_SSL("smtp.gmail.com",465) as s2:
                    s2.login(gf, gp); s2.sendmail(gf, [email], msg.as_string())
                log(f"  🔔 Alert sent: {email} | {expd} → {pl}")
                updates.append((i, today_s))
            except Exception as e2: log(f"  ⚠️ Alert: {e2}")
        if updates:
            try:
                wl.spreadsheet.values_batch_update({"valueInputOption":"USER_ENTERED",
                    "data":[{"range":f"Watchlist!F{r}","values":[[d]]} for r,d in updates]})
            except Exception: pass
    except Exception as e: log(f"  ⚠️ Watchlist: {e}")

def run():
    if args.digest:
        log("📧 Digest-only mode"); get_sheet(); send_digest(); return

    if getattr(args, "backfill_ai", False):
        log("🤖 BACKFILL AI MODE — enriching existing rows with AI evaluation")
        _run_ai_backfill(); return

    today     = datetime.now()
    date_to   = today
    date_from = today - timedelta(weeks=WEEKS_BACK)

    log("=" * 70)
    log(f"🏗️  PlanningScout Madrid — Engine v14 (datos-noProbe+21kws+CM14d+Apollo+watchlist+BOCM107+BORME)")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}  |  Mode: {MODE.upper()}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"⚙️  {N_WORKERS} processing workers  |  ⏱️ Budget: {MAX_RUN_MINUTES}min")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction'}")
    log(f"💰  {'Min €' + f'{MIN_VALUE_EUR:,.0f}' if MIN_VALUE_EUR else 'No PEM filter'}")
    log("=" * 70)

    get_sheet(); load_seen()

    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming: {len(all_urls)} URLs from queue")
    else:
        all_urls = []
        # global_seen = BOCM-IDs already collected (prevents re-queueing same doc)
        global_seen = set()

        def add_url(u):
            norm = normalise_url(u)
            if not norm or is_bad_url(norm): return False
            if not url_date_ok(norm, date_from): return False
            if norm in _seen_urls: return False
            bid = extract_bocm_id(norm)
            if bid and bid in _seen_bocm_ids: return False
            key = bid if bid else norm
            if key in global_seen: return False
            global_seen.add(key)
            all_urls.append(norm)
            return True

        # ── SOURCE 1: Per-day section scan ──────────────────────────────────────
        # For DAILY mode: scan only the last 2 working days (fast, comprehensive).
        # For WEEKLY/FULL: scan all working days in the date range.
        # This is the MOST COMPREHENSIVE source — gets everything published each day.
        log(f"\n{'─'*55}")
        log(f"📅 SOURCE 1: Per-day Section III scan  [{MODE} mode]")

        if MODE == "daily":
            # Only scan last 2 working days (yesterday + day before)
            scan_days = []
            d = today - timedelta(days=1)
            while len(scan_days) < 2:
                if d.weekday() < 5: scan_days.append(d)
                d -= timedelta(days=1)
        else:
            # Scan all working days in range
            scan_days = []
            d = date_from
            while d <= date_to:
                if d.weekday() < 5: scan_days.append(d)
                d += timedelta(days=1)

        log(f"  Scanning {len(scan_days)} working days…")
        day_total = 0
        for day in scan_days:
            if not time_ok(need_s=60):
                log(f"  ⏱️  Time budget reached — day scan stopped at {day.strftime('%d/%m/%Y')}")
                break
            day_urls = scrape_day_section(day, sec=SECTION_III, global_seen=global_seen)
            added    = sum(1 for u in day_urls if add_url(u))
            if added > 0:
                log(f"  📅 {day.strftime('%d/%m/%Y')} [III]: +{added}")
                day_total += added
            else:
                # Always log — so you can confirm dates were checked
                # 0 results = Semana Santa/holiday, or all docs already seen
                log(f"  📅 {day.strftime('%d/%m/%Y')} [III]: 0 (ya visto o sin publicación)")
            time.sleep(0.4)

        # Section II (CM-level plans) — scan 2× per week
        # Section II (CM-level plans) — scan every day in daily mode, every 3rd in weekly/full
        step = 1 if MODE == "daily" else 3
        for day in scan_days[::step]:
            
                if not time_ok(need_s=60): break
                day_urls = scrape_day_section(day, sec=SECTION_II, global_seen=global_seen)
                added    = sum(1 for u in day_urls if add_url(u))
                if added > 0:
                    log(f"  📅 {day.strftime('%d/%m/%Y')} [II]: +{added}"); day_total += added
                time.sleep(0.4)

        log(f"  Day scan total: +{day_total} | {len(all_urls)} unique")

        # ── SOURCE 2: Keyword searches ───────────────────────────────────────────
        # DAILY: Skip (day scan covers everything)
        # WEEKLY: 25 focused keywords, 1-week chunks
        # FULL: All keywords, full date-chunking

        if MODE != "daily":
            log(f"\n{'─'*55}")
            log(f"🔎 SOURCE 2: Keyword search  [{MODE} mode]")

            kw_list = KW_WEEKLY
            if MODE == "full": kw_list = KW_WEEKLY + KW_EXTRA_FULL

            kw_total  = 0
            kw_n      = len(kw_list)
            log(f"  {kw_n} keywords  |  each chunk capped at "
                f"{MAX_PAGES_BACKFILL if MODE == 'full' else 'per-kw'} pages")
            for kw_idx, (kw, sec, max_pg, tag) in enumerate(kw_list, 1):
                if not time_ok(need_s=60): break
                # Time estimate: minutes remaining vs keywords remaining
                mins_left  = (MAX_RUN_MINUTES * 60 - elapsed()) / 60
                kws_left   = kw_n - kw_idx + 1
                eta_note   = f"  ~{mins_left:.0f}m left, {kws_left} kws remaining"
                # Reduce max_pages in full mode to stay within time budget
                effective_max_pg = min(max_pg, MAX_PAGES_BACKFILL) if MODE == "full" else max_pg
                log(f"  🔎 [{tag:12s}] '{kw}' [{sec}]  {kw_idx}/{kw_n}{eta_note}")
                urls = search_keyword_chunked(
                    kw, date_from, date_to,
                    global_seen=global_seen,
                    sec=sec, max_pages=effective_max_pg,
                    chunk_days=(7 if MODE in ("weekly","full") else 999))
                added = sum(1 for u in urls if add_url(u))
                if added > 0:
                    log(f"    +{added}"); kw_total += added
                time.sleep(1)
            log(f"  Keyword total: +{kw_total} | {len(all_urls)} unique")

        # ── SOURCE 3: Section V (ICIO notifications) ─────────────────────────────
        if MODE in ("weekly","full") and time_ok(need_s=120):
            log(f"\n{'─'*55}")
            log(f"📢 SOURCE 3: Section V (ICIO, anuncios)")
            # Scan last 4 weeks for Section V (ICIO notifications are recent)
            sec5_from = max(date_from, today - timedelta(weeks=4))
            sec5_days = [d for d in scan_days if d >= sec5_from]
            sec5_total = 0
            for day in sec5_days:
                if not time_ok(need_s=60):
                    log(f"  ⏱️  Budget reached at {day.strftime('%d/%m')} — Section V stopped")
                    break
                day_urls = scrape_day_section(day, sec=SECTION_V, global_seen=global_seen)
                added    = sum(1 for u in day_urls if add_url(u))
                if added > 0:
                    log(f"  📢 {day.strftime('%d/%m')} [V]: +{added}")
                    sec5_total += added
                else:
                    log(f"  📢 {day.strftime('%d/%m')} [V]: 0 (sin publicación ICIO)")
                time.sleep(0.4)
            log(f"  Section V: +{sec5_total} | {len(all_urls)} unique")

        # ── SOURCE 4: RSS ─────────────────────────────────────────────────────────
        if time_ok(need_s=120):
            log(f"\n{'─'*55}")
            log(f"📡 SOURCE 4: RSS")
            rss_urls  = get_rss_links(date_from, date_to, global_seen)
            rss_added = sum(1 for u in rss_urls if add_url(u))
            log(f"  RSS: +{rss_added} | {len(all_urls)} unique")

        # ═══SOURCE 5: BOE ═════════════════════════════════════════════════════════
        # ── SOURCE 5: BOE (Boletín Oficial del Estado) ───────────────────────────
        # State-level licitaciones: ADIF, Ministerios, AENA, Comunidad de Madrid
        # Uses XML extraction (fast, accurate) instead of PDF parsing
        if MODE in ("weekly", "full") and time_ok(need_s=180):
            log(f"\n{'─'*55}")
            log(f"📰 SOURCE 5: BOE (state licitaciones - XML extraction)")
            
            boe_items = search_boe_construction(date_from, date_to, global_seen)
            
            if boe_items:
                log(f"  Processing {len(boe_items)} BOE items concurrently...")
                boe_saved = boe_skipped = boe_errors = 0
                
                with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
                    boe_futures = {
                        executor.submit(process_boe_item, boe_id, title, dept, idx+1, len(boe_items)): boe_id
                        for idx, (boe_id, title, dept) in enumerate(boe_items)
                        if time_ok(need_s=10)
                    }
                    
                    for future in as_completed(boe_futures):
                        try:
                            s, sk, e = future.result()
                            boe_saved += s; boe_skipped += sk; boe_errors += e
                        except Exception as ex:
                            log(f"  ❌ BOE future error: {ex}"); boe_errors += 1
                
                log(f"  BOE results: ✅{boe_saved} saved | ⏭️{boe_skipped} skipped | ❌{boe_errors} errors")
            else:
                log(f"  📰 BOE: No relevant items found in date range")

        # ── SOURCE 6: Comunidad de Madrid Contratos ATOM feed ───────────────────
        # CM publishes a live ATOM/XML feed at contratos-publicos.comunidad.madrid
        # Contains: Canal Isabel II, Metro, EMVS, hospitals, roads — all CM agencies.
        # Completely distinct from BOCM: these are contract tenders, not planning docs.
        # FCC: licitaciones activas 6-18 months early. Kiloutou: adjudicaciones = NOW.
        # Molecor: saneamiento/agua contracts = direct PVC pipe sales opportunity.
        if MODE in ("daily","weekly","full") and time_ok(need_s=60):
            log(f"\n{'─'*55}")
            log(f"🏗️  SOURCE 6: CM Contratos Públicos (ATOM feed)")
            cm_items = search_cm_contratos(date_from, date_to, global_seen)
            if cm_items:
                cm_saved = cm_skipped = cm_errors = 0
                with ThreadPoolExecutor(max_workers=min(N_WORKERS, 3)) as executor:
                    cm_futures = {
                        executor.submit(process_cm_contrato, url, title, summary, idx+1, len(cm_items)): url
                        for idx, (url, title, summary) in enumerate(cm_items)
                        if time_ok(need_s=5)
                    }
                    for future in as_completed(cm_futures):
                        try:
                            s, sk, e = future.result()
                            cm_saved += s; cm_skipped += sk; cm_errors += e
                        except Exception as ex:
                            log(f"  ❌ CM Contrato future: {ex}"); cm_errors += 1
                log(f"  CM Contratos: ✅{cm_saved} saved | ⏭️{cm_skipped} skipped | ❌{cm_errors} errors")
            else:
                log(f"  CM Contratos: no new construction contracts")

        # ── SOURCE 7: datos.madrid.es — Licencias Urbanísticas Ayuntamiento Madrid ─
        # THE MOST IMPORTANT SOURCE FOR CAMBIO DE USO + OBRA MAYOR + REHABILITACIÓN.
        # BOCM doesn't publish individual building licences for Madrid capital.
        # This API provides every licence granted by the Ayuntamiento de Madrid:
        #   - Cambio de uso (oficinas→vivienda) ← Sharing Co holy grail
        #   - Obra mayor nueva construcción ← MEP + constructora
        #   - Rehabilitación integral ← hospe + MEP
        #   - Primera ocupación ← ACTUAR ESTA SEMANA
        # Runs in DAILY mode too — this data is continuous, not batch.
        if time_ok(need_s=60):
            log(f"\n{'─'*55}")
            log(f"🏛️  SOURCE 7: datos.madrid.es (licencias urbanísticas Madrid capital)")
            dm_items = search_datos_madrid(date_from, date_to, global_seen)
            if dm_items:
                dm_saved = dm_skipped = dm_errors = 0
                with ThreadPoolExecutor(max_workers=min(N_WORKERS, 3)) as executor:
                    dm_futures = {
                        executor.submit(
                            process_datos_madrid_item,
                            exp, rec, source_url, profile_hint, idx+1, len(dm_items)
                        ): exp
                        for idx, (exp, rec, source_url, profile_hint) in enumerate(dm_items)
                        if time_ok(need_s=5)
                    }
                    for future in as_completed(dm_futures):
                        try:
                            s, sk, e = future.result()
                            dm_saved += s; dm_skipped += sk; dm_errors += e
                        except Exception as ex:
                            log(f"  ❌ datos.madrid future: {ex}"); dm_errors += 1
                log(f"  datos.madrid: ✅{dm_saved} saved | ⏭️{dm_skipped} skipped | ❌{dm_errors} errors")
            else:
                log(f"  datos.madrid: 0 licencias — ")
                if not DATOS_MADRID_PROXY:
                    log(f"     WAF/IP block. Set DATOS_MADRID_PROXY secret (see engine source for setup).")
                else:
                    log(f"     No new licencias in date range via proxy (normal if no new grants).")

        # ── Remove already-seen from the collected BOCM queue ──────────────────
        all_urls = [u for u in all_urls
                    if u not in _seen_urls and
                    (not extract_bocm_id(u) or extract_bocm_id(u) not in _seen_bocm_ids)]

        log(f"\n{'═'*55}")
        log(f"📋 TOTAL: {len(all_urls)} new BOCM URLs to process (elapsed: {elapsed_str()})")
        log(f"{'═'*55}")

        with open(QUEUE_FILE, "w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume to restart if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new.")
        if today.weekday() == 0: send_digest()
        return

    # ── CONCURRENT PROCESSING ─────────────────────────────────────────────────
    saved = skipped = errors = 0
    log(f"\n{'─'*55}")
    log(f"⚙️  Processing {len(all_urls)} BOCM URLs with {N_WORKERS} workers…")
    log(f"{'─'*55}")

    with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {
            executor.submit(process_one, url, idx+1, len(all_urls)): url
            for idx, url in enumerate(all_urls)
            if time_ok(need_s=10)
        }
        completed = 0
        for future in as_completed(futures):
            if not time_ok(need_s=10):
                for f in futures:
                    f.cancel()
                log(f"\n⏰ Time budget reached — {len(futures)-completed} URLs not processed")
                log(f"💾 Queue still at {QUEUE_FILE} — re-run with --resume to continue")
                break
            try:
                s, sk, e = future.result()
                saved += s; skipped += sk; errors += e
            except Exception as ex:
                log(f"  ❌ Future error: {ex}"); errors += 1
            completed += 1
            if completed % 25 == 0:
                log(f"  ⚙️  {completed}/{len(all_urls)} | "
                    f"✅{saved} 💾 ⏭️{skipped} ❌{errors} | {elapsed_str()}")

    log(f"\n{'='*70}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors | {elapsed_str()}")
    log(f"📊 Acceptance rate: {100*saved//max(1,saved+skipped+errors)}%")
    log(f"ℹ️  'Skipped' breakdown (normal): duplicates already in sheet + admin noise "
        f"+ application-phase solicitudes + small activity licences.")
    log(f"   To see per-doc reasons, set log level to DEBUG or review individual ⏭️ lines above.")
    log("=" * 70)
    
    if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
    if today.weekday() == 0: log("\n📧 Monday → digest"); send_digest()

# ──────────────────────────────────────────────────────────────
# BOE CONFIGURATION
# ──────────────────────────────────────────────────────────────

BOE_SEARCH_URL = "https://www.boe.es/buscar/boe.php"

# Target departments (Departamento/Emisor codes)
BOE_DEPARTMENTS = [
    "Administración Local",
    "Ministerio de Transportes y Movilidad Sostenible", 
    "Ministerio de Vivienda y Agenda Urbana",
    "Ministerio para la Transición Ecológica y el Reto Demográfico",
    "Agencia de Infraestructuras Ferroviarias",  # ADIF
    "Comunidad de Madrid",
    "Agencia Estatal de Seguridad Aérea",  # AESA
]

# Construction/urbanismo keywords for title filtering
BOE_CONSTRUCTION_KEYWORDS = [
    "obras", "urbanización", "reparcelación", "construcción", 
    "infraestructura", "saneamiento", "edificación", "rehabilitación",
    "plan parcial", "plan especial", "licitación obras",
    "ejecución de obras", "proyecto de urbanización",
    "ordenación urbana", "equipamiento urbano", "obra civil",
    "infraestructura ferroviaria", "infraestructura viaria",
    "abastecimiento", "depuración", "pavimentación",
    "nueva construcción", "reforma", "ampliación edificio",
]

# Exclusion keywords (noise filters)
BOE_EXCLUSION_KEYWORDS = [
    "suministro de", "servicios de limpieza", "servicios de vigilancia",
    "servicios de mantenimiento de jardines", "servicios informáticos",
    "consultoría", "asistencia técnica", "software", "hardware",
    "mobiliario", "vehículos", "alimentación", "catering",
    "seguros", "transporte escolar", "recogida de residuos",
]

def build_boe_search_url(date_from, date_to, page=1):
    """
    Construct BOE advanced search URL with proper filters.
    
    Parameters:
    - campo[0]=ORIS & dato[0][5]=5 → Section V (Anuncios)
    - campo[6]=FPU → Date range
    - campo[2]=DEM → Department filter (applied in results filtering)
    
    Returns search results page URL.
    """
    df = date_from.strftime("%d/%m/%Y")
    dt = date_to.strftime("%d/%m/%Y")
    
    # Build the search URL
    # ORIS=5 targets Section V (Anuncios - Licitaciones)
    url = (
        f"{BOE_SEARCH_URL}?"
        f"campo%5B0%5D=ORIS&dato%5B0%5D%5B5%5D=5"  # Section V
        f"&campo%5B6%5D=FPU"  # Date field
        f"&dato%5B6%5D%5B0%5D={quote(df)}"  # Start date
        f"&dato%5B6%5D%5B1%5D={quote(dt)}"  # End date
        f"&page_hits=50"  # Results per page
        f"&page={page}"  # Page number
        f"&sort_field%5B0%5D=FPU"  # Sort by publication date
        f"&sort_order%5B0%5D=desc"  # Descending (newest first)
        f"&accion=Buscar"
    )
    
    return url

def filter_by_title(title):
    """
    Check if document title contains construction keywords.
    Returns True if relevant, False if noise.
    """
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Reject if contains exclusion keywords
    for excl in BOE_EXCLUSION_KEYWORDS:
        if excl in title_lower:
            return False
    
    # Accept if contains construction keywords
    for kw in BOE_CONSTRUCTION_KEYWORDS:
        if kw in title_lower:
            return True
    
    return False

def extract_boe_xml_text(boe_id):
    """
    Fetch BOE document as XML and extract clean text.
    
    BOE provides XML at: /diario_boe/xml.php?id=BOE-B-YYYY-NNNNN
    This is 100x faster and more accurate than PDF parsing.
    
    Returns (text, xml_url, metadata_dict)
    """
    xml_url = f"{BOE_BASE}/diario_boe/xml.php?id={boe_id}"
    
    try:
        r = safe_get(xml_url, timeout=25, thread_local=True)
        if not r or r.status_code != 200:
            return None, xml_url, {}
        
        # Parse XML
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError:
            # Fallback to BeautifulSoup for malformed XML
            soup = BS4(r.content, 'xml')
            root = soup
        
        # Extract metadata
        metadata = {
            "emisor": "",
            "fecha_pub": "",
            "seccion": "",
            "departamento": "",
        }
        
        # Try to extract metadata from XML structure
        # BOE XML structure: <documento><metadatos>...</metadatos><texto>...</texto></documento>
        if hasattr(root, 'find'):
            meta_node = root.find('.//metadatos') or root.find('metadatos')
            if meta_node is not None:
                # Extract emisor/departamento
                emisor = meta_node.find('.//emisor') or meta_node.find('emisor')
                if emisor is not None:
                    metadata["emisor"] = emisor.text or ""
                
                # Extract fecha
                fecha = meta_node.find('.//fecha_publicacion') or meta_node.find('fecha_publicacion')
                if fecha is not None:
                    metadata["fecha_pub"] = fecha.text or ""
                
                # Extract departamento
                dept = meta_node.find('.//departamento') or meta_node.find('departamento')
                if dept is not None:
                    metadata["departamento"] = dept.text or ""
        
        # Extract main text from <texto> node
        text_parts = []
        
        # Method 1: Find <texto> node
        if hasattr(root, 'find'):
            texto_node = root.find('.//texto') or root.find('texto')
            if texto_node is not None:
                # Get all text content recursively
                text_parts.append(ET.tostring(texto_node, encoding='unicode', method='text'))
        
        # Method 2: BeautifulSoup fallback
        if not text_parts:
            soup = BS4(r.content, 'xml')
            texto = soup.find('texto')
            if texto:
                text_parts.append(texto.get_text(separator=' ', strip=True))
        
        # Method 3: Get all text from XML
        if not text_parts:
            soup = BS4(r.content, 'xml')
            # Remove metadata nodes
            for tag in soup.find_all(['metadatos', 'diario', 'sumario']):
                tag.decompose()
            text_parts.append(soup.get_text(separator=' ', strip=True))
        
        full_text = ' '.join(text_parts)
        full_text = re.sub(r'\s+', ' ', full_text).strip()
        
        if len(full_text) < 100:
            return None, xml_url, metadata
        
        return full_text, xml_url, metadata
        
    except Exception as e:
        log(f"    ⚠️ BOE XML error [{boe_id}]: {e}")
        return None, xml_url, {}

def search_boe_construction(date_from, date_to, global_seen):
    """
    Search BOE for construction/urbanismo/obra-civil licitaciones using the
    BOE DAILY SUMARIO XML API — structured, reliable, no HTML guessing.

    URL: https://www.boe.es/diario_boe/xml.php?id=BOE-S-YYYYMMDD
    Returns a structured XML with every item published that day, including
    section, subsection, title, emisor/department, and BOE ID.

    Targets Section V (Anuncios de licitación) and filters by:
      - Construction keywords in the title
      - Relevant departments (ADIF, Ministerio Transportes, Canal Isabel II, etc.)

    Gran Infraestructura (Obra Civil) targets:
      - ADIF / Adif Alta Velocidad (railway infrastructure)
      - Ministerio de Transportes y Movilidad Sostenible (roads, airports)
      - Canal de Isabel II (Madrid water infrastructure)
      - SEITT / MITMA (motorway concessions)
      - Dirección General de Carreteras
      - Comunidad de Madrid (regional infrastructure)
      - Puertos del Estado (port civil works)
    """
    boe_items = []
    seen_local = set()

    # Build list of working days to scan
    scan_days = []
    d = date_from
    while d <= date_to:
        if d.weekday() < 5:   # Mon–Fri only (BOE publishes on working days)
            scan_days.append(d)
        d += timedelta(days=1)

    log(f"  🔍 BOE Sumario XML: scanning {len(scan_days)} working days")

    # Keywords that identify a construction-relevant document in BOE
    _CONST_KWS = [
        "obras", "urbanización", "reparcelación", "construcción",
        "infraestructura", "saneamiento", "edificación", "rehabilitación",
        "plan parcial", "plan especial", "licitación obras",
        "ejecución de obras", "proyecto de urbanización",
        "obra civil", "infraestructura ferroviaria", "infraestructura viaria",
        "abastecimiento", "depuración", "pavimentación",
        "nueva construcción", "reforma", "ampliación",
        "autovía", "carretera", "túnel", "puente", "viaducto",
        "línea ferroviaria", "plataforma ferroviaria", "electrificación",
        "canal", "embalse", "depuradora", "estación depuradora",
        "puerto deportivo", "puerto comercial",
        "aeropuerto", "terminal", "pista",
        "subestación eléctrica", "línea eléctrica",
        # Gran Infraestructura específico
        "alta velocidad", "corredor ferroviario", "nave industrial",
        "polígono industrial", "plataforma logística",
    ]
    _EXCL_KWS = [
        "suministro de", "servicios de limpieza", "servicios de vigilancia",
        "consultoría", "asistencia técnica", "software", "hardware",
        "mobiliario", "vehículos", "alimentación", "catering",
        "seguros", "recogida de residuos", "servicios informáticos",
        "arrendamiento", "transporte escolar",
    ]
    # Departments relevant to Gran Infraestructura + Madrid area
    _TARGET_DEPTS = [
        "adif", "alta velocidad", "administrador de infraestructuras",
        "ministerio de transportes", "ministerio de vivienda",
        "ministerio para la transición ecológica",
        "dirección general de carreteras", "dirección general de infraestructuras",
        "canal de isabel ii", "seitt", "mitma",
        "comunidad de madrid", "administración local",
        "ayuntamiento", "mancomunidad", "diputación",
        "puertos del estado", "autoridad portuaria",
        "aena", "enaire",
        "red eléctrica", "endesa", "iberdrola",  # utility infrastructure
        "confederación hidrográfica",
    ]

    def _title_ok(title):
        if not title: return False
        tl = title.lower()
        if any(e in tl for e in _EXCL_KWS): return False
        return any(k in tl for k in _CONST_KWS)

    def _dept_ok(dept):
        if not dept: return True   # unknown dept → accept, classify later
        dl = dept.lower()
        return any(d in dl for d in _TARGET_DEPTS)

    for day in scan_days:
        if not time_ok(need_s=30): break
        sumario_id = f"BOE-S-{day.strftime('%Y%m%d')}"
        xml_url    = f"{BOE_BASE}/diario_boe/xml.php?id={sumario_id}"
        try:
            # BOE requires a clean session — BOCM cookies cause BOE to redirect to HTML.
            # Use a dedicated BOE session with BOE-specific headers only.
            boe_sess = requests.Session()
            boe_sess.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "application/xml, text/xml, */*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9",
                "Referer": "https://www.boe.es/",
                "Connection": "keep-alive",
            })
            r = None
            for _attempt in range(3):
                try:
                    r = boe_sess.get(xml_url, timeout=30, verify=False, allow_redirects=True)
                    if r and r.status_code == 200: break
                    time.sleep(3 * (_attempt + 1))
                except Exception: time.sleep(5)
            if not r or r.status_code != 200:
                log(f"    ⚠️ BOE {sumario_id}: HTTP {r.status_code if r else 'no response'}")
                continue
            # Detect HTML response regardless of Content-Type header
            # (BOE sends HTML for holidays/non-publication days, sometimes with text/xml CT)
            body_peek = r.content[:300].decode("utf-8", errors="replace").lower()
            if "<!doctype" in body_peek or "<html" in body_peek or "<head>" in body_peek:
                # Silent skip — this date had no BOE edition (holiday or error page)
                continue
            try:
                root = ET.fromstring(r.content)
            except ET.ParseError as pe:
                log(f"    ⚠️ BOE {sumario_id}: XML parse error — {pe}")
                continue

            # BOE sumario XML structure:
            # <sumario> → <diario> → <seccion num="5"> → <departamento>
            #   → <item id="BOE-B-..."> with <titulo> and info attrs
            for item in root.iter("item"):
                boe_id = item.get("id", "")
                if not boe_id: continue
                # Only Section V (Anuncios) — id format BOE-B-... or BOE-A-...
                # Section B = Anuncios de organismos públicos (licitaciones)
                # Accept BOE-B (anuncios) and BOE-A (disposiciones, for planning docs)
                if not re.match(r'BOE-[AB]-\d{4}-\d+', boe_id, re.I): continue

                boe_id = boe_id.upper()
                if boe_id in seen_local or boe_id in global_seen: continue

                # Extract title
                titulo_el = item.find("titulo")
                title = titulo_el.text.strip() if titulo_el is not None and titulo_el.text else ""

                # Walk up to find the departamento/emisor
                # Walk up to find the departamento — nombre is an XML ATTRIBUTE not element
                department = ""
                for dept_el in root.iter("departamento"):
                    if dept_el.find(f".//item[@id='{boe_id}']") is not None:
                        # FIXED: use .get() for XML attributes, not .find() for child elements
                        department = (dept_el.get("nombre") or "").strip()
                        break

                # Apply filters
                if not _title_ok(title): continue
                if not _dept_ok(department): continue

                seen_local.add(boe_id)
                boe_items.append((boe_id, title, department))

        except Exception as e:
            log(f"  ⚠️ BOE sumario [{day.strftime('%d/%m')}]: {type(e).__name__}: {e}")
        else:
            if boe_items:
                log(f"    📰 {day.strftime('%d/%m')}: cumulative {len(boe_items)} items found")
        time.sleep(0.5)

    log(f"  📰 BOE Sumario XML: {len(boe_items)} relevant items found")
    return boe_items


def search_cm_contratos(date_from, date_to, global_seen):
    """
    SOURCE 6: Portal de Contratación Pública de la Comunidad de Madrid.
    
    Why this matters:
    - The CM publishes an ATOM/XML feed of ALL licitaciones at:
      https://contratos-publicos.comunidad.madrid/feed/licitaciones2
    - This feed is updated in near-real-time (minutes after publication)
    - Contains licitaciones de obras from Comunidad de Madrid agencies:
      Canal de Isabel II, Metro de Madrid, EMVS, hospitals, roads, etc.
    - DISTINCT from BOCM: these are contract awards/tenders, not planning documents
    - FCC Construcción (Fernando Tejada) needs these 6-18 months early
    - Kiloutou: adjudicación = machinery needed immediately
    - Molecor: saneamiento + agua contracts = direct PVC pipe sales
    
    Data: XML/ATOM with structured fields (title, description, budget, entity, date)
    Returns: list of (url, title, entity, budget, date) tuples for processing
    """
    CM_FEED = "https://contratos-publicos.comunidad.madrid/feed/licitaciones2"
    # Fallback feeds — CM publishes several ATOM feeds
    CM_FEEDS_EXTRA = [
        "https://contratos-publicos.comunidad.madrid/feed/licitaciones",
        "https://contratos-publicos.comunidad.madrid/feed/adjudicaciones2",
    ]
    # Keywords to filter construction-relevant contracts
    _CONSTR_KWS = [
        "obra", "construcción", "urbanización", "rehabilitación", "reforma",
        "saneamiento", "abastecimiento", "infraestructura", "vial", "pavimentación",
        "demolición", "edificio", "instalación", "reparación", "conservación",
        "mantenimiento", "canalización", "red de", "colector",
        "nave", "almacén", "polígono", "parque", "ampliación",
        "suministro e instalación", "actuaciones de mejora", "ejecución de",
        "proyecto de", "contrato de obras", "obras en",
    ]
    results = []
    seen_local = set()
    
    try:
        if not time_ok(need_s=60): return []
        r = safe_get(CM_FEED, timeout=30)
        if not r or r.status_code != 200:
            log(f"  ⚠️ CM Contratos feed unavailable (HTTP {r.status_code if r else 'timeout'})")
            return []
        
        from xml.etree import ElementTree as ET
        # Parse ATOM/XML feed
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError:
            log("  ⚠️ CM Contratos: XML parse error")
            return []
        
        # ATOM namespace
        ns = {"atom": "http://www.w3.org/2005/Atom",
              "": "http://www.w3.org/2005/Atom"}
        
        # Find all entries
        entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        if not entries:
            # Try without namespace
            entries = root.findall(".//entry")
        
        for entry in entries:
            def _get(tag):
                el = entry.find(f"{{http://www.w3.org/2005/Atom}}{tag}")
                if el is None: el = entry.find(tag)
                return (el.text or "").strip() if el is not None else ""
            
            title = _get("title")
            url_el = entry.find("{http://www.w3.org/2005/Atom}link")
            if url_el is None: url_el = entry.find("link")
            url = url_el.get("href", "") if url_el is not None else ""
            summary = _get("summary") + " " + _get("content")
            published = _get("published") or _get("updated")
            
            if not url or not title: continue
            if url in seen_local or url in _seen_urls: continue
            
            # Filter: accept anything published in the last 14 days
            # (not just date_from, because the CM feed accumulates recent items
            # and we might catch contracts published slightly before our window)
            if published:
                try:
                    from dateutil import parser as dp
                    pub_date = dp.parse(published).replace(tzinfo=None)
                    cutoff   = date_from - timedelta(days=14)
                    if pub_date.date() < cutoff.date():
                        continue
                except Exception:
                    pass
            
            # Filter: must be construction-related
            combined = (title + " " + summary).lower()
            if not any(kw in combined for kw in _CONSTR_KWS):
                continue
            
            seen_local.add(url)
            results.append((url, title, summary[:500]))
        
        log(f"  🏗️ CM Contratos ATOM: {len(results)} construction contracts found")

        # ── Try extra CM feeds if primary returned 0 ──────────────────────────
        if not results:
            for _extra_feed in CM_FEEDS_EXTRA:
                if not time_ok(need_s=30): break
                try:
                    r2 = safe_get(_extra_feed, timeout=20)
                    if not r2 or r2.status_code != 200: continue
                    root2 = ET.fromstring(r2.content)
                    entries2 = root2.findall(".//{http://www.w3.org/2005/Atom}entry") or root2.findall(".//entry")
                    for entry in entries2:
                        def _get2(tag):
                            el = entry.find(f"{{http://www.w3.org/2005/Atom}}{tag}")
                            if el is None: el = entry.find(tag)
                            return (el.text or "").strip() if el is not None else ""
                        title2   = _get2("title")
                        url2_el  = entry.find("{http://www.w3.org/2005/Atom}link") or entry.find("link")
                        url2     = url2_el.get("href","") if url2_el is not None else ""
                        summary2 = _get2("summary") + " " + _get2("content")
                        pub2     = _get2("published") or _get2("updated")
                        if not url2 or not title2: continue
                        if url2 in seen_local or url2 in _seen_urls: continue
                        if pub2:
                            try:
                                from dateutil import parser as dp2
                                pd2 = dp2.parse(pub2).replace(tzinfo=None)
                                if pd2.date() < (date_from - timedelta(days=14)).date(): continue
                            except Exception: pass
                        c2 = (title2 + " " + summary2).lower()
                        if not any(kw in c2 for kw in _CONSTR_KWS): continue
                        seen_local.add(url2)
                        results.append((url2, title2, summary2[:500]))
                except Exception: continue
            if results:
                log(f"  🏗️ CM Contratos extra feeds: +{len(results)} total")
        
    except Exception as e:
        log(f"  ⚠️ CM Contratos error: {e}")
    
    return results


def process_cm_contrato(url, title, summary, idx, total):
    """
    Process a single CM Contratos item.
    These are licitaciones/adjudicaciones from Comunidad de Madrid agencies.
    Returns (saved, skipped, error) counts.
    """
    try:
        with _sheet_lock:
            if url in _seen_urls:
                return 0, 1, 0
        
        # Build a structured permit dict from the feed data
        combined = (title + " " + summary).lower()

        # CM Contratos are PRE-VERIFIED government tenders from the official
        # Comunidad de Madrid procurement portal — already passed construction
        # keyword filter in search_cm_contratos(). Do NOT call classify_permit()
        # because CM contract text ("mantenimiento", "conservación", "reparación")
        # lacks BOCM grant phrases and gets falsely rejected. Only strip admin noise.
        _CM_NOISE = [
            "suministro de alimentos", "limpieza de oficinas",
            "servicio de vigilancia", "seguro de ", "seguros de ",
            "transporte escolar", "catering", "arrendamiento de vehículos",
            "servicios informáticos", "consultoría de gestión",
        ]
        if any(n in combined for n in _CM_NOISE):
            return 0, 1, 0
        
        # Extract PEM from summary
        pem = None
        import re as _re
        for pat in [
            r'presupuesto[^€\d]*€?\s*([\d.,]+)',
            r'valor\s+estimado[^€\d]*€?\s*([\d.,]+)',
            r'importe[^€\d]*€?\s*([\d.,]+)',
            r'([\d.,]+)\s*€',
        ]:
            m = _re.search(pat, summary, _re.I)
            if m:
                try:
                    v = m.group(1).replace('.','').replace(',','.')
                    v_num = float(_re.sub(r'[^\d.]','', v))
                    if 10_000 < v_num < 3_000_000_000:
                        pem = v_num
                        break
                except: pass
        
        # Determine permit type and phase
        permit_type = "licitación de obras"
        phase = "licitacion"
        if any(k in combined for k in ["adjudicado", "adjudicación", "contrato formalizado"]):
            phase = "adjudicacion"
        elif any(k in combined for k in ["urbanización", "reparcelación"]):
            permit_type = "urbanización"
        elif any(k in combined for k in ["rehabilitación", "reforma"]):
            permit_type = "obra mayor rehabilitación"
        
        # Build entity as applicant
        applicant = ""
        for entity in ["Canal de Isabel II", "Metro de Madrid", "EMVS", 
                       "Ayuntamiento de Madrid", "Comunidad de Madrid",
                       "MINTRA", "Planifica Madrid"]:
            if entity.lower() in summary.lower():
                applicant = entity
                break
        if not applicant:
            applicant = "Comunidad de Madrid"
        
        p = {
            "source_url": url,
            "pdf_url": "",
            "date_granted": "",
            "municipality": "Madrid",
            "address": "",
            "applicant": applicant,
            "permit_type": permit_type,
            "declared_value_eur": pem,
            "description": (title[:300] + " — " + summary[:100]).strip(),
            "extraction_mode": "cm_contratos",
            "confidence": "medium",
            "phase": phase,
            "expediente": "",
            "lead_score": 0,
            "estimated_pem": f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000 else (f"€{int(pem/1000)}K" if pem else ""),
            "ai_evaluation": (
                f"{'⚡ Licitación ACTIVA' if phase == 'licitacion' else '✅ Contrato adjudicado'} "
                f"— Portal Contratación CM. {title[:150]}. "
                f"FCC Construcción: evaluar pliego técnico y presentar oferta"
                + (" URGENTE" if phase == "licitacion" else " — ya adjudicado, contactar adjudicatario para subcontratos") + ". "
                f"Kiloutou: contactar inmediatamente al adjudicatario para maquinaria. "
                + (f"Molecor: evaluar si incluye saneamiento/abastecimiento para cotización PVC. " if any(k in combined for k in ["saneamiento","agua","colector"]) else "")
            ),
            "supplies_needed": generate_supplies_estimate(permit_type, pem, title, summary),
            "project_size": "",
            "action_window": "⚡ ACTUAR ESTA SEMANA" if phase == "licitacion" else "📞 CONTACTAR EN 30 DÍAS",
            "key_contacts": f"Entidad: {applicant}",
            "obra_timeline": "",
        }
        p["lead_score"] = score_lead(p)
        p = _enhance_profile_fit(p, combined)
        
        if write_permit(p, ""):
            return 1, 0, 0
        return 0, 1, 0
        
    except Exception as e:
        log(f"  ❌ CM Contrato [{idx}] {e}")
        return 0, 0, 1


def search_datos_madrid(date_from, date_to, global_seen):
    """
    SOURCE 7: datos.madrid.es Open Data API — Licencias Urbanísticas.

    datos.madrid.es publishes EVERY individual licencia urbanística granted by
    the Ayuntamiento de Madrid since 2015. This data is NOT in BOCM.

    ACCESS STRATEGY (WAF bypass):
    ──────────────────────────────
    datos.madrid.es blocks cloud/datacenter IPs at CDN level.
    We use a layered approach — most reliable first:

    TIER 1 (always wins): Cloudflare Worker proxy
      Set DATOS_MADRID_PROXY = https://your-worker.workers.dev in GitHub Secrets.
      The Worker runs on Cloudflare edge IPs that datos.madrid.es does not block.
      Setup: workers.cloudflare.com → new Worker → paste JS from DATOS_MADRID_PROXY
      constant → Deploy → copy URL → add as GitHub secret.

    TIER 2 (fallback, no setup needed): Direct with extended timeout
      GitHub Actions IPs get TCP connection accepted (not instantly rejected).
      The server stalls the HTTP response (tarpit behavior).
      We use 45s timeout, full browser fingerprint, session warmup.
      Works occasionally depending on CDN edge node assignment.

    TIER 3 (last resort): Alternative CKAN SQL endpoint
      Different URL path, sometimes different CDN routing.

    NO FAST-FAIL PROBE — the probe was wrong:
      Sandbox/AWS IPs → instant 403 (probe detects correctly but exits early)
      GitHub Actions IPs → TCP connects, HTTP stalls (probe times out but so does GH)
      Solution: let each keyword try independently; abort after 5 consecutive fails.
    """
    DATOS_API   = "https://datos.madrid.es/api/3/action/datastore_search"
    DATOS_SQL   = "https://datos.madrid.es/api/3/action/datastore_search_sql"
    RESOURCE_ID = "300193-10-licencias-urbanisticas"

    # ── 21 keywords — ordered by commercial value ─────────────────────────────
    # Based on DoubleTrade data coverage: cambio de uso, obra mayor, rehabilitación
    # are the three highest-yield types for Madrid capital licencias.
    DATOS_KEYWORDS = [
        # Hospe / Sharing Co — cambio de uso is the holy grail for flexliving
        ("cambio de uso",              "hospe+mep+retail"),
        ("cambio de destino",          "hospe+mep+retail"),
        ("modificación de uso",        "hospe+mep+retail"),
        ("cambio de actividad",        "hospe+retail"),
        # Primera ocupación — building done, operator needed TODAY
        ("primera ocupación",          "hospe+mep"),
        ("licencia de primera",        "hospe+mep"),
        # Obra mayor — Gran Constructora + MEP + Kiloutou
        ("obra mayor",                 "constructora+mep+alquiler+materiales"),
        ("nueva construcción",         "constructora+mep+alquiler+materiales"),
        ("nueva planta",               "constructora+mep+alquiler+materiales"),
        # Rehabilitación — MEP + hospe
        ("rehabilitación",             "hospe+mep+materiales"),
        ("reforma integral",           "hospe+mep+materiales"),
        ("gran rehabilitación",        "hospe+mep+materiales"),
        # Declaración responsable — fast-track licencias
        ("declaración responsable",    "mep+constructora"),
        # ACTIU targets — offices, hotels, coworking
        ("oficinas",                   "actiu+mep"),
        ("hotel",                      "actiu+mep+hospe"),
        ("coworking",                  "actiu+mep"),
        ("residencia de estudiantes",  "actiu+con+mep"),
        # Retail
        ("local comercial",            "retail+mep"),
        ("apertura de",                "retail"),
        # Industrial / logístico
        ("nave industrial",            "industrial+alquiler+materiales"),
        ("almacén",                    "industrial+materiales"),
    ]

    # ── Fixed PEM parser ──────────────────────────────────────────────────────
    def _parse_pem_es(raw):
        """Parse Spanish number '150.000,00' → 150000.0"""
        s = str(raw or "").strip()
        if not s or s in ("None", "nan", ""): return 0.0
        try:
            if "," in s and "." in s:  return float(s.replace(".", "").replace(",", "."))
            if "," in s:               return float(s.replace(",", "."))
            return float(s)
        except Exception:              return 0.0

    from dateutil import parser as _dp

    # ── Session builder — full browser fingerprint ────────────────────────────
    def _make_dm_session():
        s = requests.Session()
        s.verify = False
        s.headers.update({
            "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/124.0.0.0 Safari/537.36"),
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin":          "https://datos.madrid.es",
            "Referer":         "https://datos.madrid.es/dataset/300193-0-licencias-urbanisticas",
            "DNT":             "1",
            "Connection":      "keep-alive",
            "Sec-Fetch-Dest":  "empty",
            "Sec-Fetch-Mode":  "cors",
            "Sec-Fetch-Site":  "same-origin",
        })
        return s

    # ── Proxy-aware request ───────────────────────────────────────────────────
    def _dm_get(api_url, sess=None):
        """
        Returns (response_or_None, fail_reason_str).
        fail_reason: 'blocked' | 'timeout' | 'error' | None (success)
        """
        if DATOS_MADRID_PROXY:
            # TIER 1: Cloudflare Worker proxy — always bypasses WAF
            proxy_url = f"{DATOS_MADRID_PROXY}?url={quote(api_url, safe='')}"
            try:
                r = requests.get(proxy_url, timeout=25, verify=False,
                                 headers={"User-Agent": "PlanningScout/1.0",
                                          "Accept":     "application/json"})
                if r.status_code == 200: return r, None
                return None, f"proxy-{r.status_code}"
            except requests.Timeout:
                return None, "proxy-timeout"
            except Exception as e:
                return None, f"proxy-error"

        # TIER 2: Direct with full session + 45s timeout
        s = sess or _make_dm_session()
        try:
            r = s.get(api_url, timeout=45, allow_redirects=True)
            if r.status_code == 200:
                return r, None
            if r.status_code == 403:
                return None, "blocked"    # definitive CDN block for this IP
            return None, f"http-{r.status_code}"
        except requests.Timeout:
            return None, "timeout"        # GH Actions tarpit — try next keyword
        except Exception:
            return None, "error"

    # ── Shared session for all direct requests (avoids repeated TLS handshakes) ─
    _dm_sess = _make_dm_session()

    # ── Source status log ─────────────────────────────────────────────────────
    if DATOS_MADRID_PROXY:
        log(f"  🔀 datos.madrid: via proxy {DATOS_MADRID_PROXY[:50]}")
    else:
        log(f"  🌐 datos.madrid: direct (45s timeout per keyword — may work from GH Actions)")

    # ── Query loop ────────────────────────────────────────────────────────────
    results   = []
    seen_exp  = set()
    consec_fail = 0   # consecutive keyword failures
    blocked_at  = None

    for kw, _profile_hint in DATOS_KEYWORDS:
        if not time_ok(need_s=30): break
        if consec_fail >= 5:
            log(f"  ⚠️ datos.madrid: 5 consecutive failures — aborting remaining keywords")
            break
        if blocked_at:
            # Definitive 403 block detected — no point trying more keywords
            break

        api_url = (f"{DATOS_API}?resource_id={quote(RESOURCE_ID)}"
                   f"&q={quote(kw)}&limit=100&offset=0")

        r, fail_reason = _dm_get(api_url, _dm_sess)

        if r is None:
            if fail_reason == "blocked":
                blocked_at = kw
                log(f"  ❌ datos.madrid: WAF/IP block (HTTP 403) — this IP is blocked.")
                log(f"     Fix: deploy Cloudflare Worker proxy (5 min, free)")
                log(f"     Setup instructions in engine source at DATOS_MADRID_PROXY constant")
                break
            consec_fail += 1
            log(f"  ⚠️ datos.madrid [{kw}]: {fail_reason} ({consec_fail}/5)")
            time.sleep(1.5)
            continue

        consec_fail = 0   # reset on any success

        try:
            data = r.json()
        except Exception:
            continue

        if not data.get("success"):
            continue

        records = data.get("result", {}).get("records", [])
        kw_hits = 0

        for rec in records:
            exp = str(rec.get("EXPEDIENTE", "")).strip()
            if not exp or exp in seen_exp:
                continue

            # Date filter — use FECHA_OTORGAMIENTO or FECHA_SOLICITUD fallback
            fecha = (str(rec.get("FECHA_OTORGAMIENTO","") or "")
                     or str(rec.get("FECHA_SOLICITUD","")  or "")).strip()
            if fecha:
                try:
                    rec_date = _dp.parse(fecha[:10]).date()
                    if rec_date < date_from.date() or rec_date > date_to.date():
                        continue
                except Exception:
                    pass   # keep if parse fails

            # Skip failed/withdrawn results
            resultado = str(rec.get("RESULTADO","") or "").strip().lower()
            if resultado in ("inadmitida","desistida","caducada","denegada","archivada"):
                continue

            # Build combined text for filtering
            obj      = str(rec.get("OBJETO","")      or "").lower()
            desc_l   = str(rec.get("DESCRIPCION","") or "").lower()
            barrio   = str(rec.get("BARRIO","")      or "").lower()
            distrito = str(rec.get("DISTRITO","")    or "").lower()
            combined = f"{obj} {desc_l} {barrio} {distrito}"

            if any(exc in combined for exc in KEYWORDS_EXCLUDE):
                continue

            # Noise filter — very specific minor works
            _DM_NOISE = [
                "cambio de escaparate", "cambio de rótulo", "instalación de rótulo",
                "velador", "terraza desmontable", "cata de terreno", "ensayo geotécnico",
                "antena de telefonía",
            ]
            if any(n in combined for n in _DM_NOISE):
                continue

            # Skip micro works (<€30K PEM)
            pem_val = _parse_pem_es(rec.get("PEM"))
            if 0 < pem_val < 30_000:
                continue

            seen_exp.add(exp)
            exp_enc    = exp.replace("/","%2F").replace(" ","%20")
            source_url = (f"https://sede.madrid.es/portal/site/tramites/"
                          f"menuitem.62876cb64654a55e2dbd7003a8a409a0/"
                          f"?expediente={exp_enc}")
            results.append((exp, rec, source_url, _profile_hint))
            kw_hits += 1

        if kw_hits > 0:
            log(f"  🏛️ datos.madrid [{kw}]: +{kw_hits}")
        time.sleep(0.5)

    if not results and not blocked_at:
        log(f"  🏛️ datos.madrid: 0 licencias in date range "
            f"({'via proxy' if DATOS_MADRID_PROXY else 'direct — if consistently 0, set DATOS_MADRID_PROXY'})")
    elif results:
        log(f"  🏛️ datos.madrid: {len(results)} licencias found total")
    return results


def process_datos_madrid_item(exp, rec, source_url, profile_hint, idx, total):
    """
    Convert a datos.madrid.es licence record into a PlanningScout permit dict
    and write it to the sheet.

    This source gives the CAMBIO DE USO, OBRA MAYOR, and REHABILITACIÓN
    leads that BOCM doesn't provide for Madrid capital.

    Returns (saved, skipped, errors).
    """
    try:
        with _sheet_lock:
            if source_url in _seen_urls:
                return 0, 1, 0

        obj    = str(rec.get("OBJETO", "") or "").strip()
        desc   = str(rec.get("DESCRIPCION", "") or "").strip()
        addr   = str(rec.get("DIRECCION", "") or "").strip()
        barrio = str(rec.get("BARRIO", "") or "").strip()
        dist   = str(rec.get("DISTRITO", "") or "").strip()
        fecha  = str(rec.get("FECHA_OTORGAMIENTO", "") or "").strip()[:10]
        clase  = str(rec.get("CLASE_LICENCIA", "") or "").strip()
        result = str(rec.get("RESULTADO", "") or "").strip()

        # PEM extraction
        pem_val = 0.0
        pem_raw = str(rec.get("PEM", "") or "")
        try:
            if "," in pem_raw and "." in pem_raw:
                pem_val = float(pem_raw.replace(".", "").replace(",", "."))
            elif "," in pem_raw:
                pem_val = float(pem_raw.replace(",", "."))
            elif pem_raw.strip():
                pem_val = float(pem_raw.strip())
        except Exception:
            pem_val = 0.0

        # Build full text for classification
        combined = f"{obj} {desc} {clase}".lower()

        # Classify
        is_lead, reason, tier = classify_permit(combined)
        if not is_lead:
            # datos.madrid gives us confirmed licences — be more permissive
            # Only skip if it's truly noise
            if any(x in combined for x in ["valla", "terraza", "velador", "señal", "rótulo",
                                             "piscina individual", "jardin", "tala"]):
                return 0, 1, 0
            # If it mentions obra mayor, cambio de uso, rehabilitación → keep
            if not any(x in combined for x in ["obra", "cambio", "rehabilit", "reforma",
                                                "construcción", "vivienda", "ocupación"]):
                return 0, 1, 0

        # Permit type mapping
        obj_lower = obj.lower()
        if "cambio de uso" in obj_lower or "cambio de destino" in obj_lower:
            permit_type = "cambio de uso"
            phase = "definitivo" if "otorgada" in result.lower() else "en_tramite"
        elif "nueva construcción" in obj_lower or "nueva planta" in obj_lower:
            permit_type = "obra mayor nueva construcción"
            phase = "definitivo" if "otorgada" in result.lower() else "en_tramite"
        elif "rehabilitación" in obj_lower or "reforma integral" in obj_lower:
            permit_type = "obra mayor rehabilitación"
            phase = "definitivo" if "otorgada" in result.lower() else "en_tramite"
        elif "primera ocupación" in obj_lower:
            permit_type = "licencia primera ocupación"
            phase = "primera_ocupacion"
        elif "obra mayor" in obj_lower:
            permit_type = "obra mayor nueva construcción"
            phase = "definitivo" if "otorgada" in result.lower() else "en_tramite"
        elif "declaración responsable" in clase.lower():
            permit_type = "declaración responsable obra mayor"
            phase = "definitivo"
        else:
            permit_type = "obra mayor nueva construcción"
            phase = "definitivo" if "otorgada" in result.lower() else "en_tramite"

        # Action window
        if phase == "primera_ocupacion":
            action_window = "⚡ ACTUAR ESTA SEMANA"
        elif phase == "definitivo" and "cambio" in permit_type:
            action_window = "📞 CONTACTAR EN 30 DÍAS"
        elif phase == "definitivo":
            action_window = "📞 CONTACTAR EN 30 DÍAS"
        else:
            action_window = "📅 MONITORIZAR (3-6 meses)"

        # Build description
        location = f"{addr}, {barrio}, {dist}, Madrid" if barrio else f"{addr}, {dist}, Madrid"
        full_desc = f"{obj} — {desc[:150]}" if desc and desc.lower() != obj.lower() else obj
        full_desc = f"{full_desc}. {location}"
        if pem_val > 0:
            pem_s = f"€{pem_val/1e6:.1f}M" if pem_val >= 1e6 else f"€{pem_val/1e3:.0f}K"
            full_desc += f" PEM: {pem_s}"

        # AI evaluation (rule-based since no PDF available)
        ai_eval = (
            f"{'✅ Licencia Otorgada' if 'otorgada' in result.lower() else '🔄 En tramitación'} — "
            f"{obj}, {location}. "
        )
        if "cambio de uso" in permit_type:
            ai_eval += (
                f"Cambio de uso en Madrid capital — exactamente lo que busca Sharing Co / Room00. "
                f"Contactar al promotor AHORA antes de que busque operador en el mercado abierto. "
                f"{'PEM: ' + pem_s if pem_val > 0 else 'PEM no declarado'}. "
                f"Barrio: {barrio or dist}."
            )
        elif "primera_ocupacion" in phase:
            ai_eval += (
                f"Primera ocupación concedida — edificio TERMINADO. "
                f"Sharing Co / Room00: contactar al promotor HOY para gestión de activo. "
                f"Instaladores MEP: legalización instalaciones + OCA + revisiones finales."
            )
        elif "rehabilitación" in permit_type:
            ai_eval += (
                f"Rehabilitación integral en Madrid capital. "
                f"Instaladores MEP: renovación completa de instalaciones. "
                f"Sharing Co: edificio en rehabilitación = futuro activo operacional. "
                f"{'PEM: ' + pem_s if pem_val > 0 else 'Sin PEM declarado'}."
            )
        elif "nueva construcción" in permit_type:
            ai_eval += (
                f"Licencia de obra mayor nueva construcción en {dist}, Madrid capital. "
                f"Gran Constructora: obra en ejecución o próxima. "
                f"Instaladores MEP: instalar HVAC, electricidad, fontanería. "
                f"{'PEM: ' + pem_s if pem_val > 0 else 'Sin PEM declarado'}."
            )
        else:
            ai_eval += f"Licencia urbanística en {dist}. {'PEM: ' + pem_s if pem_val > 0 else ''}."

        supplies = generate_supplies_estimate(permit_type, pem_val if pem_val > 0 else None, full_desc)

        p = {
            "source_url":         source_url,
            "date_granted":       fecha,
            "municipality":       "Madrid",
            "address":            addr,
            "applicant":          "",
            "permit_type":        permit_type,
            "declared_value_eur": pem_val if pem_val > 0 else None,
            "description":        full_desc[:350],
            "extraction_mode":    "datos_madrid",
            "confidence":         "high" if "otorgada" in result.lower() else "medium",
            "phase":              phase,
            "expediente":         exp,
            "lead_score":         0,
            "estimated_pem":      (f"€{pem_val/1e6:.1f}M" if pem_val >= 1e6
                                   else (f"€{pem_val/1e3:.0f}K" if pem_val > 0 else "")),
            "ai_evaluation":      ai_eval[:600],
            "supplies_needed":    supplies,
            "project_size":       "",
            "action_window":      action_window,
            "key_contacts":       "",
            "obra_timeline":      "",
        }
        p["lead_score"] = score_lead(p)
        p = _enhance_profile_fit(p, combined)

        if write_permit(p, ""):
            return 1, 0, 0
        return 0, 1, 0

    except Exception as e:
        log(f"  ❌ datos.madrid [{idx}]: {e}")
        return 0, 0, 1


def process_boe_item(boe_id, title, department, idx, total):
    """
    Process a single BOE item (to be used in ThreadPoolExecutor).
    
    Returns (saved, skipped, error) counts.
    """
    try:
        # Extract XML text
        text, xml_url, metadata = extract_boe_xml_text(boe_id)
        
        if not text or len(text) < 100:
            return 0, 1, 0  # skip - no content
        
        # Secondary department filter using metadata
        meta_dept = metadata.get("departamento", "") or metadata.get("emisor", "")
        dept_ok = False
        if meta_dept:
            for target in BOE_DEPARTMENTS:
                if target.lower() in meta_dept.lower():
                    dept_ok = True
                    break
        else:
            dept_ok = True  # No metadata, proceed
        
        if not dept_ok:
            return 0, 1, 0  # skip - wrong department
        
        # Classify
        is_lead, reason, tier = classify_permit(text)
        if not is_lead:
            return 0, 1, 0  # skip
        
        # Build URL for the document
        html_url = f"{BOE_BASE}/diario_boe/txt.php?id={boe_id}"
        # PDF URL requires the publication date — use metadata or leave blank;
        # html_url (txt.php) always works and is linked from the XML.
        pdf_url  = f"{BOE_BASE}/diario_boe/xml.php?id={boe_id}"  # XML is reliable
        
        # Extract data
        pub_date = metadata.get("fecha_pub", "") or extract_date_from_url(boe_id)
        
        # Use AI extraction (or keyword fallback)
        p = extract(text, html_url, pub_date)
        
        if p is None:
            return 0, 1, 0  # skip
        
        # Override/enhance with BOE-specific data
        if not p.get("applicant") and department:
            p["applicant"] = department
        
        if not p.get("municipality") and "Madrid" in text:
            # Try to extract municipality from text
            p["municipality"] = extract_municipality(text)
        
        # Add BOE-specific note to description
        if p.get("description"):
            p["description"] = f"[BOE] {p['description']}"
        else:
            p["description"] = f"[BOE] {title[:200]}"
        
        # Check minimum value
        dec = p.get("declared_value_eur")
        if MIN_VALUE_EUR and dec and isinstance(dec, (int, float)) and dec < MIN_VALUE_EUR:
            return 0, 1, 0  # below minimum
        
        # Estimate PEM if missing
        if not p.get("estimated_pem"):
            if dec and isinstance(dec, (int, float)) and dec > 0:
                p["estimated_pem"] = f"✅ PEM confirmado (BOE): €{dec:,.0f}"
            else:
                est_result = _estimate_pem_from_pdf(text)
                if est_result.get("estimated_pem"):
                    ep     = est_result["estimated_pem"]
                    ep_lo  = est_result.get("estimated_pem_low")
                    ep_hi  = est_result.get("estimated_pem_high")
                    def _fpb(v):
                        if v >= 1_000_000: return f"€{v/1_000_000:.1f}M"
                        if v >= 1_000:     return f"€{int(v/1000)}K"
                        return f"€{int(v):,}"
                    rng_b = (f"{_fpb(ep_lo)} – {_fpb(ep_hi)}" if ep_lo and ep_hi
                             else f"€{ep:,.0f}")
                    p["estimated_pem"] = rng_b + " 🟡"
                    if not dec:
                        p["declared_value_eur"] = ep
                        p["lead_score"] = score_lead(p)
                elif USE_AI:
                    p["estimated_pem"] = _ai_estimate_pem(
                        text,
                        permit_type=p.get("permit_type", ""),
                        municipality=p.get("municipality", "Madrid"),
                        description=p.get("description", ""),
                    )
                    _ai_num = _parse_pem_from_estimated_string(p["estimated_pem"])
                    if _ai_num and not dec:
                        p["declared_value_eur"] = _ai_num
                        p["lead_score"] = score_lead(p)
                else:
                    p["estimated_pem"] = "⚪ Sin PEM en BOE"

        # ── Extract project size for BOE leads ───────────────────────────────
        if not p.get("project_size"):
            p["project_size"] = _extract_project_size(text)
            if not p["project_size"] and USE_AI:
                p["project_size"] = _ai_extract_project_size(
                    text,
                    permit_type=p.get("permit_type", ""),
                    description=p.get("description", ""),
                )

        # Ensure AI evaluation exists
        if not p.get("ai_evaluation") or len(str(p.get("ai_evaluation", "")).strip()) < 20:
            pt = (p.get("permit_type") or "").lower()
            pem = p.get("declared_value_eur")
            pem_s = (f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000
                    else (f"€{int(pem/1000):.0f}K" if pem else "PEM no declarado"))
            
            p["ai_evaluation"] = (
                f"[BOE - Licitación estatal] {department or 'Administración Pública'} — {pem_s}. "
                f"Licitación de alto valor publicada en BOE (ámbito nacional/autonómico). "
                f"Grandes constructoras: revisar pliego técnico urgente. "
                f"Contratos estatales típicamente requieren pre-calificación y garantías significativas."
            )
        
        # Ensure supplies needed exists
        if not p.get("supplies_needed") or len(str(p.get("supplies_needed", "")).strip()) < 10:
            p["supplies_needed"] = generate_supplies_estimate(
                p.get("permit_type", ""), p.get("declared_value_eur"), p.get("description", ""),
                full_text=text or "")
        
        # Write to sheet
        if write_permit(p, pdf_url):
            return 1, 0, 0  # saved
        return 0, 1, 0  # dup
        
    except Exception as e:
        log(f"  ❌ BOE [{idx}] {boe_id}: {e}")
        return 0, 0, 1  # error


# ════════════════════════════════════════════════════════════
if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
        try:
            from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
        except: pass

run()
