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
    help="1=daily(last 2 working days), 2-8=weekly window, 9+=full backfill. "
         "DAILY mode (--weeks 1): fast 20min scan of the last 2 working days. "
         "WEEKLY (--weeks 2-8): full window scan. "
         "Example: --weeks 4 = scan last 4 weeks completely.")
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
#      export default {
#      async fetch(req) {
#        const url = new URL(req.url);
#        const t = url.searchParams.get("url");
#        if (!t?.startsWith("https://datos.madrid.es/")) return new Response("Unauthorized",{status:403});
#        const accept = req.headers.get("Accept") || "*/*";
#        const r = await fetch(t,{
#          headers:{"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#                   "Accept":accept,"Accept-Language":"es-ES,es;q=0.9","Referer":"https://datos.madrid.es/"},
#          redirect:"follow"}});
#        const ct = r.headers.get("content-type") || "application/octet-stream";
#        const body = await r.arrayBuffer();
#        return new Response(body,{status:r.status,headers:{"Content-Type":ct,"Access-Control-Allow-Origin":"*","Content-Length":String(body.byteLength)}});
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
    # ── New keywords for warm leads (coliving/actiu/kiloutou/molecor/kinépolis) ──
    ("coliving",                              SECTION_III,  4, "HOSPE+PRO+RET"),
    ("residencia de estudiantes",             SECTION_III,  4, "HOSPE+PRO+ACTIU"),
    ("residencia de jóvenes",                 SECTION_III,  3, "HOSPE+PRO"),
    ("uso hospedaje",                         SECTION_III,  4, "HOSPE"),
    ("proyecto básico",                       SECTION_III,  4, "ACTIU+CON+PRO"),
    ("certificado final de obra",             SECTION_III,  4, "ACTIU+MEP+HOSPE"),
    ("dirección facultativa",                 SECTION_III,  3, "ACTIU+CON"),
    ("acta de inicio de obras",               SECTION_III,  4, "ALQUILER+CON"),
    ("orden de inicio de obra",               SECTION_III,  3, "ALQUILER+CON"),
    ("inicio de ejecución",                   SECTION_III,  3, "ALQUILER+CON"),
    ("red de distribución de agua",           SECTION_III,  4, "MAT+INFRA"),
    ("instalación de tubería",                SECTION_III,  4, "MAT+INFRA+CON"),
    ("colector de pluviales",                 SECTION_III,  4, "MAT+INFRA+CON"),
    ("complejo de ocio",                      SECTION_III,  4, "RET+IND"),
    ("equipamiento cultural",                 SECTION_III,  3, "RET+CON"),
    ("plan especial de equipamiento comercial", SECTION_III,3, "RET+CON+PRO"),
    ("propuesta de adjudicación",             SECTION_III,  4, "CON+INFRA+MAT"),
    ("apertura de plicas",                    SECTION_III,  3, "CON+INFRA"),
    ("mesa de contratación",                  SECTION_III,  3, "CON+INFRA"),
    ("autorización de apertura",              SECTION_III,  4, "RET+HOSPE"),
    ("expediente de actividad",               SECTION_III,  4, "RET+HOSPE+MEP"),
    ("proyecto de compensación",              SECTION_III,  4, "PRO+CON"),
    ("modificación puntual del pgou",         SECTION_III,  3, "PRO+CON"),

    ("declaración de impacto ambiental",   SECTION_III,  3, "INFRA+CON"),
    ("nave logística",                     SECTION_III,  5, "IND+MAT"),     # logistics warehouse
    # ── NEW SECTOR-SPECIFIC KEYWORDS ─────────────────────────────────────────
    # MANGO / VIMAD / Expansión Retail — fashion retail location intelligence
    ("centro comercial",                   SECTION_III,  5, "RET+CON"),     # shopping centre permit
    ("galería comercial",                  SECTION_III,  4, "RET+CON"),     # commercial gallery
    ("local comercial",                    SECTION_III,  4, "RET"),         # retail unit
    ("apertura de establecimiento",        SECTION_III,  4, "RET+HOSPE"),   # shop opening
    # KILOUTOU — additional machinery rental triggers
    ("plataforma de trabajo",              SECTION_III,  3, "ALQUILER"),    # work platform = rental equipment
    ("grúa torre",                         SECTION_III,  4, "ALQUILER+CON"), # tower crane = active site
    ("andamiaje",                          SECTION_III,  3, "ALQUILER+CON"), # scaffolding = active site
    # UVESCO / Promotores RE — land development signals
    ("segregación",                        SECTION_III,  4, "PRO"),         # land segregation
    ("agrupación de parcelas",             SECTION_III,  4, "PRO+CON"),     # plot aggregation
    ("normalización de fincas",            SECTION_III,  4, "PRO+CON"),     # plot regularisation
    ("plan de sectorización",              SECTION_II,   5, "PRO+CON"),     # sectorisation = new land
    # ACTIU — workplace fit-out signals
    ("acondicionamiento de oficinas",      SECTION_III,  4, "ACTIU"),       # office fit-out
    ("instalación de mezzanine",           SECTION_III,  3, "ACTIU+IND"),   # mezzanine = warehouse/office
    ("reforma de planta",                  SECTION_III,  4, "ACTIU+MEP"),   # floor refurb
    # MOLECOR — canal and network keywords
    ("conducción de agua",                 SECTION_III,  4, "MAT+INFRA"),   # water pipeline
    ("reparación de colector",             SECTION_III,  4, "MAT+INFRA"),   # collector repair
    ("renovación de red de agua",          SECTION_III,  5, "MAT+INFRA"),   # water network renewal
    # GRAN INFRAESTRUCTURA / FCC — public works signals from Section II
    ("proyecto de trazado",                SECTION_II,   5, "INFRA+CON"),   # road alignment = major civil
    ("desdoblamiento de calzada",          SECTION_II,   4, "INFRA+CON"),   # road widening
    ("variante de carretera",              SECTION_II,   5, "INFRA+CON"),   # road bypass
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
    # ── Primary logistics belt (SE arc: Valdemoro → Torrejón → Alcalá) ────────
    "Valdemoro", "Getafe", "Pinto", "Parla", "Ciempozuelos",
    "Torrejón de Ardoz", "Coslada", "San Fernando de Henares",
    "Mejorada del Campo", "Rivas-Vaciamadrid", "Arganda del Rey",
    "Velilla de San Antonio", "Loeches", "Torres de la Alameda",
    # ── Northern tech corridor (A-1/A-10) ────────────────────────────────────
    "Alcalá de Henares", "Alcobendas", "Tres Cantos",
    "San Sebastián de los Reyes", "Guadalix de la Sierra",
    "Paracuellos de Jarama", "Daganzo de Arriba", "Meco", "Algete",
    # ── Western residential/logistics (A-6/M-40) ─────────────────────────────
    "Majadahonda", "Las Rozas de Madrid", "Pozuelo de Alarcón",
    "Boadilla del Monte", "Villanueva de la Cañada", "Galapagar",
    # ── Southern manufacturing (A-4/A-42) ────────────────────────────────────
    "Móstoles", "Leganés", "Fuenlabrada", "Alcorcón", "Navalcarnero",
    "Humanes de Madrid", "Griñón", "Casarrubuelos", "Torrejón de Velasco",
]

# ── Complete lookup: all 179 municipios of the Comunidad de Madrid ────────────
# Used by extract_municipality() for fast, accurate name normalisation.
_MADRID_MUNIS_179 = {
    # A
    "acebeda":                 "La Acebeda",
    "la acebeda":              "La Acebeda",
    "ajalvir":                 "Ajalvir",
    "alamo":                   "El Álamo",
    "el alamo":                "El Álamo",
    "alcala de henares":       "Alcalá de Henares",
    "alcalá de henares":       "Alcalá de Henares",
    "alcobendas":              "Alcobendas",
    "alcorcon":                "Alcorcón",
    "alcorcón":                "Alcorcón",
    "aldea del fresno":        "Aldea del Fresno",
    "algete":                  "Algete",
    "alpedrete":               "Alpedrete",
    "ambite":                  "Ambite",
    "anchuelo":                "Anchuelo",
    "aranjuez":                "Aranjuez",
    "arganda del rey":         "Arganda del Rey",
    "arroyomolinos":           "Arroyomolinos",
    # B
    "batres":                  "Batres",
    "becerril de la sierra":   "Becerril de la Sierra",
    "belmonte de tajo":        "Belmonte de Tajo",
    "berrueco":                "El Berrueco",
    "el berrueco":             "El Berrueco",
    "berzosa del lozoya":      "Berzosa del Lozoya",
    "boadilla del monte":      "Boadilla del Monte",
    "braojos":                 "Braojos",
    "brea de tajo":            "Brea de Tajo",
    "brunete":                 "Brunete",
    "buitrago del lozoya":     "Buitrago del Lozoya",
    "bustarviejo":             "Bustarviejo",
    # C
    "cabanillas de la sierra": "Cabanillas de la Sierra",
    "cadalso de los vidrios":  "Cadalso de los Vidrios",
    "camarma de esteruelas":   "Camarma de Esteruelas",
    "campo real":              "Campo Real",
    "canencia":                "Canencia",
    "carabana":                "Carabaña",
    "carabaña":                "Carabaña",
    "casarrubuelos":           "Casarrubuelos",
    "cercedilla":              "Cercedilla",
    "cervera de buitrago":     "Cervera de Buitrago",
    "ciempozuelos":            "Ciempozuelos",
    "cobena":                  "Cobeña",
    "cobeña":                  "Cobeña",
    "collado mediano":         "Collado Mediano",
    "collado villalba":        "Collado Villalba",
    "colmenar de oreja":       "Colmenar de Oreja",
    "colmenar del arroyo":     "Colmenar del Arroyo",
    "colmenar viejo":          "Colmenar Viejo",
    "colmenarejo":             "Colmenarejo",
    "corpa":                   "Corpa",
    "coslada":                 "Coslada",
    "cubas de la sagra":       "Cubas de la Sagra",
    # D
    "daganzo de arriba":       "Daganzo de Arriba",
    # E
    "el atazar":               "El Atazar",
    "el escorial":             "El Escorial",
    "el molar":                "El Molar",
    "el olmeda de las fuentes":"El Olmeda de las Fuentes",
    "el vellon":               "El Vellón",
    "el vellón":               "El Vellón",
    # F
    "fresnedillas de la oliva":"Fresnedillas de la Oliva",
    "fresno de torote":        "Fresno de Torote",
    "fuenlabrada":             "Fuenlabrada",
    "fuente el saz de jarama": "Fuente el Saz de Jarama",
    # G
    "galapagar":               "Galapagar",
    "garganta de los montes":  "Garganta de los Montes",
    "gargantilla del lozoya":  "Gargantilla del Lozoya",
    "gascones":                "Gascones",
    "getafe":                  "Getafe",
    "griñon":                  "Griñón",
    "griñón":                  "Griñón",
    "guadalix de la sierra":   "Guadalix de la Sierra",
    "guadarrama":              "Guadarrama",
    # H
    "hoyo de manzanares":      "Hoyo de Manzanares",
    "humanes de madrid":       "Humanes de Madrid",
    # L
    "la cabrera":              "La Cabrera",
    "la hiruela":              "La Hiruela",
    "la puebla de la sierra":  "La Puebla de la Sierra",
    "la serna del monte":      "La Serna del Monte",
    "las rozas de madrid":     "Las Rozas de Madrid",
    "las rozas":               "Las Rozas de Madrid",
    "leganes":                 "Leganés",
    "leganés":                 "Leganés",
    "loeches":                 "Loeches",
    "lozoya":                  "Lozoya",
    "lozoyuela-navas-sieteiglesias": "Lozoyuela-Navas-Sieteiglesias",
    # M
    "madarcos":                "Madarcos",
    "madrid":                  "Madrid",
    "majadahonda":             "Majadahonda",
    "manzanares el real":      "Manzanares el Real",
    "meco":                    "Meco",
    "mejorada del campo":      "Mejorada del Campo",
    "miraflores de la sierra": "Miraflores de la Sierra",
    "moraleja de enmedio":     "Moraleja de Enmedio",
    "moralzarzal":             "Moralzarzal",
    "morata de tajuna":        "Morata de Tajuña",
    "morata de tajuña":        "Morata de Tajuña",
    "mostoles":                "Móstoles",
    "móstoles":                "Móstoles",
    # N
    "navacerrada":             "Navacerrada",
    "navalafuente":            "Navalafuente",
    "navalagamella":           "Navalagamella",
    "navalcarnero":            "Navalcarnero",
    "navarredonda y san mames":"Navarredonda y San Mamés",
    "navarredonda y san mamés":"Navarredonda y San Mamés",
    "navas del rey":           "Navas del Rey",
    "nuevo baztan":            "Nuevo Baztán",
    "nuevo baztán":            "Nuevo Baztán",
    # O
    "olmeda de las fuentes":   "Olmeda de las Fuentes",
    "orusco de tajuna":        "Orusco de Tajuña",
    "orusco de tajuña":        "Orusco de Tajuña",
    # P
    "paracuellos de jarama":   "Paracuellos de Jarama",
    "parla":                   "Parla",
    "patones":                 "Patones",
    "pedrezuela":              "Pedrezuela",
    "pelayos de la presa":     "Pelayos de la Presa",
    "perales de tajuna":       "Perales de Tajuña",
    "perales de tajuña":       "Perales de Tajuña",
    "pezuela de las torres":   "Pezuela de las Torres",
    "pinilla del valle":       "Pinilla del Valle",
    "pinto":                   "Pinto",
    "pinuecar-gandullas":      "Piñuécar-Gandullas",
    "piñuécar-gandullas":      "Piñuécar-Gandullas",
    "pozuelo de alarcon":      "Pozuelo de Alarcón",
    "pozuelo de alarcón":      "Pozuelo de Alarcón",
    "pozuelo del rey":         "Pozuelo del Rey",
    "pradena del rincon":      "Prádena del Rincón",
    "prádena del rincón":      "Prádena del Rincón",
    # Q
    "quijorna":                "Quijorna",
    # R
    "rascafria":               "Rascafría",
    "rascafría":               "Rascafría",
    "redueña":                 "Redueña",
    "ribatejada":              "Ribatejada",
    "rivas-vaciamadrid":       "Rivas-Vaciamadrid",
    "rivas vaciamadrid":       "Rivas-Vaciamadrid",
    "robledillo de la jara":   "Robledillo de la Jara",
    "robledo de chavela":      "Robledo de Chavela",
    "robregordo":              "Robregordo",
    "rozas de madrid":         "Las Rozas de Madrid",
    # S
    "san agustin del guadalix":"San Agustín del Guadalix",
    "san agustín del guadalix":"San Agustín del Guadalix",
    "san fernando de henares": "San Fernando de Henares",
    "san lorenzo de el escorial": "San Lorenzo de El Escorial",
    "san martin de la vega":   "San Martín de la Vega",
    "san martín de la vega":   "San Martín de la Vega",
    "san martin de valdeiglesias": "San Martín de Valdeiglesias",
    "san martín de valdeiglesias": "San Martín de Valdeiglesias",
    "san sebastian de los reyes": "San Sebastián de los Reyes",
    "san sebastián de los reyes": "San Sebastián de los Reyes",
    "santa maria de la alameda": "Santa María de la Alameda",
    "santa maría de la alameda": "Santa María de la Alameda",
    "santorcaz":               "Santorcaz",
    "santos de la humosa":     "Santos de la Humosa",
    "serranillos del valle":   "Serranillos del Valle",
    "sevilla la nueva":        "Sevilla la Nueva",
    "somosierra":              "Somosierra",
    "soto del real":           "Soto del Real",
    # T
    "talamanca del jarama":    "Talamanca del Jarama",
    "tielmes":                 "Tielmes",
    "titulcia":                "Titulcia",
    "torrejon de ardoz":       "Torrejón de Ardoz",
    "torrejón de ardoz":       "Torrejón de Ardoz",
    "torrejon de la calzada":  "Torrejón de la Calzada",
    "torrejón de la calzada":  "Torrejón de la Calzada",
    "torrejon de velasco":     "Torrejón de Velasco",
    "torrejón de velasco":     "Torrejón de Velasco",
    "torrelaguna":             "Torrelaguna",
    "torrelodones":            "Torrelodones",
    "torres de la alameda":    "Torres de la Alameda",
    "tres cantos":             "Tres Cantos",
    # V
    "valdaracete":             "Valdaracete",
    "valdeavero":              "Valdeavero",
    "valdemanco":              "Valdemanco",
    "valdemaqueda":            "Valdemaqueda",
    "valdemorillo":            "Valdemorillo",
    "valdemoro":               "Valdemoro",
    "valdeolmos-alalpardo":    "Valdeolmos-Alalpardo",
    "valdepielagos":           "Valdepielagos",
    "valdetorres de jarama":   "Valdetorres de Jarama",
    "valdilecha":              "Valdilecha",
    "valverde de alcala":      "Valverde de Alcalá",
    "valverde de alcalá":      "Valverde de Alcalá",
    "velilla de san antonio":  "Velilla de San Antonio",
    "venturada":               "Venturada",
    "villa del prado":         "Villa del Prado",
    "villaconejos":            "Villaconejos",
    "villalbilla":             "Villalbilla",
    "villamanrique de tajo":   "Villamanrique de Tajo",
    "villamanta":              "Villamanta",
    "villamantilla":           "Villamantilla",
    "villanueva de la canada": "Villanueva de la Cañada",
    "villanueva de la cañada": "Villanueva de la Cañada",
    "villanueva de perales":   "Villanueva de Perales",
    "villanueva del pardillo": "Villanueva del Pardillo",
    "villarejo de salvanes":   "Villarejo de Salvanés",
    "villarejo de salvanés":   "Villarejo de Salvanés",
    "villaviciosa de odon":    "Villaviciosa de Odón",
    "villaviciosa de odón":    "Villaviciosa de Odón",
    # Z
    "zarzalejo":               "Zarzalejo",
}

# ── GPS coordinates for all 179 municipios ────────────────────────────────────
# Used for map display in dashboard
_MUNI_COORDS_179 = {
    "La Acebeda": (41.1703, -3.6397), "Ajalvir": (40.5415, -3.4632),
    "El Álamo": (40.2200, -3.9700), "Alcalá de Henares": (40.4818, -3.3642),
    "Alcobendas": (40.5472, -3.6415), "Alcorcón": (40.3456, -3.8264),
    "Aldea del Fresno": (40.2756, -4.2344), "Algete": (40.5957, -3.4949),
    "Alpedrete": (40.6588, -3.9934), "Ambite": (40.3200, -3.2300),
    "Anchuelo": (40.4200, -3.3700), "Aranjuez": (40.0327, -3.6039),
    "Arganda del Rey": (40.3053, -3.4392), "Arroyomolinos": (40.3769, -3.9989),
    "Batres": (40.2100, -3.8900), "Becerril de la Sierra": (40.7188, -3.8906),
    "Belmonte de Tajo": (40.1600, -3.3300), "El Berrueco": (40.8700, -3.5800),
    "Berzosa del Lozoya": (41.0200, -3.7600), "Boadilla del Monte": (40.4050, -3.9200),
    "Braojos": (41.0400, -3.7100), "Brea de Tajo": (40.1900, -3.2400),
    "Brunete": (40.4014, -3.9976), "Buitrago del Lozoya": (40.9988, -3.6352),
    "Bustarviejo": (40.8000, -3.7300), "Cabanillas de la Sierra": (40.7600, -3.6800),
    "Cadalso de los Vidrios": (40.3200, -4.3700), "Camarma de Esteruelas": (40.5300, -3.4000),
    "Campo Real": (40.3400, -3.3700), "Canencia": (40.8400, -3.7400),
    "Carabaña": (40.2600, -3.2500), "Casarrubuelos": (40.2020, -3.8890),
    "Cercedilla": (40.7411, -4.0570), "Cervera de Buitrago": (40.9800, -3.5600),
    "Ciempozuelos": (40.1600, -3.6215), "Cobeña": (40.5636, -3.4841),
    "Collado Mediano": (40.6972, -3.8844), "Collado Villalba": (40.6343, -4.0042),
    "Colmenar de Oreja": (40.1050, -3.3817), "Colmenar del Arroyo": (40.4700, -4.0700),
    "Colmenar Viejo": (40.6594, -3.7700), "Colmenarejo": (40.5600, -4.0300),
    "Corpa": (40.4400, -3.3100), "Coslada": (40.4250, -3.5617),
    "Cubas de la Sagra": (40.2158, -3.8384), "Daganzo de Arriba": (40.5464, -3.4341),
    "El Atazar": (40.9500, -3.6000), "El Boalo": (40.7019, -3.9027),
    "El Escorial": (40.5817, -4.1262), "El Molar": (40.7158, -3.5879),
    "El Olmeda de las Fuentes": (40.4700, -3.2600), "El Vellón": (40.7300, -3.5400),
    "Fresnedillas de la Oliva": (40.4700, -4.0800), "Fresno de Torote": (40.5700, -3.4200),
    "Fuenlabrada": (40.2850, -3.7945), "Fuente el Saz de Jarama": (40.6235, -3.4856),
    "Galapagar": (40.5866, -4.0023), "Garganta de los Montes": (40.9000, -3.7600),
    "Gargantilla del Lozoya": (40.9600, -3.7400), "Gascones": (41.0200, -3.6500),
    "Getafe": (40.3055, -3.7326), "Griñón": (40.2125, -3.8684),
    "Guadalix de la Sierra": (40.7636, -3.6364), "Guadarrama": (40.6727, -4.0829),
    "Hoyo de Manzanares": (40.6300, -3.9100), "Humanes de Madrid": (40.2593, -3.8270),
    "La Cabrera": (40.8574, -3.6133), "La Hiruela": (41.0700, -3.5600),
    "La Puebla de la Sierra": (41.0400, -3.5800), "La Serna del Monte": (40.9800, -3.5800),
    "Las Rozas de Madrid": (40.4944, -3.8711), "Leganés": (40.3282, -3.7641),
    "Loeches": (40.3700, -3.4100), "Lozoya": (40.9600, -3.7700),
    "Lozoyuela-Navas-Sieteiglesias": (40.8800, -3.7000), "Madarcos": (41.0400, -3.6300),
    "Madrid": (40.4168, -3.7038), "Majadahonda": (40.4744, -3.8721),
    "Manzanares el Real": (40.7249, -3.8600), "Meco": (40.5530, -3.3350),
    "Mejorada del Campo": (40.3961, -3.4920), "Miraflores de la Sierra": (40.8155, -3.7726),
    "Moraleja de Enmedio": (40.2200, -3.8700), "Moralzarzal": (40.7113, -3.8817),
    "Morata de Tajuña": (40.2500, -3.4500), "Móstoles": (40.3228, -3.8632),
    "Navacerrada": (40.7811, -4.0110), "Navalafuente": (40.7700, -3.6300),
    "Navalagamella": (40.4700, -4.1400), "Navalcarnero": (40.2856, -3.9965),
    "Navarredonda y San Mamés": (40.6900, -3.9700), "Navas del Rey": (40.2900, -4.2600),
    "Nuevo Baztán": (40.3200, -3.2800), "Olmeda de las Fuentes": (40.4700, -3.2600),
    "Orusco de Tajuña": (40.2700, -3.2600), "Paracuellos de Jarama": (40.5065, -3.5271),
    "Parla": (40.2381, -3.7653), "Patones": (40.8800, -3.6000),
    "Pedrezuela": (40.7100, -3.6100), "Pelayos de la Presa": (40.2900, -4.2500),
    "Perales de Tajuña": (40.2600, -3.3500), "Pezuela de las Torres": (40.4100, -3.2800),
    "Pinilla del Valle": (40.9800, -3.8000), "Pinto": (40.2472, -3.6964),
    "Piñuécar-Gandullas": (41.0400, -3.6800), "Pozuelo de Alarcón": (40.4350, -3.8146),
    "Pozuelo del Rey": (40.3900, -3.3100), "Prádena del Rincón": (41.0200, -3.6100),
    "Quijorna": (40.4168, -3.9900), "Rascafría": (40.8900, -3.8700),
    "Redueña": (40.7900, -3.5600), "Ribatejada": (40.5900, -3.3700),
    "Rivas-Vaciamadrid": (40.3526, -3.5278), "Robledillo de la Jara": (40.9200, -3.5600),
    "Robledo de Chavela": (40.5068, -4.2424), "Robregordo": (41.0600, -3.6200),
    "San Agustín del Guadalix": (40.7107, -3.6171), "San Fernando de Henares": (40.4200, -3.5300),
    "San Lorenzo de El Escorial": (40.5906, -4.1427), "San Martín de la Vega": (40.2078, -3.5680),
    "San Martín de Valdeiglesias": (40.3600, -4.3800), "San Sebastián de los Reyes": (40.5508, -3.6265),
    "Santa María de la Alameda": (40.5700, -4.3600), "Santorcaz": (40.4400, -3.3100),
    "Santos de la Humosa": (40.4200, -3.3700), "Serranillos del Valle": (40.2300, -3.9600),
    "Sevilla la Nueva": (40.3556, -3.9711), "Somosierra": (41.1278, -3.5831),
    "Soto del Real": (40.7666, -3.7813), "Talamanca del Jarama": (40.6700, -3.5300),
    "Tielmes": (40.2400, -3.3000), "Titulcia": (40.1900, -3.5400),
    "Torrejón de Ardoz": (40.4556, -3.4818), "Torrejón de la Calzada": (40.2300, -3.7900),
    "Torrejón de Velasco": (40.2000, -3.7500), "Torrelaguna": (40.8300, -3.5800),
    "Torrelodones": (40.5667, -3.9328), "Torres de la Alameda": (40.4284, -3.3774),
    "Tres Cantos": (40.5959, -3.7047), "Valdaracete": (40.2400, -3.1200),
    "Valdeavero": (40.5300, -3.3800), "Valdemanco": (40.8200, -3.6000),
    "Valdemaqueda": (40.5200, -4.2400), "Valdemorillo": (40.5000, -4.0700),
    "Valdemoro": (40.1918, -3.6762), "Valdeolmos-Alalpardo": (40.6200, -3.4900),
    "Valdepielagos": (40.6800, -3.5500), "Valdetorres de Jarama": (40.6000, -3.4500),
    "Valdilecha": (40.3468, -3.2897), "Valverde de Alcalá": (40.4000, -3.3100),
    "Velilla de San Antonio": (40.3600, -3.4900), "Venturada": (40.7600, -3.5800),
    "Villa del Prado": (40.2762, -4.2777), "Villaconejos": (40.1600, -3.4700),
    "Villalbilla": (40.4284, -3.3017), "Villamanrique de Tajo": (40.1700, -3.2900),
    "Villamanta": (40.3300, -4.1200), "Villamantilla": (40.3500, -4.0900),
    "Villanueva de la Cañada": (40.4500, -3.9700), "Villanueva de Perales": (40.3700, -4.0900),
    "Villanueva del Pardillo": (40.4748, -3.9354), "Villarejo de Salvanés": (40.1700, -3.3400),
    "Villaviciosa de Odón": (40.3556, -3.9003), "Zarzalejo": (40.5400, -4.1700),
}

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
# Section V (ICIO) keywords — now searching Section III because BOCM moved ICIO
# notifications from Section V to Section III "Administración Local" subsection.
# Using the official BOCM terminology for ICIO/tax notifications.
DAY_SCAN_KWS_V = [
    "base imponible",          # Core ICIO term — always present when PEM is declared
    "impuesto sobre construcciones",  # Official ICIO title
    "liquidación del impuesto",       # Payment notification
    "autoliquidación",                # Self-assessment (fastest signal)
]

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
    """
    SOURCE 4: BOCM RSS feed — daily bulletin links.
    
    The BOCM RSS at /boletines.rss provides one entry per daily bulletin.
    Each entry links to an HTML page that lists the individual BOCM document PDFs.
    We extract the individual PDF URLs from that page.
    
    BOCM PDF URL patterns (all valid):
      /CM_Orden_BOCM-YYYYMMDD-NNN.pdf
      /BOCM-YYYYMMDD-N.pdf
      Any href containing BOCM and a date
    """
    log("📡 RSS…")
    urls = []; seen = set()
    r = safe_get(BOCM_RSS, timeout=20)
    if not r: return urls
    try:
        import xml.etree.ElementTree as ET
        # Try to parse as RSS/Atom XML first
        try:
            root  = ET.fromstring(r.content)
            items = root.findall(".//item") or root.findall(".//entry")
        except ET.ParseError:
            items = []
        
        # Fallback: parse with BeautifulSoup (handles malformed RSS)
        if not items:
            rsoup = BeautifulSoup(r.text, "html.parser")
            items = rsoup.find_all("item") or rsoup.find_all("entry")
        
        for item in items:
            # Extract publication date
            pub = ""
            if hasattr(item, 'find'):  # BeautifulSoup element
                for tag in ["pubdate","published","updated","date"]:
                    el = item.find(tag)
                    if el and el.get_text(): pub = el.get_text(); break
                link_el = item.find("link")
                burl = (link_el.get_text() or link_el.get("href","")) if link_el else ""
            else:  # ET element
                for tag in ["pubDate","published","updated","date"]:
                    el = item.find(tag)
                    if el is not None and el.text: pub = el.text; break
                link_el = item.find("link")
                burl = link_el.text if link_el is not None else ""
            
            if not burl: continue
            
            pub_date = None
            try:
                from dateutil import parser as dp
                pub_date = dp.parse(pub).replace(tzinfo=None) if pub else None
            except Exception: pass
            if pub_date and (pub_date.date() < date_from.date() or
                             pub_date.date() > date_to.date()):
                continue
            
            # Fetch the daily bulletin landing page
            br = safe_get(burl.strip(), timeout=20)
            if not br: continue
            bsoup = BeautifulSoup(br.text, "html.parser")
            
            # Extract document links — match any BOCM PDF href pattern
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                # Match BOCM document patterns
                is_bocm_doc = (
                    ("BOCM" in full.upper() and ".PDF" in full.upper()) or
                    ("CM_Orden_BOCM" in full) or
                    ("/boletin/" in full and re.search(r"BOCM-\d{8}", full, re.I))
                )
                if is_bocm_doc:
                    norm = normalise_url(full)
                    if not norm: continue
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
    # Kiloutou triggers — obra started = machinery needed today
    "acta de inicio de obras",
    "orden de inicio de obra",
    "acta de replanteo",
    "inicio de ejecución de",
    # Actiu triggers — building ready = furnishing needed
    "certificado final de obra",
    "licencia de primera ocupación",
    # Molecor / pipe suppliers
    "colocación de tubería",
    "instalación de colector",
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

    # Molecor — PVC pipe supplier signals
    "tubería", "tuberías", "tubería de pvc", "instalación de tubería",
    "red de distribución", "colector de pluviales", "tubería de fundición",
    # Kiloutou — machinery rental signals (obra started)
    "acta de inicio", "orden de inicio", "certificado de inicio",
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
        # Sharing Co additions
        "coliving", "flexliving", "residencia de jóvenes", "alojamiento temporal",
        "residencia universitaria",
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
        elif any(k in desc for k in ["complejo de ocio","sala de espectáculos","equipamiento cultural",
                                      "gran superficie comercial","parque comercial","centro comercial"]):
            score += 30   # Kinépolis / Saona / Mango trigger
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
    logistics_munis = {m.lower() for m in LOGISTICS_MUNICIPALITIES}
    if any(m in muni for m in logistics_munis) and any(k in pt for k in ["industrial","logístic","almacén","nave"]):
        score += 8   # full logistics belt bonus, not just industrial

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

    # ── GLOBAL NOISE PENALTY — service contracts that are NOT construction leads ───
    # "Asistencia Técnica para vigilancia/coordinación de seguridad y salud"
    # "Redacción de proyecto técnico" / "Consultoría de gestión"
    # These appear in BOCM/CM feeds with high PEM but are SERVICE contracts.
    # No profile benefits from knowing about them. Apply a hard penalty.
    _pure_service_signals = [
        "asistencia técnica", "coordinación de seguridad y salud",
        "vigilancia y control de obras", "dirección facultativa",
        "redacción de proyecto", "proyecto técnico de",
        "consultoría", "asesoramiento técnico",
        "control de calidad de las obras",
        "servicios de ingeniería", "estudios y proyectos",
        "asistencia al director", "trabajos de vigilancia",
    ]
    _construction_signals = [  # must have at least one to override
        "ejecución de obras", "contrato de obras", "licitación de obras",
        "obra de construcción", "nueva construcción", "urbanización",
        "junta de compensación", "reparcelación",
    ]
    if any(k in desc for k in _pure_service_signals):
        # Only penalise if NOT also a construction contract
        if not any(k in desc for k in _construction_signals):
            score -= 25   # hard penalty — makes them sink below min_score for all profiles

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
    """Extract municipality name from BOCM document text.
    
    Priority: direct 179-municipio lookup → regex patterns → "Madrid".
    Covers all 179 municipios of the Comunidad de Madrid with accent-tolerant matching.
    """
    import unicodedata as _ud

    def _norm(s):
        """Normalize: lowercase, remove accents, strip."""
        return ''.join(c for c in _ud.normalize('NFD', s.lower()) if _ud.category(c) != 'Mn').strip()

    t_lower = text.lower()

    # Step 1: Fast lookup against all 179 municipios with word-boundary matching.
    # Longest-first to avoid "parla" matching in "declaración responsable".
    # Use word boundaries so "getafe" doesn't fire in "getafe-sur-industrial".
    import re as _re
    _tn = _norm(text)
    for raw_name, canonical in sorted(_MADRID_MUNIS_179.items(), key=lambda x: -len(x[0])):
        # Build word-boundary pattern (handles hyphens/accents)
        _pat = r"(?<![a-záéíóúñ-])" + re.escape(raw_name) + r"(?![a-záéíóúñ-])"
        if re.search(_pat, t_lower):
            return canonical
        # Accent-insensitive word-boundary fallback
        _pat_n = r"(?<![a-zaeioun-])" + re.escape(_norm(raw_name)) + r"(?![a-zaeioun-])"
        if re.search(_pat_n, _tn):
            return canonical

    # Step 2: Regex patterns for cases where the name appears in a sentence
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
            nl = name.lower()
            if nl not in noise and 3 < len(name) < 65:
                # Check if extracted name matches a known municipio
                canonical = _MADRID_MUNIS_179.get(nl) or _MADRID_MUNIS_179.get(_norm(name))
                return canonical if canonical else name.title()
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

Your subscribers are B2B professionals across these sectors — tailor analysis for their role:
- Gran Constructora / Gran Infraestructura: licitaciones públicas, obra civil Madrid. Value = 6-18mo lead time before licitación.
- Expansión Retail / Restauración: new centros comerciales, high-footfall urbanizaciones, plan especial commercial/mixed.
- Flexliving / Hospedaje operador: cambio de uso residencial = highest value signal. Primera ocupación = contact promotor now.
- Contract Furniture / Mobiliario oficina: every new edificio de oficinas, hotel, coworking, hospital = fit-out sale.
- Alquiler de Maquinaria: demolición/vaciado/excavación ASAP — before obra starts = 30-60 day lead.
- Materiales PVC / Saneamiento: every urbanización = saneamiento + abastecimiento pipes. Colectores = direct product.
- RE Investment / Promotores: reparcelaciones, plan parcial, suelo urbanizable.
- MEP / Instalaciones: any obra mayor, rehabilitación integral, edificio plurifamiliar.

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
   Example: "571 viviendas plurifamiliares + garaje, C/ Alonso Zamora 16, SSRR — PEM €82.2M.
   Sharing Co / Room00: contactar a AEDAS Homes AHORA para gestión de activo antes de que salga al mercado."
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

AI_EVALUATION — THE MOST IMPORTANT FIELD. SECTOR-SPECIFIC, ACTIONABLE (NO COMPANY NAMES):

CRITICAL EXTRACTION FIRST — before writing, extract from the document:
□ Plazo de ejecución: look for "plazo de ejecución de X meses" or "etapa 1: X meses"
□ Superficie: "superficie del ámbito", "superficie total", "m² edificables", "parcelas aportadas"
□ PEM/PBL: "presupuesto de ejecución material", "base imponible", "presupuesto base licitación"
□ Junta de Compensación: "presidente:", "gerente:", "Junta de Compensación de [NAME]"
□ Número de parcelas/propietarios: "número de parcelas", "propietarios"
□ Criterios adjudicación: "criterios de adjudicación", "precio X%", "técnico X%"
□ Fianza provisional: "fianza provisional", "garantía provisional"
□ Número de viviendas: "viviendas", "pisos", "unidades residenciales"
□ Arquitecto/aparejador: "director de obra", "director de ejecución", "arquitecto técnico"
□ Suelos contaminados: declaration present/absent
□ Cuenta de liquidación: coste obras urbanización stated

Then write 3-6 sentences. NEVER generic. NEVER company names. Use SECTOR ROLE labels.
Structure:
1. WHAT + WHERE + PEM: "Proyecto de urbanización definitivo [NAME], [MUNI] — PEM €X.XM."
2. SCALE/CONTEXT: extracted data (parcelas, m², viviendas, plazo)
3. TIMING: phase, extracted plazo, next milestone.
4. SECTOR CALLOUTS — use ROLE labels, never company names:
   🏗️ Gran Constructora / Infraestructura: "Gran Constructora: pre-calificarse para licitación civil — PBL €XM — estimado X meses."
   🚧 Alquiler Maquinaria: "Alquiler Maquinaria: exc.30t × N semanas + compactadora estimado [Mes YYYY] — llamar promotor/contratista hoy."
   🏠 Flexliving / Hospedaje: "Operador Flexliving: edificio [año est.], [N] plantas, único/comunidad propietario — ANTES de comercialización."
   🛒 Materiales: "Materiales PVC: colector DN-400 ~X.Xkm + abast. DN-200 ~X.Xkm — ~Xt PVC — cotizar YA."
   🏪 Retail: "Expansión Retail: [N] hab. futuros | renta €X/año | [competidores] — ventana apertura [fecha est.]."
   💼 Contract: "Contract: [N] puestos | €X-XM equipamiento | arquitecto: [nombre] — contactar en proyecto básico."
   📐 RE / Promotores: "Inversión RE: [m² ámbito], FAR [X], cargas ~€XM — JC: [contacto] — actuar ahora."
   🔧 MEP: "MEP: [N] plantas | [Xm²] | HVAC [tipo] | [N] ascensores CTE — ventana subcontrata [X] meses."
   🏭 Industrial: "Industrial: [Xm² nave] | alt. libre [Xm] | corredor [A-X] | yield ~[X]% — llamar promotor."
5. QUANTITIES: m², viviendas, pipes, machinery, CPV codes from document.
6. TIMING: "⚡ ACTUAR HOY" (adjudicación/1ª ocupación/CdU) | "📞 ESTA SEMANA" (definitivo) | "📅 30 DÍAS" (inicial) | "🔮 PIPELINE 6-18M" (solicitud/plan parcial: estimate from m² edificables ÷ 90m²/vivienda (Madrid standard).
"const_uso_previsto":
  "residencial libre 70% + VPO 30% + PB terciario"
  Madrid VPO requirement: 30% of residential in protected soils.
"const_tipologia":
  "Plurifamiliar X plantas + Y sótanos" | "Unifamiliar adosado" | "Industrial"
  Extract from proyecto básico, plan especial, or estimate from PEM/m²
"const_promotor_cif":
  CIF from PDF header or Junta de Compensación membership list. Format: A-28XXXXXX
"const_aparejador":
  Director de Ejecución / Aparejador from project documentation.
  This is who approves subcontractor access to site.
"const_plazo_ejecucion":
  ALWAYS look for "plazo de ejecución de X meses a contar desde firma del acta de replanteo"
  Also note etapas: "Etapa 1: 18m | Etapa 2: 24m" if phased.
"const_suelo_contaminado":
  "Declaración conforme: sin actividad contaminante previa" (standard text in reparcelación)
  or "PENDIENTE informe Confederación Hidrográfica" if near water bodies.

━━━ 🏪 EXPANSIÓN RETAIL — MOST IMPORTANT SECTOR (9 + 3 extra fields) ━━━
REAL MARKET DATA (JLL/CBRE/Savills 2024-2026):
  - Spain opened 17M sqm commercial space in 2024, projects 500K+ sqm new for 2025-26
  - Prime Madrid rents: €267.5/sqm/month | Secondary: €80-120/sqm/month
  - 70% new openings in shopping centres; 30% high-street
  - Standard supermarket (Mercadona, Dia): 1,000-1,800m² | Mango: 300-800m²
  - Saona/restaurant: 150-400m² PB ideally with terrace
  - Key expansion chains 2025-26: Aldi (40/yr), Mercadona, Dia, Mango, Action, Rossmann

Fill for EVERY urbanización, plan parcial, cambio de uso, licencia de actividad.

"retail_pob_futura_est":
  Compute: viviendas × 2.5 hab/viv = residentes futuros (INE 2024 avg 2.5 personas/hogar Madrid)
  "~4,200 residentes (1,680 viviendas × 2.5 hab/viv) — horizonte 2028"
"retail_renta_capita":
  Use INE 2023 Atlas de Distribución de Renta Urbana (ADRHU) by municipality:
  Madrid capital: €32,400 | Pozuelo: €41,500 | Boadilla: €42,100 | Las Rozas: €34,100
  Majadahonda: €36,200 | Tres Cantos: €33,500 | Alcobendas: €31,200
  Getafe: €22,400 | Fuenlabrada: €19,800 | Móstoles: €20,100 | Alcorcón: €23,600
  Leganés: €20,900 | Parla: €17,600 | Alcalá de Henares: €22,100 | Valdemoro: €24,300
  Coslada: €21,800 | Torrejón: €22,600 | Colmenar Viejo: €27,800 | Navalcarnero: €26,500
  Profile: "€24,300/año — clase media-baja, sensible al precio, favorable a Dia/Aldi/Lidl"
"retail_m2_comercial_est":
  Compute: residentes × 0.8 m²/hab (CBRE retail density Madrid metro area)
  "~3,360 m² demanda retail total (4,200 hab × 0.8m²/hab)"
  Note if PAU plan specifies suelo terciario %: add that m² figure.
"retail_competencia_1km":
  Use your knowledge of Madrid urban geography to identify:
  "Mercadona ~1.2km (calle X) · Lidl ~2.1km · SIN Dia ni Mango en radio 1km"
  If you cannot estimate reliably: "Sin datos disponibles — verificar con Google Maps"
"retail_zona_tipo":
  Classify using renta per cápita + zona geography:
  >€35K/año = "Alta gama — moda premium, restauración ticket alto" (Pozuelo, Boadilla)
  €25-35K = "Clase media — alimentación + moda media-alta" (Madrid centro, Tres Cantos)
  €20-25K = "Clase media-baja — discount, restauración quick service" (Getafe, Alcorcón)
  <€20K = "Sensible precio — descuento puro, supermercados proximidad" (Parla, Fuenlabrada)
"retail_transporte":
  Extract from geography: cercanías (C1-C10), metro (M1-M12), autovía exits.
  "Cercanías C-4 a 600m · Metro L1 a 400m · A-4 salida 18 a 2km"
  Critical threshold: >500m from metro/cercanías = significant footfall penalty for non-food retail
"retail_apertura_est":
  Timeline: urbanización inicial + 24m construcción + 6m comercialización local
  "Urbanización definitiva + 24m obra = viviendas habitables estimado Q2 2028.
   Ventana apertura local: Q3 2028 – Q1 2029 (pre-population = negociar mejor renta)"
"retail_local_m2":
  For cambio de uso / licencia actividad: extract exact m² from PDF.
  Note floor level: "PB" (ideal), "P1" (20-30% footfall penalty), "SS" (require escalators)
"retail_oportunidad":
  ONE sentence bottom-line verdict. Be specific. Use real data from above fields.
  Format: "[TIPO OPERACIÓN] en [ZONA + DESCRIPCIÓN] — [MOTIVO URGENCIA]"
  Good: "Nueva zona 4,200 residentes sin supermercado en 1km — ventana 18m antes de población."
  Good: "Local PB 320m² nueva calle peatonal zona universitaria €22K renta — hostelería A/B."
  Bad: "Oportunidad de expansión en zona en crecimiento." ← too generic

━━━ 📐 PROMOTORES / RE (urbanización, plan parcial/especial, reparcelación) ━━━
Based on actual BOCM reparcelación documents: key data always present:
1 punto/m² terreno aportado (valoración standard), cesión 10% aprovechamiento lucrativo,
plazo 24 meses desde acta de replanteo, declaración suelos contaminados.

"re_sup_total_m2":
  "Superficie total ámbito: X m²" — look for "ámbito", "superficie total", "suelo afectado"
"re_sup_edificable_m2":
  m² edificables = aprovechamiento lucrativo × coeficiente uso.
  "X m² edificables (FAR 0.XX)" — FAR = sup.edificable / sup.parcela
  Benchmark Madrid: Residencial general 0.35-0.60 | APR 0.40-0.90 | Centro 1.5-3.0
"re_num_parcelas":
  "X parcelas aportadas por Y propietarios" — from Junta de Compensación data
"re_junta_contacto":
  PRIORITY FIELD: Extract from PDF:
  "Presidente de la Junta: [NAME] | Gerente/Administrador: [ENTITY]"
  Also note: "Secretario: [NAME]" if present.
"re_cargas_pendientes":
  "€X.XM cuota de urbanización (X €/m²)" — extract from cuenta de liquidación provisional
  Benchmark: urbanización Madrid municipal 150-400 €/m² suelo
"re_tipo_suelo":
  Official classification from plan:
  APR = Área Planeamiento Remitido | APE = Área Planeamiento Específico
  SGPLU = Sistema General Parques Línea Urbana | UZP = Unidad de Zonificación
  PP = Plan Parcial | SAU = Sector Ámbito Urbanístico
"re_suelo_contaminado":
  Standard BOCM text: "Todos los propietarios declaran no haber realizado actividad
  potencialmente contaminante" = LIMPIO. Flag if missing or conditional.
"re_plazo_urbanizacion":
  "Fase I: 24m desde acta de replanteo + Garantía 2 años recepción definitiva"
  Note: 3 months from recepción definitiva → cesión suelo al Ayuntamiento (LSCM)

━━━ 🔧 INSTALADORES MEP (obra nueva, rehabilitación, primera ocupación) ━━━
Legal reference: CTE DB-SI (fire), CTE DB-SU (accessibility), RITE (thermal),
REBT (electrical). Key Spanish construction law thresholds:
- Rociadores obligatorios: edificios > 28m altura o > 1,000m² en uso administrativo
- Ascensor obligatorio: ≥ 4 plantas o ≥ 3 si hay personas con movilidad reducida
- Ventilación mecánica: garajes > 5 vehículos; baños sin ventilación natural

"mep_num_plantas":
  Extract from PDF. If not found: estimate from PEM/m² ratio.
  Note sótanos: "12 plantas + 2 sótanos (3,200m² garaje aprox.)"
"mep_sup_m2":
  Superficie total construida. If not in PDF: PEM ÷ 850 €/m² (residencial standard)
  or PEM ÷ 1,200 €/m² (oficinas) or PEM ÷ 600 €/m² (industrial)
"mep_hvac_est":
  Estimate based on use type and Spanish CTE + market norms:
  Residencial < 3 plantas: "splits individuales por vivienda — sin contrato centralizado"
  Residencial 4-8 plantas: "VRF ~80-140kW (estimado) — cotizar sistema central"
  Residencial > 8 plantas: "VRF ~150-220kW o geotermia — sistema centralizado obligatorio"
  Oficinas: "Clima centralizado ~40W/m² = [X*40/1000]kW total — VRF o enfriadoras"
  Hotel: "HVAC zonal por habitación + ACS centralizado + cocina industrial"
  Hospital/residencia: "Sistema cuádruple tubo + 100% renovación aire — alta complejidad"
"mep_ascensores_est":
  CTE SU threshold: "3+ plantas = ascensor obligatorio en residencial colectivo"
  Estimate: 1 ascensor per 4-6 viviendas | 1 per 1,500-2,000m² oficinas
  "3 ascensores × 10 paradas (CTE SU — obligatorio ≥3 plantas)"
"mep_pci_tipo":
  Apply CTE DB-SI automatically:
  H>28m: "Rociadores automáticos OBLIGATORIOS (CTE DB-SI) + BIEs + detección automática"
  H<28m residencial: "BIEs en escalera + detección básica + extintores"
  Parking > 5 veh: "Ventilación forzada antihumos + detección CO + rociadores si > 500m²"
  Uso terciario > 1,000m²: "Rociadores + BIEs + central incendios + voz evacuación"
"mep_cert_energetica":
  Infer from: location (Madrid = Zona climática C/D), PEM/m², and use type.
  New residential Madrid 2026: "Clase A/B (obligatorio CTE HE1 2022 — demanda casi nula)"
  Rehabilitation: "Clase C/D objetivo (mejora desde clase E/F existente)"
"mep_director_tecnico":
  Extract "director de ejecución" or "aparejador" from PDF. This person approves MEP bids.

━━━ 🏭 INDUSTRIAL / LOG. (obra mayor industrial, licencia nave, urbanización industrial) ━━━
MARKET DATA (Naves Madrid / JLL 2025):
  - Corredor A-2 (Coslada, Torrejón, Alcalá): 5.5-6.5€/m²/mes, prime logistics
  - Corredor A-4 (Getafe, Pinto, Valdemoro): 4.5-5.5€/m²/mes, distribution
  - Corredor A-42 (Fuenlabrada, Leganés): 4.0-4.8€/m²/mes, industrial
  - Standard clear height: logistics ≥10m | manufacturing 6-8m | last-mile ≥8m
  - Loading docks: 1/1,000m² logistics | 1/800m² e-commerce
  - Madrid gross yield industrial: 6.5-7.5% (Naves Madrid 2025)

"ind_sup_parcela_m2": Extract total land area. "15,200 m² parcela total"
"ind_sup_nave_m2":
  Built area = 60-70% of parcela (includes campa/patio for trucks).
  Extract from PDF or: "~9,800 m² nave (65% parcela — estándar logístico)"
"ind_altura_libre_m":
  CRITICAL: extract from PDF or estimate by typology:
  Nueva construcción logística 2024-26: "12-15m libre interior (estándar clase A)"
  Nave industrial existente pre-2000: "6-8m libre interior (clase B/C)"
  If unclear: "Estimado ~Xm (verificar con promotor)"
"ind_muelles_est":
  Standard: 1 muelle/1,000m² logistics | 1/800m² e-commerce
  "~X muelles niveladoras + X puertas seccionales (ratio estándar Madrid)"
  Note if campa available: "Campa ~X m² para tráileres"
"ind_potencia_kva":
  Extract from pliego or estimate:
  Logística estándar: 400-630 kVA | Industria alimentaria: 800-1,200 kVA
  Data centre / cold chain: 2,000+ kVA | Manufactura pesada: 1,600+ kVA
"ind_poligono_nombre":
  Named polígono from address. Key Madrid polígonos:
  A-2: Centro Transportes Coslada, P.I. Los Gavilanes, San Fernando Empresarial
  A-4: P.I. Los Olivos Getafe, P.I. La Isla, Parque Empresarial Carpetania
  A-42: Cobo Calleja Fuenlabrada, P.I. Leganés, P.I. Las Canteras Valdemoro
"ind_renta_mercado":
  Benchmark from location: "~5.0-5.5€/m²/mes (zona Getafe A-4 estándar 2025)"
  Helps industrial developer value the project and assess ROI.
"ind_yield_est":
  "~7% yield bruto estimado (referencia Madrid industrial zona sur 2025)"
  Source: Naves Madrid market data.

━━━ 🚧 ALQUILER MAQUINARIA (adjudicaciones, urbanizaciones, demoliciones) ━━━
Kiloutou Madrid locations: Torrejón de Ardoz (main depot, Av. del Sol 11).
Most rented in Madrid construction: excavadoras 20-30t, compactadoras vibratorias,
dumpers articulados, plataformas tijera/articuladas, retroexcavadoras.
Commercial window after adjudicación publication: 24-48 HOURS maximum.

"alq_contratista":
  MOST IMPORTANT FIELD. From adjudicación document: company name + CIF.
  "Ferrovial Servicios SA (A-28543124) — adjudicatario confirmado"
  If licitación (not yet awarded): "EN LICITACIÓN — adjudicación estimada: [fecha]"
"alq_importe_adj":
  Adjudicación amount as number. "€4,800,000"
"alq_inicio_obra_est":
  Estimate: adjudicación + 30 días firma contrato + 15 días replanteo = inicio real.
  "Estimado: [mes+45 días] (adjudicación + 45 días trámites + firma acta replanteo)"
"alq_maquinaria_est":
  Use PEM tiers (Kiloutou market knowledge):
  <€500K: "Retroexcavadora + dumper + compactadora pequeña"
  €500K-€2M: "Excavadora 20t × 6-8 semanas + compactadora vibratoria + dumper 10t"
  €2M-€10M: "Excavadora 30t × 2 unidades × 12 semanas + compactadora + grúa torre 30m"
  >€10M: "Flota completa: exc.30t × 3 + compactadora + grúa pluma + plataformas + dumpers"
  Add: "Plataformas elevadoras para fase acabados (X unidades)"
"alq_m3_tierras_est":
  Urbanización: 6-10 m³/m² suelo | Demolición + excavación: 1.5-2.5 m³/m² planta
  "~45,000 m³ movimiento tierras (8m³/m² × 5,625m² urbanización)"
"alq_duracion_meses":
  From pliego or standard: urbanización 18-24m | obra nueva residencial 18-30m
  "18 meses (desde pliego)" or "~24 meses (estimado urbanización estándar Madrid)"
"alq_jefe_obra":
  Extract "director de obra" or "jefe de obra" from PDF. Call this person directly.
"alq_urgencia":
  "🔴 LLAMAR HOY" (adjudicación ya publicada) |
  "🟡 PREPARAR OFERTA" (licitación activa, cierre en X días) |
  "🟢 PIPELINE" (aprobación inicial/definitiva, obra en 3-12m)

━━━ 🛒 COMPRAS / MATERIALES (urbanización, obra nueva, saneamiento, licitación) ━━━
Reference quantities for Spanish urbanisation (Molecor / Lafarge / Holcim norms):
  - Colector saneamiento: 200m colector DN-400 per hectare urbanised
  - Red abastecimiento: DN-200/160 ~0.8km/ha | Pluviales: DN-500+ colector principal
  - Hormigón: 0.25-0.35m³/m² for pavimentación + 0.15m³/m² for estructuras
  - Zahorra artificial (áridos): 0.3m³/m² road base = ~600kg/m² = 0.6t/m²
  - Acero B500S: 45kg/m² residencial | 60kg/m² oficinas | 30kg/m² industrial
  - Tubería PVC corrugada: colector saneamiento | PVC rígido: abastecimiento

"mat_colector_dn_km":
  Compute from ámbito m²: 1ha → ~200m DN-400 collector + 300m DN-300 laterals
  "Saneamiento: colector principal DN-400 ~X.Xkm + laterales DN-300 ~X.Xkm (separativo)"
"mat_red_abast_dn_km":
  "Abastecimiento: DN-200 red distribución ~X.Xkm + DN-160 acometidas ~X.Xkm"
"mat_pluviales_dn_km":
  "Pluviales: colector principal DN-600 ~X.Xkm + sumideros ~X uds"
"mat_hormigon_m3_est":
  "~X,XXX m³ HA-25/B/20/IIa (pavimentación + cunetas + bordillos estándar)"
"mat_aridos_t_est":
  "~X,XXXt zahorra artificial Z-1 (base sub-base viario)"
"mat_acero_t_est":
  Only for obra edificación: "~XXXt B500S (45kg/m² × X,XXXm² construidos)"
"mat_contratista":
  If adjudicado: company name to call for supply contract.
  If licitación: "EN LICITACIÓN — adjudicatario pendiente — cotizar a licitadores"
"mat_plazo_entrega":
  When materials are needed on site:
  "Inicio suministro estimado: [fase] → JIT requerido (plazo obra: Xm)"

━━━ 💼 CONTRACT & OFICINAS (oficinas, hotel, universidad, hospital, obra nueva terciario) ━━━
MARKET DATA (Cushman & Wakefield / JLL 2025-2026):
  - Office fit-out standard Madrid: 7.5m²/puesto open plan | 10m²/puesto celdular
  - Hotel: 45-55m² por habitación constructivo | 25-35m² útil habitación
  - LEED Gold / BREEAM Very Good = standard for class A offices Madrid 2024+
  - Contract furniture typical spend: 300-600€/m² officinas | 1,200-2,500€/hab hotel
  - Timeline: contact promotor at "proyecto básico" stage → 18m before ocupación

"cont_uso_edificio":
  "Oficinas clase A open plan" | "Hotel 4* (70 habitaciones)" | "Residencia universitaria 200 plazas"
  | "Hospital/clínica terciario" | "Centro de datos" | "Coworking/flex offices"
"cont_m2_oficinas":
  Useful m² for fitting-out. Gross-to-net ratio: offices ~80% | hotels ~65%
  "4,200 m² útiles oficinas (gross-to-net 80% de 5,250m² totales)"
"cont_puestos_trabajo":
  Compute: m² útiles ÷ 7.5m²/puesto open plan | ÷ 10m²/puesto mixto
  "~560 puestos (4,200m² ÷ 7.5m²/puesto NIA open plan 2024)"
"cont_num_plantas":
  "8 plantas tipo + PB + 2 sótanos garaje"
"cont_arquitecto":
  Extract architecture firm from PDF. They specify furniture and equipment.
  This is the MOST IMPORTANT contact for Contract & Oficinas sector.
"cont_certificacion":
  Infer from: use (Class A offices 2024 = LEED/BREEAM mandatory for institutional investors)
  "LEED Gold objetivo (estándar edificios oficinas clase A Madrid 2024-2026)"
  Hotel: "Certificación BREEAM In-Use prevista (SOCIMI/REIT requirement)"
"cont_entrega_est":
  Timeline: aprobación definitiva + 20m obra + 2m equipamiento = primera ocupación
  "1ª ocupación estimada: [mes/año] — ventana de equipamiento: [3-6 meses antes]"
"cont_fit_out_presupuesto_est":
  Estimate furniture/equipment budget for the promotor:
  Oficinas A: "€300-500/m² = ~€[m²×400/1000]M equipamiento estimado"
  Hotel 4*: "€1,500-2,500/habitación × N hab = ~€[N*2000/1000]M"
  Residencia: "€8,000-15,000/plaza × N plazas"

━━━ 🏠 FLEXLIVING & HOSTELERÍA (cambio de uso, rehabilitación, primera ocupación) ━━━
REAL MARKET DATA (CBRE/Savills/Geräh 2025-2026):
  - Madrid coliving stock: 11,375 units active + 14,000 in development (CBRE 2024)
  - Average tariff: €1,000/mes coliving Madrid | €3,000 corporate | €1,180 suburban
  - Occupancy professionally managed: 90-95% | private: 70-80%
  - Yield: 8-11% coliving vs 4-6% traditional rental (Geräh Real Estate 2025)
  - OPEX = 30-35% total revenue (Cushman & Wakefield)
  - Investment ticket 2024: avg €40M (fell to €30M in 2025)
  - CRITICAL: 80% investors need <20 min from metro/train (suburban CBRE standard)
  - Fastest growing: Smart Living (suelo terciario, periferias) — oficinas reconversión
  - Muppy: 825 units, Grupo HIVE: 700+ units, PIMCO: NUGA Castellana

"flex_anno_construccion":
  Extract from PDF or estimate by area/typology:
  Ensanche Salamanca/Chamberí: 1890-1960 | Ciudad Lineal/Tetuán: 1950-1975
  PAU years: Sanchinarro/Las Tablas: 2000-2010 | New developments: 2020+
"flex_num_unidades":
  Minimum viable for operators: 30 units (Muppy/Sharing Co standard)
  Professional management threshold: 50+ units
  "48 unidades residenciales (viviendas/apartamentos) — VIABLE para coliving"
"flex_sup_total_m2":
  Extract from PDF. Note m²/unidad: <25m² = habitaciones | 25-50m² = estudios | >50m² = 1-2bed
"flex_uso_anterior":
  CRITICAL field. Conversion path value:
  "Oficinas → coliving" = GOLD (suelo terciario, adaptación normativa compleja but high yield)
  "Residencial vacío → coliving" = SILVER (less conversion, lower capex)
  "Hostelería → flexliving" = PLATINUM (ya tiene uso turístico, mínima adaptación)
"flex_propietario_tipo":
  MAKE-OR-BREAK VARIABLE:
  "Único propietario: [Fondo/Empresa] — ALTO potencial (operación en bloque posible)"
  "Comunidad X propietarios — BAJO potencial (consenso imposible para gestión unitaria)"
  Extract: Junta de Compensación president = usually single or small group control
"flex_dist_metro_min":
  Compute from geography: "Metro L1 a 800m (~10 min a pie) — CUMPLE criterio CBRE <20min"
  CBRE benchmark: <20 min from metro/cercanías in suburban = investment grade
"flex_potencial_coliving":
  Score using real market criteria:
  ALTO: único propietario + ≥40 unidades + <20min metro + vacío/partial + suelo terciario
  MEDIO: único/few propietarios + 25-40 unidades + buena zona + uso mixto
  BAJO: comunidad copropietarios OR <25 unidades OR solo residencial sin posibilidad CdU
"flex_irr_est":
  Use real Madrid yields (CBRE/Geräh 2025-2026):
  Centro (<M-30): "~6-7% yield neto (tarifa €1,000/mes, ocupación 93%, OPEX 33%)"
  Suburban (cercanías <20min): "~8-10% yield neto (tarifa €1,180/mes suburban 2025)"
  Corporate target: "~8-11% yield bruto (€3,000/mes corporate, OPEX 35%)"
  Compute: (ingresos_anuales × 0.67) ÷ valor_activo × 100 = % yield neto


━━━ 🖥️ DATA CENTERS & ENERGÍA (NEW SECTOR 2026) ━━━
Madrid: Microsoft, Blackstone ($5B), Amazon, Telefónica all building.
Alcalá de Henares corridor = #1 data centre zone in Southern Europe 2025-2026.
Every data centre = 200-2,000 kVA electrical supply + UPS + cooling = major MEP.

"dc_potencia_mw": Extract or estimate: "50MW IT load phase 1" — standard hyperscale
"dc_tipo": "Hyperscale (>20MW)" | "Colocation (5-20MW)" | "Edge/Campus (<5MW)"
"dc_tier": "Tier III objetivo (99.982% uptime — estándar comercial 2024)" or from pliego
"dc_pue_target": "PUE < 1.3 objetivo (estándar 2024 — UE Energy Efficiency Directive)"
"dc_m2_terreno": Extract or estimate: typical hyperscale = 10-50ha
"energia_tipo": "Fotovoltaica" | "Eólica" | "Mixta + BESS" | "Grid-connected"
"energia_mw_instalado": MW capacity from permit/plan

━━━ 🏢 RESIDENCIAS / SENIOR LIVING (FAST-GROWING 2026-2030) ━━━
CBRE: Madrid needs 40,000+ senior beds by 2030 — only 12,000 exist today.
Typical investment: €8,000-15,000/plaza × 100-200 plazas = €1-3M per facility.
Revenue model: €1,500-3,000/mes por plaza (private) | €900-1,200 público.

"senior_num_plazas": Extract from permit. Minimum viable: 50 plazas.
"senior_tipo": "Residencia tercera edad" | "Centro de día" | "Viviendas tuteladas" | "Senior living activo"
"senior_promotor_tipo": "SOCIMI/fondo" | "Grupo hospitalario" | "Promotor privado"
"senior_yield_est": "~7-9% yield bruto estimado (referencia Madrid senior living 2025)"
"senior_capex_est": "~€X.XM capex construcción (€X,000/plaza × N plazas)"

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

        # ── Catastro enrichment — non-blocking, fills building age + floors ───
        # For flexliving, MEP, and Contract profiles: look up Catastro data.
        # This gives us building construction year (anno_construccion for flex),
        # number of floors (mep_num_plantas), and m² (mep_sup_m2) without AI tokens.
        _cat_profiles = ("hospe", "mep", "actiu", "retail")
        _cat_fit = str(d.get("profile_fit","")).lower()
        if (any(pt in _cat_fit for pt in _cat_profiles)
                and d.get("address") and not d.get("flex_anno_construccion")):
            try:
                _cat = catastro_enrich(d.get("address",""), d.get("municipality","Madrid"))
                if _cat.get("anno_construccion") and not d.get("flex_anno_construccion"):
                    d["flex_anno_construccion"] = _cat["anno_construccion"]
                if _cat.get("num_plantas") and not d.get("mep_num_plantas"):
                    d["mep_num_plantas"] = f"{_cat['num_plantas']} plantas (Catastro)"
                if _cat.get("sup_m2") and not d.get("mep_sup_m2"):
                    d["mep_sup_m2"] = f"{_cat['sup_m2']} m² (Catastro)"
                if _cat.get("uso_catastral"):
                    d["_catastro_uso"] = _cat["uso_catastral"]
                if _cat.get("ref_catastral"):
                    d["_ref_catastral"] = _cat["ref_catastral"]
            except Exception:
                pass  # enrichment is best-effort only

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
# ── Google Sheet column headers ─────────────────────────────────────────────
# BASE COLUMNS (cols A-AB) — all profiles
# ── Sheet columns — single "Leads" tab, all profiles ──────────────────────
# 21 core columns only. All sector intelligence lives in AI Evaluation field.
# Dashboard handles per-sector filtering from Profile Fit column.
HDRS_BASE = [
    "Date Granted",        # A — fecha concesión
    "Municipality",        # B — municipio
    "Full Address",        # C — dirección completa
    "Applicant",           # D — solicitante / promotor
    "Permit Type",         # E — tipo de licencia / expediente
    "Declared Value PEM (€)",  # F — PEM official ONLY when extracted from document
    "Est. Build Value (€)",    # G — estimated obra cost (PEM / ratio)
    "Maps Link",           # H — Google Maps URL
    "Description",         # I — descripción del proyecto
    "Source URL",          # J — URL fuente (BOCM, CM Contratos, BOE…)
    "PDF URL",             # K — enlace directo al PDF
    "Mode",                # L — extraction mode (ai / keyword)
    "Confidence",          # M — confidence level
    "Date Found",          # N — fecha de procesado por el engine
    "Lead Score",          # O — puntuación 0-100
    "Expediente",          # P — número de expediente
    "Phase",               # Q — fase (inicial/definitivo/licitacion/adjudicacion…)
    "Estimated PEM",       # R — AI-estimated PEM when not officially declared
    "AI Evaluation",       # S — análisis IA específico por sector
    "Supplies Needed",     # T — materiales/maquinaria estimada
    "Profile Fit",         # U — perfiles a los que aplica este lead
]

HDRS = HDRS_BASE  # alias

# SECTOR COLUMNS (cols AC+) — profile-specific intelligence
# Engine writes these; dashboard reads them filtered per active profile
HDRS_SECTOR = [
    # 🏗️ Gran Infraestructura (7)
    "infra_cpv_codes","infra_pbl_eur","infra_deadline",
    "infra_criteria","infra_clasificacion","infra_procedure","infra_contracting_body",
    # 🏢 Gran Constructora (6)
    "const_num_viviendas","const_uso_previsto","const_tipologia",
    "const_promotor_cif","const_aparejador","const_plazo_ejecucion",
    # 🏪 Expansión Retail (9)
    "retail_pob_futura_est","retail_renta_capita","retail_m2_comercial_est",
    "retail_competencia_1km","retail_zona_tipo","retail_transporte",
    "retail_apertura_est","retail_local_m2","retail_oportunidad",
    # 📐 Promotores / RE (6)
    "re_sup_total_m2","re_sup_edificable_m2","re_num_parcelas",
    "re_junta_contacto","re_cargas_pendientes","re_tipo_suelo",
    # 🔧 Instaladores MEP (6)
    "mep_num_plantas","mep_sup_m2","mep_hvac_est",
    "mep_ascensores_est","mep_pci_tipo","mep_director_tecnico",
    # 🏭 Industrial / Log. (6)
    "ind_sup_parcela_m2","ind_sup_nave_m2","ind_altura_libre_m",
    "ind_muelles_est","ind_potencia_kva","ind_poligono_nombre",
    # 🚧 Alquiler Maquinaria (6)
    "alq_contratista","alq_importe_adj","alq_inicio_obra_est",
    "alq_maquinaria_est","alq_m3_tierras_est","alq_duracion_meses",
    # 🛒 Compras / Materiales (6)
    "mat_colector_dn_km","mat_red_abast_dn_km","mat_hormigon_m3_est",
    "mat_aridos_t_est","mat_acero_t_est","mat_contratista",
    # 💼 Contract & Oficinas (6)
    "cont_uso_edificio","cont_m2_oficinas","cont_puestos_trabajo",
    "cont_arquitecto","cont_certificacion","cont_entrega_est",
    # 🏠 Flexliving & Hostelería (9)
    "flex_anno_construccion","flex_num_unidades","flex_sup_total_m2",
    "flex_uso_anterior","flex_propietario_tipo","flex_dist_metro_min",
    "flex_potencial_coliving","flex_irr_est",
    # Extra deep-research fields across sectors
    "const_suelo_contaminado",       # Gran Constructora
    "re_suelo_contaminado",          # Promotores/RE
    "re_plazo_urbanizacion",         # Promotores/RE
    "ind_renta_mercado",             # Industrial/Log (market rent benchmark)
    "ind_yield_est",                 # Industrial/Log (gross yield estimate)
    "alq_urgencia",                  # Alquiler (urgency traffic light)
    "alq_jefe_obra",                 # Alquiler (site manager to call)
    "mat_pluviales_dn_km",           # Materiales (storm drainage pipe)
    "cont_num_plantas",              # Contract (number of floors)
    "cont_fit_out_presupuesto_est",  # Contract (furniture budget estimate)
]

HDRS = HDRS_BASE + HDRS_SECTOR

# ── Profile-tab definitions ───────────────────────────────────────────────────
# Each profile gets its OWN tab in Google Sheets — a filtered VIEW of the Leads tab
# containing only the base columns + that profile's sector columns.
# The engine creates and updates these tabs automatically.
# The dashboard reads from a profile tab if the user switches profile,
# giving a cleaner, faster experience with pre-filtered data.
PROFILE_TABS = {
    "infrastructura": {
        "tab_name": "📊 Infraestructura",
        "profile_keys": ["infrastructura","constructora"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("infra_") or c.startswith("const_")],
        "min_score": 40, "min_pem": 500_000,
    },
    "constructora": {
        "tab_name": "📊 Constructora",
        "profile_keys": ["constructora","infrastructura","alquiler","materiales"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("const_") or c.startswith("alq_") or c.startswith("mat_")],
        "min_score": 35, "min_pem": 300_000,
    },
    "retail": {
        "tab_name": "📊 Retail",
        "profile_keys": ["retail","hospe"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("retail_") or c.startswith("flex_")],
        "min_score": 40, "min_pem": 0,
    },
    "promotores": {
        "tab_name": "📊 Promotores",
        "profile_keys": ["promotores"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("re_")],
        "min_score": 45, "min_pem": 500_000,
    },
    "mep": {
        "tab_name": "📊 MEP",
        "profile_keys": ["mep"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("mep_")],
        "min_score": 30, "min_pem": 80_000,
    },
    "industrial": {
        "tab_name": "📊 Industrial",
        "profile_keys": ["industrial"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("ind_")],
        "min_score": 35, "min_pem": 200_000,
    },
    "alquiler": {
        "tab_name": "📊 Alquiler Maq.",
        "profile_keys": ["alquiler"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("alq_")],
        "min_score": 25, "min_pem": 200_000,
    },
    "materiales": {
        "tab_name": "📊 Materiales",
        "profile_keys": ["materiales"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("mat_") or c.startswith("alq_")],
        "min_score": 30, "min_pem": 200_000,
    },
    "actiu": {
        "tab_name": "📊 Contract",
        "profile_keys": ["actiu"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("cont_")],
        "min_score": 35, "min_pem": 300_000,
    },
    "hospe": {
        "tab_name": "📊 Flexliving",
        "profile_keys": ["hospe"],
        "sector_cols": [c for c in HDRS_SECTOR if c.startswith("flex_") or c.startswith("retail_")],
        "min_score": 30, "min_pem": 100_000,
    },
}
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
    """
    Write a permit to the Leads sheet.

    UPSERT LOGIC — prevents duplicate cards when the same project advances in phase:
    - BOCM publishes a NEW document each time a project changes phase.
    - Each new document has a new URL and BOCM ID → old code always appended a new row.
    - NEW: if the new document shares an expediente with an existing row, we UPDATE
      that row's Phase, Date Granted, Source URL, Lead Score, AI Evaluation, etc.
      We also store the old phase in "Previous Phase" and stamp "Last Updated".
    - If no matching expediente exists → append as before.
    """
    ws  = get_sheet()
    url = p.get("source_url","")
    bocm_id = extract_bocm_id(url)

    with _sheet_lock:
        if bocm_id and bocm_id in _seen_bocm_ids:
            return False
        if url in _seen_urls:
            return False

        dec_raw   = p.get("declared_value_eur")
        mode_src = p.get("extraction_mode", "")
        # RULE: col F = ONLY officially declared PEM from BOCM/BOE documents.
        # If the value came from AI estimation (not a formally declared PEM), keep col F empty.
        # The "declared" flag is set by extract() when it finds "presupuesto de ejecución material"
        # or similar official language in the document text.
        _is_declared = bool(p.get("pem_is_declared", True))   # default True for backward compat
        # CM Contratos and datos.madrid licences never have a formally declared PEM
        if mode_src in ("cm_contratos", "datos_madrid", "borme"):
            _is_declared = False
        dec = dec_raw if (_is_declared and dec_raw) else None
        if "boe.es" in (url or "").lower():
            fuente = "BOE"
        elif mode_src == "cm_contratos":
            fuente = "CM-Contratos"
        elif mode_src == "datos_madrid":
            fuente = "Madrid-Licencias"
        else:
            fuente = "BOCM"
        # Est. Build Value (col G):
        # When official PEM exists: divide by type-based ratio (ICIO standards)
        # When NO official PEM: estimate from AI-extracted figures or context
        _permit_t_lower = (p.get("permit_type","") or "").lower()
        if "urbanización" in _permit_t_lower or "plan parcial" in _permit_t_lower:
            _pem_ratio = 0.025   # civil works: lower ICIO/PEM ratio
        elif "rehabilitación" in _permit_t_lower:
            _pem_ratio = 0.045
        elif "industrial" in _permit_t_lower:
            _pem_ratio = 0.040
        elif "licitación" in _permit_t_lower or "contribuciones especiales" in _permit_t_lower:
            _pem_ratio = 0.015   # PBL = total budget, not just PEM
        elif "nueva construcción" in _permit_t_lower:
            _pem_ratio = 0.035
        else:
            _pem_ratio = 0.030

        if dec and isinstance(dec, (int, float)) and dec > 0:
            est = round(dec / _pem_ratio)
        elif dec_raw and isinstance(dec_raw, (int, float)) and dec_raw > 0:
            # Use raw (non-officially-declared) value to still compute build cost
            est = round(dec_raw / _pem_ratio)
        else:
            est = ""
        addr = p.get("address") or ""
        muni = p.get("municipality") or "Madrid"
        # Use pre-built maps URL if supplied (e.g. from process_cm_contrato),
        # otherwise build one from address. Always build a maps URL — even a
        # municipality-level one is better than nothing.
        maps = p.get("maps","") or ""
        if not maps:
            if addr:
                maps = ("https://www.google.com/maps/search/"
                        + (addr + " " + muni + " España").replace(" ","+").replace(",",""))
            elif muni and muni.lower() not in ("madrid",""):
                # At minimum, link to the municipality
                maps = f"https://www.google.com/maps/search/{muni.replace(' ','+')},+Madrid,+España"
            else:
                maps = "https://www.google.com/maps/search/Madrid,+España"
        profile_fit = p.get("profile_fit", [])
        if isinstance(profile_fit, list):
            profile_fit_str = ", ".join(profile_fit)
        else:
            profile_fit_str = str(profile_fit) if profile_fit else ""

        new_phase    = str(p.get("phase","") or "").strip()
        new_exp      = str(p.get("expediente","") or "").strip()
        now_str      = datetime.now().strftime("%Y-%m-%d %H:%M")
        today_date   = datetime.now().strftime("%Y-%m-%d")

        # ── UPSERT: try to find existing row by expediente ─────────────────────
        # Only attempt if expediente is a formal reference (not a synthetic BOCM hash)
        _updated_existing = False
        if new_exp and ws and not new_exp.startswith("BOCM-"):
            try:
                all_rows = ws.get_all_values()
                if len(all_rows) > 1:
                    hdrs = all_rows[0]
                    try:
                        exp_col   = hdrs.index("Expediente")
                        phase_col = hdrs.index("Phase")
                        score_col = hdrs.index("Lead Score")
                        url_col   = hdrs.index("Source URL")
                        ai_col    = hdrs.index("AI Evaluation") if "AI Evaluation" in hdrs else -1
                        date_col  = hdrs.index("Date Granted")
                        upd_col   = hdrs.index("Last Updated")   if "Last Updated"   in hdrs else len(hdrs)
                        prev_col  = hdrs.index("Previous Phase") if "Previous Phase" in hdrs else len(hdrs) + 1
                        pem_col   = hdrs.index("Declared Value PEM (€)")

                        _PHASE_ORDER = {"solicitud":1,"inicial":2,"en_tramite":2,
                                        "definitivo":3,"licitacion":4,
                                        "adjudicacion":5,"en_obra":6,"primera_ocupacion":7}

                        for row_idx, row_vals in enumerate(all_rows[1:], start=2):
                            if len(row_vals) > exp_col and row_vals[exp_col].strip() == new_exp:
                                old_phase = row_vals[phase_col].strip() if len(row_vals) > phase_col else ""
                                old_order = _PHASE_ORDER.get(old_phase.lower(), 0)
                                new_order = _PHASE_ORDER.get(new_phase.lower(), 0)

                                if new_order > old_order:
                                    # Phase advanced — UPDATE the existing row in place
                                    updates = []
                                    updates.append((row_idx, phase_col+1, new_phase))
                                    updates.append((row_idx, score_col+1, p.get("lead_score",0)))
                                    updates.append((row_idx, url_col+1, url))
                                    updates.append((row_idx, date_col+1, p.get("date_granted","")))
                                    if ai_col >= 0:
                                        updates.append((row_idx, ai_col+1, (p.get("ai_evaluation") or "")[:600]))
                                    if dec:
                                        updates.append((row_idx, pem_col+1, dec))
                                    # Stamp Last Updated + Previous Phase
                                    updates.append((row_idx, upd_col+1, now_str))
                                    updates.append((row_idx, prev_col+1, old_phase))
                                    for r, c, v in updates:
                                        ws.update_cell(r, c, v)
                                    _seen_urls.add(url)
                                    if bocm_id: _seen_bocm_ids.add(bocm_id)
                                    log(f"  🔄 [{p.get('lead_score',0):02d}pts] UPDATED {new_exp}: "
                                        f"{old_phase} → {new_phase} | {muni}")
                                    _updated_existing = True
                                else:
                                    # Same or older phase — mark URL seen so we don't re-process
                                    _seen_urls.add(url)
                                    if bocm_id: _seen_bocm_ids.add(bocm_id)
                                    log(f"  ⏭️  Same/earlier phase for {new_exp} ({old_phase}→{new_phase}) — skipped")
                                    _updated_existing = True  # treat as "handled"
                                break
                    except ValueError:
                        pass  # column not found — fall through to append
            except Exception as _e:
                log(f"  ⚠️  Upsert check failed ({_e}) — falling back to append")

        if _updated_existing:
            return True

        # ── APPEND: no existing row found by expediente ────────────────────────
        # col F = declared PEM only; col R = estimated PEM (AI or keyword-derived)
        _col_f_pem = dec if dec else ""   # blank if not officially declared
        # Build Estimated PEM for col R from dec_raw when dec is empty
        _est_pem = p.get("estimated_pem","") or ""
        if not _est_pem and dec_raw and not dec:
            # Non-declared value → goes into col R as estimate
            _est_pem = (f"€{dec_raw/1_000_000:.1f}M" if dec_raw >= 1_000_000
                        else f"€{int(dec_raw/1000)}K" if dec_raw >= 1000 else "")
        # est = structural cost estimate (only meaningful when official PEM is known)
        # PEM-to-obra ratio calibrated per permit type (research-backed):
        # urbanización: 2.5% (civil works have lower ICIO/PEM ratio)
        # obra mayor residencial: 3.5% (residential construction cost ratios)
        # nave industrial: 4.0% (steel structure, higher material vs labour ratio)
        # rehabilitación: 4.5% (ICIO on PEM including all rehab finishes)
        # licitación pública infra: 1.5% (base imponible = total budget, not PEM)
        _permit_t_lower = (p.get("permit_type","") or "").lower()
        if "urbanización" in _permit_t_lower or "plan parcial" in _permit_t_lower:
            _pem_divisor = 0.025
        elif "rehabilitación" in _permit_t_lower:
            _pem_divisor = 0.045
        elif "industrial" in _permit_t_lower:
            _pem_divisor = 0.040
        elif "licitación" in _permit_t_lower or "contribuciones especiales" in _permit_t_lower:
            _pem_divisor = 0.015
        elif "nueva construcción" in _permit_t_lower:
            _pem_divisor = 0.035
        else:
            _pem_divisor = 0.030  # default (cambio de uso, declaración responsable, etc.)
        est = round(dec / _pem_divisor) if dec and isinstance(dec,(int,float)) and dec > 0 else ""

        row = [
            p.get("date_granted",""),          # A Date Granted
            muni,                              # B Municipality
            addr,                              # C Full Address
            p.get("applicant") or "",          # D Applicant
            p.get("permit_type") or "obra mayor",  # E Permit Type
            _col_f_pem,                        # F Declared Value PEM — ONLY when confirmed in document
            est,                               # G Est. Build Value
            maps,                              # H Maps Link
            (p.get("description") or "")[:350],    # I Description
            url,                               # J Source URL
            pdf_url or "",                     # K PDF URL
            p.get("extraction_mode","keyword"),    # L Mode
            p.get("confidence",""),            # M Confidence
            today_date,                        # N Date Found
            p.get("lead_score",0),             # O Lead Score
            new_exp,                           # P Expediente
            new_phase,                         # Q Phase
            _est_pem,                          # R Estimated PEM
            (p.get("ai_evaluation") or "")[:800],  # S AI Evaluation
            (p.get("supplies_needed") or "")[:600],# T Supplies Needed
            profile_fit_str,                   # U Profile Fit
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
            phase_s = new_phase or "?"
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

        # ── Sector-specific field extraction from AI JSON — 65 fields × 10 profiles
        # Gran Infraestructura
        p["infra_cpv_codes"]       = p.get("infra_cpv_codes")       or ""
        p["infra_pbl_eur"]         = p.get("infra_pbl_eur")         or ""
        p["infra_deadline"]        = p.get("infra_deadline")        or ""
        p["infra_criteria"]        = p.get("infra_criteria")        or ""
        p["infra_clasificacion"]   = p.get("infra_clasificacion")   or ""
        p["infra_procedure"]       = p.get("infra_procedure")       or ""
        p["infra_contracting_body"]= p.get("infra_contracting_body")or ""
        # Gran Constructora
        p["const_num_viviendas"]   = p.get("const_num_viviendas")   or ""
        p["const_uso_previsto"]    = p.get("const_uso_previsto")    or ""
        p["const_tipologia"]       = p.get("const_tipologia")       or ""
        p["const_promotor_cif"]    = p.get("const_promotor_cif")    or ""
        p["const_aparejador"]      = p.get("const_aparejador")      or ""
        p["const_plazo_ejecucion"] = p.get("const_plazo_ejecucion") or ""
        # Expansión Retail (9 fields — hottest sector)
        p["retail_pob_futura_est"] = p.get("retail_pob_futura_est") or ""
        p["retail_renta_capita"]   = p.get("retail_renta_capita")   or ""
        p["retail_m2_comercial_est"]=p.get("retail_m2_comercial_est")or ""
        p["retail_competencia_1km"]= p.get("retail_competencia_1km")or ""
        p["retail_zona_tipo"]      = p.get("retail_zona_tipo")      or ""
        p["retail_transporte"]     = p.get("retail_transporte")     or ""
        p["retail_apertura_est"]   = p.get("retail_apertura_est")   or ""
        p["retail_local_m2"]       = p.get("retail_local_m2")       or ""
        p["retail_oportunidad"]    = p.get("retail_oportunidad")    or ""
        # Promotores / RE
        p["re_sup_total_m2"]       = p.get("re_sup_total_m2")       or ""
        p["re_sup_edificable_m2"]  = p.get("re_sup_edificable_m2")  or ""
        p["re_num_parcelas"]       = p.get("re_num_parcelas")       or ""
        p["re_junta_contacto"]     = p.get("re_junta_contacto")     or ""
        p["re_cargas_pendientes"]  = p.get("re_cargas_pendientes")  or ""
        p["re_tipo_suelo"]         = p.get("re_tipo_suelo")         or ""
        # Instaladores MEP
        p["mep_num_plantas"]       = p.get("mep_num_plantas")       or ""
        p["mep_sup_m2"]            = p.get("mep_sup_m2")            or ""
        p["mep_hvac_est"]          = p.get("mep_hvac_est")          or ""
        p["mep_ascensores_est"]    = p.get("mep_ascensores_est")    or ""
        p["mep_pci_tipo"]          = p.get("mep_pci_tipo")          or ""
        p["mep_director_tecnico"]  = p.get("mep_director_tecnico")  or ""
        # Industrial / Log.
        p["ind_sup_parcela_m2"]    = p.get("ind_sup_parcela_m2")    or ""
        p["ind_sup_nave_m2"]       = p.get("ind_sup_nave_m2")       or ""
        p["ind_altura_libre_m"]    = p.get("ind_altura_libre_m")    or ""
        p["ind_muelles_est"]       = p.get("ind_muelles_est")       or ""
        p["ind_potencia_kva"]      = p.get("ind_potencia_kva")      or ""
        p["ind_poligono_nombre"]   = p.get("ind_poligono_nombre")   or ""
        # Alquiler Maquinaria
        p["alq_contratista"]       = p.get("alq_contratista")       or ""
        p["alq_importe_adj"]       = p.get("alq_importe_adj")       or ""
        p["alq_inicio_obra_est"]   = p.get("alq_inicio_obra_est")   or ""
        p["alq_maquinaria_est"]    = p.get("alq_maquinaria_est")    or ""
        p["alq_m3_tierras_est"]    = p.get("alq_m3_tierras_est")    or ""
        p["alq_duracion_meses"]    = p.get("alq_duracion_meses")    or ""
        # Compras / Materiales
        p["mat_colector_dn_km"]    = p.get("mat_colector_dn_km")    or ""
        p["mat_red_abast_dn_km"]   = p.get("mat_red_abast_dn_km")   or ""
        p["mat_hormigon_m3_est"]   = p.get("mat_hormigon_m3_est")   or ""
        p["mat_aridos_t_est"]      = p.get("mat_aridos_t_est")      or ""
        p["mat_acero_t_est"]       = p.get("mat_acero_t_est")       or ""
        p["mat_contratista"]       = p.get("mat_contratista")       or ""
        # Contract & Oficinas
        p["cont_uso_edificio"]     = p.get("cont_uso_edificio")     or ""
        p["cont_m2_oficinas"]      = p.get("cont_m2_oficinas")      or ""
        p["cont_puestos_trabajo"]  = p.get("cont_puestos_trabajo")  or ""
        p["cont_arquitecto"]       = p.get("cont_arquitecto")       or ""
        p["cont_certificacion"]    = p.get("cont_certificacion")    or ""
        p["cont_entrega_est"]      = p.get("cont_entrega_est")      or ""
        # Flexliving & Hostelería
        p["flex_anno_construccion"]= p.get("flex_anno_construccion")or ""
        p["flex_num_unidades"]     = p.get("flex_num_unidades")     or ""
        p["flex_sup_total_m2"]     = p.get("flex_sup_total_m2")     or ""
        p["flex_uso_anterior"]     = p.get("flex_uso_anterior")     or ""
        p["flex_propietario_tipo"] = p.get("flex_propietario_tipo") or ""
        p["flex_potencial_coliving"]=p.get("flex_potencial_coliving")or ""
        p["flex_dist_metro_min"]   = p.get("flex_dist_metro_min")   or ""
        p["flex_irr_est"]          = p.get("flex_irr_est")          or ""
        # ── Deep-research extra fields ──────────────────────────────────────
        p["const_suelo_contaminado"]= p.get("const_suelo_contaminado") or ""
        p["re_suelo_contaminado"]   = p.get("re_suelo_contaminado")    or ""
        p["re_plazo_urbanizacion"]  = p.get("re_plazo_urbanizacion")   or ""
        p["ind_renta_mercado"]      = p.get("ind_renta_mercado")       or ""
        p["ind_yield_est"]          = p.get("ind_yield_est")           or ""
        p["alq_urgencia"]           = p.get("alq_urgencia")            or ""
        p["alq_jefe_obra"]          = p.get("alq_jefe_obra")           or ""
        p["mat_pluviales_dn_km"]    = p.get("mat_pluviales_dn_km")     or ""
        p["cont_num_plantas"]       = p.get("cont_num_plantas")        or ""
        p["cont_fit_out_presupuesto_est"]=p.get("cont_fit_out_presupuesto_est") or ""

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
    SOURCE 8: BORME — Boletín Oficial del Registro Mercantil.

    Scans for new construction/promotor company registrations in Madrid.
    These are EARLY SIGNALS — new promotores incorporated 12-24 months
    before their projects appear in BOCM.

    Uses www.boe.es/diario_borme/xml.php (same accessible server as SOURCE 5).
    api.boe.es is NOT used — it's unreachable from GitHub Actions (DNS failure).
    The www.boe.es XML endpoint is free, official, and works from all IPs.
    """
    results   = []
    _consec_err = 0   # abort after 5 consecutive connection errors

    # Always scan at least 7 working days to avoid missing BORME days
    from datetime import timedelta as _td_b
    _borme_start = min(date_from, date_to - _td_b(days=10))
    if (date_to - _borme_start).days > 14:
        _borme_start = date_to - _td_b(days=14)  # cap at 14 days to avoid timeout
    d = _borme_start
    while d <= date_to:
        if d.weekday() >= 5:            # skip weekends
            d += timedelta(days=1); continue
        if not time_ok(need_s=30): break
        if _consec_err >= 5:
            log(f"  📋 BORME: 5 consecutive errors — aborting")
            break

        # BORME XML sumario via www.boe.es (accessible from GitHub Actions)
        borme_url = (f"https://www.boe.es/diario_borme/xml.php"
                     f"?id=BORME-S-{d.strftime('%Y%m%d')}")
        try:
            r = safe_get(borme_url, timeout=20)
            if not r or r.status_code not in (200, 201):
                _consec_err += 1
                d += timedelta(days=1); continue

            _consec_err = 0
            import xml.etree.ElementTree as _ET_B

            # BORME XML can contain malformed entities — try lxml first, then stdlib
            try:
                root = _ET_B.fromstring(r.content)
            except _ET_B.ParseError:
                # Try recovering with lxml if available
                try:
                    from lxml import etree as _lxml_et
                    root = _lxml_et.fromstring(r.content, parser=_lxml_et.XMLParser(recover=True))
                    # Convert to stdlib ET for consistent iteration
                    root = _ET_B.fromstring(_lxml_et.tostring(root))
                except Exception:
                    # Last resort: strip non-XML characters and retry
                    clean = r.content.decode("utf-8", errors="replace")
                    clean = re.sub(r'[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]', '', clean)
                    try:
                        root = _ET_B.fromstring(clean.encode("utf-8"))
                    except Exception:
                        _consec_err += 1
                        d += timedelta(days=1); continue

            # BORME XML structure:
            # <sumario><diario><seccion nombre="SECCIÓN SEGUNDA...">
            #   <departamento nombre="REGISTRO MERCANTIL DE MADRID">
            #     <anuncio id="BORME-A-YYYY-NNNN">
            #       <titulo>CONSTITUCIÓN</titulo> or AMPLIACIÓN CAPITAL, NOMBRAMIENTO...
            #       <url_pdf>...</url_pdf>
            # We want CONSTITUCIÓN + AMPLIACIÓN DE CAPITAL in REGISTRO MERCANTIL DE MADRID

            _INTERESTING_ACTS = {
                "constitución", "constituciones", "ampliación de capital",
                "modificaciones estatutarias", "cambio de objeto social",
            }
            _CONSTRUCT_TERMS = [
                "construccion", "construc", "promoci", "inmobili", "urban",
                "edificac", "obras", "rehab", "reform", "desarrollo",
                "real estate", "inversion", "patrimon", "proyecto",
            ]

            for seccion in root.iter("seccion"):
                sec_nombre = (seccion.get("nombre") or "").lower()
                # Only Sección Segunda (Anuncios y avisos legales — Registro Mercantil)
                # AND Sección Primera (Empresas)
                if "segunda" not in sec_nombre and "primera" not in sec_nombre:
                    continue

                for dep in seccion.iter("departamento"):
                    dep_nombre = (dep.get("nombre") or "").lower()
                    if "madrid" not in dep_nombre and "registro mercantil" not in dep_nombre:
                        continue

                    # BORME XML has two possible structures:
                    # A) Newer: <item tipoanuncio="Constitución" id="BORME-A-...">
                    #           <denominacion>EMPRESA SL</denominacion>
                    # B) Older: <anuncio id="..."><titulo>CONSTITUCIÓN</titulo>
                    #           <texto>EMPRESA SL. ...</texto>
                    # We try A first (more reliable), then B.
                    _items_b = list(dep.iter("item")) or list(dep.iter("anuncio"))

                    for anuncio in _items_b:
                        anuncio_id = anuncio.get("id","")

                        # Act type: try tipoanuncio attr (format A) then <titulo> (format B)
                        tipo_anuncio = (anuncio.get("tipoanuncio") or "").lower()
                        if not tipo_anuncio:
                            _t_el = anuncio.find("titulo") or anuncio.find("acto")
                            tipo_anuncio = (_t_el.text or "").lower() if _t_el is not None else ""

                        # Match act type: exact OR prefix
                        _ACT_PREFIXES = ("constitu", "ampliac", "modificac de objeto",
                                         "cambio de objeto", "transfor")
                        if not any(act in tipo_anuncio for act in _INTERESTING_ACTS) and                            not any(tipo_anuncio.startswith(p) for p in _ACT_PREFIXES):
                            continue

                        # Company name: <denominacion> (format A) or <texto> (format B)
                        empresa = ""
                        for _tag in ("denominacion", "razon_social", "nombre"):
                            _el = anuncio.find(_tag)
                            if _el is not None and _el.text and _el.text.strip():
                                empresa = _el.text.strip()[:120]; break
                        if not empresa:
                            texto_el = anuncio.find("texto")
                            if texto_el is not None and texto_el.text:
                                empresa = texto_el.text.strip().split(".")[0][:80]
                        if not empresa:
                            titulo_el = anuncio.find("titulo")
                            empresa = (titulo_el.text if titulo_el is not None else anuncio_id) or anuncio_id

                        # Filter: construction/RE sector keywords in company name
                        emp_lower = empresa.lower()
                        if not any(ct in emp_lower for ct in _CONSTRUCT_TERMS):
                            continue

                        pdf_el = anuncio.find("url_pdf") or anuncio.get("url_pdf")
                        borme_link = ""
                        if isinstance(pdf_el, str):
                            borme_link = f"https://www.boe.es/{pdf_el.lstrip('/')}" if not pdf_el.startswith("http") else pdf_el
                        elif pdf_el is not None and hasattr(pdf_el, 'text') and pdf_el.text:
                            borme_link = (f"https://www.boe.es/{pdf_el.text.lstrip('/')}"
                                          if not pdf_el.text.startswith("http") else pdf_el.text)

                        results.append({
                            "company":  empresa,
                            "date":     d.strftime("%Y-%m-%d"),
                            "act":      tipo_anuncio.title() or "Constitución",
                            "borme_id": anuncio_id,
                            "url":      borme_link,
                        })

        except Exception as e:
            err_str = str(e)
            if "ConnectionError" in type(e).__name__ or "MaxRetry" in err_str:
                _consec_err += 1
            # Silently skip individual day errors

        d += timedelta(days=1)
        time.sleep(0.5)

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
    """
    Run after each engine cycle — email subscribers when a saved project advances in phase.

    Matches watchlist entries against Leads sheet by:
    1. Formal expediente (e.g. "01/4.778/26")
    2. BOCM-slug key (e.g. "BOCM-20260327") — synthetic key used for projects without expediente
    3. Source URL exact match

    Only sends when phase order has genuinely advanced (uses _PHASE_ORDER).
    Records last_alerted date to avoid duplicate emails on the same day.
    Also updates phase_at_add in the Watchlist row to the new phase after alerting,
    so the next alert only fires on the NEXT phase change.
    """
    gf = os.environ.get("GMAIL_FROM","")
    gp = os.environ.get("GMAIL_APP_PASSWORD","")
    if not all([gf, gp]):
        log("  ℹ️  GMAIL_FROM / GMAIL_APP_PASSWORD not set — watchlist emails skipped")
        return
    ws_main = get_sheet()
    if not ws_main: return
    try:
        ss   = ws_main.spreadsheet
        wl   = _get_watchlist_tab(ss)
        subs = wl.get_all_records()
        if not subs:
            log("  ℹ️  Watchlist empty — no alerts to send")
            return
        leads = ss.worksheet("Leads").get_all_records()

        # Build lookups: expediente → latest row, BOCM_slug → latest row, url → row
        exp_map  = {}   # expediente str → latest lead row
        slug_map = {}   # BOCM-YYYYMMDD → latest lead row
        url_map  = {}   # source URL → lead row

        import re as _re
        for row in leads:
            exp  = str(row.get("Expediente","") or "").strip()
            lurl = str(row.get("Source URL","")  or "").strip()
            if exp:
                # Keep most phase-advanced row for each expediente
                _PHASE_ORDER = {"solicitud":1,"inicial":2,"en_tramite":2,"definitivo":3,
                                "licitacion":4,"adjudicacion":5,"en_obra":6,"primera_ocupacion":7}
                if exp not in exp_map:
                    exp_map[exp] = row
                else:
                    p_new = _PHASE_ORDER.get(str(row.get("Phase","")).lower(),0)
                    p_old = _PHASE_ORDER.get(str(exp_map[exp].get("Phase","")).lower(),0)
                    if p_new > p_old:
                        exp_map[exp] = row
            if lurl:
                url_map[lurl] = row
                slug_m = _re.search(r'BOCM[-_](\d{8})', lurl, _re.I)
                if slug_m:
                    key = f"BOCM-{slug_m.group(1)}"
                    if key not in slug_map:
                        slug_map[key] = row
                    else:
                        p_new = _PHASE_ORDER.get(str(row.get("Phase","")).lower(),0)
                        p_old = _PHASE_ORDER.get(str(slug_map[key].get("Phase","")).lower(),0)
                        if p_new > p_old:
                            slug_map[key] = row

        _PHASE_ORDER = {"solicitud":1,"inicial":2,"en_tramite":2,"definitivo":3,
                        "licitacion":4,"adjudicacion":5,"en_obra":6,"primera_ocupacion":7}
        _PHASE_LABELS = {
            "inicial":         "🟡 Aprobación Inicial",
            "definitivo":      "🟢 Aprobación Definitiva",
            "licitacion":      "🔵 Licitación activa",
            "adjudicacion":    "🏆 Adjudicación",
            "en_obra":         "🏗️ Obra en ejecución",
            "primera_ocupacion":"⚪ Primera Ocupación",
            "en_tramite":      "📋 En tramitación",
        }
        today_s = datetime.now().strftime("%Y-%m-%d")
        updates = []  # (row_index, new_last_alerted, new_phase_at_add)

        for i, sub in enumerate(subs, start=2):
            email   = str(sub.get("email","") or "").strip()
            expd    = str(sub.get("expediente","") or "").strip()
            p_add   = str(sub.get("phase_at_add","") or "").strip()
            p_last  = str(sub.get("last_alerted","") or "").strip()
            sub_url = str(sub.get("source_url","") or "").strip()
            if not email or p_last == today_s: continue

            # Find matching lead — try all lookup strategies
            cur = (exp_map.get(expd)
                   or slug_map.get(expd)
                   or url_map.get(sub_url))
            if not cur: continue

            p_cur = str(cur.get("Phase","") or "").strip()
            if _PHASE_ORDER.get(p_cur.lower(),0) <= _PHASE_ORDER.get(p_add.lower(),0):
                continue  # phase hasn't advanced

            pl = _PHASE_LABELS.get(p_cur.lower(), p_cur.capitalize())
            pl_old = _PHASE_LABELS.get(p_add.lower(), p_add.capitalize() or "—")
            muni_s  = cur.get("Municipality","") or ""
            pem_raw = cur.get("Declared Value PEM (€)","")
            pem_s   = f"€{float(str(pem_raw).replace(',','').replace('.','')):.0f}" if pem_raw else "No declarado"
            bocm_url = cur.get("Source URL","") or ""

            subj = f"🔔 PlanningScout — {muni_s}: {expd} ha avanzado a {pl}"
            body = f"""<html><body style="font-family:'Arial',sans-serif;max-width:600px;margin:24px auto;color:#1a1a2e;">
<div style="background:#1e3a5f;color:#fff;padding:20px 28px;border-radius:10px 10px 0 0;">
  <h2 style="margin:0;font-size:20px;font-weight:700;">🔔 Tu proyecto ha avanzado</h2>
  <p style="margin:6px 0 0;font-size:13px;opacity:.8;">PlanningScout Madrid — Alerta automática</p>
</div>
<div style="border:1px solid #e2e8f0;border-top:none;padding:24px 28px;border-radius:0 0 10px 10px;background:#fff;">
  <table style="width:100%;font-size:14px;border-collapse:collapse;">
    <tr><td style="color:#94a3b8;padding:6px 0;width:130px;">Expediente</td><td style="font-weight:600;color:#0d1a2b;">{expd}</td></tr>
    <tr><td style="color:#94a3b8;padding:6px 0;">Municipio</td><td style="color:#334155;">{muni_s}</td></tr>
    <tr><td style="color:#94a3b8;padding:6px 0;">PEM</td><td style="color:#334155;">{pem_s}</td></tr>
  </table>
  <div style="background:#f0fdf4;border-left:4px solid #16a34a;padding:14px 18px;margin:18px 0;border-radius:6px;">
    <div style="font-size:16px;font-weight:700;color:#15803d;">{pl}</div>
    <div style="font-size:12px;color:#64748b;margin-top:4px;">Antes: {pl_old}</div>
  </div>
  <p style="font-size:13px;color:#64748b;line-height:1.6;">{str(cur.get('Description',''))[:220]}…</p>
  {'<a href="' + bocm_url + '" style="display:inline-block;background:#1e3a5f;color:#fff;padding:11px 22px;border-radius:8px;text-decoration:none;font-size:14px;font-weight:600;margin-top:8px;">↗ Ver en BOCM</a>' if bocm_url else ''}
  <p style="font-size:11px;color:#aaa;margin-top:24px;border-top:1px solid #f1f5f9;padding-top:14px;">
    Para cancelar esta alerta visita tu panel en <a href="https://planningscout.streamlit.app" style="color:#1e3a5f;">planningscout.streamlit.app</a> → Mis alertas.<br>
    O escríbenos a <a href="mailto:info@planningscout.com" style="color:#1e3a5f;">info@planningscout.com</a>
  </p>
</div>
</body></html>"""
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
                updates.append((i, today_s, p_cur))
            except Exception as e2:
                log(f"  ⚠️ Alert send error: {e2}")

        if updates:
            try:
                # Update last_alerted (col F) and phase_at_add (col E) in one batch
                wl_hdrs = wl.row_values(1)
                f_col = wl_hdrs.index("last_alerted") + 1  if "last_alerted"  in wl_hdrs else 6
                e_col = wl_hdrs.index("phase_at_add") + 1  if "phase_at_add"  in wl_hdrs else 5
                batch = []
                for r, today_str, new_phase in updates:
                    batch.append({"range": f"Watchlist!{chr(64+f_col)}{r}", "values": [[today_str]]})
                    batch.append({"range": f"Watchlist!{chr(64+e_col)}{r}", "values": [[new_phase]]})
                wl.spreadsheet.values_batch_update({
                    "valueInputOption": "USER_ENTERED",
                    "data": batch,
                })
                log(f"  ✅ {len(updates)} watchlist rows updated")
            except Exception as _ue:
                log(f"  ⚠️ Watchlist update error: {_ue}")
        else:
            log("  ✅ No phase changes detected — no alerts sent")
    except Exception as e:
        log(f"  ⚠️ Watchlist alerts: {e}")


# ════════════════════════════════════════════════════════════════════════
# SOURCE 9: Plataforma de Contratación del Estado (PLACE)
# ════════════════════════════════════════════════════════════════════════
# This is the national procurement platform — FCC, Kiloutou, Molecor ALL
# monitor this daily. It covers state and autonomous community tenders >€30K.
# ATOM feed is publicly available without authentication.
# CPV code 45 = Construction; 44163000 = pipes/fittings; 43000000 = machinery.
# ────────────────────────────────────────────────────────────────────────
def search_place_national(date_from, date_to):
    """
    Scan Plataforma de Contratación del Estado ATOM feed for Madrid-area
    construction, infrastructure and supply tenders.
    Returns list of (url, title, summary, pem) tuples.
    """
    # ── PLACE feed strategy ────────────────────────────────────────────────────
    # contrataciondelestado.es and contrataciondelsectorpublico.gob.es: BOTH return
    # malformed XML or HTML error pages from GitHub Actions IPs (WAF blocked).
    # 
    # WORKING alternative: re-use the contratos-publicos.comunidad.madrid domain
    # (same domain as Source 6 which consistently works) for adjudicaciones.
    # These include state-level tenders relevant to Madrid that the CM portal re-publishes.
    # PLACE feed strategy:
    # contrataciondelsectorpublico.gob.es is WAF-blocked from GitHub Actions (403).
    # Use CM portal feeds only (same domain as Source 6 — confirmed working).
    # CM portal re-publishes both regional AND national tenders relevant to Madrid.
    PLACE_FEEDS = [
        "https://contratos-publicos.comunidad.madrid/feed/licitaciones2",   # primary
        "https://contratos-publicos.comunidad.madrid/feed/licitaciones",    # fallback
        "https://contratos-publicos.comunidad.madrid/feed/adjudicaciones2", # adjudicaciones
        "https://contratos-publicos.comunidad.madrid/feed/adjudicaciones",  # fallback
        "https://contratos-publicos.comunidad.madrid/feed/contratos",       # all types
    ]
    # Madrid-area entities and CPV codes that matter
    _MADRID_ENTITIES = [
        "madrid","comunidad de madrid","ayuntamiento de","canal de isabel",
        "adif","metro de madrid","renfe","ministerio de fomento",
        "ministerio de transportes","ministerio de vivienda",
    ]
    _CONST_KEYWORDS = [
        "obra", "construcción", "urbanización", "rehabilitación", "reforma",
        "saneamiento", "abastecimiento", "infraestructura", "vial", "colector",
        "tubería", "demolición", "cimentación", "excavación", "edificio",
        "nave", "instalación", "pavimentación", "ampliación",
    ]
    results = []
    seen_urls: set = set()

    for feed_url in PLACE_FEEDS:
        if not time_ok(need_s=30): break
        try:
            r = safe_get(feed_url, timeout=25)
            if not r or r.status_code != 200: continue

            from xml.etree import ElementTree as _ET

            # Multi-strategy parse: handles RSS 2.0 (CM portal), ATOM, and malformed feeds.
            # CM contratos-publicos.comunidad.madrid returns RSS 2.0, not ATOM.
            raw_text  = r.content.decode("utf-8", errors="replace")
            _is_rss   = "<rss" in raw_text[:400] or "rss version" in raw_text[:400].lower()
            root      = None
            for _strat in ("direct", "lxml", "amp_fix"):
                if root is not None: break
                try:
                    if _strat == "direct":
                        root = _ET.fromstring(r.content)
                    elif _strat == "lxml":
                        from lxml import etree as _lxml_et
                        _lr = _lxml_et.fromstring(r.content,
                                parser=_lxml_et.XMLParser(recover=True))
                        root = _ET.fromstring(
                            _lxml_et.tostring(_lr, encoding="unicode").encode("utf-8"))
                    elif _strat == "amp_fix":
                        _c2 = re.sub(r'&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[\da-fA-F]+);)',
                                     "&amp;", raw_text)
                        root = _ET.fromstring(_c2.encode("utf-8"))
                except Exception:
                    pass
            if root is None:
                log(f"  ⚠️  PLACE: XML parse failed after 3 strategies — skipping {feed_url[:60]}")
                continue

            _NS_ATOM   = {"a": "http://www.w3.org/2005/Atom"}
            if _is_rss:
                entries = root.findall(".//item") or []
            else:
                entries = (root.findall(".//a:entry", _NS_ATOM) or
                           root.findall(".//entry") or
                           root.findall(".//item"))
            if not entries:
                continue

            # entries already parsed above via multi-strategy parser — proceed directly

            for entry in entries:
                def _g(tag):
                    # Try Atom namespace, then plain, then Dublin Core
                    for ns in ("http://www.w3.org/2005/Atom", ""):
                        _t = f"{{{ns}}}{tag}" if ns else tag
                        el = entry.find(_t)
                        if el is not None: return (el.text or "").strip()
                    return ""

                title   = _g("title")
                # RSS 2.0 uses <link> as text content; ATOM uses href attribute
                link_el = (entry.find("{http://www.w3.org/2005/Atom}link") or
                           entry.find("link"))
                if link_el is not None:
                    url = (link_el.get("href","") or link_el.text or "").strip()
                else:
                    url = ""
                summary = _g("description") or _g("summary") or _g("content")
                pub     = _g("pubDate") or _g("published") or _g("updated") or _g("dc:date")

                if not url or url in seen_urls: continue
                if not title: continue

                # Date filter
                if pub:
                    try:
                        from dateutil import parser as _dp2
                        pd = _dp2.parse(pub).replace(tzinfo=None)
                        if pd.date() < (date_from - timedelta(days=7)).date(): continue
                    except Exception: pass

                combined = (title + " " + summary).lower()

                # Madrid filter
                if not any(m in combined for m in _MADRID_ENTITIES):
                    continue

                # Construction keyword filter
                if not any(k in combined for k in _CONST_KEYWORDS):
                    continue

                # Extract budget from summary (PLACE embeds importe)
                pem = 0.0
                pem_m = re.search(r"(?:importe|valor|presupuesto)[^0-9]*([0-9][0-9.,]+)", combined)
                if pem_m:
                    try:
                        raw_p = pem_m.group(1).replace(".","").replace(",",".")
                        pem = float(raw_p)
                    except Exception: pass

                seen_urls.add(url)
                results.append((url, title, summary[:500], pem))

        except Exception as _e:
            log(f"  ⚠️  PLACE feed error: {_e}")
            continue

    return results

def search_sede_madrid_obras(date_from, date_to) -> list:
    """
    SOURCE 10: Licencias urbanísticas del Ayuntamiento de Madrid — complemento de Source 7.

    Source 7 downloads the quarterly XLSX. Source 10 queries the CKAN API for
    the same dataset to get entries NOT yet in the XLSX (more recent).

    Uses DATOS_MADRID_PROXY (same Cloudflare Worker as Source 7) for reliability.
    Tries multiple resource IDs (Madrid increments these yearly).

    Returns list of (exp, rec_dict, source_url, profile_hint) tuples.
    """
    results  = []
    if not time_ok(need_s=30): return results

    import urllib.parse as _up10

    _CKAN_BASE     = "https://datos.madrid.es"
    # Madrid increments resource IDs yearly: 300193-1=2024, 300193-2=2025, 300193-3=2026
    _RESOURCE_IDS  = [
        f"300193-{date_to.year - 2024 + 2}",  # most likely current year
        f"300193-{date_to.year - 2024 + 1}",  # previous year
        "300193-2", "300193-3",               # known IDs
    ]

    _TIPO_VALUABLE = {
        "primera ocupacion","primera ocupación","cambio de uso","cambio de destino",
        "obra mayor","rehabilitacion integral","rehabilitación integral",
        "licencia urbanistica de actividad","licencia urbanística de actividad",
        "demolicion","demolición","urbanizacion","urbanización",
        "obra mayor nueva planta","obra mayor rehabilitacion","licencia de obras",
        "declaracion responsable","declaración responsable",
    }

    _d_from_str = (date_from.strftime("%Y-%m-%d") if hasattr(date_from,"strftime")
                   else str(date_from)[:10])

    def _ckan_fetch(url):
        """Fetch CKAN URL via proxy if configured, else direct via safe_get."""
        if DATOS_MADRID_PROXY and "datos.madrid.es" in url:
            _px = f"{DATOS_MADRID_PROXY}?url={_up10.quote(url, safe='')}"
            try:
                _pr = requests.get(_px, timeout=25, verify=False,
                                   headers={"User-Agent":"PlanningScout/1.0","Accept":"*/*"})
                if _pr.status_code == 200: return _pr
            except Exception: pass
        return safe_get(url, timeout=20)

    _got_data = False
    for _RID in dict.fromkeys(_RESOURCE_IDS):  # deduplicate while preserving order
        if _got_data or not time_ok(need_s=15): break
        for _offset in (0, 1000, 2000):
            if not time_ok(need_s=12): break
            _url = (f"{_CKAN_BASE}/api/3/action/datastore_search"
                    f"?resource_id={_RID}&limit=1000&offset={_offset}&sort=_id+desc")
            try:
                r = _ckan_fetch(_url)
                if not r or r.status_code != 200: break
                try:
                    _data = r.json()
                except Exception:
                    break
                if not _data.get("success"): break
                _records = _data.get("result", {}).get("records", [])
                if not _records: break

                _batch = 0
                for rec in _records:
                    fecha_raw = str(rec.get("FECHA_CONCESION") or
                                    rec.get("Fecha concesión") or "")[:10]
                    if fecha_raw and fecha_raw < _d_from_str: continue

                    tipo = str(rec.get("TIPO_EXPEDIENTE") or
                               rec.get("Tipo de expediente") or "").strip()
                    if tipo.lower() not in _TIPO_VALUABLE:
                        if not any(t in tipo.lower() for t in
                                   ["obra mayor","cambio de uso","primera ocupaci","rehab"]):
                            continue

                    addr = " ".join(filter(None, [
                        rec.get("NOMBRE_VIA") or rec.get("Nombre Via") or "",
                        rec.get("NUM_VIA")    or rec.get("Número")     or "",
                        rec.get("DISTRITO")   or rec.get("Distrito")   or "",
                        rec.get("BARRIO")     or rec.get("Barrio")     or "",
                    ])).strip().title()

                    exp_raw = str(rec.get("EXPEDIENTE") or
                                  rec.get("Número de expediente") or "").strip()
                    if not exp_raw:
                        exp_raw = f"SEDE10-{abs(hash(addr+tipo+fecha_raw))%10**8}"

                    try:
                        pem_val = float(str(rec.get("PEM") or 0).replace(",","."))
                    except Exception:
                        pem_val = 0

                    _src = (f"https://sede.madrid.es/portal/site/tramites/menuitem"
                            f".62876cb64654a55e2dbd7003a8a409a0/?q={_up10.quote(addr[:50])}")
                    results.append((exp_raw,
                        {"TIPO_EXPEDIENTE":tipo,"DIRECCION":addr,
                         "DISTRITO":str(rec.get("DISTRITO") or ""),
                         "FECHA_OTORGAMIENTO":fecha_raw,"PEM":pem_val,
                         "EXPEDIENTE":exp_raw,"INTERESADO":"Persona jurídica",
                         "BARRIO":str(rec.get("BARRIO") or ""),"PROCEDIMIENTO":tipo},
                        _src, "mep+constructora+hospe+retail+actiu+alquiler"))
                    _batch += 1

                if _batch > 0: _got_data = True
                if _batch == 0 and _offset > 0: break
                if len(_records) < 1000: break

            except Exception as _ce:
                log(f"  ⚠️  Sede Madrid GIS (CKAN {_RID} @{_offset}): {_ce}")
                break

    if results:
        log(f"  🏛️  Sede Madrid GIS: {len(results)} licencias (CKAN)")
    elif not _got_data:
        log(f"  🏛️  Sede Madrid GIS: CKAN unavailable — Source 7 covers Madrid capital")
    return results

def _proc_ckan_records(records: list, results: list, valid_tipos: set):
    """Process CKAN datastore records into (exp, rec, source_url, profile_hint) tuples."""
    seen = set()
    for i, row in enumerate(records):
        tipo = str(row.get("Tipo de expediente","") or "").strip()
        if not tipo or tipo.lower() not in valid_tipos: continue
        tipo_via = str(row.get("Tipo Via","")    or "").strip()
        nombre   = str(row.get("Nombre Via","")  or "").strip()
        numero   = str(row.get("Número","")      or "").strip()
        dist     = str(row.get("Descripción Distrito","") or "").strip().title()
        barrio   = str(row.get("Descripción Barrio","")   or "").strip().title()
        fecha    = str(row.get("Fecha concesión","") or "").strip()
        interesado = str(row.get("Interesado","") or "").strip()
        try: numero = str(int(float(numero))) if numero and numero!="nan" else ""
        except: numero = ""
        addr = " ".join(filter(None, [tipo_via, nombre, numero])).strip().title()
        if dist: addr += f", {dist}"
        if barrio and barrio != dist: addr += f" ({barrio})"
        if not addr and dist: addr = f"Madrid - {dist}"
        if not addr: continue
        exp = f"CKAN-{i}"
        if exp in seen: continue
        seen.add(exp)
        rec = {"TIPO_EXPEDIENTE": tipo, "DIRECCION": addr, "DISTRITO": dist,
               "FECHA_OTORGAMIENTO": fecha, "PEM": None, "EXPEDIENTE": exp,
               "INTERESADO": interesado, "BARRIO": barrio, "PROCEDIMIENTO": ""}
        q = f"{nombre}+{numero}+Madrid".replace(" ","+")
        src = f"https://sede.madrid.es/portal/site/tramites/menuitem.62876cb64654a55e2dbd7003a8a409a0/?vgnextoid=fa3a74&q={q}"
        results.append((exp, rec, src, "mep+constructora+hospe+retail+actiu+alquiler"))


def _proc_arcgis_features(features: list, results: list, valid_tipos: set):
    """Process ArcGIS FeatureServer features into result tuples."""
    seen = set()
    for feat in features:
        attrs = feat.get("attributes", {})
        tipo  = str(attrs.get("TipoExpediente","") or attrs.get("TIPO","") or "").strip()
        if not tipo or tipo.lower() not in valid_tipos: continue
        addr  = str(attrs.get("Direccion","") or attrs.get("DIRECCION","") or "").strip().title()
        dist  = str(attrs.get("Distrito","")  or attrs.get("DISTRITO","")  or "").strip().title()
        pem   = attrs.get("Presupuesto") or attrs.get("PRESUPUESTO") or 0
        try:   pem = float(str(pem).replace(",",".")) if pem else 0
        except: pem = 0
        fecha_ms = attrs.get("FechaConcesion") or 0
        fecha_s  = ""
        if fecha_ms:
            try:
                from datetime import timezone as _tz
                fecha_s = datetime.fromtimestamp(fecha_ms/1000, tz=_tz.utc).strftime("%Y-%m-%d")
            except: pass
        exp_raw = str(attrs.get("NumExpediente","") or attrs.get("EXPEDIENTE","") or "").strip()
        if not exp_raw: exp_raw = f"SEDE-MAD-{abs(hash(addr+tipo))%10**8}"
        if exp_raw in seen: continue
        seen.add(exp_raw)
        rec = {"TIPO_EXPEDIENTE": tipo, "DIRECCION": addr, "DISTRITO": dist,
               "FECHA_OTORGAMIENTO": fecha_s, "PEM": pem, "EXPEDIENTE": exp_raw,
               "INTERESADO": "Persona jurídica", "BARRIO": "", "PROCEDIMIENTO": ""}
        src = (f"https://sede.madrid.es/portal/site/tramites/menuitem"
               f".62876cb64654a55e2dbd7003a8a409a0/?vgnextoid=fa3a74&q={addr.replace(' ','+')}")
     
        results.append((exp_raw, rec, src, "mep+constructora+hospe+retail+actiu+alquiler"))
def _compute_phase_velocity(prev_phase: str, new_phase: str,
                            fecha_added: str, today_str: str) -> str:
    """
    Returns "⚡ FAST TRACK (<60 días inicial→definitivo)" if the project
    moved from inicial to a more advanced phase in under 60 days.
    Used to surface the most time-critical projects for Alquiler Maquinaria
    and Compras / Materiales who need to act before obra starts.
    """
    _PHASE_ORDER = {
        "solicitud": 0, "en_tramite": 1, "inicial": 2,
        "definitivo": 3, "licitacion": 4, "adjudicacion": 5,
        "en_obra": 6, "primera_ocupacion": 7,
    }
    if not (prev_phase and new_phase and fecha_added): return ""
    if _PHASE_ORDER.get(new_phase, -1) <= _PHASE_ORDER.get(prev_phase, -1): return ""
    # Only flag if advanced by ≥2 stages
    if _PHASE_ORDER.get(new_phase, 0) - _PHASE_ORDER.get(prev_phase, 0) < 1: return ""
    try:
        from datetime import datetime
        d0 = datetime.strptime(fecha_added[:10], "%Y-%m-%d")
        d1 = datetime.strptime(today_str[:10],   "%Y-%m-%d")
        delta_days = (d1 - d0).days
        if delta_days <= 60:
            return f"⚡ FAST TRACK ({delta_days} días {prev_phase}→{new_phase})"
        elif delta_days <= 120:
            return f"🚀 Avance rápido ({delta_days} días {prev_phase}→{new_phase})"
    except Exception:
        pass
    return ""




# ── M30 distance lookup for Industrial/Log profile ───────────────────────────
# Distance from M30 ring road centroid (40.4168, -3.7038) to logistics hubs.
# Used to populate "km_m30" column for Industrial/Log filtering.
# Source: Haversine distance computed from verified GPS centroids.
_M30_KM = {
    # Prime logistics belt — <25km from M30
    "getafe":           14, "leganés":          11, "fuenlabrada":      17,
    "móstoles":         19, "alcorcón":         13, "parla":            22,
    "valdemoro":        29, "pinto":             24, "seseña":           41,
    "coslada":          12, "san fernando de henares": 18, "torrejón de ardoz": 22,
    "alcalá de henares": 32, "arganda del rey":  26, "rivas-vaciamadrid": 18,
    "madrid":            5, "majadahonda":       16, "las rozas":        22,
    "pozuelo de alarcón": 13, "boadilla del monte": 18,
    # Secondary belt — 25-50km
    "villaverde":       10, "vallecas":          9,  "villaviciosa de odón": 25,
    "illescas":         44, "ciempozuelos":      35, "arroyomolinos":    26,
    "navalcarnero":     35, "brunete":           27, "collado villalba": 36,
    "tres cantos":      24, "alcobendas":        16, "san sebastián de los reyes": 20,
    "colmenar viejo":   34, "paracuellos de jarama": 18,
}

def _km_from_m30(municipality: str) -> str:
    """Return distance from M30 ring road to municipality centroid, or empty string."""
    if not municipality: return ""
    key = municipality.lower().strip()
    km  = _M30_KM.get(key, "")
    return f"{km} km" if km else ""

def _compute_phase_velocity(prev_phase: str, new_phase: str,
                            fecha_added: str, today_str: str) -> str:
    """
    Returns "⚡ FAST TRACK (<60 días inicial→definitivo)" if the project
    moved from inicial to a more advanced phase in under 60 days.
    Used to surface the most time-critical projects for Alquiler Maquinaria
    and Compras / Materiales who need to act before obra starts.
    """
    _PHASE_ORDER = {
        "solicitud": 0, "en_tramite": 1, "inicial": 2,
        "definitivo": 3, "licitacion": 4, "adjudicacion": 5,
        "en_obra": 6, "primera_ocupacion": 7,
    }
    if not (prev_phase and new_phase and fecha_added): return ""
    if _PHASE_ORDER.get(new_phase, -1) <= _PHASE_ORDER.get(prev_phase, -1): return ""
    # Only flag if advanced by ≥2 stages
    if _PHASE_ORDER.get(new_phase, 0) - _PHASE_ORDER.get(prev_phase, 0) < 1: return ""
    try:
        from datetime import datetime
        d0 = datetime.strptime(fecha_added[:10], "%Y-%m-%d")
        d1 = datetime.strptime(today_str[:10],   "%Y-%m-%d")
        delta_days = (d1 - d0).days
        if delta_days <= 60:
            return f"⚡ FAST TRACK ({delta_days} días {prev_phase}→{new_phase})"
        elif delta_days <= 120:
            return f"🚀 Avance rápido ({delta_days} días {prev_phase}→{new_phase})"
    except Exception:
        pass
    return ""


def create_or_update_profile_tabs(sh):
    """
    Create/refresh profile-specific tabs in Google Sheets.

    Each profile tab is a filtered VIEW of the Leads tab:
    - Only rows where Profile Fit contains the profile key(s)
    - Only the base columns + that profile's sector columns
    - Sorted by Lead Score descending
    - Applied min_score and min_pem filters

    This lets the dashboard:
    1. Load faster (only relevant rows per profile)
    2. Display clean profile-specific columns without showing empty ones
    3. Give each client a "their" tab they can bookmark or export directly

    Called at the end of every engine run.
    """
    try:
        leads_ws = sh.worksheet("Leads")
        all_rows = leads_ws.get_all_values()
        if len(all_rows) < 2:
            log("  ℹ️  Profile tabs: Leads tab empty — skipping")
            return

        header_row = all_rows[0]
        data_rows  = all_rows[1:]

        # Build column index for each HDRS key
        col_idx = {h: i for i, h in enumerate(header_row) if h}

        # Profile Fit column index
        pf_col = col_idx.get("Profile Fit", None)
        sc_col = col_idx.get("Lead Score", None)
        pem_col= col_idx.get("Declared Value PEM (€)", None)

        if pf_col is None:
            log("  ⚠️  Profile tabs: 'Profile Fit' column not found in Leads")
            return

        for prof_key, conf in PROFILE_TABS.items():
            try:
                tab_name     = conf["tab_name"]
                prof_keys    = conf["profile_keys"]
                sector_cols  = conf["sector_cols"]
                min_score    = conf["min_score"]
                min_pem      = conf["min_pem"]

                # Columns for this tab: base + sector
                tab_cols = HDRS_BASE + sector_cols
                tab_col_indices = [col_idx.get(c) for c in tab_cols]
                tab_col_indices = [i for i in tab_col_indices if i is not None]

                # Filter rows
                filtered = []
                for row in data_rows:
                    if len(row) <= pf_col: continue
                    pf_val = str(row[pf_col]).lower()
                    # Check if any of this profile's keys appear in Profile Fit
                    if not any(pk in pf_val for pk in prof_keys): continue
                    # Score filter
                    if sc_col is not None:
                        try:
                            if int(row[sc_col] or 0) < min_score: continue
                        except: pass
                    # PEM filter
                    if pem_col is not None and min_pem > 0:
                        try:
                            pem_v = float(str(row[pem_col]).replace("€","").replace(",","").strip() or 0)
                            if pem_v < min_pem: continue
                        except: pass
                    # Extract only the relevant columns
                    filtered_row = [(row[i] if i < len(row) else "") for i in tab_col_indices]
                    filtered.append(filtered_row)

                # Sort by score descending
                if sc_col is not None and sc_col in tab_col_indices:
                    sort_idx = tab_col_indices.index(sc_col)
                    filtered.sort(key=lambda r: int(r[sort_idx] or 0) if sort_idx < len(r) else 0,
                                  reverse=True)

                # Build header row for this tab
                tab_headers = [header_row[i] for i in tab_col_indices]

                # Get or create the tab
                try:
                    tab_ws = sh.worksheet(tab_name)
                    tab_ws.clear()
                except Exception:
                    tab_ws = sh.add_worksheet(tab_name, rows=max(500, len(filtered)+10),
                                              cols=len(tab_headers)+2)

                # Write header + data
                all_tab_data = [tab_headers] + filtered
                tab_ws.update(all_tab_data, "A1")

                # Color header row navy
                tab_ws.format("A1:ZZ1", {
                    "backgroundColor": {"red": 0.118, "green": 0.227, "blue": 0.373},
                    "textFormat": {"foregroundColor": {"red":1,"green":1,"blue":1},
                                   "bold": True, "fontSize": 10},
                })

                log(f"  📊 Profile tab '{tab_name}': {len(filtered)} rows × {len(tab_headers)} cols")

            except Exception as tab_e:
                log(f"  ⚠️  Profile tab '{conf['tab_name']}': {tab_e}")

    except Exception as e:
        log(f"  ⚠️  create_or_update_profile_tabs: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ADDITIONAL SOURCES IDENTIFIED (to implement in next sprint):
# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 11: Portal de Suelo CM (comunidad.madrid/inversion/portal-suelo-40)
#   - Lists plots available for sale/concession owned by CM
#   - Relevant: Promotores/RE + Industrial/Log
#   - Format: HTML scrape → extract plot ID, m², use, contracting portal link
#   - Already confirmed working (HTTP 200)
#
# SOURCE 12: INE Population API (ine.es/jaxiT3/Tabla.htm?t=2879)
#   - Municipal population data updated annually
#   - Relevant: Expansión Retail (catchment), Flexliving (demand assessment)
#   - API: api.ine.es/jsonstat-suite/getTable/json?query=...
#   - Use to VALIDATE retail_pob_futura_est against current municipal population
#
# SOURCE 13: Catastro Virtual (sede.catastro.gob.es OVCCallejero)
#   - Building age, floors, use classification per address
#   - API: ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero
#   - Relevant: Flexliving (anno_construccion), MEP (plantas), Contract (m²)
#   - Already partially implemented in SOURCE 10 (GIS) — extend to Catastro
#
# SOURCE 14: BOE XML API (boe.es/buscar/xml.php?coleccion=boe)
#   - National-level tenders and urbanismo not covered by BOCM
#   - Relevant: Gran Infraestructura (ADIF, MITMA), Compras/Materiales
#   - API format: XML REST, publicly available
#
# SOURCE 15: datos.madrid.es Licencias de Actividad API
#   - Retail: competitor mapping per zone (which chains are already there)
#   - Format: JSON API, already discovered in SOURCE 7 (XLSX)
#   - Use to fill retail_competencia_1km automatically
# ══════════════════════════════════════════════════════════════════════════════


def catastro_enrich(address: str, municipality: str = "Madrid") -> dict:
    """
    Query the Catastro REST API to get building metadata for a given address.
    Returns a dict with: anno_construccion, num_plantas, sup_m2, ref_catastral, uso_catastral
    
    API: https://ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/rest/
    Endpoint: Consulta_DNPLOC (search by location: tipo_via, nombre_via, numero, municipio)
    
    Returns {} on any error (non-blocking — enrichment is best-effort only).
    
    USAGE: catastro_enrich("Calle Mayor, 14", "Getafe")
    → {"anno_construccion": "1978", "num_plantas": "6", "sup_m2": "3240", 
       "ref_catastral": "0606914VK3600N0001EJ", "uso_catastral": "Residencial"}
    """
    import re as _re
    result = {}
    if not address: return result
    
    try:
        # Parse address components
        # Patterns: "Calle Mayor 14, Getafe" | "Av. de la Paz, 21" | "C/ Fuente, 3"
        _tipo_map = {
            "calle": "CL", "c/": "CL", "cl": "CL",
            "avenida": "AV", "av.": "AV", "av ": "AV",
            "paseo": "PG", "plaza": "PZ", "pl.": "PZ",
            "camino": "CM", "carretera": "CR", "ronda": "RD",
            "bulevar": "BL", "travesía": "TV", "glorieta": "GL",
        }
        addr_l = address.lower().strip()
        tipo_via = "CL"  # default
        via_nombre = address
        via_num = "S/N"
        
        for prefix, code in _tipo_map.items():
            if addr_l.startswith(prefix):
                tipo_via = code
                via_nombre = address[len(prefix):].strip().lstrip("/").strip()
                break
        
        # Extract number from address
        num_m = _re.search(r'(\d+)', via_nombre)
        if num_m:
            via_num = num_m.group(1)
            via_nombre = via_nombre[:num_m.start()].strip().rstrip(",").strip()
        
        # Clean municipality
        muni_q = municipality.replace("Madrid Capital", "Madrid").strip()
        
        # Catastro REST endpoint — no auth required, public API
        _CAT_BASE = "https://ovc.catastro.meh.es/OVCServWeb/OVCWcfCallejero/COVCCallejero.svc/rest"
        url = (f"{_CAT_BASE}/Consulta_DNPLOC"
               f"?Provincia=Madrid&Municipio={muni_q}"
               f"&TipoVia={tipo_via}&NomVia={via_nombre}&Numero={via_num}"
               f"&PC1=&PC2=")
        
        r = safe_get(url, timeout=8)
        if not r or r.status_code != 200: return result
        
        import xml.etree.ElementTree as _ET
        root = _ET.fromstring(r.content)
        
        # Extract from Catastro response XML
        # Key elements: debi/locus/loine → bico/bi/dt/lourb/dp = num_plantas
        # bico/bi/debi/ant = anno_construccion
        # bico/bi/debi/sfc = sup_m2 (surface total)
        
        bi = root.find(".//bi")
        if bi is None: return result
        
        # Building year
        ant = bi.find(".//ant")
        if ant is not None and ant.text:
            result["anno_construccion"] = ant.text.strip()
        
        # Number of floors
        np_el = bi.find(".//dp")
        if np_el is not None and np_el.text:
            result["num_plantas"] = np_el.text.strip()
        
        # Surface area
        sfc = bi.find(".//sfc")
        if sfc is not None and sfc.text:
            result["sup_m2"] = sfc.text.strip()
        
        # Cadastral reference
        rc1 = bi.find(".//rc/pc1")
        rc2 = bi.find(".//rc/pc2")
        if rc1 is not None and rc2 is not None:
            result["ref_catastral"] = f"{rc1.text or ''}{rc2.text or ''}".strip()
        
        # Use classification
        luso = bi.find(".//luso")
        if luso is not None and luso.text:
            _uso_map = {
                "R": "Residencial", "O": "Oficinas", "I": "Industrial",
                "C": "Comercial", "T": "Terciario", "E": "Equipamiento",
                "A": "Agrario", "V": "Vial", "Y": "Almacén",
            }
            result["uso_catastral"] = _uso_map.get(luso.text.strip(), luso.text.strip())
        
    except Exception:
        pass  # enrichment is non-blocking
    
    return result


def search_portal_suelo(date_from, date_to) -> list:
    """
    SOURCE 11: Portal del Suelo 4.0 — Available plots in Madrid region.
    
    Data source: datos.comunidad.madrid/dataset/parcelas_portal_suelo
    Format: JSON (confirmed available — CM open data portal)
    
    Returns parcelas available for sale or concession from CM + 97 adhered municipalities.
    KEY insight: this data is ALMOST NEVER captured by BOCM scraping because
    available plots do NOT generate a BOCM publication — they go straight to contracting portal.
    
    Relevant profiles: Promotores/RE, Industrial/Log, Gran Constructora, Expansión Retail
    
    Returns: list of (exp_id, permit_dict) tuples
    """
    results = []
    if not time_ok(need_s=30): return results
    
    # CM Open Data — parcelas portal suelo 4.0
    # CKAN API format confirmed for datos.comunidad.madrid
    # The resource ID for parcelas is known from the CM open data portal.
    _SUELO_URLS = [
        # Direct JSON export (simplest, most reliable)
        "https://datos.comunidad.madrid/catalogo/dataset/parcelas_portal_suelo/resource/parcelas_portal_suelo_json/download/parcelas_portal_suelo.json",
        # CKAN datastore_search API
        "https://datos.comunidad.madrid/api/3/action/datastore_search?resource_id=parcelas_portal_suelo_json&limit=500",
        # Alternative CKAN endpoint
        "https://datos.comunidad.madrid/catalogo/dataset/parcelas_portal_suelo/resource/parcelas_portal_suelo_json",
    ]

    def _suelo_get(url):
        """Fetch Portal Suelo URL via proxy if configured, else direct."""
        if DATOS_MADRID_PROXY and ("comunidad.madrid" in url or "datos.madrid" in url):
            from urllib.parse import quote as _q11
            _px = f"{DATOS_MADRID_PROXY}?url={_q11(url, safe='')}"
            try:
                _pr = requests.get(_px, timeout=25, verify=False,
                                   headers={"User-Agent":"PlanningScout/1.0","Accept":"*/*"})
                if _pr.status_code == 200: return _pr
            except Exception: pass
        return safe_get(url, timeout=25)

    for url in _SUELO_URLS:
        if not time_ok(need_s=20): break
        try:
            r = _suelo_get(url)
            if not r or r.status_code != 200: continue
            
            try:
                data = r.json()
            except Exception:
                continue
            
            # Handle multiple response formats:
            # - Direct JSON: [...] list of objects
            # - CKAN datastore: {"result": {"records": [...], "total": N}}
            # - CKAN package: {"success": true, "result": {"resources": [...]}}
            if isinstance(data, list):
                parcelas = data
            elif isinstance(data, dict):
                if data.get("success") and isinstance(data.get("result"), dict):
                    parcelas = data["result"].get("records", [])
                elif "records" in data:
                    parcelas = data["records"]
                elif "result" in data and isinstance(data["result"], list):
                    parcelas = data["result"]
                else:
                    parcelas = []
            else:
                parcelas = []
            if not parcelas:
                continue
            
            log(f"  🏛️  Portal Suelo: {len(parcelas)} parcelas available")
            
            for i, p in enumerate(parcelas):
                # Extract fields — CM uses both snake_case and Spanish labels
                def _g(*keys):
                    for k in keys:
                        v = p.get(k) or p.get(k.lower()) or p.get(k.upper())
                        if v and str(v).strip() not in ("", "null", "None"): 
                            return str(v).strip()
                    return ""
                
                muni      = _g("municipio", "MUNICIPIO", "municipality")
                uso       = _g("uso_principal", "uso", "USO_PRINCIPAL", "use")
                sup_m2    = _g("superficie_m2", "superficie", "SUPERFICIE_M2", "area_m2")
                edific    = _g("indice_edificabilidad", "edificabilidad", "EDIFICABILIDAD")
                clasif    = _g("clasificacion", "CLASIFICACION", "classification")
                estado    = _g("estado_urbanizacion", "estado", "ESTADO")
                ref_cat   = _g("referencia_catastral", "ref_catastral", "REFERENCIA_CATASTRAL")
                precio    = _g("precio_venta", "precio", "PRECIO_VENTA")
                regimen   = _g("regimen", "REGIMEN", "concession_type")
                contrato_url = _g("url_contratacion", "enlace_contratacion", "URL_CONTRATACION")
                
                if not muni or not uso: continue
                
                # Only Madrid region municipalities
                _MADRID_MUNIS = {
                    "madrid", "getafe", "alcobendas", "alcorcón", "leganés",
                    "torrejón de ardoz", "fuenlabrada", "alcalá de henares",
                    "móstoles", "valdemoro", "parla", "coslada", "pozuelo",
                    "majadahonda", "las rozas", "tres cantos", "san sebastián de los reyes",
                    "rivas-vaciamadrid", "villaviciosa de odón", "villanueva de la cañada",
                    "boadilla del monte", "pinto", "arganda del rey", "humanes",
                }
                if not any(m in muni.lower() for m in _MADRID_MUNIS):
                    continue
                
                # Determine profile fit from uso
                uso_l = uso.lower()
                if any(k in uso_l for k in ["industrial", "logíst", "almacén", "productiv"]):
                    profile_hint = "industrial+constructora+materiales"
                    tipo = "suelo industrial disponible"
                elif any(k in uso_l for k in ["residencial", "vivienda", "vpo"]):
                    profile_hint = "promotores+constructora+retail"
                    tipo = "suelo residencial disponible"
                elif any(k in uso_l for k in ["terciario", "comercial", "oficinas"]):
                    profile_hint = "retail+actiu+promotores"
                    tipo = "suelo terciario disponible"
                elif any(k in uso_l for k in ["dotacional", "equipamiento", "público"]):
                    profile_hint = "constructora+actiu"
                    tipo = "suelo dotacional disponible"
                else:
                    continue  # skip undeveloped/agricultural
                
                # PEM estimate from edificabilidad × precio construcción
                pem_est = None
                try:
                    sup_num = float(str(sup_m2).replace(",",".").replace(" ",""))
                    edif_num = float(str(edific).replace(",",".").replace(" ",""))
                    # Typical construction cost: residencial €850/m², industrial €500/m², terciario €1,100/m²
                    _coste_m2 = {"residencial": 850, "industrial": 500, "terciario": 1100}.get(
                        "industrial" if "industrial" in uso_l else
                        "terciario" if "terciario" in uso_l else "residencial", 850)
                    pem_est = sup_num * edif_num * _coste_m2
                    if pem_est > 3_000_000_000: pem_est = None
                except Exception:
                    pass
                
                exp_id = f"PS11-{i}-{abs(hash(ref_cat or muni + sup_m2))%10**6}"
                
                desc = (f"Parcela en venta — {uso} | {muni} | {sup_m2}m² | "
                        f"FAR {edific} | {clasif} | {estado}")
                
                perm = {
                    "source_url":         contrato_url or "https://www.comunidad.madrid/inversion/inicia-desarrolla-tu-empresa/portal-suelo-40",
                    "date_granted":       "",
                    "municipality":       muni,
                    "address":            muni,
                    "applicant":          "Comunidad de Madrid / Ayuntamiento",
                    "permit_type":        tipo,
                    "declared_value_eur": None,
                    "estimated_pem":      f"€{pem_est/1_000_000:.1f}M est." if pem_est and pem_est >= 1_000_000 else "",
                    "description":        desc[:350],
                    "extraction_mode":    "portal_suelo",
                    "confidence":         "high",
                    "phase":              "licitacion",
                    "expediente":         exp_id,
                    "lead_score":         0,
                    "ai_evaluation":      (
                        f"Parcela disponible Portal Suelo CM — {uso} en {muni}. "
                        f"Superficie: {sup_m2}m² | FAR: {edific} | Régimen: {regimen}. "
                        f"{'Precio venta: ' + precio + '. ' if precio else ''}"
                        f"Promotores/RE: evaluar adquisición o concesión AHORA — estas parcelas "
                        f"rara vez aparecen en BOCM y la ventana de decisión es corta."
                    )[:500],
                    "profile_fit":        profile_hint,
                    "action_window":      "⚡ ACTUAR ESTA SEMANA",
                    "re_sup_total_m2":    sup_m2,
                    "re_tipo_suelo":      clasif,
                    "re_cargas_pendientes": "",
                    "ind_sup_parcela_m2": sup_m2 if "industrial" in uso_l else "",
                    "ind_renta_mercado":  "",
                    "pem_is_declared":    False,
                }
                results.append((exp_id, perm))
            
            if results: break  # got data from first working URL
            
        except Exception as _e11:
            log(f"  ⚠️  Portal Suelo: {_e11}")
            continue
    
    return results


def search_ite_padron(date_from, date_to) -> list:
    """
    SOURCE 12: ITE/IEE Padrón de Edificios — annual BOCM publication.
    
    Each October, the BOCM publishes "Padrón de Edificios y Construcciones
    cuyos propietarios deben efectuar la Inspección Técnica de Edificios durante el año X."
    
    Buildings that FAIL their ITE are legally required to rehabilitate.
    This is the highest-quality MEP and rehabilitation pipeline available:
    - Buildings over 50 years old (Madrid mandate: built before 1975)
    - Owners notified by Ayuntamiento with 3-month deadline
    - Failure to comply = fines €1,000-€3,000 + forced execution
    
    Profiles: MEP Instaladores, Gran Constructora (rehab), Contract & Oficinas
    
    Search strategy: keyword scan for the specific BOCM publication
    """
    results = []
    if not time_ok(need_s=20): return results
    
    import re as _re
    
    # The ITE padrón is a Section III document published each October
    # BOCM search for it directly
    # ITE/IEE search across BOTH sections:
    # SECTION_II (8386) = Anuncios de Ayuntamientos — where mandatory rehabilitation
    #   orders appear year-round after failed ITE inspections.
    # SECTION_III (8387) = Administración Local — where padrón list is published.
    # "Orden de ejecución de obras" appears in SECTION_II constantly — high value.
    _ITE_SEARCHES = [
        # Year-round: mandatory works orders after failed ITE (Section II)
        ("orden de ejecución de obras",          SECTION_II),   # most frequent
        ("declaración de ruina",                  SECTION_II),   # demolish/rehab
        ("rehabilitación forzosa",                SECTION_II),
        ("inspección técnica de edificios",       SECTION_II),
        # Annual padrón (October) in Section III
        ("padrón de edificios",                   SECTION_III),
        ("informe de evaluación de edificios",    SECTION_III),
        ("inspección técnica de edificios",       SECTION_III),
    ]
    
    _ite_urls = set()
    for kw, sec in _ITE_SEARCHES:
        if not time_ok(need_s=10): break
        try:
            # Use up to 18 months back — mandatory orders can reference old buildings
            _ite_start = min(date_from, date_to - timedelta(days=540))
            urls = search_bocm_keyword(kw, sec, _ite_start, date_to, max_pages=4)
            _ite_urls.update(urls)
            time.sleep(0.4)
        except Exception:
            pass

    # Section II (Anuncios Ayuntamientos) — mandatory works orders year-round
    _SECTION_II_ITE = "8386"
    for kw in ["orden de ejecución de obras", "declaración de ruina",
               "rehabilitación forzosa", "expediente de ruina"]:
        if not time_ok(need_s=8): break
        try:
            _start2 = min(date_from, date_to - timedelta(days=180))
            urls2 = search_bocm_keyword(kw, _SECTION_II_ITE, _start2, date_to, max_pages=3)
            _ite_urls.update(urls2)
            time.sleep(0.3)
        except Exception:
            pass
    
    if _ite_urls:
        log(f"  🏛️  ITE Padrón: {len(_ite_urls)} potential ITE documents found")
    
    # These URLs go into the main BOCM processing queue — they'll be AI-evaluated
    # with MEP/rehab focus. The keyword tags ensure proper profile fit.
    for url in _ite_urls:
        results.append(("ITE-URL", url))
    
    return results

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
    log(f"🏗️  PlanningScout Madrid — Engine v30 (s3-icio-fix+s4-rss+s5-boe+s6-ai-eval+s7-pem+s8-borme+s9-place+s10-gis)")
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
        # Scans ALL working days in the date_from → date_to window.
        # DAILY mode (--weeks 1) → scans all working days in that week (typically 5).
        # For speed on daily GitHub Actions runs, we limit to last 2 working days
        # ONLY when running genuinely daily (WEEKS_BACK == 1 AND today is a working day
        # that already ran yesterday). For any explicit --weeks run, always scan full window.
        log(f"\n{'─'*55}")
        log(f"📅 SOURCE 1: Per-day Section III scan  [{MODE} mode]")

        scan_days = []
        d = date_from
        while d <= date_to:
            if d.weekday() < 5: scan_days.append(d)
            d += timedelta(days=1)

        # In daily-cron mode (exactly 1 week window), cap to last 3 working days
        # to keep the run under 20 minutes. For any other window, scan everything.
        if MODE == "daily" and WEEKS_BACK == 1:
            scan_days = scan_days[-3:]   # last 3 working days of the week window
            log(f"  Scanning last 3 working days (daily mode — use --weeks 2+ for full week scan)")
        else:
            log(f"  Scanning {len(scan_days)} working days ({date_from.strftime('%d/%m')} → {date_to.strftime('%d/%m')})…")

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
                log(f"  📅 {day.strftime('%d/%m')} [II]: +{added}"); day_total += added
            time.sleep(0.4)

        log(f"  Day scan total: +{day_total} | {len(all_urls)} unique")

        # ── SOURCE 2: Keyword searches ───────────────────────────────────────────
        # DAILY:  Top 15 highest-yield keywords over last 2 days (fast, ~15min)
        # WEEKLY: All 107 keywords, 1-week chunks
        # FULL:   All keywords, full date-chunking

        if True:   # runs in ALL modes — daily uses short KW list + 2-day window
            log(f"\n{'─'*55}")
            log(f"🔎 SOURCE 2: Keyword search  [{MODE} mode]")

            # ALL keywords run in ALL modes — no daily restriction.
            # Skipping keywords in daily mode would miss ~80% of leads.
            # Source 1 (day scan) catches same-day documents;
            # Source 2 (keyword search) catches documents indexed with delay and
            # all documents in the full date_from → date_to window.
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
        if time_ok(need_s=120):   # ← runs in ALL modes
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
        # Runs in ALL modes — BOE publishes continuously, not in batches.
        if time_ok(need_s=180):
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
                        executor.submit(process_cm_contrato, url, title, summary, idx+1, len(cm_items), published): url
                        for idx, (url, title, summary, published) in enumerate(cm_items)
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
                # Normalize tuple format: old=(exp,rec,url,hint), new=(exp,rec,url,hint,aw,phase)
                dm_items = [(t[0],t[1],t[2],t[3],t[4] if len(t)>4 else "",
                             t[5] if len(t)>5 else "") for t in dm_items]
                dm_saved = dm_skipped = dm_errors = 0
                with ThreadPoolExecutor(max_workers=min(N_WORKERS, 3)) as executor:
                    dm_futures = {
                        executor.submit(
                            process_datos_madrid_item,
                            exp, rec, source_url, profile_hint, idx+1, len(dm_items)
                        ): exp
                        for idx, (exp, rec, source_url, profile_hint, _aw, _ph) in enumerate(dm_items)
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
                pass   # datos.madrid logs its own status internally

        # ── SOURCE 8: BORME — new construction company registrations ──────────────
        # DoubleTrade's "corporate data" layer: newly formed promotores and
        # construction companies in Madrid = early signals 12-24mo before BOCM.
        # api.boe.es/BORME/v2/ is free, official, no WAF block from GitHub Actions.
        # Runs in ALL modes — new company registrations happen every working day.
        if time_ok(need_s=60):
            log(f"\n{'─'*55}")
            log("📋 SOURCE 8: BORME (nuevas empresas constructoras/promotoras)")
            log("   Using www.boe.es/diario_borme/xml.php (same server as working BOE source)")
            try:
                borme_items = search_borme_new_companies(date_from, date_to)
                if borme_items:
                    log(f"  📋 BORME: {len(borme_items)} new construction companies")
                    for item in borme_items[:15]:
                        log(f"    🏢 {item.get('company','')[:50]} | {item.get('act','')[:30]} | {item.get('date','')}")
                else:
                    log("  📋 BORME: no new construction companies in date range")
            except Exception as _be:
                log(f"  ⚠️ BORME: {_be}")

        # ── SOURCE 9: Plataforma de Contratación del Estado (PLACE) ──────────────
        # National construction/infrastructure tenders > €30K.
        # Directly relevant for: Gran Constructora, Alquiler Maquinaria, Materiales.
        # Runs in ALL modes — national tenders publish every working day.
        if time_ok(need_s=60):
            log(f"\n{'─'*55}")
            log("🏛️  SOURCE 9: PLACE (Plataforma Contratación del Estado)")
            try:
                place_items = search_place_national(date_from, date_to)
                if place_items:
                    log(f"  🏛️  PLACE: {len(place_items)} construction tenders found")
                    place_saved = place_skipped = place_errors = 0
                    with ThreadPoolExecutor(max_workers=min(N_WORKERS, 3)) as executor:
                        place_futures = {
                            executor.submit(
                                process_cm_contrato,   # same processing path as CM Contratos
                                _pu, _pt,
                                (_pt + " " + _ps)[:500],
                                idx + 1, len(place_items)
                            ): _pu
                            for idx, (_pu, _pt, _ps, _pp) in enumerate(place_items)
                            if time_ok(need_s=5)
                        }
                        for future in as_completed(place_futures):
                            try:
                                s, sk, e = future.result()
                                place_saved += s; place_skipped += sk; place_errors += e
                            except Exception as _pfe:
                                log(f"  ❌ PLACE future: {_pfe}"); place_errors += 1
                    log(f"  PLACE: ✅{place_saved} saved | ⏭️{place_skipped} skipped | ❌{place_errors} errors")
                else:
                    log("  🏛️  PLACE: 0 Madrid construction tenders this period")
            except Exception as _place_e:
                log(f"  ⚠️  PLACE: {_place_e}")

        # ── SOURCE 10: Sede Electrónica Ayuntamiento Madrid — GIS Licencias ──────
        # Complements Source 7 with GeoJSON data including PEM, m², and exact lat/lon.
        # Runs in ALL modes — licences are granted every working day.
        if time_ok(need_s=60):
            log(f"\n{'─'*55}")
            log("🏛️  SOURCE 10: Sede Madrid GIS (licencias urbanísticas con PEM)")
            try:
                sede_items = search_sede_madrid_obras(date_from, date_to)
                if sede_items:
                    sede_saved = sede_skipped = sede_errors = 0
                    with ThreadPoolExecutor(max_workers=min(N_WORKERS, 3)) as executor:
                        sede_futures = {
                            executor.submit(
                                process_datos_madrid_item,
                                exp, rec, source_url, profile_hint, idx+1, len(sede_items)
                            ): exp
                            for idx, (exp, rec, source_url, profile_hint) in enumerate(sede_items)
                            if time_ok(need_s=5)
                        }
                        for future in as_completed(sede_futures):
                            try:
                                s, sk, e = future.result()
                                sede_saved += s; sede_skipped += sk; sede_errors += e
                            except Exception as _sfe:
                                log(f"  ❌ Sede future: {_sfe}"); sede_errors += 1
                    log(f"  Sede Madrid GIS: ✅{sede_saved} saved | ⏭️{sede_skipped} skipped | ❌{sede_errors} errors")
                else:
                    log("  🏛️  Sede Madrid GIS: 0 licences in date range (endpoint may be unavailable)")
            except Exception as _sede_e:
                log(f"  ⚠️  Sede Madrid GIS: {_sede_e}")

        # ── SOURCE 11: Portal del Suelo CM — available plots ───────────────────
        # Direct JSON from CM open data — plots NOT visible in BOCM.
        # Relevant: Promotores/RE, Industrial/Log, Gran Constructora, Retail.
        if time_ok(need_s=30):
            log(f"\n{'─'*55}")
            log("🏛️  SOURCE 11: Portal del Suelo 4.0 (parcelas CM disponibles)")
            try:
                ps_items = search_portal_suelo(date_from, date_to)
                if ps_items:
                    ps_saved = ps_skipped = ps_errors = 0
                    for _ps_id, _ps_perm in ps_items:
                        if not time_ok(need_s=3): break
                        try:
                            _ps_perm["lead_score"] = score_lead(_ps_perm)
                            _ps_perm = _enhance_profile_fit(_ps_perm,
                                str(_ps_perm.get("description","")).lower())
                            if write_permit(_ps_perm, ""):
                                ps_saved += 1
                            else:
                                ps_skipped += 1
                        except Exception as _pse:
                            ps_errors += 1
                    log(f"  Portal Suelo: ✅{ps_saved} saved | ⏭️{ps_skipped} skipped | ❌{ps_errors} errors")
                else:
                    log("  🏛️  Portal Suelo: 0 parcelas found (endpoint may have changed)")
            except Exception as _ps_e:
                log(f"  ⚠️  Portal Suelo: {_ps_e}")

        # ── SOURCE 12: ITE Padrón — buildings mandated to rehabilitate ─────────
        # Highest-quality MEP + rehab pipeline. Legally mandated obras.
        # >10,000 buildings in Madrid mandated for ITE in 2026.
        if time_ok(need_s=20):
            log(f"\n{'─'*55}")
            log("🏛️  SOURCE 12: ITE Padrón (edificios obligados a inspección técnica)")
            try:
                ite_items = search_ite_padron(date_from, date_to)
                ite_added = 0
                for _ite_tag, _ite_url in ite_items:
                    if add_url(_ite_url): ite_added += 1
                log(f"  ITE Padrón: +{ite_added} documentos ITE en cola")
            except Exception as _ite_e:
                log(f"  ⚠️  ITE Padrón: {_ite_e}")

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

    # ── WATCHLIST ALERTS — send phase-change emails to subscribers ─────────────
    # Runs after every engine cycle. Only sends when phase actually advanced.
    log("\n🔔 Checking watchlist alerts...")
    try:
        send_watchlist_alerts()
    except Exception as _wa_err:
        log(f"  ⚠️ Watchlist alerts error: {_wa_err}")

    # ── Refresh profile-specific tabs ──────────────────────────────────────────
    # Creates/updates one tab per sector profile in Google Sheets.
    # Each tab shows only rows+columns relevant to that profile.
    try:
        _sh_for_tabs = get_sheet().spreadsheet
        log(f"\n{chr(9472)*55}")
        log("📊 Refreshing profile tabs…")
        create_or_update_profile_tabs(_sh_for_tabs)
    except Exception as _pt_err:
        log(f"  ⚠️  Profile tabs refresh: {_pt_err}")


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
        # Core BOE licitación signals
        "licitación", "anuncio de licitación", "contrato de obras",
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
        # New: BOE anuncio title patterns
        "actuación de urbanización", "obras de infraestructura",
        "proyecto de ejecución", "conservación y mantenimiento de",
        "tramo", "variante", "circunvalación",
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
        "dirección general de vivienda", "secretaría de estado",
        "canal de isabel ii", "seitt", "mitma",
        "comunidad de madrid", "administración local",
        "ayuntamiento", "mancomunidad", "diputación",
        "puertos del estado", "autoridad portuaria",
        "aena", "enaire",
        "red eléctrica", "endesa", "iberdrola",  # utility infrastructure
        "confederación hidrográfica",
        "fomento", "infraestructuras",  # old ministry names still appear
        "entidad pública empresarial", "empresa municipal",
        "consejería", "agencia",  # regional bodies
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
    # CM Contratos feeds — try multiple in order. The portal changes URLs occasionally.
    # ALL feeds are tried on each run; primary first, extras if primary returns 0.
    CM_FEED = "https://contratos-publicos.comunidad.madrid/feed/licitaciones2"
    CM_FEEDS_EXTRA = [
        "https://contratos-publicos.comunidad.madrid/feed/licitaciones",
        "https://contratos-publicos.comunidad.madrid/feed/adjudicaciones2",
        "https://contratos-publicos.comunidad.madrid/feed/adjudicaciones",
        # Direct portal search — returns all contracts in last 30 days as ATOM
        "https://contratos-publicos.comunidad.madrid/feed/contratos",
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
            # Pass published date through so process_cm_contrato can set date_granted
            results.append((url, title, summary[:500], published))
        
        log(f"  🏗️ CM Contratos ATOM: {len(results)} construction contracts found")

        # ── Try extra CM feeds if primary returned 0 ──────────────────────────
        # Always try extra feeds too — CM feed 404s are common.
        # Trying all feeds ensures we never miss a contract due to a temporary URL issue.
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
                        results.append((url2, title2, summary2[:500], pub2))
                except Exception: continue
        if results:
            log(f"  🏗️ CM Contratos extra feeds: +{len(results)} total")
        
    except Exception as e:
        log(f"  ⚠️ CM Contratos error: {e}")
    
    return results


def _build_cm_ai_evaluation(title: str, summary: str, permit_type: str,
                             phase: str, pem, applicant: str, combined: str) -> str:
    """
    Build a sector-specific evaluation for CM Contratos entries.
    No OpenAI call — pure keyword analysis + market knowledge.
    
    Produces a structured, actionable evaluation for EACH sector:
    - Phase-specific urgency signal
    - Entity context (what Canal de Isabel II means for Molecor)
    - Sector-by-sector action items with specific data points
    - CPV codes estimation where applicable
    - Timing estimate for obra start
    """
    import re as _re
    
    pem_s = (f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000
             else f"€{int(pem/1000)}K" if pem and pem >= 1000 else "no declarado")
    
    # ── Phase signals ─────────────────────────────────────────────────────────
    _PHASE_LABELS = {
        "licitacion":   ("⚡ LICITACIÓN ACTIVA", "ACCIÓN INMEDIATA — plazo de presentación de ofertas abierto.",
                         "🟡 PREPARAR OFERTA"),
        "adjudicacion": ("✅ CONTRATO ADJUDICADO", "Obra adjudicada — contactar adjudicatario para subcontratos.",
                         "🔴 LLAMAR HOY"),
        "definitivo":   ("🟢 APROBACIÓN DEFINITIVA", "Licitación estimada en 6-12 meses.", "🟢 PIPELINE"),
        "en_obra":      ("🏗️ OBRA EN EJECUCIÓN", "Obra en marcha — contactar jefe de obra.", "🔴 LLAMAR HOY"),
    }
    phase_label, urgency_text, alq_urgencia = _PHASE_LABELS.get(
        phase, (f"📋 {phase.upper()}", "Monitorizar.", "🟢 PIPELINE"))
    
    # ── Entity intelligence ───────────────────────────────────────────────────
    _ENTITY_CTX = {
        "canal de isabel ii": {
            "context": "Canal de Isabel II (CYII) — mayor operador hídrico Madrid. €800M capex 2025-2030.",
            "sectors": {
                "materiales": "Molecor: tubería PVC DN-200 a DN-600 saneamiento + PE abastecimiento — cotizar si incluye red.",
                "infra": "Gran Constructora: obra civil clave para licitación Madrid saneamiento. CPV ~45231300.",
                "alquiler": "Alquiler Maquinaria: excavadora 20-30t para zanjas colectores — obra típica 8-18m."
            }
        },
        "metro de madrid": {
            "context": "Metro de Madrid — obras subterráneas, alta complejidad técnica, clasificación Grupo D.",
            "sectors": {
                "infra": "Gran Constructora: requiere clasificación Grupo D (ferroviario) — evaluación técnica mínima.",
                "alquiler": "Alquiler Maquinaria: equipos especializados túnel/galería — consultar pliego.",
                "mep": "MEP: instalaciones eléctricas BT + iluminación LED + ventilación forzada."
            }
        },
        "emvs": {
            "context": "EMVS (Empresa Municipal Vivienda y Suelo Madrid) — vivienda protegida + rehabilitación.",
            "sectors": {
                "constructora": "Gran Constructora: EMVS adjudica regularmente lotes residenciales 30-80 viviendas VPO.",
                "mep": "MEP Instaladores: rehabilitación integral = HVAC nuevo + eléctrica + ACS centralizado.",
                "actiu": "Contract & Oficinas: si hay zonas comunes/sociales — propuesta mobiliario público."
            }
        },
        "adif": {
            "context": "ADIF — infraestructura ferroviaria nacional. Clasificación obligatoria Grupo D/E.",
            "sectors": {
                "infra": "Gran Constructora: licitación ADIF = obra de referencia nacional. Pre-calificación obligatoria.",
                "alquiler": "Alquiler Maquinaria: equipos AVE/vía convencional — plataformas, grúas pórtico.",
            }
        },
        "ayuntamiento de madrid": {
            "context": "Ayuntamiento de Madrid — 46 contratos obras adjudicados en 5 años a grandes constructoras.",
            "sectors": {
                "infra": "Gran Constructora: Ayuntamiento Madrid = cliente prioritario. Portal de Contratación CM.",
                "materiales": "Compras/Materiales: obra civil municipal = zahorra + hormigón + señalización vial.",
                "alquiler": "Alquiler Maquinaria: obra municipal = maquinaria urbana (mini-excavadoras, plataformas)."
            }
        },
        "comunidad de madrid": {
            "context": "Comunidad de Madrid — hospitales, colegios, carreteras regionales.",
            "sectors": {
                "infra": "Gran Constructora: carreteras + hospitales = obra pública mayor. Revisar CM portal.",
                "actiu": "Contract & Oficinas: hospitales + colegios = mayor contrato mobiliario público CM.",
                "mep": "MEP Instaladores: hospital CM = HVAC cuádruple tubo + quirófanos + sistemas PCI complejos."
            }
        },
        "hospital": {
            "context": "Hospital / infraestructura sanitaria — muy alta complejidad MEP.",
            "sectors": {
                "mep": "MEP Instaladores: hospital = instalaciones cuádruple tubo + UPS médico + quirófanos.",
                "actiu": "Contract & Oficinas: mobiliario hospitalario + zonas espera + despachos. Presupuesto €500-800/m².",
                "constructora": "Gran Constructora: hospital = clasificación Grupo C alta (>€4.8M). Licitación restringida."
            }
        },
    }
    
    # Find matching entity context
    entity_match = next(
        (v for k, v in _ENTITY_CTX.items() if k in combined.lower() or k in applicant.lower()),
        {"context": f"{applicant}: contrato de obras públicas CM.", "sectors": {}}
    )
    
    # ── Sector-specific actions from keywords ─────────────────────────────────
    actions = []
    
    # CPV estimation from content
    cpv_est = ""
    if any(k in combined for k in ["saneamiento", "colector", "red de abastecimiento", "pluviales"]):
        cpv_est = "CPV estimado: 45231300 (tuberías distribución)"
        actions.append(f"Materiales PVC/HDPE: {entity_match['sectors'].get('materiales', 'evaluar tubería PVC saneamiento + abastecimiento — cotizar')}")
    
    if any(k in combined for k in ["urbanización", "obra civil", "vial", "pavimentación", "carretera"]):
        cpv_est = cpv_est or "CPV estimado: 45000000 (obras construcción)"
        actions.append(f"Alquiler Maquinaria: {alq_urgencia} — {entity_match['sectors'].get('alquiler', 'excavadoras + compactadoras para obra civil')}")
        actions.append(f"Gran Constructora: {entity_match['sectors'].get('infra', 'revisar pliego técnico portal CM')}")
    
    if any(k in combined for k in ["hospital", "centro de salud", "edificio", "sede", "rehabilit"]):
        actions.append(f"MEP Instaladores: {entity_match['sectors'].get('mep', 'instalaciones HVAC + PCI + eléctrica')}")
        actions.append(f"Contract & Oficinas: {entity_match['sectors'].get('actiu', 'evaluar mobiliario si hay oficinas/zonas comunes')}")
    
    if any(k in combined for k in ["licitación", "adjudicación", "obras públicas", "contrato de obras"]):
        if not any("Gran Constructora" in a for a in actions):
            actions.append(f"Gran Constructora: {entity_match['sectors'].get('infra', 'revisar pliego técnico portal CM y evaluar oferta')}")
    
    # Timing estimate
    timing = ""
    if phase == "licitacion":
        timing = f"Plazo estimado obras: adjudicación + 45 días trámites = inicio estimado {_next_quarter()}."
    elif phase == "adjudicacion":
        timing = "Adjudicatario confirmado — obra inicia en ~45 días. ACCIÓN HOY."
    elif phase == "definitivo":
        timing = "Aprobación definitiva = licitación en 3-9 meses. Preparar oferta técnica."
    
    if not actions:
        actions = ["Evaluar oportunidad de subcontratación con adjudicatario."]
    
    return (
        f"{phase_label} — {applicant}. Presupuesto: {pem_s}. "
        f"{entity_match['context']} "
        f"{urgency_text} "
        + (f"{cpv_est}. " if cpv_est else "")
        + f"{timing} "
        f"|| ".join(actions[:3])
    )[:700]


def _next_quarter() -> str:
    """Return the next calendar quarter label, e.g. 'Q3 2026'."""
    from datetime import datetime as _dt
    n = _dt.now()
    q = (n.month - 1) // 3 + 2  # next quarter
    y = n.year + (1 if q > 4 else 0)
    q = (q - 1) % 4 + 1
    return f"Q{q} {y}"
def process_cm_contrato(url, title, summary, idx, total, published=""):
    """
    Process a single CM Contratos item.
    Extracts: date (from feed), address hint (from title/summary NLP), Maps URL.
    Returns (saved, skipped, error) counts.
    """
    try:
        with _sheet_lock:
            if url in _seen_urls:
                return 0, 1, 0
        
        combined = (title + " " + summary).lower()

        # ── Admin noise filter ──────────────────────────────────────────────────
        _CM_NOISE = [
            "suministro de alimentos", "limpieza de oficinas",
            "servicio de vigilancia", "seguro de ", "seguros de ",
            "transporte escolar", "catering", "arrendamiento de vehículos",
            "servicios informáticos", "consultoría de gestión",
        ]
        if any(n in combined for n in _CM_NOISE):
            return 0, 1, 0
        
        import re as _re
        
        # ── Date extraction (Col A) — from ATOM feed published field ───────────
        date_granted = ""
        if published:
            try:
                from dateutil import parser as _dp3
                _pub_dt = _dp3.parse(published).replace(tzinfo=None)
                date_granted = _pub_dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        if not date_granted:
            # Try to extract date from URL or summary as fallback
            _dm = _re.search(r'(\d{4}-\d{2}-\d{2})', url + " " + summary)
            if _dm: date_granted = _dm.group(1)

        # ── Address extraction — mine location from title + summary ─────────────
        # CM Contratos titles follow patterns like:
        #   "Obras de urbanización en Calle Mayor, 45, Madrid"
        #   "Rehabilitación de la sede en Paseo de la Castellana 200"
        #   "Saneamiento Carretera M-40 PK 14.2"
        #   "Hospital La Paz — Ampliación de urgencias"
        #   "Línea 11 Metro de Madrid"
        address = ""
        municipality = "Madrid"
        maps_url = ""
        
        _full_text = title + " " + summary
        
        # Pattern 1: explicit "en <location>" or "en el municipio de <X>"
        _loc_m = _re.search(
            r'\ben\s+((?:la\s+|el\s+|los\s+|las\s+)?(?:calle|plaza|avenida|paseo|carretera|autovía|autopista|vía|camino|ronda|barrio|polígono|parque|hospital|estadio|aeropuerto|universidad|campus|sede|edificio)\s+[A-ZÁÉÍÓÚÑ][^,.\n]{3,50})',
            _full_text, _re.I
        )
        if _loc_m:
            address = _loc_m.group(1).strip().title()
        
        # Pattern 2: "Carretera M-XXX / A-XXX / N-XXX" (highways/roads)
        if not address:
            _road_m = _re.search(
                r'\b((?:carretera|autovía|autopista|variante|vía)\s+(?:M|A|N|R|AP|CV|GR|SE|MA|CA|CO|J|JA|AL|MU|TO|CU|AB|V|TE|Z|HU|LO|BU|VA|PA|SA|AV|SG|SO|VI|SS|BI|NA|PM|TF|GC|LE|OR|PO|LU|C|GI|B|T|L|LL|AD|BA|CC|CR|CT|IB|GU)[-\s]\d+)',
                _full_text, _re.I
            )
            if _road_m:
                address = _road_m.group(1).strip().title()
        
        # Pattern 3: named infrastructure (hospital, metro line, etc.)
        if not address:
            _infra_m = _re.search(
                r'\b((?:hospital|clínica|centro\s+de\s+salud|línea\s+\d+|metro|cercanías|ave|estación\s+de|aeropuerto|puerto|universidad|campus|polideportivo|parque\s+empresarial|polígono\s+industrial)\s+(?:de\s+)?[A-ZÁÉÍÓÚÑ][A-Za-záéíóúñÁÉÍÓÚÑ\s\d]{2,40})',
                _full_text, _re.I
            )
            if _infra_m:
                address = _infra_m.group(1).strip().title()
        
        # Pattern 4: municipio extraction ("en el municipio de X" / "de X al Y")
        if not address:
            _muni_m = _re.search(
                r'municipio\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñÁÉÍÓÚÑ\s]{3,30})',
                _full_text, _re.I
            )
            if _muni_m:
                municipality = _muni_m.group(1).strip().title()
                address = f"{municipality}, Madrid"

        # Build Google Maps URL from extracted address
        if address:
            _maps_q = f"{address}, {municipality}, Madrid, España".replace(" ", "+")
            maps_url = f"https://www.google.com/maps/search/{_maps_q}"
        elif municipality and municipality != "Madrid":
            _maps_q = f"{municipality}, Madrid, España".replace(" ", "+")
            maps_url = f"https://www.google.com/maps/search/{_maps_q}"
        
        # ── PEM extraction ──────────────────────────────────────────────────────
        pem = None
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
                        pem = v_num; break
                except: pass
        
        # ── Permit type + phase ─────────────────────────────────────────────────
        permit_type = "licitación de obras"
        phase = "licitacion"
        if any(k in combined for k in ["adjudicado", "adjudicación", "contrato formalizado"]):
            phase = "adjudicacion"
        elif any(k in combined for k in ["urbanización", "reparcelación"]):
            permit_type = "urbanización"
        elif any(k in combined for k in ["rehabilitación", "reforma"]):
            permit_type = "obra mayor rehabilitación"
        
        # ── Applicant (contracting entity) ──────────────────────────────────────
        _CM_ENTITIES = [
            "Canal de Isabel II", "Metro de Madrid", "EMVS", "Ayuntamiento de Madrid",
            "Comunidad de Madrid", "MINTRA", "Planifica Madrid", "ADIF", "RENFE",
            "Aeropuertos Españoles", "AENA", "Ministerio de Transportes",
            "Ministerio de Vivienda", "Consejería", "Agencia de Vivienda Social",
        ]
        applicant = next(
            (e for e in _CM_ENTITIES if e.lower() in (_full_text).lower()),
            "Comunidad de Madrid"
        )
        
        p = {
            "source_url":          url,
            "pdf_url":             "",
            "date_granted":        date_granted,
            "municipality":        municipality,
            "address":             address,
            "applicant":           applicant,
            "permit_type":         permit_type,
            "declared_value_eur":  None,       # CM Contratos: never officially declared PEM (col F stays empty)
            "pem_is_declared":     False,      # Force to col R (Estimated PEM)
            "description":         (title[:300] + " — " + summary[:100]).strip(),
            "extraction_mode":     "cm_contratos",
            "confidence":          "medium",
            "phase":               phase,
            "expediente":          "",
            "lead_score":          0,
            # PEM from feed goes into estimated_pem (col R), not declared (col F)
            "estimated_pem":       (f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000
                                   else f"€{int(pem/1000)}K" if pem else ""),
            "ai_evaluation":       _build_cm_ai_evaluation(title, summary, permit_type, phase, pem, applicant, combined),
            "supplies_needed":     generate_supplies_estimate(permit_type, pem, title, summary),
            "project_size":        "",
            "action_window":       "⚡ ACTUAR ESTA SEMANA" if phase == "licitacion" else "📞 CONTACTAR EN 30 DÍAS",
            "key_contacts":        f"Entidad: {applicant}",
            "obra_timeline":       "",
        }
        # Inject Maps URL into the maps field
        if maps_url:
            p["maps"] = maps_url

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
    SOURCE 7: datos.madrid.es — Licencias Urbanísticas Ayuntamiento de Madrid.

    WHAT THIS SOURCE PROVIDES (different from BOCM):
    ─────────────────────────────────────────────────
    BOCM covers all 179 Madrid municipalities — but for Madrid CAPITAL,
    the Ayuntamiento uses its own SLIM system and publishes every licence
    as per-year XLSX files on datos.madrid.es. This covers:
      - Declaración Responsable Residencial   (residential works)
      - LICENCIA URBANÍSTICA RESIDENCIAL      (residential building licence)
      - Declaración Responsable Actividad     (commercial activity)
      - LICENCIA URBANÍSTICA DE ACTIVIDAD     (commercial licence)
      - Declaración Responsable - Primera Ocupación (building done!)
      - Licencia de funcionamiento de actividad

    DOWNLOAD URL (confirmed from portal screenshots):
      https://datos.madrid.es/egob/catalogo/300193-[N]-licencias-urbanisticas-xlsx.xlsx
      where [N] changes each year — the engine tries multiple candidates.
      2026 = 300193-2  (78 KB,  ~520 rows = Jan-current)
      2025 = 300193-1  (1.5 MB, ~10K rows = full year)
      
    XLSX COLUMNS (from actual downloaded file):
      Fecha concesión, Procedimiento, Tipo de expediente,
      Tipo Via + Nombre Via + Número + Distrito + Barrio (full address),
      Interesado (Persona jurídica / Persona física), NDP_EDIFICIO

    NOTE: 'Objeto de la licencia' is EMPTY in this dataset (no description).
    We use Tipo de expediente to classify leads by value.

    ACCESS:
      GitHub Actions IPs reach the origin (proved by 404 on wrong endpoint).
      /egob/catalogo/ file downloads should return 200 from GH Actions.
      If blocked → set DATOS_MADRID_PROXY (CF Worker, 5 min, free).
    """
    BASE = "https://datos.madrid.es"

   # ── XLSX / CSV candidate URLs ─────────────────────────────────────────────
    # datos.madrid changes resource IDs every year — we auto-discover then try
    # a broad candidate list so it works regardless of what ID they chose.
    _current_year = date_to.year

    # Step 1: try auto-discovery from the dataset landing page
    _DATASET_LANDING = (
        f"{BASE}/portal/site/egob/menuitem.400a817358ce98c34e937436a8a409a0/"
        f"?vgnextoid=300193&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD"
    )
    _DATASET_EGOB = f"{BASE}/egob/catalogo/300193-0-licencias-urbanisticas"

    def _discover_xlsx_url():
        """Fetch the dataset downloads page and scrape the real XLSX/CSV href.
        
        Madrid portal serves downloads from /dataset/*/downloads HTML page.
        The "Descarga" buttons link to the real file URLs.
        We scrape these rather than guessing paths.
        """
        _DOWNLOADS_PAGE = f"{BASE}/dataset/300193-0-licencias-urbanisticas/downloads"
        for page_url in [_DOWNLOADS_PAGE, _DATASET_EGOB, _DATASET_LANDING]:
            try:
                if DATOS_MADRID_PROXY:
                    proxy_url = f"{DATOS_MADRID_PROXY}?url={quote(page_url, safe='')}"
                    r = requests.get(proxy_url, timeout=15, verify=False,
                                     headers={"Accept": "text/html,*/*"})
                else:
                    r = _dm_sess.get(page_url, timeout=20, allow_redirects=True)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                # Scrape all download links — Madrid portal uses several patterns:
                # a) /egob/catalogo/300193-N-licencias-*.xlsx (direct file)
                # b) /portal/site/egob/menuitem.*/download?id=...
                # c) data-href or onclick attributes on download buttons
                all_links = []
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    full = (f"{BASE}{href}" if href.startswith("/") else href)
                    if not full.startswith("http"): continue
                    hl = href.lower()
                    text = a.get_text(strip=True).lower()
                    # Match any link related to licencias / 300193 dataset
                    is_relevant = ("licencias" in hl or "300193" in hl or
                                   "descarga" in text or "download" in text)
                    if is_relevant:
                        if hl.endswith(".xlsx"):
                            log(f"  🏛️ datos.madrid: found XLSX → {full.split('/')[-1]}")
                            return full, None
                        if hl.endswith(".csv"):
                            all_links.append(("csv", full))
                        elif "download" in hl or "descarga" in text:
                            all_links.append(("download", full))
                # Return best non-XLSX link found
                for link_type, full in all_links:
                    if link_type == "csv":
                        log(f"  🏛️ datos.madrid: found CSV → {full.split('/')[-1]}")
                        return full, None
                    log(f"  🏛️ datos.madrid: found download link → {full[:80]}")
                    return full, None
            except Exception as e:
                continue
        return None, None

    _discovered_url, _ = _discover_xlsx_url()

    # Step 2: candidate list (discovered URL first, then broad guesses)
    # Resource IDs datos.madrid has used historically:
    # 300193-0 (2023), 300193-1 (2024), 300193-2 (2025), 300193-3 (2026), 300193-4 (future)
    _XLSX_CANDIDATES = []
    if _discovered_url:
        _XLSX_CANDIDATES.append(_discovered_url)
    # Candidate URLs — Madrid portal uses /egob/catalogo/ pattern
    # Resource IDs rotate yearly (300193-0=2024, 300193-1=2025, 300193-2=2026, etc.)
    # NOTE: if all return 404, update the CF Worker JS (see engine source top) 
    # to forward Accept: */* instead of application/json
    _current_year = date_to.year
    _XLSX_CANDIDATES += [
        # Current year + recent (try 3 most likely IDs first)
        f"{BASE}/egob/catalogo/300193-{_current_year - 2024 + 2}-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-{_current_year - 2024 + 1}-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-{_current_year - 2024}-licencias-urbanisticas-xlsx.xlsx",
        # Fixed known IDs
        f"{BASE}/egob/catalogo/300193-2-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-1-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-3-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-0-licencias-urbanisticas-xlsx.xlsx",
        f"{BASE}/egob/catalogo/300193-4-licencias-urbanisticas-xlsx.xlsx",
        # CSV variants (smaller, faster)
        f"{BASE}/egob/catalogo/300193-{_current_year - 2024 + 2}-licencias-urbanisticas.csv",
        f"{BASE}/egob/catalogo/300193-2-licencias-urbanisticas.csv",
        f"{BASE}/egob/catalogo/300193-0-licencias-urbanisticas.csv",
    ]
    # Deduplicate while preserving order
    _seen_cand: set = set()
    _XLSX_CANDIDATES_dedup = []
    for _u in _XLSX_CANDIDATES:
        if _u not in _seen_cand:
            _seen_cand.add(_u)
            _XLSX_CANDIDATES_dedup.append(_u)
    _XLSX_CANDIDATES = _XLSX_CANDIDATES_dedup

    if DATOS_MADRID_PROXY:
        log(f"  🏛️ datos.madrid: proxy configured ({DATOS_MADRID_PROXY.split('/')[2]})")
    else:
        log(f"  🏛️ datos.madrid: direct (no proxy — may fail from GitHub Actions)")

    # ── _fetch must be defined FIRST (before any nested function that uses it) ─
    from dateutil import parser as _dp

    _dm_sess = requests.Session()
    _dm_sess.verify = False
    _dm_sess.headers.update({
        "User-Agent":      ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"),
        "Accept":          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
                           "text/csv,application/octet-stream,*/*",
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         f"{BASE}/dataset/300193-0-licencias-urbanisticas",
        "Connection":      "keep-alive",
    })

    def _fetch(url, timeout=90, accept=None):
        """Returns (response_or_None, error_string)."""
        # Proxy path (if configured)
        if DATOS_MADRID_PROXY and "datos.madrid.es" in url:
            proxy_url = f"{DATOS_MADRID_PROXY}?url={quote(url, safe='')}"
            try:
                r = requests.get(proxy_url, timeout=30, verify=False,
                                 headers={"User-Agent":"PlanningScout/1.0",
                                          "Accept":"*/*"})
                if r.status_code == 200: return r, None
                return None, f"proxy-{r.status_code}"
            except Exception:
                return None, "proxy-error"
        # Direct path
        if accept:
            _dm_sess.headers["Accept"] = accept
        try:
            r = _dm_sess.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200: return r, None
            return None, f"http-{r.status_code}"
        except requests.Timeout:
            return None, "timeout"
        except Exception as e:
            return None, f"error-{type(e).__name__}"

    # ── PEM parser (for CSV variant) ──────────────────────────────────────────
    def _parse_pem_es(raw):
        s = str(raw or "").strip()
        if not s or s in ("None","nan",""): return 0.0
        try:
            if "," in s and "." in s: return float(s.replace(".","").replace(",","."))
            if "," in s:              return float(s.replace(",","."))
            return float(s)
        except Exception:             return 0.0

    # ── HIGH-VALUE tipo types for lead scoring ────────────────────────────────
    # Maps Tipo de expediente → profile hints + urgency
    # NOTE: any tipo NOT in this map uses the fallback tuple at _TIPO_MAP.get()
    _TIPO_MAP = {
        # ── PRIMERA OCUPACIÓN — building complete, contact operator NOW ──────────
        "Declaración Responsable - Primera Ocupación":    ("hospe+mep+actiu",  "primera_ocupacion", "⚡ ACTUAR ESTA SEMANA"),
        "Licencia de primera ocupación":                   ("hospe+mep+actiu",  "primera_ocupacion", "⚡ ACTUAR ESTA SEMANA"),
        "Certificado de primera ocupación":                ("hospe+mep+actiu",  "primera_ocupacion", "⚡ ACTUAR ESTA SEMANA"),
        # ── ACTIVIDAD COMMERCIAL — Saona / Malvón / Kinépolis / Sharing Co ──────
        "LICENCIA URBANÍSTICA DE ACTIVIDAD":               ("retail+hospe+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Declaración Responsable Actividad":               ("retail+hospe+mep",  "solicitud",         "📅 MONITORIZAR"),
        "Licencia de funcionamiento de actividad":         ("retail+hospe",      "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Licencia básica urbanística actividad":           ("retail+hospe+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Autorización ambiental de actividad":             ("retail+hospe+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Licencia de apertura de actividad":               ("retail+hospe",      "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Comunicación previa de actividad":                ("retail+hospe",      "solicitud",         "📅 MONITORIZAR"),
        # ── OBRAS RESIDENCIALES — MEP + Constructora + ACTIU ────────────────────
        "LICENCIA URBANÍSTICA RESIDENCIAL":                ("constructora+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Declaración Responsable Residencial":             ("constructora+mep",  "solicitud",         "📅 MONITORIZAR"),
        "Declaración Responsable Obras":                   ("constructora+mep",  "solicitud",         "📅 MONITORIZAR"),
        "Licencia de obra mayor":                          ("constructora+mep+actiu", "definitivo",   "📞 CONTACTAR EN 30 DÍAS"),
        "Licencia de obras":                               ("constructora+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        "Obra mayor nueva planta":                         ("constructora+mep+actiu", "definitivo",   "📞 CONTACTAR EN 30 DÍAS"),
        "Obra mayor rehabilitación":                       ("constructora+mep+actiu", "definitivo",   "📞 CONTACTAR EN 30 DÍAS"),
        "Obra mayor ampliación":                           ("constructora+mep",  "definitivo",        "📞 CONTACTAR EN 30 DÍAS"),
        # ── CAMBIO DE USO — Sharing Co holy grail ───────────────────────────────
        "Cambio de uso":                                   ("hospe+retail+mep",  "definitivo",        "⚡ ACTUAR ESTA SEMANA"),
        "Cambio de destino":                               ("hospe+retail+mep",  "definitivo",        "⚡ ACTUAR ESTA SEMANA"),
        "Cambio de uso y obras":                           ("hospe+retail+mep",  "definitivo",        "⚡ ACTUAR ESTA SEMANA"),
        "Modificación de uso":                             ("hospe+retail",      "solicitud",         "📅 MONITORIZAR"),
        # ── URBANIZACIÓN — Molecor + Kiloutou + FCC ─────────────────────────────
        "Licencia de urbanización":                        ("constructora+infra+mep+mat", "definitivo","📞 CONTACTAR EN 30 DÍAS"),
        "Proyecto de urbanización":                        ("constructora+infra+mat", "solicitud",    "📅 MONITORIZAR"),
        # ── DEMOLICIÓN — Kiloutou ────────────────────────────────────────────────
        "Licencia de demolición":                          ("alquiler+con",      "definitivo",        "⚡ ACTUAR ESTA SEMANA"),
        "Demolición de edificio":                          ("alquiler+con",      "definitivo",        "⚡ ACTUAR ESTA SEMANA"),
    }
    # Types with no commercial value
    _SKIP_TIPOS = {"Consulta urbanística", "Declaración Responsable Ocupación Vía Pública",
                   "Declaración Responsable Actividad - Migracion Platea"}

    # ── Result builder ────────────────────────────────────────────────────────
    results  = []
    seen_exp = set()

    def _process_xlsx_row(row_dict: dict, row_idx: int):
        """Process one row from the XLSX. Appends to results if lead-worthy."""
        # Fuzzy column lookup — handles encoding variations in Spanish column names
        def _col(primary, *aliases):
            v = row_dict.get(primary)
            if v is not None and str(v).strip() not in ("","nan","None"): return str(v).strip()
            for a in aliases:
                v = row_dict.get(a)
                if v is not None and str(v).strip() not in ("","nan","None"): return str(v).strip()
            # Try case-insensitive match as last resort
            pk_l = primary.lower().replace(" ","").replace("ó","o").replace("ú","u").replace("é","e").replace("á","a").replace("ñ","n")
            for k in row_dict:
                if k.lower().replace(" ","").replace("ó","o").replace("ú","u").replace("é","e").replace("á","a").replace("ñ","n") == pk_l:
                    v = row_dict[k]
                    if v is not None and str(v).strip() not in ("","nan","None"): return str(v).strip()
            return ""

        tipo      = _col("Tipo de expediente", "TIPO DE EXPEDIENTE", "TipoExpediente")
        fecha_raw = _col("Fecha concesión",    "FECHA CONCESION", "Fecha Concesion", "FechaConcesion", "Fecha concesion")
        tipo_via  = _col("Tipo Via",           "TIPO VIA", "TipoVia")
        nombre    = _col("Nombre Via",         "NOMBRE VIA", "NombreVia", "Nombre Vía")
        numero    = _col("Número",             "NUMERO", "Num", "Nº")
        dist      = _col("Descripción Distrito","DESCRIPCION DISTRITO","Descripcion Distrito","Distrito").title()
        barrio    = _col("Descripción Barrio", "DESCRIPCION BARRIO","Descripcion Barrio","Barrio").title()
        uso       = _col("Uso","USO")
        interesado= _col("Interesado","INTERESADO")
        proc      = _col("Procedimiento","PROCEDIMIENTO")
        _lat_raw  = _col("Latitud","LATITUD","lat","latitude")
        _lon_raw  = _col("Longitud","LONGITUD","lon","longitude")

        if tipo in _SKIP_TIPOS or not tipo:
            return

        profile_hint, phase, action_window = _TIPO_MAP.get(
            tipo, ("constructora+mep", "solicitud", "📅 MONITORIZAR"))

        # Date filter — Fecha concesión (handles Excel serial → "DD/MM/YYYY" from _cell_val)
        if fecha_raw and str(fecha_raw).strip() not in ("", "nan", "None"):
            _include_row = True
            _fecha_str = str(fecha_raw).strip()
            for _fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    import datetime as _dtf_mod
                    _rdate = _dtf_mod.datetime.strptime(_fecha_str[:10], _fmt).date()
                    # 6-month buffer: accept records up to 6m before date_from
                    _buf = (date_from - _dtf_mod.timedelta(days=180)).date()
                    if _rdate < _buf or _rdate > date_to.date():
                        _include_row = False
                    break
                except ValueError:
                    continue
            else:
                # Last resort: dateutil
                try:
                    _rdate = _dp.parse(_fecha_str, dayfirst=True).date()
                    _buf2 = (date_from - __import__("datetime").timedelta(days=180)).date()
                    if _rdate < _buf2 or _rdate > date_to.date():
                        _include_row = False
                except Exception:
                    pass  # unparseable → include (conservative)
            if not _include_row:
                return

        # Build address
        try:
            numero = str(int(float(numero))) if numero and numero != "nan" else ""
        except Exception:
            numero = ""
        addr = " ".join(filter(None, [tipo_via, nombre, numero])).strip().title()
        if dist:  addr += f", {dist}"
        if barrio and barrio != dist: addr += f" ({barrio})"

        # GPS coordinates — use Latitud/Longitud if available
        _lat_val = _lon_val = None
        def _dms_to_dec(dms_str):
            """Convert '40º27\'58.79'' N' → 40.465776"""
            import re as _r
            m = _r.search(r"(\d+)[º°](\d+)'([\d.]+)''?\s*([NSEW])", dms_str)
            if not m: return None
            d2,mn,s,hemi = int(m.group(1)),int(m.group(2)),float(m.group(3)),m.group(4)
            dec = d2 + mn/60 + s/3600
            return -dec if hemi in ("S","W") else dec
        if _lat_raw: _lat_val = _dms_to_dec(_lat_raw)
        if _lon_raw: _lon_val = _dms_to_dec(_lon_raw)

        # If no address, use district as fallback (don't skip)
        if not addr or addr.strip() in (",",""):
            if dist:
                addr = f"Madrid - {dist}"
            elif not addr:
                return  # truly no location info at all

        # Build unique ID (no formal EXPEDIENTE in this dataset)
        exp = f"DM-{_current_year}-{row_idx}"
        if exp in seen_exp: return
        seen_exp.add(exp)

        # Source URL — CONEX public search for this address
        q = (f"{nombre}+{numero}+Madrid").replace(" ", "+")
        source_url = (f"https://sede.madrid.es/portal/site/tramites/menuitem"
                      f".62876cb64654a55e2dbd7003a8a409a0/?vgnextoid=fa3a74&q={q}")

        is_company = interesado == "Persona jurídica"
        desc = f"{tipo} | {proc} | {dist} {barrio}"

        rec = {
            "TIPO_EXPEDIENTE":    tipo,
            "PROCEDIMIENTO":      proc,
            "OBJETO":             tipo,
            "DESCRIPCION":        desc,
            "DIRECCION":          addr,
            "BARRIO":             barrio,
            "DISTRITO":           dist,
            "FECHA_OTORGAMIENTO": fecha_raw,
            "RESULTADO":          "Otorgada",
            "PEM":                None,
            "EXPEDIENTE":         exp,
            "INTERESADO":         interesado,
        }
        results.append((exp, rec, source_url, profile_hint, action_window, phase))

    # ══════════════════════════════════════════════════════════════════════════
    # DOWNLOAD XLSX (primary) or CSV (fallback)
    # ══════════════════════════════════════════════════════════════════════════
    _current_year = date_to.year
    _r_data = None
    _used_url = None
    _err_last = "not tried"

    log(f"  🏛️ datos.madrid: downloading {_current_year} XLSX...")

    for url in _XLSX_CANDIDATES:
        if not time_ok(need_s=30): break
        _r_data, _err_last = _fetch(url, timeout=90)
        if _r_data:
            _used_url = url
            log(f"  🏛️ datos.madrid: ✅ {url.split('/')[-1]} ({len(_r_data.content)//1024}KB)")
            break
        log(f"  ⚠️ datos.madrid [{url.split('/')[-1]}]: {_err_last}")

    if not _r_data:
        if "403" in str(_err_last):
            log(f"  ❌ datos.madrid: WAF/IP block (HTTP 403).")
            log(f"     Fix: deploy Cloudflare Worker proxy → set DATOS_MADRID_PROXY secret")
            log(f"     Setup JS snippet: see DATOS_MADRID_PROXY constant in engine source")
        else:
            log(f"  ⚠️ datos.madrid: all download attempts failed ({_err_last})")
            log(f"     If this persists, set DATOS_MADRID_PROXY secret for reliable access")
        return []

    # ── Parse XLSX ─────────────────────────────────────────────────────────────
    # Uses Python stdlib only (zipfile + xml.etree) — no openpyxl dependency.
    # XLSX is a ZIP of XML files; xl/sharedStrings.xml holds strings,
    # xl/worksheets/sheet1.xml holds cell references.
    if _used_url and _used_url.endswith(".xlsx"):
        try:
            import zipfile as _zf, io as _io
            import xml.etree.ElementTree as _XE
            _NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

            zf  = _zf.ZipFile(_io.BytesIO(_r_data.content))
            nl  = zf.namelist()

            # Read shared strings table
            _ss: list = []
            if "xl/sharedStrings.xml" in nl:
                _ss_root = _XE.fromstring(zf.read("xl/sharedStrings.xml"))
                for si in _ss_root.findall(f".//{{{_NS}}}si"):
                    parts = si.findall(f".//{{{_NS}}}t")
                    _ss.append("".join((p.text or "") for p in parts))

            # Find first sheet
            _ws_files = sorted(n for n in nl
                                if n.startswith("xl/worksheets/sheet") and n.endswith(".xml"))
            if not _ws_files:
                raise ValueError("No worksheet found in XLSX")

            _ws_root  = _XE.fromstring(zf.read(_ws_files[0]))
            rows_raw  = _ws_root.findall(f".//{{{_NS}}}row")

            def _cell_val(cell_el):
                ct  = cell_el.get("t","")
                v   = cell_el.find(f"{{{_NS}}}v")
                if v is None or not v.text: return ""
                if ct == "s":
                    try:    return _ss[int(v.text)]
                    except: return v.text
                if ct == "inlineStr":
                    t = cell_el.find(f".//{{{_NS}}}t")
                    return t.text if t is not None else ""
                # number/date: detect Excel serial dates (40000-55000 ≈ 2009-2050)
                raw_val = v.text
                try:
                    _n = float(raw_val)
                    if 40000 <= _n <= 55000:
                        import datetime as _dt_mod
                        _base = _dt_mod.datetime(1899, 12, 31)
                        _converted = _base + _dt_mod.timedelta(days=_n)
                        return _converted.strftime("%d/%m/%Y")
                except (TypeError, ValueError):
                    pass
                return raw_val

            def _col_idx(ref):
                """Convert column letter(s) to 0-based index: A→0, B→1, Z→25, AA→26"""
                letters = "".join(c for c in ref if c.isalpha())
                idx = 0
                for c in letters:
                    idx = idx * 26 + (ord(c.upper()) - 64)
                return idx - 1

            # First row = headers
            headers_by_idx: dict = {}
            if rows_raw:
                for cell in rows_raw[0].findall(f"{{{_NS}}}c"):
                    ref = cell.get("r","")
                    if ref:
                        headers_by_idx[_col_idx(ref)] = _cell_val(cell).strip()

            row_idx = 0
            for row_el in rows_raw[1:]:
                row_dict: dict = {}
                for cell in row_el.findall(f"{{{_NS}}}c"):
                    ref = cell.get("r","")
                    if not ref: continue
                    col = _col_idx(ref)
                    hdr = headers_by_idx.get(col, f"col_{col}")
                    row_dict[hdr] = _cell_val(cell).strip()
                if any(row_dict.values()):
                    _process_xlsx_row(row_dict, row_idx)
                    row_idx += 1
            log(f"  🏛️ datos.madrid XLSX: {row_idx} rows scanned → {len(results)} leads")
        except Exception as e:
            log(f"  ⚠️ datos.madrid XLSX parse error: {e}")

    # ── Parse CSV (fallback) ──────────────────────────────────────────────────
    elif _used_url and _used_url.endswith(".csv"):
        try:
            import csv as _csv, io as _io
            raw = _r_data.content
            enc = "utf-8-sig"
            for _enc in ("utf-8-sig","utf-8","latin-1","cp1252"):
                try: raw.decode(_enc); enc = _enc; break
                except: pass
            text   = raw.decode(enc, errors="replace")
            reader = _csv.DictReader(_io.StringIO(text), delimiter=";")
            row_idx = 0
            for row in reader:
                rec_norm = {k.strip(): v.strip() for k,v in row.items()}
                # Map CSV columns to XLSX-compatible names
                for _alias, _canon in [
                    ("TIPO_EXPEDIENTE","Tipo de expediente"),
                    ("PROCEDIMIENTO","Procedimiento"),
                    ("NOMBRE_VIA","Nombre Via"), ("TIPO_VIA","Tipo Via"),
                    ("NUM","Número"), ("DISTRITO_DESC","Descripción Distrito"),
                    ("BARRIO_DESC","Descripción Barrio"),
                    ("FECHA_CONCESION","Fecha concesión"),
                    ("INTERESADO","Interesado"),
                ]:
                    if _alias in rec_norm and _canon not in rec_norm:
                        rec_norm[_canon] = rec_norm[_alias]
                _process_xlsx_row(rec_norm, row_idx)
                row_idx += 1
            log(f"  🏛️ datos.madrid CSV: {row_idx} rows scanned → {len(results)} leads")
        except Exception as e:
            log(f"  ⚠️ datos.madrid CSV parse error: {e}")

    log(f"  🏛️ datos.madrid: {len(results)} licencias total for {date_from.date()}→{date_to.date()}")
    return results


def process_datos_madrid_item(exp, rec, source_url, profile_hint, action_window_hint="", phase_hint="", idx=0, total=0):
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

        obj    = str(rec.get("OBJETO", "") or rec.get("TIPO_EXPEDIENTE","") or "").strip()
        desc   = str(rec.get("DESCRIPCION", "") or "").strip()
        addr   = str(rec.get("DIRECCION", "") or "").strip()
        barrio = str(rec.get("BARRIO", "") or "").strip()
        dist   = str(rec.get("DISTRITO", "") or "").strip()
        fecha  = str(rec.get("FECHA_OTORGAMIENTO", "") or rec.get("Fecha concesión","") or "").strip()[:10]
        clase  = str(rec.get("CLASE_LICENCIA", "") or rec.get("Tipo de expediente","") or "").strip()
        result = str(rec.get("RESULTADO", "") or "Otorgada").strip()
        # Override phase/action_window from XLSX data if provided
        if action_window_hint: p_override_aw = action_window_hint
        if phase_hint:         p_override_ph = phase_hint

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
