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
parser.add_argument("--client",  required=True)
parser.add_argument("--weeks",   type=int, default=4,
    help="1=daily(1-2 days), 2-3=weekly, 4+=full backfill")
parser.add_argument("--digest",  action="store_true")
parser.add_argument("--resume",  action="store_true",
    help="Skip collection, process saved queue from previous run")
parser.add_argument("--backfill-ai", action="store_true",
    help="Re-run AI evaluation on existing sheet rows that have empty AI Evaluation column")
parser.add_argument("--workers", type=int, default=4,
    help="Concurrent processing threads (default 4)")
args = parser.parse_args()

with open(args.client, "r", encoding="utf-8") as f:
    CFG = json.load(f)

SHEET_ID         = CFG["sheet_id"]
CLIENT_EMAIL_VAR = CFG["email_to_secret_name"]
MIN_VALUE_EUR    = CFG.get("min_declared_value_eur", 0)
WEEKS_BACK       = args.weeks
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "").strip()
USE_AI           = bool(OPENAI_API_KEY)
QUEUE_FILE       = "/tmp/bocm_queue.json"
N_WORKERS        = max(1, min(args.workers, 8))

# ── Run mode ──────────────────────────────────────────────────────────────────
# DAILY  (--weeks 1)  : scan last 2 working days only. Target: 30-45 min.
# WEEKLY (--weeks 2-3): scan all days in window + focused keywords. 1-2 hrs.
# FULL   (--weeks 4+) : everything. 2-4 hrs. Use --resume for safety.
MODE = "daily" if WEEKS_BACK <= 1 else ("weekly" if WEEKS_BACK <= 3 else "full")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}|{elapsed_str()}] {msg}", flush=True)

# ════════════════════════════════════════════════════════════
# HTTP — thread-local sessions for concurrent processing
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"

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
    return m.group(1).upper() if m else None

def normalise_url(url):
    """Any PDF/JSON URL → HTML entry page (has JSON-LD with full text)."""
    m = re.search(r'(bocm-\d{8}-\d+)', url, re.I)
    if m: return f"{BOCM_BASE}/{m.group(1).lower()}"
    return url

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
KW_WEEKLY = [
    # ── Core licencias ─────────────────────────────────────────────────────────
    ("obra mayor",              SECTION_III, 15, "ALL"),
    ("licencia urbanística",    SECTION_III, 12, "ALL"),
    ("declaración responsable", SECTION_III, 10, "ALL"),
    ("primera ocupación",       SECTION_III, 10, "MEP+MAT"),

    # ── Urbanismo / plans ──────────────────────────────────────────────────────
    ("proyecto de urbanización", SECTION_III, 12, "PRO+CON"),
    ("proyecto de urbanización", SECTION_II,   8, "PRO+CON"),
    ("reparcelación",            SECTION_III, 10, "PRO"),
    ("junta de compensación",    SECTION_III, 10, "PRO+CON"),
    ("plan parcial",             SECTION_III, 10, "PRO+CON"),
    ("plan especial",            SECTION_III, 12, "PRO+CON+RET"),
    ("plan especial",            SECTION_II,   8, "PRO+CON"),
    ("aprobación definitiva",    SECTION_III, 12, "ALL"),
    ("modificación puntual",     SECTION_III,  8, "PRO+CON"),
    ("convenio urbanístico",     SECTION_III,  8, "PRO"),
    ("estudio de detalle",       SECTION_III,  8, "PRO"),

    # ── Industrial / logistics ─────────────────────────────────────────────────
    ("nave industrial",          SECTION_III, 10, "IND+MAT"),
    ("plataforma logística",     SECTION_III,  8, "IND+MAT"),
    ("parque empresarial",       SECTION_III,  8, "IND+CON+MAT"),
    ("actividades productivas",  SECTION_III,  8, "IND+MAT"),

    # ── Contracts / procurement ────────────────────────────────────────────────
    ("licitación de obras",      SECTION_III, 10, "CON+MAT"),
    ("licitación de obras",      SECTION_II,   8, "CON+MAT"),
    ("adjudicación de obras",    SECTION_III,  8, "CON+MAT"),

    # ── ICIO — confirmed construction with exact PEM ───────────────────────────
    ("base imponible",           SECTION_III, 10, "ALL"),
    ("base imponible",           SECTION_V,    8, "ALL"),
    ("liquidación icio",         SECTION_V,    8, "ALL"),
]

KW_EXTRA_FULL = [
    # Additional keywords for full backfill only
    ("licencia de edificación",  SECTION_III,  8, "ALL"),
    ("autorización de obras",    SECTION_III,  8, "ALL"),
    ("se expide licencia",       SECTION_III,  8, "ALL"),
    ("edificio plurifamiliar",   SECTION_III,  8, "MEP"),
    ("nueva construcción",       SECTION_III,  8, "MEP+CON+MAT"),
    ("nueva planta",             SECTION_III,  8, "MEP+CON+MAT"),
    ("rehabilitación integral",  SECTION_III,  8, "MEP+MAT"),
    ("cambio de uso",            SECTION_III,  8, "MEP+RET"),
    ("demolición",               SECTION_III,  6, "CON+IND"),
    ("hotel",                    SECTION_III,  6, "MEP+CON+MAT"),
    ("residencia de mayores",    SECTION_III,  6, "MEP"),
    ("residencia de estudiantes",SECTION_III,  6, "MEP"),
    ("centro de salud",          SECTION_III,  6, "MEP"),
    ("edificio de oficinas",     SECTION_III,  6, "MEP+RET+MAT"),
    ("instalación de ascensor",  SECTION_III,  6, "MEP"),
    ("uso terciario",            SECTION_III,  6, "RET"),
    ("gran superficie",          SECTION_III,  6, "RET+CON"),
    ("centro comercial",         SECTION_III,  8, "RET+CON"),
    ("local comercial",          SECTION_III,  8, "RET"),
    ("licencia de actividad",    SECTION_III,  8, "RET+IND"),
    ("almacén",                  SECTION_III,  8, "IND+MAT"),
    ("polígono industrial",      SECTION_III,  8, "IND+MAT"),
    ("centro de distribución",   SECTION_III,  6, "IND+MAT"),
    ("zona logística",           SECTION_III,  6, "IND+MAT"),
    ("plan parcial",             SECTION_II,   8, "PRO+CON"),
    ("junta de compensación",    SECTION_II,   8, "PRO+CON"),
    ("obras de urbanización",    SECTION_III,  8, "CON+MAT"),
    ("contrato de obras",        SECTION_III,  8, "CON+MAT"),
    ("valor estimado",           SECTION_III,  8, "CON+MAT"),
    ("impuesto construcciones",  SECTION_V,    6, "ALL"),
    ("notificación tributaria",  SECTION_V,    6, "ALL"),
    ("sector de suelo",          SECTION_III,  6, "PRO+CON"),
    ("suelo urbanizable",        SECTION_III,  6, "PRO+CON"),
    ("modificación del plan",    SECTION_II,   6, "PRO+CON"),
    # ── FCC / Gran Infraestructura ────────────────────────────────────────────
    ("plan de sectorización",    SECTION_III,  8, "FCC+CON"),
    ("obra civil",               SECTION_III,  8, "FCC+CON"),
    ("obras de infraestructura", SECTION_III,  8, "FCC+CON"),
    ("concesión de obra",        SECTION_III,  8, "FCC+CON"),
    ("aprobación definitiva",    SECTION_II,  10, "FCC+CON"),
    ("contrato de obras",        SECTION_II,   8, "FCC+CON"),
    ("licitación de obras",      SECTION_II,  10, "FCC+CON"),
    # ── Kiloutou / Alquiler Maquinaria ────────────────────────────────────────
    ("obras de reforma",         SECTION_III,  8, "KILOUTOU+MAT"),
    ("obras de adecuación",      SECTION_III,  6, "KILOUTOU"),
    ("obras de ampliación",      SECTION_III,  6, "KILOUTOU+MEP"),
    ("nueva construcción",       SECTION_II,   8, "KILOUTOU+CON"),
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
DAY_SCAN_KWS   = ["licencia", "urbanización", "licitación"]
DAY_SCAN_KWS_V = ["base imponible", "icio", "notificación"]

# ── Profile trigger words (used in scoring and PDF analysis) ────────────────
# Presence of these in the document text boosts score for the matching profile.
PROFILE_TRIGGERS = {
    "fcc": [
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
    "kiloutou": [
        "obra mayor", "nueva construcción", "rehabilitación", "demolición",
        "licitación de obras", "nave industrial", "urbanización",
        "movimiento de tierras", "pavimentación",
    ],
}

def is_bad_url(url):
    if not url or "bocm.es" not in url: return True
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
    Enhanced PDF extraction:
    1. Standard text extraction
    2. Table extraction for PEM/ETAPA financial rows
    All pages extracted (not capped).
    """
    try:
        r = (get_thread_session() if threading.current_thread().name != "MainThread"
             else get_session()).get(
            url, timeout=50, verify=False, allow_redirects=True,
            headers={**make_headers(referer=BOCM_BASE), "Accept":"application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 400: return ""
        if r.content[:4] != b"%PDF": return ""

        text_parts  = []
        table_parts = []
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages:
                t = pg.extract_text()
                if t: text_parts.append(t)
                for table in pg.extract_tables():
                    if not table: continue
                    for row in table:
                        if not row: continue
                        rt = " | ".join(str(c or "") for c in row)
                        if any(kw in rt.upper() for kw in
                               ["ETAPA","PEM","IMPORTE","PRESUPUESTO","ICIO",
                                "BASE IMPONIBLE","TOTAL","LICITACIÓN"]):
                            table_parts.append(rt)
        full = "\n".join(text_parts)
        if table_parts:
            full += "\n\nTABLA_DATOS:\n" + "\n".join(table_parts)
        return full[:20000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""


def _fetch_pem_only_from_pdf(pdf_url):
    """
    Lightweight PDF scan for PEM/presupuesto values only.
    Reads first 4 pages looking for financial tables.
    Called when JSON-LD text exists but has no PEM.
    Returns a short string with any PEM-like values found.
    """
    try:
        sess = (get_thread_session() if threading.current_thread().name != "MainThread"
                else get_session())
        r = sess.get(pdf_url, timeout=30, verify=False,
                     headers={**make_headers(referer=BOCM_BASE), "Accept":"application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 500: return ""
        if r.content[:4] != b"%PDF": return ""

        parts = []
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages[:4]:
                # Table extraction first (PEM often in summary table)
                for tbl in (pg.extract_tables() or []):
                    for row in (tbl or []):
                        if not row: continue
                        row_s = " | ".join(str(c or "") for c in row)
                        if any(k in row_s.upper() for k in
                               ["PEM","PRESUPUESTO","IMPORTE","BASE IMPONIBLE",
                                "EJECUCI","TOTAL","ETAPA","€"]):
                            parts.append(row_s)
                # Text scan for PEM lines only (not full text)
                t = pg.extract_text() or ""
                for line in t.split("\n"):
                    if any(k in line.upper() for k in
                           ["PRESUPUESTO DE EJECUCIÓN","P.E.M","BASE IMPONIBLE",
                            "ETAPA", "EUROS", "€"]):
                        parts.append(line.strip())

        if parts:
            return "TABLA_DATOS:\n" + "\n".join(parts[:30])
        return ""
    except Exception as e:
        return ""

def fetch_announcement(url):
    """Returns (text, pdf_url, pub_date, doc_title)."""
    url_low = url.lower()
    pdf_url = None

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
    "normas subsidiarias de urbanismo",
    "criterio interpretativo vinculante",
    "corrección de errores del bocm", "corrección de hipervínculo",
    "licitación de servicios de", "licitación de suministro de",
    "contrato de servicios de limpieza", "contrato de mantenimiento de",
    "servicio de limpieza", "servicio de recogida",
    # FINISHED projects (no more opportunity)
    "disolución de la junta de compensación",
    "disolver la junta de compensación",
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
    "rehabilitación integral", "rehabilitación de edificio",
    "reforma integral", "reforma estructural",
    "demolición y construcción", "demolición y nueva planta",
    "ampliación de edificio",
    "nave industrial", "naves industriales",
    "almacén industrial", "almacén", "centro logístico",
    "plataforma logística", "parque empresarial",
    "instalación industrial", "actividades productivas",
    "edificio industrial", "uso industrial",
    "hotel", "bloque de viviendas", "demolición", "derribo",
    "cambio de uso", "primera ocupación",
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
]

SMALL_ACTIVITY = [
    "peluquería", "barbería", "salón de belleza",
    "pastelería", "panadería", "carnicería", "pescadería",
    "frutería", "estanco", "locutorio", "quiosco",
    "taller mecánico", "academia de idiomas", "academia de danza",
    "centro de yoga", "pilates", "clínica dental", "consulta médica",
    "farmacia", "bar ", "cafetería", "restaurante",
    "heladería", "pizzería", "kebab",
    "lavandería", "tintorería", "zapatería", "cerrajería",
    "papelería", "floristería", "gestoría",
]

def classify_permit(text):
    """Returns (is_lead, reason, tier 1-5)."""
    t = text.lower()

    for kw in HARD_REJECT:
        if kw in t: return False, f"Admin noise: '{kw}'", 0

    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 2: return False, "Application phase (not granted)", 0

    for kw in DENIAL_SIGNALS:
        if kw in t: return False, f"Denial: '{kw}'", 0

    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)
    if not has_grant:        return False, "No grant language", 0
    if not has_construction: return False, "Grant but no construction content", 0

    has_major = any(p in t for p in [
        "obra mayor","nueva construcción","nueva planta","nave industrial",
        "proyecto de urbanización","rehabilitación integral","plan especial",
        "plan parcial","bloque de viviendas","junta de compensación",
        "licitación de obras","base imponible"])
    if not has_major:
        for kw in SMALL_ACTIVITY:
            if kw in t: return False, f"Small activity: '{kw}'", 0

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

    if any(p in t for p in ["obra mayor","reforma integral","cambio de uso",
                             "ampliación de edificio","declaración responsable"]):
        return True, "Tier-4: Obra mayor / cambio de uso", 4

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
        score += 22
    elif pt in ("obra mayor",):
        score += 18
    elif pt in ("licencia primera ocupación",):
        score += 15
    elif pt in ("licencia de actividad",):
        score += 10
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
    if phase == "definitivo":   score += 8
    elif phase == "licitacion": score += 10
    elif phase == "inicial":    score -= 5

    # Budget
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)) and val > 0:
        if val >= 50_000_000:   score += 38
        elif val >= 10_000_000: score += 35
        elif val >= 2_000_000:  score += 28
        elif val >= 500_000:    score += 20
        elif val >= 100_000:    score += 12
        elif val >= 50_000:     score += 6

    # Logistics corridor bonus
    logistics_munis = {"valdemoro","getafe","coslada","alcalá de henares","torrejón de ardoz",
                       "arganda del rey","fuenlabrada","alcobendas","san sebastián de los reyes",
                       "rivas-vaciamadrid","mejorada del campo","pinto","parla"}
    if any(m in muni for m in logistics_munis) and "industrial" in pt:
        score += 5

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

    # 6. Public contract budget
    for pat in [
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'valor\s+estimado[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

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
    if any(p in t for p in ["licitación de obras","contrato de obras","se convoca licitación",
                             "convocatoria de licitación","adjudicación de obras"]):
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
    elif any(p in t for p in ["plan especial de cambio de uso","cambio de uso de local a vivienda"]):
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
    return res


def generate_supplies_estimate(permit_type, pem, description):
    """Keyword-based supplies/equipment estimate — fallback when AI doesn't provide it."""
    pt  = (permit_type or "").lower()
    pem = pem or 0
    d   = (description or "").lower()
    pem_s = f"€{pem/1_000_000:.1f}M" if pem >= 1_000_000 else (f"€{int(pem/1000)}K" if pem >= 1000 else "N/D")

    if "urbanización" in pt or "urbaniz" in d:
        return (f"🔧 Redes eléctrica BT/MT, alumbrado público, CT | "
                f"🛒 Hormigón HA-25 ~500m³, tuberías PVC DN200-500, áridos | "
                f"🚧 Excavadoras, compactadores, dúmpers ({pem_s})")
    if "nueva construcción" in pt or "plurifamiliar" in d or "nueva planta" in pt:
        m2 = int(pem/1800) if pem else 0
        elev = max(1, m2//500) if m2 else 2
        return (f"🔧 Ascensores ×{elev}, HVAC centralizado, PCI | "
                f"🛒 Acero ~{int(m2*0.05)}t, hormigón ~{int(m2*0.25)}m³ | "
                f"🚧 Grúa torre, andamios, plataformas elevadoras")
    if "industrial" in pt or "nave" in d:
        m2 = int(pem/500) if pem else 0
        return (f"🔧 Instal. eléctrica MT, clima industrial, PCI/rociadores | "
                f"🛒 Perfil metálico {int(m2*0.04)}t, panel sándwich {m2}m², solera | "
                f"🚧 Grúas, robots demolición, explanación")
    if "rehabilitación" in pt or "reforma" in pt:
        return (f"🔧 Sustitución instalaciones (eléctrica, fontanería, HVAC) | "
                f"🛒 Aislamiento, carpintería, revestimientos | "
                f"🚧 Andamios fachada, plataformas tijera")
    if "licitación" in pt:
        return (f"🏗️ Licitación {pem_s} — presentar oferta técnica + económica | "
                f"🚧 Adjudicatario necesitará maquinaria de construcción | "
                f"🛒 Acordar precios de materiales con ganador")
    if "primera ocupación" in pt:
        return ("🔧 Revisiones finales, legalización contadores, OCA | "
                "🛒 Acabados finales: pavimento, pintura, carpintería | "
                "🚧 Plataformas elevadoras para remates")
    return (f"🏗️ Proyecto {pem_s} — revisar PDF para detalles técnicos | "
            "🛒 Materiales según especificaciones del proyecto")

def ai_extract(text, url, pub_date):
    if not USE_AI: return keyword_extract(text, url, pub_date)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        sys_prompt = """You are an elite construction intelligence analyst for Spain.
You read BOCM (Boletín Oficial de la Comunidad de Madrid) documents to extract actionable leads.

Clients: MEP Installers (elevators/HVAC/fire) | Retail Expansion | Promotores/RE
         Gran Constructora | Industrial/Logistics | Materials Suppliers

CRITICAL RULES:
1. Return ONLY valid JSON — no markdown, no text outside JSON.
2. If NOT a specific construction project → {"permit_type":"none","confidence":"low"}
3. Required fields: applicant, address, municipality, permit_type, description,
   declared_value_eur, date_granted, confidence, lead_score, expediente, phase.
4. permit_type (exact strings only):
   "urbanización" | "plan especial" | "plan especial / parcial" |
   "obra mayor nueva construcción" | "obra mayor industrial" | "obra mayor rehabilitación" |
   "cambio de uso" | "declaración responsable obra mayor" | "licencia primera ocupación" |
   "licencia de actividad" | "licitación de obras" | "none"
5. declared_value_eur: Extract PEM / ICIO base imponible / licitación budget.
   For multi-stage projects: SUM all Etapa PEMs. Hard cap 3,000,000,000. NUMBER or null.
   ICIO base imponible = PEM exactly (Spanish tax law Art. 102 TRLRHL).
6. applicant: The PROMOTOR / company building. For urbanización = "Junta de Compensación [NAME]".
   For licitación = "Ayuntamiento de [MUNI]". Never blank.
7. municipality: Specific Madrid town (e.g. "Getafe","Las Rozas"). NOT "Comunidad de Madrid".
8. description: ONE sentence, commercially focused. Include: what is built, m² if available,
   location specifics, budget, timeline, commercial opportunity.
   Examples:
   "Urbanización Las Tablas Oeste (74ha), Fuencarral-El Pardo — PEM €74M, 2 etapas 24+36 meses"
   "Nave industrial 12.000m² Polígono Valdemoro — logística, promotor DHL Supply Chain"
   "Rehab. integral edificio 48 viviendas + garaje, C/López de Hoyos 220, Madrid — PEM €3.2M"
   "Licitación obras pabellón deportivo Alcalá de Henares — presupuesto €1.8M, 18 meses"
9. lead_score: 0–100 integer. Large PEM + definitivo approval = 70-85. No PEM + inicial = 25-40.
10. phase: "definitivo"|"inicial"|"licitacion"|"primera_ocupacion"|"en_tramite"
11. confidence: "high" (all fields confirmed) | "medium" | "low"

DOCUMENT CLASSIFICATION RULES:
- "se ha SOLICITADO" + "plazo de veinte días" → APPLICATION not grant → permit_type:"none"
- "aprobar DEFINITIVAMENTE" → FINAL APPROVAL → phase:"definitivo", confidence:"high"
- "aprobación INICIAL" → first step, public comment follows → phase:"inicial", confidence:"medium"
- "licitación de obras" → public tender → permit_type:"licitación de obras", phase:"licitacion"
- "base imponible del ICIO" → CONFIRMED construction, PEM = base imponible exactly
- "disolución de la junta de compensación" → PROJECT FINISHED → permit_type:"none"
- "Dejar sin efecto" → CORRECTION of old error, current doc IS valid → keep as lead
- "declaración responsable de obra mayor" → valid permit since Ley 1/2020 = licencia equivalent
- Reparcelación/convenio urbanístico/estudio de detalle → early-stage urbanismo → phase:"inicial"

ai_evaluation RULES (NEVER leave empty):
- Always 2-3 sentences in Spanish, commercially focused.
- Sentence 1: What this is commercially (project scale, who benefits).
- Sentence 2: Specific action + timing ("Contactar a la Junta ANTES de que cierren contratos de obra civil en 6-12 meses").
- Sentence 3: Risk/caveat or competitive intelligence.
- Example: "Urbanización definitiva en corredor norte de Madrid con JC ya constituida. Gran Constructora debe pre-calificarse para las futuras licitaciones de obra civil (estimado 2026-2027). Oportunidad directa para instaladores MEP y suministradores de materiales."

profile_fit RULES (add this field):
- List which profiles benefit: e.g. ["fcc","constructora","mep","kiloutou"]
- Based on project type: urbanización/licitación → fcc+constructora; obra mayor → mep+compras; industrial → industrial+kiloutou

TABLA_DATOS extraction: If text contains "TABLA_DATOS:", extract PEM from those rows.
declared_value_eur: SUM all "ETAPA X" values. For ICIO: use "BASE IMPONIBLE" value. """

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":sys_prompt},
                      {"role":"user","content":f"URL: {url}\n\nTexto BOCM:\n{text[:5500]}"}],
            temperature=0, max_tokens=750,
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

        # Force ai_evaluation: never save a lead with empty AI analysis
        if not d.get("ai_evaluation") or len(str(d.get("ai_evaluation","")).strip()) < 20:
            pt   = (d.get("permit_type") or "").lower()
            pem  = d.get("declared_value_eur")
            muni = d.get("municipality","Madrid")
            pem_s = f"€{pem/1_000_000:.1f}M" if pem and pem >= 1_000_000 else (f"€{int(pem/1000):.0f}K" if pem else "PEM no declarado")
            if "urbanización" in pt or "reparcelación" in pt:
                d["ai_evaluation"] = (f"Proyecto de urbanización definitivo en {muni} — {pem_s}. "
                    f"Gran Constructora y FCC-style deben pre-calificarse para futura licitación civil (estimado 12-24 meses). "
                    f"Instaladores MEP y suministradores de materiales: contactar a la Junta de Compensación ahora.")
            elif "licitación" in pt:
                d["ai_evaluation"] = (f"Licitación activa en {muni} — {pem_s}. "
                    f"Plazo de oferta activo. Constructoras deben presentar oferta técnica y económica urgente. "
                    f"Suministradores: acordar precios con futuro adjudicatario.")
            elif "plan especial" in pt or "plan parcial" in pt:
                d["ai_evaluation"] = (f"Aprobación de planeamiento en {muni}. "
                    f"Este paso habilita la futura urbanización y obra nueva — oportunidad de inteligencia anticipada. "
                    f"Promotores RE y Gran Constructora: monitorizar para entrada en JC o propuesta técnica.")
            elif "industrial" in pt or "nave" in pt:
                d["ai_evaluation"] = (f"Proyecto industrial en {muni} — {pem_s}. "
                    f"Oportunidad directa para instaladores eléctricos MT, PCI y suministradores de estructura metálica. "
                    f"Kiloutou y empresas de alquiler: contactar al promotor antes del inicio de obra.")
            elif "nueva construcción" in pt or "rehabilitación" in pt:
                d["ai_evaluation"] = (f"Obra mayor en {muni} — {pem_s}. "
                    f"Instaladores MEP deben contactar al promotor antes de que el constructor cierre contratos. "
                    f"Ascensores, HVAC y PCI se adjudican típicamente en fase de estructura.")
            else:
                d["ai_evaluation"] = (f"Proyecto de construcción en {muni} — {pem_s}. "
                    f"Revisar el PDF original para detalles técnicos y cronograma. "
                    f"Contactar al promotor o Ayuntamiento para confirmar fase de ejecución.")

        # Supplies needed: generate if missing
        if not d.get("supplies_needed") or len(str(d.get("supplies_needed","")).strip()) < 10:
            d["supplies_needed"] = generate_supplies_estimate(
                d.get("permit_type",""), d.get("declared_value_eur"), d.get("description",""))

        return d

    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def extract(text, url, pub_date):
    return ai_extract(text, url, pub_date) if USE_AI else keyword_extract(text, url, pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — 17 columns
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value PEM (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Lead Score","Expediente","Phase",
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
            ws = sh.add_worksheet("Leads", 2000, 20)
            
        if not ws.row_values(1):
            ws.update(values=[HDRS], range_name="A1"); log("✅ Headers written")
        else:
            log("✅ Sheet connected")
        _ws = ws; return _ws
    except Exception as e:
        log(f"❌ Sheet connection failed: {e}"); return None

def load_seen():
    global _seen_urls, _seen_bocm_ids
    ws = get_sheet()
    if not ws: return
    try:
        all_vals = ws.get_all_values()
        for row in all_vals[1:]:
            if len(row) > 9 and row[9].strip():
                url = row[9].strip()
                _seen_urls.add(url)
                bid = extract_bocm_id(url)
                if bid: _seen_bocm_ids.add(bid)
        log(f"✅ {len(_seen_urls)} URLs / {len(_seen_bocm_ids)} BOCM IDs loaded")
    except Exception as e:
        log(f"⚠️  load_seen: {e}")

def write_permit(p, pdf_url=""):
    ws  = get_sheet()
    url = p.get("source_url","")
    bocm_id = extract_bocm_id(url)

    with _sheet_lock:
        if bocm_id and bocm_id in _seen_bocm_ids:
            return False
        if url in _seen_urls:
            return False

        dec  = p.get("declared_value_eur")
        est  = round(dec/0.03) if dec and isinstance(dec,(int,float)) and dec > 0 else ""
        addr = p.get("address") or ""
        muni = p.get("municipality") or "Madrid"
        maps = ""
        if addr:
            maps = ("https://www.google.com/maps/search/"
                    + (addr + " " + muni + " España").replace(" ","+").replace(",",""))

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
def process_one(url, idx, total):
    """Process a single URL. Returns (saved, skipped, error) counts."""
    try:
        text, pdf_url, pub_date, doc_title = fetch_announcement(url)
        if not text or len(text.strip()) < 80:
            return 0, 1, 0  # skip

        is_lead, reason, tier = classify_permit(text)
        if not is_lead:
            return 0, 1, 0  # skip

        p = extract(text, url, pub_date)
        if p is None:
            return 0, 1, 0  # skip

        dec = p.get("declared_value_eur")
        if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec < MIN_VALUE_EUR:
            return 0, 1, 0  # below minimum

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
                dec = f"€{int(float(re.sub(r'[^\d.]','',raw_v.replace('.','').replace(',','.')))):,}" if raw_v else "—"
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
    except Exception as e:
        log(f"❌ Digest error: {e}"); import traceback; traceback.print_exc()

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

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
    log(f"🏗️  PlanningScout Madrid — Engine v9")
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
            if not time_ok(need_s=60): break
            day_urls = scrape_day_section(day, sec=SECTION_III, global_seen=global_seen)
            added    = sum(1 for u in day_urls if add_url(u))
            if added > 0:
                log(f"  📅 {day.strftime('%d/%m/%Y')} [III]: +{added}"); day_total += added
            time.sleep(0.4)

        # Section II (CM-level plans) — scan 2× per week
        if MODE != "daily":
            for day in scan_days[::3]:  # every 3rd day (approx 2× per week)
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

            log(f"  {len(kw_list)} keywords")
            kw_total = 0
            for kw, sec, max_pg, tag in kw_list:
                if not time_ok(need_s=60): break
                log(f"  🔎 [{tag:12s}] '{kw}' [{sec}]")
                urls = search_keyword_chunked(
                    kw, date_from, date_to,
                    global_seen=global_seen,
                    sec=sec, max_pages=max_pg,
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
                if not time_ok(need_s=60): break
                day_urls = scrape_day_section(day, sec=SECTION_V, global_seen=global_seen)
                added    = sum(1 for u in day_urls if add_url(u))
                if added > 0:
                    log(f"  📢 {day.strftime('%d/%m')} [V]: +{added}"); sec5_total += added
                time.sleep(0.4)
            log(f"  Section V: +{sec5_total} | {len(all_urls)} unique")

        # ── SOURCE 4: RSS ─────────────────────────────────────────────────────────
        if time_ok(need_s=120):
            log(f"\n{'─'*55}")
            log(f"📡 SOURCE 4: RSS")
            rss_urls  = get_rss_links(date_from, date_to, global_seen)
            rss_added = sum(1 for u in rss_urls if add_url(u))
            log(f"  RSS: +{rss_added} | {len(all_urls)} unique")

        # Remove already-seen
        all_urls = [u for u in all_urls
                    if u not in _seen_urls and
                    (not extract_bocm_id(u) or extract_bocm_id(u) not in _seen_bocm_ids)]

        log(f"\n{'═'*55}")
        log(f"📋 TOTAL: {len(all_urls)} new URLs to process (elapsed: {elapsed_str()})")
        log(f"{'═'*55}")

        with open(QUEUE_FILE,"w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume to restart if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new.")
        if today.weekday() == 0: send_digest()
        return

    # ── CONCURRENT PROCESSING ────────────────────────────────────────────────────
    saved = skipped = errors = 0
    log(f"\n{'─'*55}")
    log(f"⚙️  Processing {len(all_urls)} URLs with {N_WORKERS} workers…")
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
                # Cancel remaining futures gracefully
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
    log("=" * 70)

    if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
    if today.weekday() == 0: log("\n📧 Monday → digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
