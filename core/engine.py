import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib, random
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
# ARGS
# ════════════════════════════════════════════════════════════
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--client",  required=True)
parser.add_argument("--weeks",   type=int, default=4,
                    help="Weeks back. Daily=1, weekly=2, backfill=8.")
parser.add_argument("--digest",  action="store_true")
parser.add_argument("--resume",  action="store_true")
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

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ════════════════════════════════════════════════════════════
# HTTP — rotating user agents to avoid pattern detection
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"

USER_AGENTS = [
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"),
]

def make_headers(referer=None):
    ua = random.choice(USER_AGENTS)
    h = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        h["Referer"] = referer
    return h

_session = None
_consecutive_bad = 0
MAX_BAD = 5

def make_session():
    s = requests.Session()
    s.headers.update(make_headers())
    for name in ["cookies-agreed", "cookie-agreed", "has_js",
                 "bocm_cookies", "cookie_accepted"]:
        s.cookies.set(name, "1", domain="www.bocm.es")
    return s

def get_session():
    global _session
    if _session is None: _session = make_session()
    return _session

def rotate_session():
    global _session, _consecutive_bad
    log("  🔄 Rotating session…")
    _session = make_session(); _consecutive_bad = 0; time.sleep(12)

def safe_get(url, timeout=30, retries=3, backoff_base=8, referer=None):
    global _consecutive_bad
    sess = get_session()
    if referer:
        sess.headers.update({"Referer": referer})
    for attempt in range(retries):
        try:
            r = sess.get(url, timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code == 200:
                _consecutive_bad = 0; return r
            if r.status_code in (502, 503, 429):
                _consecutive_bad += 1
                wait = backoff_base * (2 ** attempt)
                log(f"  ⚠️  HTTP {r.status_code} — wait {wait}s")
                time.sleep(wait)
                if _consecutive_bad >= MAX_BAD: rotate_session()
                continue
            log(f"  HTTP {r.status_code}: {url[:80]}")
            return r
        except requests.exceptions.Timeout:
            wait = backoff_base * (2 ** attempt)
            log(f"  ⏱️ Timeout — wait {wait}s"); time.sleep(wait)
        except Exception as e:
            log(f"  ❌ {type(e).__name__}: {e}")
            if attempt < retries - 1: time.sleep(backoff_base)
    return None

# ════════════════════════════════════════════════════════════
# BOCM DOCUMENT ID — canonical dedup key
# Same doc appears as PDF, JSON, HTML — all get the same ID.
# ════════════════════════════════════════════════════════════
def extract_bocm_id(url):
    m = re.search(r'(BOCM-\d{8}-\d+)', str(url), re.I)
    return m.group(1).upper() if m else None

def pdf_url_to_html_url(url):
    """Any PDF/JSON URL → HTML entry page (always has JSON-LD)."""
    m = re.search(r'(BOCM-\d{8}-\d+)\.(PDF|json|PDF)$', url, re.I)
    if m:
        return f"{BOCM_BASE}/{m.group(1).lower()}"
    return None

def normalise_url(url):
    """Convert any URL variant → HTML entry page URL."""
    html = pdf_url_to_html_url(url)
    return html if html else url

# ════════════════════════════════════════════════════════════
# BOCM SECTIONS
# I=8385  II=8386  III=8387  IV=8388  V=8389
#
# III = Administración Local Ayuntamientos (licencias, contratos)
# II  = Comunidad de Madrid (plans CM, grandes proyectos)
# V   = Anuncios (ICIO notifications, subastas, public notices)
# ════════════════════════════════════════════════════════════
SECTION_I        = "8385"  # Disposiciones generales (rarely useful)
SECTION_II       = "8386"  # Comunidad de Madrid (big plans, DIR)
SECTION_III      = "8387"  # Administración Local (MAIN)
SECTION_IV       = "8388"  # Entidades públicas
SECTION_V        = "8389"  # Anuncios (ICIO, subastas, notificaciones)
SECTION_LOCAL    = SECTION_III  # default
BOCM_RSS         = "https://www.bocm.es/boletines.rss"

def build_search_url(keyword, date_from, date_to, section=SECTION_III):
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    return (
        f"{BOCM_BASE}/advanced-search"
        f"?search_api_views_fulltext_1={quote(keyword)}"
        f"&field_bulletin_field_date%5Bdate%5D={df}"
        f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
        f"&field_orden_seccion={section}"
        f"&field_orden_apartado_1=All&field_orden_tipo_disposicin_1=All"
        f"&field_orden_organo_y_organismo_1_1=All&field_orden_organo_y_organismo_1=All"
        f"&field_orden_organo_y_organismo_2=All&field_orden_apartado_adm_local_3=All"
        f"&field_orden_organo_y_organismo_3=All&field_orden_apartado_y_organo_4=All"
        f"&field_orden_organo_5=All"
    )

def build_page_url(keyword, date_from, date_to, page, section=SECTION_III):
    df  = date_from.strftime("%d-%m-%Y")
    dt  = date_to.strftime("%d-%m-%Y")
    kw  = quote(keyword)
    return (
        f"{BOCM_BASE}/advanced-search/p"
        f"/field_bulletin_field_date/date__{df}"
        f"/field_bulletin_field_date_1/date__{dt}"
        f"/field_orden_organo_y_organismo_1_1/All/field_orden_organo_y_organismo_1/All"
        f"/field_orden_organo_y_organismo_2/All/field_orden_organo_y_organismo_3/All"
        f"/field_orden_apartado_y_organo_4/All"
        f"/busqueda/{kw}/seccion/{section}"
        f"/apartado/All/disposicion/All/administracion_local/All/organo_5/All"
        f"/search_api_aggregation_2/{kw}/page/{page}"
    )

# ════════════════════════════════════════════════════════════
# SEARCH KEYWORDS — by profile
# Short keywords = more matches (BOCM Solr AND-matches all words)
# Tagged by profile for context in logging
# ════════════════════════════════════════════════════════════

# Core licencias (ALL profiles)
KW_LICENCIAS = [
    ("licencia de obra mayor",        SECTION_III, "ALL"),
    ("licencia urbanística",           SECTION_III, "ALL"),
    ("licencia de obras",              SECTION_III, "ALL"),
    ("declaración responsable",        SECTION_III, "ALL"),
    ("primera ocupación",              SECTION_III, "MEP+MAT"),
    ("licencia de edificación",        SECTION_III, "ALL"),
    ("autorización de obras",          SECTION_III, "ALL"),
    ("resolución favorable",           SECTION_III, "ALL"),
    ("se expide licencia",             SECTION_III, "ALL"),
    ("licencia municipal de obras",    SECTION_III, "ALL"),
]

# Urbanismo (PRO + CON + RET)
KW_URBANISMO = [
    ("proyecto de urbanización",       SECTION_III, "PRO+CON+RET"),
    ("proyecto de urbanización",       SECTION_II,  "PRO+CON"),    # CM-level plans
    ("junta de compensación",          SECTION_III, "PRO+CON"),
    ("junta de compensación",          SECTION_II,  "PRO+CON"),
    ("reparcelación",                  SECTION_III, "PRO"),
    ("proyecto de reparcelación",      SECTION_III, "PRO"),
    ("acuerdo de reparcelación",       SECTION_III, "PRO"),
    ("área de desarrollo",             SECTION_III, "PRO+CON"),
    ("unidad de ejecución",            SECTION_III, "PRO+CON"),
    ("unidad de actuación",            SECTION_III, "PRO+CON"),
    ("plan parcial",                   SECTION_III, "PRO+CON"),
    ("plan parcial",                   SECTION_II,  "PRO+CON"),
    ("plan especial",                  SECTION_III, "PRO+CON+RET"),
    ("plan especial",                  SECTION_II,  "PRO+CON"),
    ("aprobación definitiva",          SECTION_III, "ALL"),
    ("aprobación definitiva",          SECTION_II,  "PRO+CON"),
    ("estudio de detalle",             SECTION_III, "PRO"),
    ("modificación puntual",           SECTION_III, "PRO+CON"),   # plan amendments = new buildable
    ("convenio urbanístico",           SECTION_III, "PRO"),       # developer-council deals
    ("modificación del plan general",  SECTION_II,  "PRO+CON"),
    ("sector de suelo",                SECTION_III, "PRO+CON"),
    ("suelo urbanizable",              SECTION_III, "PRO+CON"),
]

# MEP installers (ascensores, HVAC, PCI)
KW_MEP = [
    ("edificio plurifamiliar",         SECTION_III, "MEP"),
    ("viviendas",                      SECTION_III, "MEP+RET"),
    ("nueva construcción",             SECTION_III, "MEP+CON+MAT"),
    ("nueva planta",                   SECTION_III, "MEP+CON+MAT"),
    ("rehabilitación integral",        SECTION_III, "MEP+MAT"),
    ("cambio de uso",                  SECTION_III, "MEP+RET"),
    ("demolición",                     SECTION_III, "CON+IND"),
    ("hotel",                          SECTION_III, "MEP+CON+MAT"),
    ("residencia de mayores",          SECTION_III, "MEP"),       # mandatory elevators
    ("residencia de estudiantes",      SECTION_III, "MEP"),
    ("centro de salud",                SECTION_III, "MEP"),       # mandatory MEP
    ("edificio de oficinas",           SECTION_III, "MEP+RET+MAT"),
    ("instalación de ascensor",        SECTION_III, "MEP"),       # direct signal
    ("instalación de climatización",   SECTION_III, "MEP"),
    ("instalación de calefacción",     SECTION_III, "MEP"),
    ("protección contra incendios",    SECTION_III, "MEP"),
]

# Retail expansion (RET)
KW_RETAIL = [
    ("uso terciario",                  SECTION_III, "RET"),       # commercial zoning
    ("local comercial",                SECTION_III, "RET"),
    ("centro comercial",               SECTION_III, "RET+CON"),
    ("gran superficie",                SECTION_III, "RET+CON"),
    ("superficie comercial",           SECTION_III, "RET"),
    ("centro de ocio",                 SECTION_III, "RET"),
    ("actividad comercial",            SECTION_III, "RET"),
    ("apertura de establecimiento",    SECTION_III, "RET"),
    ("licencia de apertura",           SECTION_III, "RET"),
    ("licencia de actividad",          SECTION_III, "RET+IND"),
]

# Industrial & Logistics (IND)
KW_INDUSTRIAL = [
    ("nave industrial",                SECTION_III, "IND+MAT"),
    ("almacén",                        SECTION_III, "IND+MAT"),
    ("parque empresarial",             SECTION_III, "IND+CON+MAT"),
    ("plataforma logística",           SECTION_III, "IND+MAT"),
    ("centro de distribución",         SECTION_III, "IND+MAT"),
    ("instalación industrial",         SECTION_III, "IND+MAT"),
    ("actividades productivas",        SECTION_III, "IND+MAT"),   # industrial use class
    ("uso industrial",                 SECTION_III, "IND+MAT"),
    ("polígono industrial",            SECTION_III, "IND+MAT"),
    ("almacenamiento",                 SECTION_III, "IND+MAT"),
    ("edificio industrial",            SECTION_III, "IND+CON+MAT"),
    ("distribución logística",         SECTION_III, "IND+MAT"),
    ("ampliación de nave",             SECTION_III, "IND+MAT"),
    ("zona logística",                 SECTION_III, "IND+MAT"),
]

# Gran Constructora — public contracts (CON)
KW_CONSTRUCTORA = [
    ("licitación de obras",            SECTION_III, "CON+MAT"),
    ("licitación de obras",            SECTION_II,  "CON+MAT"),
    ("contrato de obras",              SECTION_III, "CON+MAT"),
    ("adjudicación de obras",          SECTION_III, "CON+MAT"),
    ("adjudicación del contrato",      SECTION_III, "CON+MAT"),
    ("obras de urbanización",          SECTION_III, "CON+MAT"),
    ("obras de construcción",          SECTION_III, "CON+MAT"),
    ("obras de infraestructura",       SECTION_III, "CON"),
    ("obras de rehabilitación",        SECTION_III, "CON+MEP+MAT"),
    ("obras de reforma",               SECTION_III, "CON+MEP+MAT"),
    ("ejecución de obras",             SECTION_III, "CON+MAT"),
    ("concurso de obras",              SECTION_III, "CON+MAT"),
    ("contratación de obras",          SECTION_III, "CON+MAT"),
    ("obras de adecuación",            SECTION_III, "CON"),
    ("obras de mejora",                SECTION_III, "CON"),
    ("valor estimado",                 SECTION_III, "CON+MAT"),   # contract value in tender
]

# ICIO tax notifications (ALL — confirmed construction with exact PEM)
KW_ICIO = [
    ("liquidación icio",               SECTION_III, "ALL"),
    ("liquidación icio",               SECTION_V,   "ALL"),   # also in anuncios
    ("base imponible",                 SECTION_III, "ALL"),
    ("base imponible",                 SECTION_V,   "ALL"),
    ("impuesto construcciones",        SECTION_III, "ALL"),
    ("impuesto construcciones",        SECTION_V,   "ALL"),
    ("notificación tributaria",        SECTION_V,   "ALL"),
]

# Combine all keyword groups
SEARCH_KEYWORDS = (
    KW_LICENCIAS + KW_URBANISMO + KW_MEP + KW_RETAIL +
    KW_INDUSTRIAL + KW_CONSTRUCTORA + KW_ICIO
)

# ════════════════════════════════════════════════════════════
# LOGISTICS CORRIDOR MUNICIPALITIES — targeted searches
# These are the highest-density construction zones around Madrid.
# We search them specifically by municipality name to catch anything
# the keyword-only approach misses.
# ════════════════════════════════════════════════════════════
LOGISTICS_MUNICIPALITIES = [
    # South corridor (biggest logistics hub in Spain)
    "Valdemoro",         # Amazon, DHL, Carrefour distribution
    "Getafe",            # Airbus, industrial
    "Pinto",             # logistics parks
    "Parla",             # industrial zone
    "Torrejón de Ardoz", # logistics, industrial
    "Coslada",           # major logistics (SEUR, Correos)
    "San Fernando de Henares",  # logistics
    "Mejorada del Campo",       # industrial/logistics
    "Rivas-Vaciamadrid",        # industrial
    "Arganda del Rey",   # chemical, manufacturing
    "Alcalá de Henares", # large industrial parks
    # North corridor
    "Alcobendas",        # premium industrial/commercial
    "Tres Cantos",       # tech park
    "San Sebastián de los Reyes",  # commercial/industrial
    "Colmenar Viejo",    # growing industrial
    # West corridor
    "Majadahonda",       # commercial/residential
    "Las Rozas de Madrid",       # tech/commercial
    "Pozuelo de Alarcón",        # premium commercial
    "Boadilla del Monte",        # residential/commercial
    # Other active municipalities
    "Móstoles",          # industrial/residential
    "Leganés",           # industrial
    "Fuenlabrada",       # Polígono Cobo Calleja (one of Spain's largest)
    "Alcorcón",          # commercial
    "Paracuellos de Jarama",     # growing residential
]

def is_bad_url(url):
    if not url or "bocm.es" not in url: return True
    low = url.lower()
    bad_exts  = (".xml", ".css", ".js", ".png", ".jpg", ".gif",
                 ".ico", ".woff", ".svg", ".zip", ".epub")
    bad_paths = ("/advanced-search", "/login", "/user", "/admin",
                 "/sites/", "/modules/", "#", "javascript:", "/CM_Boletin_BOCM/")
    return (any(low.endswith(x) for x in bad_exts) or
            any(x in low for x in bad_paths))

def url_date_ok(url, date_from):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        try:
            url_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return url_date >= date_from - timedelta(days=1)
        except ValueError: pass
    return True

def extract_result_links(soup):
    links = []
    for sel in [
        "a[href*='/boletin/']", "a[href*='/anuncio/']", "a[href*='/bocm-']",
        ".view-content .views-row a", ".view-content a",
        "article h3 a", "article h2 a",
        ".field--name-title a", "h3.field-content a",
    ]:
        found = soup.select(sel)
        if found:
            for a in found:
                href = a.get("href", "")
                if href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    links.append(full)
            if links: break
    if not links:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            if "bocm.es" in full and any(s in full for s in ["/boletin/", "/anuncio/", "/bocm-"]):
                links.append(full)
    return links

# ════════════════════════════════════════════════════════════
# SEARCH WITH DATE CHUNKING
#
# THE KEY VOLUME FIX:
# BOCM returns max 10 results/page × 25 pages = 250 results per search.
# Over 8 weeks, "licencia de obra mayor" may have 600+ matches.
# → We miss 60%+ of individual building permits.
#
# Solution: Split each search into WEEKLY chunks.
# "licencia de obra mayor" over 8 weeks:
#   → 8 × weekly searches → 8 × 250 max = 2,000 max
#   → Captures virtually all results
# ════════════════════════════════════════════════════════════
def date_chunks(date_from, date_to, chunk_days=7):
    """Split a date range into weekly chunks."""
    chunks = []
    current = date_from
    while current < date_to:
        end = min(current + timedelta(days=chunk_days - 1), date_to)
        chunks.append((current, end))
        current = end + timedelta(days=1)
    return chunks

def search_keyword_chunked(keyword, date_from, date_to, section=SECTION_III, max_pages=25):
    """
    Search with date chunking to overcome the 250-result cap.
    Each week-long chunk can return up to 250 results.
    8-week search = 8 chunks × 250 = up to 2,000 results per keyword.
    """
    all_urls = []
    seen     = set()
    chunks   = date_chunks(date_from, date_to, chunk_days=7)

    for chunk_start, chunk_end in chunks:
        page   = 0
        chunk_new = 0

        while page < max_pages:
            url = (build_search_url(keyword, chunk_start, chunk_end, section) if page == 0
                   else build_page_url(keyword, chunk_start, chunk_end, page, section))

            r = safe_get(url, timeout=25, backoff_base=6,
                         referer=f"{BOCM_BASE}/advanced-search")
            if not r or r.status_code != 200:
                break

            soup  = BeautifulSoup(r.text, "html.parser")
            links = extract_result_links(soup)
            new   = 0
            for link in links:
                if is_bad_url(link): continue
                if not url_date_ok(link, date_from): continue
                norm = normalise_url(link)
                bid  = extract_bocm_id(norm)
                key  = bid if bid else norm
                if key not in seen:
                    seen.add(key); all_urls.append(norm); new += 1; chunk_new += 1

            if new == 0: break
            has_next = bool(
                soup.select_one("li.pager-next a") or
                soup.select_one(".pager__item--next a") or
                soup.find("a", string=re.compile(r"Siguiente|siguiente|Next|»", re.I))
            )
            if not has_next: break
            page += 1
            time.sleep(1)

        if chunk_new > 0:
            log(f"    {chunk_start.strftime('%d/%m')}-{chunk_end.strftime('%d/%m')}: "
                f"{chunk_new} links")
        time.sleep(0.5)

    return all_urls

# ════════════════════════════════════════════════════════════
# MUNICIPALITY TARGETED SEARCH
# For logistics/industrial municipalities, search by name to catch
# any obra-related announcements the keyword search misses.
# ════════════════════════════════════════════════════════════
def search_municipality(municipality, date_from, date_to):
    """
    Search for any construction-related document from a specific municipality.
    Pairs municipality name with construction keywords.
    """
    urls = []
    # Short date windows for municipality searches (less overlap, more precision)
    for kw in ["obra mayor", "licencia", "urbanización", "licitación obras"]:
        found = search_keyword_chunked(
            kw, date_from, date_to, section=SECTION_III, max_pages=10)
        # Filter to only URLs that contain the municipality name (from JSON-LD title)
        # We can't filter by municipality in the URL, so we collect all and classify later
        urls.extend(found)
        time.sleep(0.5)
    return urls

# ════════════════════════════════════════════════════════════
# PER-DAY BULLETIN SCAN — improved version
# Catches individual licencias not indexed by keyword search.
# ════════════════════════════════════════════════════════════
def scrape_day_section(date, section=SECTION_III):
    """
    Get all announcement URLs for a specific date in a specific section.
    Uses multiple broad keywords to maximise coverage.
    """
    urls = []
    seen = set()
    date_str     = date.strftime("%d-%m-%Y")
    date_compact = date.strftime("%Y%m%d")

    broad_kws = ["obra", "licencia", "proyecto", "aprobación", "acuerdo", "urbanismo"]

    for kw in broad_kws:
        r = safe_get(build_search_url(kw, date, date, section), timeout=20, backoff_base=4)
        if not r or r.status_code != 200: continue

        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)
        added = 0
        for link in links:
            if is_bad_url(link): continue
            if date_compact not in link: continue
            norm = normalise_url(link)
            bid  = extract_bocm_id(norm)
            key  = bid if bid else norm
            if key not in seen:
                seen.add(key); urls.append(norm); added += 1

        # Paginate if needed
        page = 1
        while added > 0 and page < 5:
            r2 = safe_get(build_page_url(kw, date, date, page, section),
                          timeout=20, backoff_base=4)
            if not r2 or r2.status_code != 200: break
            soup2 = BeautifulSoup(r2.text, "html.parser")
            links2 = extract_result_links(soup2)
            added2 = 0
            for link in links2:
                if is_bad_url(link): continue
                if date_compact not in link: continue
                norm = normalise_url(link)
                bid  = extract_bocm_id(norm)
                key  = bid if bid else norm
                if key not in seen:
                    seen.add(key); urls.append(norm); added2 += 1
            if added2 == 0: break
            if not soup2.select_one("li.pager-next a"): break
            page += 1; added = added2; time.sleep(1)

        time.sleep(0.8)

    return urls

# ════════════════════════════════════════════════════════════
# RSS FEED — supplemental
# ════════════════════════════════════════════════════════════
def get_rss_links(date_from, date_to):
    log("📡 RSS feed…")
    urls = []
    seen = set()
    r = safe_get(BOCM_RSS, timeout=20)
    if not r: log("  ⚠️  RSS unavailable"); return urls
    try:
        import xml.etree.ElementTree as ET
        root  = ET.fromstring(r.content)
        items = root.findall(".//item") or root.findall(".//entry")
        for item in items:
            pub = ""
            for tag in ["pubDate", "published", "updated", "date"]:
                el = item.find(tag)
                if el is not None and el.text: pub = el.text; break
            pub_date = None
            for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S +0000",
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
            bulletin_url = link_el.text if link_el is not None else ""
            if not bulletin_url: continue
            br = safe_get(bulletin_url, timeout=20)
            if not br: continue
            bsoup = BeautifulSoup(br.text, "html.parser")
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                if "CM_Orden_BOCM" in full and ".PDF" in full.upper():
                    norm = normalise_url(full)
                    bid  = extract_bocm_id(norm)
                    key  = bid if bid else norm
                    if key not in seen:
                        seen.add(key); urls.append(norm)
            time.sleep(0.5)
    except Exception as e:
        log(f"  ⚠️  RSS: {e}")
    log(f"  📡 RSS: {len(urls)} links")
    return urls

# ════════════════════════════════════════════════════════════
# FETCH — HTML JSON-LD first, PDF fallback
# HTML entry pages ALWAYS have structured JSON-LD text.
# PDF extraction fails on image-based/protected PDFs.
# ════════════════════════════════════════════════════════════
def extract_date_from_url(url):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return ""

def extract_jsonld(soup):
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if data.get("text"):
                text    = data["text"]
                date    = (data.get("datePublished", "") or "").replace("/", "-")
                name    = data.get("name", "")
                pdf_url = None
                for enc in data.get("encoding", []):
                    cu = enc.get("contentUrl", "")
                    if cu.upper().endswith(".PDF"):
                        pdf_url = cu; break
                return text, date[:10], name, pdf_url
        except Exception:
            continue
    return None, None, None, None

def extract_pdf_text_enhanced(url):
    """
    Enhanced PDF extraction:
    1. Normal text extraction (standard pages)
    2. Table extraction for PEM/ETAPA rows (financial tables)
    3. Reads all pages (not just first 15)
    """
    try:
        r = get_session().get(
            url, timeout=50, verify=False, allow_redirects=True,
            headers={**make_headers(referer=BOCM_BASE), "Accept": "application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 400: return ""
        if r.content[:4] != b"%PDF": return ""

        text_parts = []
        table_parts = []

        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg_num, pg in enumerate(pdf.pages):
                # Standard text
                t = pg.extract_text()
                if t:
                    text_parts.append(t)

                # Table extraction for financial data (PEM values in tables)
                tables = pg.extract_tables()
                for table in tables:
                    if not table: continue
                    for row in table:
                        if not row: continue
                        # Flatten row and look for PEM/ETAPA/IMPORTE patterns
                        row_text = " | ".join(str(c or "") for c in row)
                        if any(kw in row_text.upper() for kw in
                               ["ETAPA", "PEM", "IMPORTE", "PRESUPUESTO", "ICIO",
                                "BASE IMPONIBLE", "TOTAL"]):
                            table_parts.append(row_text)

        full_text = "\n".join(text_parts)
        if table_parts:
            full_text += "\n\nTABLA_DATOS:\n" + "\n".join(table_parts)

        return full_text[:20000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""

def fetch_announcement(url):
    """
    Returns (text, pdf_url, pub_date, doc_title).
    
    Always tries HTML JSON-LD first → most reliable.
    Falls back to enhanced PDF extraction.
    """
    url_low = url.lower()
    pdf_url = None

    # Convert to HTML entry page URL
    html_url = url
    if url_low.endswith(".pdf") or url_low.endswith(".json"):
        html_candidate = pdf_url_to_html_url(url)
        if html_candidate:
            html_url = html_candidate
            if url_low.endswith(".pdf"):
                pdf_url = url

    # Try HTML + JSON-LD
    r = safe_get(html_url, timeout=25, referer=f"{BOCM_BASE}/advanced-search")
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        jtext, jdate, jname, jpdf = extract_jsonld(soup)
        if jtext and len(jtext.strip()) > 100:
            text     = re.sub(r'\s+', ' ', jtext).strip()
            pub_date = jdate or extract_date_from_url(html_url)
            pdf_url  = pdf_url or jpdf
            return text, pdf_url, pub_date, jname or ""

    # PDF fallback (enhanced)
    if url_low.endswith(".pdf"):
        text     = extract_pdf_text_enhanced(url)
        pub_date = extract_date_from_url(url)
        if text and len(text.strip()) > 100:
            return text, url, pub_date, ""
        return "", None, pub_date, ""

    # HTML body fallback
    if not r or r.status_code != 200:
        r = safe_get(url, timeout=25)
    if not r or r.status_code != 200:
        return "", None, "", ""

    soup = BeautifulSoup(r.text, "html.parser")
    parts = []
    for sel in [".field--name-body", ".field-name-body", ".contenido-boletin",
                ".anuncio-texto", ".anuncio", "article .content", "article", "main", "#content"]:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True)); break
    if not parts:
        for tag in soup.find_all(["nav", "header", "footer", "aside", "script", "style"]):
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

    return re.sub(r'\s+', ' ', " ".join(parts)).strip(), pdf_url, pub_date, ""

# ════════════════════════════════════════════════════════════
# CLASSIFICATION — 5 stages + ICIO fast-path
# ════════════════════════════════════════════════════════════
HARD_REJECT = [
    # Financial admin (non-construction)
    "convocatoria de subvención", "bases reguladoras para la concesión de ayudas",
    "ayuda económica", "aportación dineraria",
    "modificación presupuestaria", "suplemento de crédito",
    "modificación del plan estratégico de subvenciones",
    # HR
    "nombramiento funcionari", "convocatoria de proceso selectivo",
    "convocatoria de oposiciones", "oferta de empleo público",
    "bases de la convocatoria para la cobertura",
    # Tax (non-construction)
    "ordenanza fiscal reguladora",
    "impuesto sobre actividades económicas",
    "inicio del período voluntario de pago",
    "matrícula del impuesto",
    # Events / sports
    "festejos taurinos", "certamen de",
    "convocatoria de premios", "actividades deportivas",
    "acción social en el ámbito del deporte",
    "actividades educativas", "proyectos educativos",
    # Governance
    "juez de paz", "composición del pleno",
    "composición de las comisiones", "encomienda de gestión",
    "reglamento orgánico municipal", "reglamento de participación",
    # Transport (non-construction)
    "eurotaxi", "autotaxi", "vehículos autotaxi",
    # Policy (no specific project)
    "normas subsidiarias de urbanismo",
    "criterio interpretativo vinculante",
    "corrección de errores del bocm", "corrección de hipervínculo",
    # Non-construction contracts
    "licitación de servicios de", "licitación de suministro de",
    "contrato de servicios de limpieza", "contrato de mantenimiento de",
    "servicio de limpieza", "servicio de recogida",
    # Dissolution of juntas (building COMPLETE, no more opportunity)
    # Note: "disolución de la junta de compensación" = project FINISHED
    "disolución de la junta de compensación",
    "disolver la junta de compensación",
]

APPLICATION_SIGNALS = [
    "se ha solicitado licencia",
    "ha solicitado licencia",
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
    # Direct grant
    "se concede", "se otorga", "se autoriza",
    "concesión de licencia", "licencia concedida",
    "se resuelve favorablemente", "otorgamiento de licencia",
    "se acuerda conceder", "se acuerda otorgar",
    "resolución estimatoria", "expedición de licencia",
    "se expide licencia", "licencia municipal de obras",
    # Urbanismo approvals
    "aprobar definitivamente", "aprobación definitiva",
    "aprobación inicial", "aprobación provisional",
    "se aprueba definitivamente", "se aprueba provisionalmente",
    "aprobación del proyecto", "acuerdo de aprobación",
    # Declaración responsable (Ley 1/2020)
    "declaración responsable de obra mayor",
    "declaración responsable urbanística",
    "toma de conocimiento de la declaración responsable",
    # Budget phrases (urbanización)
    "con un presupuesto", "promovido por la junta de compensación",
    # Public construction contracts
    "licitación de obras", "contrato de obras",
    "adjudicación del contrato de obras", "se convoca licitación",
    "obras de construcción", "obras de urbanización",
    "obras de rehabilitación", "convocatoria de licitación",
    # Reparcelación
    "acuerdo de reparcelación", "aprobación del proyecto de reparcelación",
    # Convenio urbanístico
    "suscripción del convenio", "aprobación del convenio urbanístico",
    # General
    "se aprueba", "se acuerda aprobar",
    # Modificación puntual (amendment = new development rights)
    "modificación puntual",
    # Estudio de detalle
    "aprobación del estudio de detalle",
]

CONSTRUCTION_SIGNALS = [
    "obra mayor", "obras mayores", "licencia de obras",
    "licencia urbanística", "licencia de edificación",
    "declaración responsable",
    "nueva construcción", "nueva planta", "obra nueva",
    "edificio de nueva", "viviendas de nueva", "edificio plurifamiliar",
    "complejo residencial", "viviendas unifamiliares",
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
    "residencia de mayores", "centro de salud", "edificio de oficinas",
    "superficie comercial", "centro comercial", "gran superficie",
    "local comercial", "uso terciario",
]

SMALL_ACTIVITY = [
    "peluquería", "barbería", "salón de belleza",
    "pastelería", "panadería", "carnicería", "pescadería",
    "frutería", "estanco", "locutorio", "quiosco",
    "taller mecánico", "academia de idiomas", "academia de danza",
    "centro de yoga", "pilates", "clínica dental", "consulta médica",
    "farmacia", "cafetería", "kebab", "lavandería",
    "tintorería", "zapatería", "cerrajería", "papelería",
    "floristería", "gestoría",
]

def classify_permit(text):
    t = text.lower()

    # ── ICIO FAST-PATH ────────────────────────────────────────────────────────
    # ICIO = confirmed construction + exact PEM. Always accept.
    is_icio = (
        ("impuesto sobre construcciones" in t and
         any(s in t for s in ["notif", "liquid", "cuota", "base imponible", "tribut"]))
        or "liquidación del icio" in t
        or ("base imponible" in t and any(s in t for s in ["construccion", "obra", "instalac"]))
    )
    if is_icio:
        return True, "ICIO: confirmed construction (PEM = base imponible)", 4

    # ── Stage 1: Hard reject ──────────────────────────────────────────────────
    for kw in HARD_REJECT:
        if kw in t:
            return False, f"Admin noise: '{kw}'", 0

    # ── Stage 2: Application (solicitud ≠ grant) ─────────────────────────────
    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 2:
        return False, f"Application phase (solicitud): {app_count} signals", 0

    # ── Stage 3: Denial ───────────────────────────────────────────────────────
    for kw in DENIAL_SIGNALS:
        if kw in t:
            return False, f"Denial: '{kw}'", 0

    # ── Stage 4: Must have grant + construction ───────────────────────────────
    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)

    if not has_grant:
        return False, "No grant language found", 0
    if not has_construction:
        return False, "Grant language but no construction content", 0

    # ── Stage 5: Filter small retail/services ────────────────────────────────
    has_major = any(p in t for p in [
        "obra mayor", "nueva construcción", "nueva planta",
        "nave industrial", "proyecto de urbanización",
        "rehabilitación integral", "plan especial", "plan parcial",
        "bloque de viviendas", "junta de compensación",
        "licitación de obras", "contrato de obras",
        "reparcelación", "estudio de detalle",
        "hotel", "residencia de mayores", "centro comercial",
        "edificio de oficinas", "superficie comercial",
    ])
    if not has_major:
        for kw in SMALL_ACTIVITY:
            if kw in t:
                return False, f"Small retail/service: '{kw}'", 0

    # ── Tier assignment ───────────────────────────────────────────────────────
    if any(p in t for p in [
            "proyecto de urbanización", "junta de compensación",
            "plan parcial", "aprobación definitiva del plan", "reparcelación"]):
        if any(p in t for p in [
                "aprobar definitivamente", "aprobación definitiva",
                "presupuesto", "acuerdo de reparcelación"]):
            return True, "Tier-1: Urbanismo definitivo (neighborhood-scale)", 1

    if any(p in t for p in [
            "plan especial", "reforma interior", "área de planeamiento",
            "estudio de detalle", "modificación puntual", "convenio urbanístico"]):
        return True, "Tier-2: Plan especial / urbanismo específico", 2

    if any(p in t for p in [
            "licitación de obras", "contrato de obras",
            "adjudicación de obras", "obras de construcción"]):
        return True, "Tier-2: Contrato público de obras", 2

    if any(p in t for p in [
            "nueva construcción", "nueva planta", "nave industrial",
            "bloque de viviendas", "demolición y construcción",
            "rehabilitación integral", "hotel", "residencia de mayores",
            "edificio de oficinas", "centro comercial"]):
        return True, "Tier-3: Obra mayor nueva construcción / industrial", 3

    if any(p in t for p in [
            "obra mayor", "reforma integral", "cambio de uso",
            "ampliación de edificio", "declaración responsable",
            "superficie comercial", "local comercial"]):
        return True, "Tier-4: Obra mayor / cambio de uso", 4

    return True, "Tier-5: Primera ocupación / actividad", 5


# ════════════════════════════════════════════════════════════
# LEAD SCORING — profile-aware
# Scores the lead based on permit type AND construction phase.
# Aprobación definitiva > inicial (confirmed vs tentative).
# Profile-specific bonuses for high-relevance combinations.
# ════════════════════════════════════════════════════════════
def score_lead(p):
    score = 0
    pt    = (p.get("permit_type") or "").lower()
    desc  = (p.get("description") or "").lower()
    muni  = (p.get("municipality") or "").lower()

    # ── Base type score (permit_type field directly) ──
    if pt in ("urbanización", "plan especial / parcial"):
        score += 40
    elif pt == "plan especial":
        score += 36
    elif pt in ("obra mayor industrial", "licitación de obras", "contrato de obras"):
        score += 33
    elif pt in ("obra mayor nueva construcción", "demolición y nueva planta"):
        score += 28
    elif pt in ("obra mayor rehabilitación", "cambio de uso",
                "declaración responsable obra mayor"):
        score += 20
    elif pt == "obra mayor":
        score += 18
    elif pt == "licencia primera ocupación":
        score += 15
    elif pt == "licencia de actividad":
        score += 10
    else:
        # Fallback
        if any(k in desc for k in ["proyecto de urbanización", "junta de compensación",
                                    "reparcelación"]):
            score += 40
        elif any(k in desc for k in ["nave industrial", "centro logístico"]):
            score += 33
        elif any(k in desc for k in ["nueva construcción", "nueva planta"]):
            score += 28
        elif "obra mayor" in desc:
            score += 18
        else:
            score += 5

    # ── Phase bonus — definitiva > inicial ──
    if any(p in desc for p in ["aprobación definitiva", "definitivamente", "concede",
                                "otorga", "se autoriza"]):
        score += 8   # confirmed = more valuable than tentative
    elif "aprobación inicial" in desc:
        score -= 5   # still a lead but lower certainty

    # ── Budget score ──
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)) and val > 0:
        if val >= 50_000_000:   score += 38  # €50M+ = national significance
        elif val >= 10_000_000: score += 35
        elif val >= 2_000_000:  score += 28
        elif val >= 500_000:    score += 20
        elif val >= 100_000:    score += 12
        elif val >= 50_000:     score += 6

    # ── Logistics corridor bonus ──
    logistics_munis = {"valdemoro", "getafe", "coslada", "alcalá de henares",
                       "torrejón de ardoz", "arganda del rey", "fuenlabrada",
                       "alcobendas", "san sebastián de los reyes", "rivas-vaciamadrid",
                       "mejorada del campo", "pinto", "parla"}
    if any(m in muni for m in logistics_munis) and pt in (
            "obra mayor industrial", "licitación de obras", "urbanización"):
        score += 5   # logistics zone bonus for industrial/construction

    # ── Data completeness ──
    if p.get("address"):    score += 8
    if p.get("applicant"):  score += 8
    if p.get("expediente"): score += 2
    if p.get("municipality") not in (None, "", "Madrid"):
        score += 2

    # ── AI confidence bonus ──
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
        try: return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
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
    noise = {"null", "madrid", "comunidad", "boletín", "oficial", "administración",
             "spain", "españa", "señor"}
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
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(".", "")
    try:
        v = float(s)
        # Sanity cap: No construction project in Spain has PEM > €3 billion.
        if v <= 0 or v > 3_000_000_000:
            return None
        return v
    except ValueError:
        return None

def extract_pem_value(text):
    """
    Extract PEM (Presupuesto de Ejecución Material).
    Enhanced with table data extraction for multi-stage projects.
    
    Priority:
    1. ICIO base imponible (= PEM by Spanish tax law — most reliable)
    2. TABLA_DATOS section (from pdfplumber table extraction)
    3. ETAPA rows (multi-stage urbanización)
    4. Explicit PEM label
    5. Presupuesto base de licitación (public contracts)
    6. Generic presupuesto (with context requirement)
    """
    c = text

    # Priority 1: ICIO base imponible (= PEM exactly, legally)
    for pat in [
        r'(?:base imponible(?:\s+del\s+ICIO)?|b\.i\.\s+del\s+icio)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'(?:cuota\s+tributaria|importe\s+icio)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 2: TABLA_DATOS from enhanced PDF table extraction
    if "TABLA_DATOS:" in c:
        tabla_section = c.split("TABLA_DATOS:", 1)[1]
        for row_line in tabla_section.split("\n"):
            if any(kw in row_line.upper() for kw in
                   ["PEM", "PRESUPUESTO", "IMPORTE", "BASE IMPONIBLE"]):
                # Find euro amounts in this row
                amounts = re.findall(
                    r'([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{1,2})?)',
                    row_line)
                for amt in amounts:
                    v = _parse_euro(amt)
                    if v and 1000 <= v <= 3_000_000_000:
                        return round(v, 2)

    # Priority 3: ETAPA rows (multi-stage urbanización)
    etapa_pems = re.findall(
        r'[Ee][Tt][Aa][Pp][Aa]\s*\d+[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*€',
        c)
    if etapa_pems:
        total = 0
        for vs in etapa_pems:
            v = _parse_euro(vs)
            if v and v >= 10000: total += v
        if total > 0: return round(total, 2)

    # Priority 4: Explicit PEM
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 5: IVA-inclusive total (urbanización)
    m = re.search(
        r'presupuesto,\s*\d+\s*%\s*IVA\s+incluido,\s*de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*euros',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    # Priority 6: Public contract budget
    for pat in [
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'valor\s+estimado[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    # Priority 7: Generic presupuesto (strict: requires "€" adjacent)
    m = re.search(
        r'(?:presupuesto|importe)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    return None

def detect_phase(text):
    """
    Detect the construction phase from the document.
    Returns one of: definitivo | inicial | en_tramite | primera_ocupacion | licitacion
    This helps profiles like Gran Constructora know how far the project is.
    """
    t = text.lower()
    if any(p in t for p in ["aprobación definitiva", "aprobar definitivamente",
                             "se concede", "se otorga", "licencia concedida"]):
        return "definitivo"
    elif "licitación" in t or "contrato de obras" in t:
        return "licitacion"
    elif "primera ocupación" in t:
        return "primera_ocupacion"
    elif "aprobación inicial" in t or "se somete a información pública" in t:
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

    # Address — comprehensive patterns
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
        r'[Áá]rea de [Dd]esarrollo\s+([A-Za-záéíóúñ0-9\s\-]+?)(?:,|\.|$)',
        r'[Pp]olígono\s+(?:[Ii]ndustrial\s+)?([A-ZÁÉÍÓÚÑ][^,\n\.\(\)]{2,40})',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;"); break

    if not res["address"]:
        for pat in [
            r'[Dd]istrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\-\s]+?)(?:,|\.|$)',
            r'parcela\s+(?:situada\s+en\s+)?([A-Za-záéíóúñ\s,º]+\d+)',
        ]:
            m = re.search(pat, c, re.I)
            if m:
                res["address"] = m.group(0).strip().rstrip(".,;"); break

    # Applicant — comprehensive patterns
    for pat in [
        r'(?:promovido por|promotora?|a cargo de)\s+(?:la\s+)?([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{5,80})',
        r'(?:a instancia de|solicitante|interesado[/a]*|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:[Jj]unta de [Cc]ompensación\s+[\"\']?)([A-ZÁÉÍÓÚÑ][^\"\']{3,60}[\"\']?)',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:\bS\.?[AL]\.?U?\.?\b|\bSLU\b|\bS\.?L\.?\b|\bS\.?A\.?\b))',
        r'(?:adjudicatario|adjudicado a|empresa adjudicataria)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:propietario|titular de la licencia)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
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
    if any(p in t for p in ["proyecto de urbanización", "obras de urbanización",
                             "junta de compensación", "reparcelación"]):
        res["permit_type"] = "urbanización"
    elif any(p in t for p in ["plan parcial", "plan especial de reforma interior", "peri"]):
        res["permit_type"] = "plan especial / parcial"
    elif "estudio de detalle" in t:
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["plan especial de cambio de uso",
                               "cambio de uso de local a vivienda"]):
        res["permit_type"] = "cambio de uso"
    elif any(p in t for p in ["plan especial para", "plan especial de"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["nave industrial", "almacén industrial", "plataforma logística",
                               "centro logístico", "parque empresarial", "actividades productivas",
                               "uso industrial", "edificio industrial"]):
        res["permit_type"] = "obra mayor industrial"
    elif any(p in t for p in ["licitación de obras", "contrato de obras",
                               "adjudicación de obras", "concurso de obras",
                               "obras de construcción", "ejecución de obras"]):
        res["permit_type"] = "licitación de obras"
    elif any(p in t for p in ["nueva construcción", "nueva planta", "obra nueva",
                               "edificio de nueva", "viviendas de nueva",
                               "edificio plurifamiliar"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral", "restauración de edificio",
                               "reforma integral", "reforma estructural"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma", "ampliación", "cambio de uso"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["demolición", "derribo"]):
        res["permit_type"] = "demolición y nueva planta"
    elif "primera ocupación" in t:
        res["permit_type"] = "licencia primera ocupación"
    elif "declaración responsable" in t:
        res["permit_type"] = "declaración responsable obra mayor"
    elif any(p in t for p in ["impuesto sobre construcciones", "liquidación del icio",
                               "base imponible"]):
        res["permit_type"] = "obra mayor"
    elif "modificación puntual" in t or "convenio urbanístico" in t:
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["actividad", "local comercial", "establecimiento"]):
        res["permit_type"] = "licencia de actividad"

    # Description — commercial and actionable
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
        for gp in ["se concede", "se otorga", "se acuerda conceder",
                   "se aprueba definitivamente", "licitación de obras",
                   "acuerdo de reparcelación", "base imponible"]:
            idx = t.find(gp)
            if idx >= 0:
                desc = c[idx:idx+300].strip(); break

    res["description"] = (desc or c[:250]).strip()[:350]
    res["lead_score"]  = score_lead(res)
    return res


def ai_extract(text, url, pub_date):
    if not USE_AI:
        return keyword_extract(text, url, pub_date)
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        sys_prompt = """You are an elite construction intelligence analyst for Spain.
You read BOCM (Madrid regional bulletin) documents to extract actionable leads for 6 client profiles:
🔧 MEP Installers (elevators/HVAC/fire) | 🏪 Retail Expansion | 📐 Promotores/RE
🏢 Gran Constructora | 🏭 Industrial/Logistics | 🛒 Materials Suppliers

CRITICAL RULES:
1. Return ONLY valid JSON — no markdown, no explanations.
2. If NOT a specific construction project → return: {"permit_type":"none","confidence":"low"}
3. Fields: applicant, address, municipality, permit_type, description, declared_value_eur, date_granted, confidence, lead_score, expediente, phase.
4. permit_type:
   "urbanización" | "plan especial" | "plan especial / parcial" |
   "obra mayor nueva construcción" | "obra mayor industrial" | "obra mayor rehabilitación" |
   "cambio de uso" | "declaración responsable obra mayor" | "licencia primera ocupación" |
   "licencia de actividad" | "licitación de obras" | "none"
5. declared_value_eur: PEM or ICIO base imponible or licitación base budget.
   Sum ETAPA values for multi-stage. Hard cap €3,000,000,000. NUMBER or null.
6. applicant: Company/person building. For urbanización = "Junta de Compensación [NAME]".
7. municipality: Specific Madrid town (e.g. "Paracuellos de Jarama"), NOT "Comunidad de Madrid".
8. description: ONE commercial sentence. What, where, budget, commercial opportunity.
   "Nave industrial 12.000m² polígono Valdemoro — logística, promotor Amazon España SL"
   "Plan Parcial AD-10 Paracuellos — 2.500 viviendas, €74M PEM, Junta Compensación"
   "Rehabilitación integral 45 viviendas, C/ Mayor 15 Getafe — PEM €2.1M"
9. lead_score: 0-100. Large budget + definitive approval = 70-85. No PEM + initial = 30-45.
10. phase: "definitivo" | "inicial" | "licitacion" | "primera_ocupacion" | "en_tramite"
11. confidence: "high" | "medium" | "low"

DOCUMENT TYPE RULES:
- "se ha SOLICITADO" + "plazo de veinte días" → APPLICATION → permit_type:"none"
- "aprobar DEFINITIVAMENTE" → FINAL APPROVAL → high confidence, phase:"definitivo"
- "licitación de obras" → public construction tender → permit_type:"licitación de obras", phase:"licitacion"
- "base imponible del ICIO" → CONFIRMED obra + exact PEM value
- "disolución de la junta de compensación" → PROJECT FINISHED → permit_type:"none"
- "Quinto.—Dejar sin efecto" → CORRECTION of error → keep as valid lead
- "declaración responsable de obra mayor" → valid since Ley 1/2020 = licencia equivalent"""

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": sys_prompt},
                      {"role": "user", "content": f"URL: {url}\n\nTexto BOCM:\n{text[:5500]}"}],
            temperature=0, max_tokens=750,
            response_format={"type": "json_object"})

        d = json.loads(resp.choices[0].message.content.strip())

        if str(d.get("permit_type", "")).lower() in ("none", "null", "", "otro", "n/a"):
            return None

        d["source_url"]      = url
        d["extraction_mode"] = "ai"
        dg = d.get("date_granted") or pub_date
        d["date_granted"] = parse_spanish_date(str(dg)) if dg else extract_date_from_url(url)

        val = d.get("declared_value_eur")
        if isinstance(val, str):
            try:
                v = val.replace(".", "").replace(",", ".").replace("€", "").strip()
                parsed = float(re.sub(r'[^\d.]', '', v)) if v else None
                d["declared_value_eur"] = (parsed if parsed and 0 < parsed <= 3_000_000_000
                                           else None)
            except:
                d["declared_value_eur"] = None
        elif isinstance(val, (int, float)):
            if val <= 0 or val > 3_000_000_000:
                d["declared_value_eur"] = None

        if not d.get("lead_score"):     d["lead_score"]    = score_lead(d)
        if not d.get("municipality"):   d["municipality"]  = extract_municipality(text)
        if not d.get("expediente"):     d["expediente"]    = extract_expediente(text)
        if not d.get("phase"):          d["phase"]         = detect_phase(text)
        return d

    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def extract(text, url, pub_date):
    return ai_extract(text, url, pub_date) if USE_AI else keyword_extract(text, url, pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — 17 columns (added Phase)
# Dedup on BOCM document ID (not URL — prevents triple entries)
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted", "Municipality", "Full Address", "Applicant",
    "Permit Type", "Declared Value PEM (€)", "Est. Build Value (€)",
    "Maps Link", "Description", "Source URL", "PDF URL",
    "Mode", "Confidence", "Date Found", "Lead Score", "Expediente", "Phase",
]
_ws             = None
_seen_urls      = set()
_seen_bocm_ids  = set()

def get_sheet():
    global _ws
    if _ws: return _ws
    sa = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa: log("❌ GCP_SERVICE_ACCOUNT_JSON not set"); return None
    try:
        info  = json.loads(sa)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try:    ws = sh.worksheet("Permits")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet("Permits", 2000, 20)
        if ws.row_values(1) != HDRS:
            ws.update(values=[HDRS], range_name="A1"); log("✅ Headers written")
        else:
            log("✅ Sheet connected")
        _ws = ws; return _ws
    except Exception as e:
        log(f"❌ Sheet: {e}"); return None

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
    url = p.get("source_url", "")

    # Dedup on BOCM document ID (prevents PDF+JSON+HTML triple entries)
    bocm_id = extract_bocm_id(url)
    if bocm_id and bocm_id in _seen_bocm_ids:
        log(f"  ⏭️  Dup BOCM-ID: {bocm_id}"); return False
    if url in _seen_urls:
        log(f"  ⏭️  Dup URL: {url[-50:]}"); return False

    dec  = p.get("declared_value_eur")
    est  = round(dec / 0.03) if dec and isinstance(dec, (int, float)) and dec > 0 else ""
    addr = p.get("address") or ""
    muni = p.get("municipality") or "Madrid"
    maps = ""
    if addr:
        maps = ("https://www.google.com/maps/search/"
                + (addr + " " + muni + " España").replace(" ", "+").replace(",", ""))

    row = [
        p.get("date_granted", ""), muni, addr,
        p.get("applicant") or "",
        p.get("permit_type") or "obra mayor",
        dec or "", est, maps,
        (p.get("description") or "")[:350],
        url, pdf_url or "",
        p.get("extraction_mode", "keyword"),
        p.get("confidence", ""),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        p.get("lead_score", 0),
        p.get("expediente", ""),
        p.get("phase", ""),
    ]
    try:
        if ws:
            ws.append_row(row, value_input_option="USER_ENTERED")
            _seen_urls.add(url)
            if bocm_id: _seen_bocm_ids.add(bocm_id)
            try:
                rn  = len(ws.get_all_values())
                sc  = p.get("lead_score", 0)
                if sc >= 65:   rb, gb, bb = 0.80, 0.93, 0.80
                elif sc >= 40: rb, gb, bb = 1.00, 0.96, 0.76
                elif sc >= 20: rb, gb, bb = 1.00, 1.00, 0.85
                else:          rb, gb, bb = 0.98, 0.93, 0.93
                ws.spreadsheet.batch_update({"requests": [{"repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": rn-1, "endRowIndex": rn},
                    "cell": {"userEnteredFormat": {"backgroundColor":
                                                   {"red": rb, "green": gb, "blue": bb}}},
                    "fields": "userEnteredFormat.backgroundColor"}}]})
            except: pass
        _dec_str = f"€{dec:,.0f}" if dec else "N/A"
        log(f"  💾 [{p.get('lead_score',0):02d}pts|{p.get('phase','?')}] "
            f"{muni} | {addr[:30]} | {p.get('permit_type','?')[:20]} | {_dec_str}")
        return True
    except Exception as e:
        log(f"  ❌ Write: {e}"); return False

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
                if datetime.strptime(row[13][:10], "%Y-%m-%d") >= cutoff:
                    recent.append(row)
            except: pass

        def get_val(r):
            try:
                s = str(r[5]).replace(".", "").replace(",", ".")
                return float(re.sub(r'[^\d.]', '', s)) if s else 0.0
            except: return 0.0

        def get_score(r):
            try: return int(r[14]) if len(r) > 14 and r[14] else 0
            except: return 0

        recent.sort(key=get_score, reverse=True)
        total      = sum(get_val(r) for r in recent)
        high_count = sum(1 for r in recent if get_score(r) >= 65)
        log(f"📧 Digest: {len(recent)} leads, €{int(total):,} PEM, {high_count} priority")

        rhtml = ""
        for r in recent:
            raw_v = str(r[5]).strip() if len(r) > 5 and r[5] else ""
            dec   = f"€{int(float(re.sub(r'[^\d.]', '', raw_v.replace('.','').replace(',','.')))):,}" if raw_v else "—"
            sc    = get_score(r)
            sc_c  = "#1b5e20" if sc >= 65 else "#e65100" if sc >= 40 else "#b71c1c"
            sc_bg = "#e8f5e9" if sc >= 65 else "#fff3e0" if sc >= 40 else "#fce4ec"
            expd  = r[15] if len(r) > 15 and r[15] else ""
            phase = r[16] if len(r) > 16 and r[16] else ""
            phase_badge = {"definitivo": "🟢 Definitivo", "inicial": "🟡 Inicial",
                          "licitacion": "🔵 Licitación", "primera_ocupacion": "⚪ 1ª Ocup."}.get(phase, "")
            maps_l = f"<a href='{r[7]}' style='color:#1565c0'>📍</a>&nbsp;" if (len(r)>7 and r[7]) else ""
            bocm_l = f"<a href='{r[9]}' style='color:#999;font-size:11px'>BOCM</a>" if (len(r)>9 and r[9]) else ""
            rhtml += f"""<tr style="border-bottom:1px solid #eee">
              <td style="padding:9px 7px;font-weight:600;font-size:13px">{r[1] or "—"}</td>
              <td style="padding:9px 7px;font-size:12px;color:#333">{r[2] or "—"}</td>
              <td style="padding:9px 7px;font-size:12px;color:#444">{r[3] or "—"}</td>
              <td style="padding:9px 7px"><span style="background:#e3f2fd;color:#0d47a1;padding:3px 7px;border-radius:10px;font-size:11px;white-space:nowrap">{r[4] or "—"}</span></td>
              <td style="padding:9px 7px;font-weight:700;color:#1565c0;font-size:14px">{dec}</td>
              <td style="padding:9px 7px;font-size:11px;color:#666">{phase_badge}</td>
              <td style="padding:9px 7px;font-size:12px;color:#555">{(r[8] or "")[:130]}</td>
              <td style="padding:9px 7px;text-align:center"><span style="background:{sc_bg};color:{sc_c};padding:3px 8px;border-radius:10px;font-size:12px;font-weight:700">{sc}</span></td>
              <td style="padding:9px 7px;white-space:nowrap;font-size:11px;color:#888">{expd}</td>
              <td style="padding:9px 7px;white-space:nowrap">{maps_l}{bocm_l}</td>
            </tr>"""

        ws_d = (datetime.now()-timedelta(days=7)).strftime("%d %b")
        we_d = datetime.now().strftime("%d %b %Y")
        est_t = f"€{int(total/0.03):,}" if total > 0 else "N/D"
        html = f"""<html><body style="font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;color:#1a1a1a">
<div style="background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ PlanningScout — Oportunidades Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">Semana {ws_d}–{we_d} · Ordenado por puntuación · {high_count} leads prioritarios (≥65 pts)</p>
</div>
<div style="display:flex;background:#e3f2fd;border-bottom:2px solid #bbdefb">
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">{len(recent)}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Proyectos detectados</div>
  </div>
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">€{int(total):,}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">PEM total</div>
  </div>
  <div style="flex:1;padding:14px 22px;border-right:1px solid #bbdefb">
    <div style="font-size:32px;font-weight:700;color:#1565c0">{est_t}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Valor obra estimado</div>
  </div>
  <div style="flex:1;padding:14px 22px">
    <div style="font-size:32px;font-weight:700;color:#1b5e20">{high_count}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">🟢 Leads prioritarios</div>
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
  <strong>PlanningScout</strong> — Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales.<br>
  PEM = Presupuesto de Ejecución Material · Est. Obra = PEM / 0.03 · Fase: 🟢Definitivo | 🟡Inicial | 🔵Licitación
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM", "")
        gp = os.environ.get("GMAIL_APP_PASSWORD", "")
        gt = os.environ.get(CLIENT_EMAIL_VAR, "")
        if not all([gf, gp, gt]): log("⚠️  Email vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (f"🏗️ PlanningScout Madrid — {len(recent)} proyectos | "
                          f"€{int(total):,} PEM | {high_count} prioritarios | {ws_d}–{we_d}")
        msg["From"] = gf; msg["To"] = gt
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(gf, gp)
            s.sendmail(gf, [t.strip() for t in gt.split(",")], msg.as_string())
        log(f"✅ Digest sent to {gt}")
    except Exception as e:
        log(f"❌ Digest error: {e}"); import traceback; traceback.print_exc()

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def run():
    if args.digest:
        log("📧 Digest-only mode"); get_sheet(); send_digest(); return

    today     = datetime.now()
    date_to   = today
    date_from = today - timedelta(weeks=WEEKS_BACK)

    log("=" * 68)
    log(f"🏗️  PlanningScout Madrid — Engine v8")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} "
        f"({WEEKS_BACK}w = {(date_to-date_from).days} days)")
    log(f"🔎  {len(SEARCH_KEYWORDS)} keywords across Sections III+II+V")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction'}")
    log(f"💰  {'Min €' + f'{MIN_VALUE_EUR:,.0f}' if MIN_VALUE_EUR else 'No PEM filter'}")
    log("=" * 68)

    get_sheet(); load_seen()

    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming: {len(all_urls)} URLs from queue")
    else:
        all_urls  = []
        seen_ids  = set()   # BOCM-ID dedup during collection

        def add_url(u):
            """Add normalised URL with BOCM-ID dedup."""
            norm = normalise_url(u)
            if not norm or is_bad_url(norm): return False
            if not url_date_ok(norm, date_from): return False
            if norm in _seen_urls: return False
            bid = extract_bocm_id(norm)
            if bid and bid in _seen_bocm_ids: return False
            key = bid if bid else norm
            if key in seen_ids: return False
            seen_ids.add(key)
            all_urls.append(norm)
            return True

        # ── SOURCE 1: Keyword search with DATE CHUNKING ──────────────────────
        # This is the biggest volume fix: weekly chunks × 250 max = 8× more results
        log(f"\n{'─'*50}")
        log(f"🔎 SOURCE 1: {len(SEARCH_KEYWORDS)} keywords × {WEEKS_BACK}-week chunks")
        log(f"   (date chunking: each 7-day window = up to 250 results per keyword)")
        log(f"{'─'*50}")

        for kw, section, profile_tag in SEARCH_KEYWORDS:
            urls  = search_keyword_chunked(kw, date_from, date_to, section=section)
            added = sum(1 for u in urls if add_url(u))
            if added > 0:
                log(f"  +{added:3d} [{profile_tag:12s}] '{kw}' [{section}]")
            time.sleep(0.8)

        log(f"\n  📊 After keyword search: {len(all_urls)} unique URLs")

        # ── SOURCE 2: Per-day full bulletin scan ─────────────────────────────
        # Scans EVERY working day, catches licencias missed by keyword search
        log(f"\n{'─'*50}")
        log(f"📅 SOURCE 2: Per-day Section III full scan")
        log(f"{'─'*50}")
        working_days = []
        d = date_from
        while d <= date_to:
            if d.weekday() < 5:
                working_days.append(d)
            d += timedelta(days=1)
        log(f"  Scanning {len(working_days)} working days…")

        day_total = 0
        for day in working_days:
            day_urls = scrape_day_section(day, section=SECTION_III)
            added    = sum(1 for u in day_urls if add_url(u))
            if added > 0:
                log(f"  📅 {day.strftime('%d/%m/%Y')}: +{added}")
                day_total += added
            time.sleep(0.5)
        log(f"  Per-day scan: +{day_total} | total {len(all_urls)}")

        # ── SOURCE 3: Section V per-day scan (ICIO + anuncios) ───────────────
        log(f"\n{'─'*50}")
        log(f"📢 SOURCE 3: Section V per-day scan (ICIO, anuncios, notificaciones)")
        log(f"{'─'*50}")
        sec5_total = 0
        # Only scan last 4 weeks for Section V (ICIO notifications are recent)
        sec5_from  = max(date_from, today - timedelta(weeks=4))
        d = sec5_from
        while d <= date_to:
            if d.weekday() < 5:
                day_urls = scrape_day_section(d, section=SECTION_V)
                added    = sum(1 for u in day_urls if add_url(u))
                if added > 0:
                    log(f"  📢 {d.strftime('%d/%m/%Y')} [Sec.V]: +{added}")
                    sec5_total += added
                time.sleep(0.5)
            d += timedelta(days=1)
        log(f"  Section V: +{sec5_total} | total {len(all_urls)}")

        # ── SOURCE 4: RSS ─────────────────────────────────────────────────────
        log(f"\n{'─'*50}")
        log(f"📡 SOURCE 4: RSS bulletin feed")
        log(f"{'─'*50}")
        rss_added = sum(1 for u in get_rss_links(date_from, date_to) if add_url(u))
        log(f"  RSS: +{rss_added} | total {len(all_urls)}")

        log(f"\n{'═'*50}")
        log(f"📋 TOTAL: {len(all_urls)} unique URLs to process")
        log(f"{'═'*50}")

        with open(QUEUE_FILE, "w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume to restart if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new.")
        if today.weekday() == 0: send_digest()
        return

    # ── PROCESSING ──────────────────────────────────────────────────────────
    saved = skipped = errors = 0
    log(f"\n{'─'*50}")
    log(f"⚙️  Processing {len(all_urls)} announcements…")
    log(f"{'─'*50}")

    for idx, url in enumerate(all_urls):
        log(f"\n[{idx+1}/{len(all_urls)}] {url[-70:]}")
        try:
            text, pdf_url, pub_date, doc_title = fetch_announcement(url)

            if not text or len(text.strip()) < 80:
                log("  ⚠️  Too little text — skip"); skipped += 1; continue

            is_lead, reason, tier = classify_permit(text)
            if not is_lead:
                log(f"  ⏭️  {reason}"); skipped += 1; continue

            log(f"  ✅ Tier-{tier} — {doc_title[:55] if doc_title else '...'}")
            p = extract(text, url, pub_date)

            if p is None:
                log("  ⏭️  Extraction rejected"); skipped += 1; continue

            log(f"  [{p.get('lead_score',0):02d}pts|{p.get('phase','?')[:4]}] "
                f"{p.get('municipality','?')} | "
                f"{p.get('permit_type','?')[:22]} | "
                f"€{p.get('declared_value_eur','?')}")

            dec = p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec, (int, float)) and dec < MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min"); skipped += 1; continue

            if write_permit(p, pdf_url or ""):
                saved += 1
            else:
                skipped += 1

        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors += 1

        time.sleep(1)

    log(f"\n{'='*68}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log(f"📊 Acceptance: {100*saved//max(1,saved+skipped+errors)}% | "
        f"Total processed: {saved+skipped+errors}")
    log("=" * 68)

    if os.path.exists(QUEUE_FILE):
        os.remove(QUEUE_FILE)

    if today.weekday() == 0:
        log("\n📧 Monday → weekly digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
