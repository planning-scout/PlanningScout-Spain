import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib
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
                    help="Weeks to look back. Daily=1, weekly=2, backfill=8.")
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
# HTTP SESSION
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
_session = None
_consecutive_bad = 0
MAX_BAD = 5

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    for name in ["cookies-agreed","cookie-agreed","has_js","bocm_cookies","cookie_accepted"]:
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

def safe_get(url, timeout=30, retries=3, backoff_base=8):
    global _consecutive_bad
    for attempt in range(retries):
        try:
            r = get_session().get(url, timeout=timeout, verify=False, allow_redirects=True)
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
# BOCM DOCUMENT ID — for deduplication across PDF/HTML/JSON variants
#
# The SAME BOCM document appears under 3 different URLs:
#   PDF:  bocm.es/boletin/CM_Orden_BOCM/2026/04/01/BOCM-20260401-96.PDF
#   JSON: bocm.es/boletin/CM_Orden_BOCM/2026/04/01/BOCM-20260401-96.json
#   HTML: bocm.es/bocm-20260401-96
#
# Deduplication on full URL → all 3 get saved → 3 duplicate rows per lead.
# Fix: extract "BOCM-YYYYMMDD-NN" from any URL → use as canonical dedup key.
# ════════════════════════════════════════════════════════════
def extract_bocm_id(url):
    """Extract the canonical BOCM document ID from any URL variant."""
    m = re.search(r'(BOCM-\d{8}-\d+)', url, re.I)
    return m.group(1).upper() if m else None

def pdf_url_to_html_url(url):
    """
    Convert PDF/JSON URL → HTML entry page URL.
    HTML entry pages ALWAYS have JSON-LD with full clean text.
    PDF extraction fails on image-based/protected PDFs.
    
    BOCM-20260401-96.PDF → bocm.es/bocm-20260401-96
    """
    m = re.search(r'(BOCM-\d{8}-\d+)\.(PDF|json)$', url, re.I)
    if m:
        doc_id = m.group(1).lower()
        return f"{BOCM_BASE}/{doc_id}"
    return None

# ════════════════════════════════════════════════════════════
# BOCM SEARCH — URL BUILDING
# Section 8387 = III. Administración Local Ayuntamientos
# Date format: DD-MM-YYYY (dashes, confirmed)
# Pagination: path-based
# ════════════════════════════════════════════════════════════
SECTION_LOCAL    = "8387"
SECTION_REGIONAL = "8386"   # Section II: Comunidad de Madrid (plans especiales CM)
BOCM_RSS         = "https://www.bocm.es/boletines.rss"

def build_search_url(keyword, date_from, date_to, section=SECTION_LOCAL):
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    params = (
        f"search_api_views_fulltext_1={quote(keyword)}"
        f"&field_bulletin_field_date%5Bdate%5D={df}"
        f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
        f"&field_orden_seccion={section}"
        f"&field_orden_apartado_1=All&field_orden_tipo_disposicin_1=All"
        f"&field_orden_organo_y_organismo_1_1=All&field_orden_organo_y_organismo_1=All"
        f"&field_orden_organo_y_organismo_2=All&field_orden_apartado_adm_local_3=All"
        f"&field_orden_organo_y_organismo_3=All&field_orden_apartado_y_organo_4=All"
        f"&field_orden_organo_5=All"
    )
    return f"{BOCM_BASE}/advanced-search?{params}"

def build_page_url(keyword, date_from, date_to, page, section=SECTION_LOCAL):
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    kw = quote(keyword)
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
# SEARCH KEYWORDS — short and precise for BOCM's Solr engine
#
# KEY INSIGHT: BOCM search treats each word as AND.
# "se concede licencia de obra mayor" requires ALL 6 words adjacent.
# → Misses: "se otorga", "licencia urbanística", "resolución favorable"
# → Use SHORT keywords: "obra mayor" catches ALL variations.
#
# PROFILE MAPPING (which profiles each keyword targets):
#   [MEP] = Instaladores MEP (ascensores, HVAC, PCI)
#   [RET] = Expansión Retail / Restauración
#   [PRO] = Promotores / Real Estate
#   [CON] = Gran Constructora / Infraestructuras
#   [IND] = Industrial / Logística
#   [MAT] = Compras / Materiales (all large projects)
#   [ALL] = All profiles benefit
# ════════════════════════════════════════════════════════════
SEARCH_KEYWORDS = [
    # ── [ALL] Core obra mayor — individual building licences ──────────────────
    # Short = catches all variants (se concede, se otorga, se autoriza, etc.)
    "licencia de obra mayor",           # [ALL] most common phrase
    "licencia urbanística",             # [ALL] post-2020 alternative term
    "licencia de obras",                # [ALL] catches all subvariants
    "declaración responsable",          # [ALL] post-Ley 1/2020 replacement
    "primera ocupación",                # [MEP][MAT] building finished, MEP final check
    "licencia de edificación",          # [ALL] construction licence

    # ── [PRO][CON][RET] Urbanismo — entire neighborhood scale ─────────────────
    "proyecto de urbanización",         # [ALL] entire neighborhoods, HIGHEST value
    "junta de compensación",            # [PRO][CON] always major development
    "reparcelación",                    # [PRO] land redistribution = buildable land
    "proyecto de reparcelación",        # [PRO] Paracuellos-type leads
    "área de desarrollo",               # [PRO][CON] development areas
    "plan parcial",                     # [PRO][CON] partial urban plan
    "plan especial",                    # [PRO][CON][RET] special urban plan
    "aprobación definitiva",            # [ALL] final approval — any planning doc
    "estudio de detalle",               # [PRO] detail study = pre-construction approval
    "unidad de ejecución",              # [PRO][CON] execution unit = new construction zone

    # ── [MEP][MAT] Building types — apartments, hotels, rehabs ───────────────
    "edificio plurifamiliar",           # [MEP] apartment blocks = elevators + HVAC
    "viviendas",                        # [MEP][RET][MAT] residential
    "rehabilitación integral",          # [MEP][MAT] full building rehab = MEP replacement
    "nueva construcción",               # [MEP][CON][MAT] new build
    "nueva planta",                     # [MEP][CON][MAT] new floor / new building
    "cambio de uso",                    # [RET][MEP] change of use = renovation + MEP
    "demolición",                       # [CON][IND] demolition + future construction

    # ── [IND][MAT][CON] Industrial & logistics ───────────────────────────────
    "nave industrial",                  # [IND][MAT] factory/warehouse = large MEP
    "almacén",                          # [IND][MAT] warehouse = logistics
    "parque empresarial",               # [IND][CON][MAT] business park
    "plataforma logística",             # [IND][MAT] logistics hub
    "centro de distribución",           # [IND][MAT] distribution centre
    "instalación industrial",           # [IND][MAT] industrial installation

    # ── [RET] Commercial & retail expansion ──────────────────────────────────
    "local comercial",                  # [RET] commercial space
    "centro comercial",                 # [RET] shopping centre = anchor tenant
    "gran superficie",                  # [RET] large retail surface
    "licencia de actividad",            # [RET][IND] activity licence for business
    "cambio de uso terciario",          # [RET] rezoning to commercial

    # ── [CON][MAT] Public construction contracts (NEW) ───────────────────────
    # Ayuntamientos publish tenders in BOCM — gold for Gran Constructora
    "licitación de obras",              # [CON][MAT] construction tender
    "contrato de obras",                # [CON][MAT] construction contract
    "adjudicación de obras",            # [CON][MAT] contract awarded
    "obras de urbanización",            # [CON][MAT] urbanisation works
    "obras de rehabilitación",          # [CON][MEP][MAT] rehab works
    "obras de construcción",            # [CON][MAT] construction works
    "obras de reforma",                 # [MEP][MAT] reform works

    # ── [ALL] ICIO tax notifications (confirmed construction with exact PEM) ──
    # ICIO = Impuesto sobre Construcciones = tax on approved obras
    # base imponible = PEM exactly. These are 100% confirmed obras.
    "liquidación icio",                 # [ALL] ICIO liquidation
    "base imponible",                   # [ALL] tax base = PEM value
    "impuesto construcciones",          # [ALL] ICIO short form

    # ── [MEP][CON][MAT] Hotel and large residential ───────────────────────────
    "hotel",                            # [MEP][CON][MAT] hotels = large MEP + construction
    "residencia",                       # [MEP][MAT] residential facility
    "bloque de viviendas",              # [MEP][CON][MAT] apartment block
]

def is_bad_url(url):
    if not url or "bocm.es" not in url: return True
    low = url.lower()
    bad_exts  = (".xml", ".css", ".js", ".png", ".jpg", ".gif",
                 ".ico", ".woff", ".svg", ".zip", ".epub")
    # .json is now allowed — some BOCM entries only have JSON URLs
    # .pdf is always allowed
    bad_paths = ("/advanced-search", "/login", "/user", "/admin",
                 "/sites/", "/modules/", "#", "javascript:", "/CM_Boletin_BOCM/")
    return any(low.endswith(x) for x in bad_exts) or any(x in low for x in bad_paths)

def url_date_ok(url, date_from):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        try:
            url_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return url_date >= date_from - timedelta(days=1)
        except ValueError: pass
    return True  # no date in URL = allow

def normalise_url(url):
    """
    Convert any BOCM URL variant to the canonical HTML entry page.
    HTML pages always have JSON-LD → most reliable text extraction.
    Falls back to original URL if no BOCM-ID found.
    """
    html = pdf_url_to_html_url(url)
    return html if html else url

def extract_result_links(soup):
    links = []
    for sel in ["a[href*='/boletin/']", "a[href*='/anuncio/']", "a[href*='/bocm-']",
                ".view-content .views-row a", ".view-content a",
                "article h3 a", "article h2 a",
                ".field--name-title a", "h3.field-content a"]:
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

def search_keyword(keyword, date_from, date_to, section=SECTION_LOCAL):
    log(f"  🔎 [{section}] '{keyword}'")
    seen = set(); urls = []; page = 0; max_pages = 25  # increased from 15

    while page < max_pages:
        url = (build_search_url(keyword, date_from, date_to, section) if page == 0
               else build_page_url(keyword, date_from, date_to, page, section))

        r = safe_get(url, timeout=25, backoff_base=6)
        if not r or r.status_code != 200:
            log(f"    No response on page {page} — stopping"); break

        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)
        new   = 0
        for link in links:
            if is_bad_url(link): continue
            if not url_date_ok(link, date_from): continue
            # Normalise to HTML entry page (avoids PDF/JSON/HTML duplicates)
            norm = normalise_url(link)
            if norm not in seen:
                seen.add(norm); urls.append(norm); new += 1

        if new == 0: break

        has_next = bool(
            soup.select_one("li.pager-next a") or
            soup.select_one(".pager__item--next a") or
            soup.find("a", string=re.compile(r"Siguiente|siguiente|Next|»", re.I))
        )
        if not has_next: break
        page += 1; time.sleep(1.5)

    return urls

# ════════════════════════════════════════════════════════════
# PER-DAY BULLETIN SCRAPING (the volume fix)
#
# Instead of relying on keyword search to find every document,
# we scrape the BOCM bulletin index for each working day.
# BOCM publishes 50-150 individual announcements per day.
# Section III (Administración Local) = 20-60 per day.
#
# Method: For each working day, search with a very common short word
# ("obra", "el", "licencia") with date range = single day.
# This returns ALL Section III announcements published that day.
# Then normalise each to HTML entry page for reliable JSON-LD extraction.
# ════════════════════════════════════════════════════════════
def scrape_day_all_announcements(date):
    """
    Get ALL Section III BOCM announcement URLs for a specific day.
    Uses the BOCM search with a single working day range and broad keywords.
    Returns list of normalised HTML entry page URLs.
    """
    urls = []
    seen = set()
    date_str = date.strftime("%d-%m-%Y")

    # Try multiple common words to maximise coverage
    # BOCM search returns different result sets for different keywords
    # "obra" → construction docs, "licencia" → licence docs, "acuerdo" → council decisions
    broad_keywords = ["obra", "licencia", "acuerdo", "proyecto", "aprobación"]

    for kw in broad_keywords:
        search_url = (
            f"{BOCM_BASE}/advanced-search"
            f"?search_api_views_fulltext_1={kw}"
            f"&field_bulletin_field_date%5Bdate%5D={date_str}"
            f"&field_bulletin_field_date_1%5Bdate%5D={date_str}"
            f"&field_orden_seccion={SECTION_LOCAL}"
            f"&field_orden_apartado_1=All&field_orden_tipo_disposicin_1=All"
            f"&field_orden_organo_y_organismo_1_1=All&field_orden_organo_y_organismo_1=All"
            f"&field_orden_organo_y_organismo_2=All&field_orden_apartado_adm_local_3=All"
            f"&field_orden_organo_y_organismo_3=All&field_orden_apartado_y_organo_4=All"
            f"&field_orden_organo_5=All"
        )

        page = 0
        while page < 8:  # 8 pages × 10 results = 80 max per keyword per day
            url = (search_url if page == 0 else
                   search_url.replace("/advanced-search?", f"/advanced-search/p/field_bulletin_field_date/date__{date_str}/field_bulletin_field_date_1/date__{date_str}/field_orden_organo_y_organismo_1_1/All/field_orden_organo_y_organismo_1/All/field_orden_organo_y_organismo_2/All/field_orden_organo_y_organismo_3/All/field_orden_apartado_y_organo_4/All/busqueda/{kw}/seccion/{SECTION_LOCAL}/apartado/All/disposicion/All/administracion_local/All/organo_5/All/search_api_aggregation_2/{kw}/page/{page}?"))

            r = safe_get(url, timeout=20, backoff_base=4)
            if not r or r.status_code != 200: break

            soup  = BeautifulSoup(r.text, "html.parser")
            links = extract_result_links(soup)
            new   = 0
            for link in links:
                if is_bad_url(link): continue
                date_compact = date.strftime("%Y%m%d")
                if date_compact not in link: continue
                norm = normalise_url(link)
                bocm_id = extract_bocm_id(norm)
                key = bocm_id if bocm_id else norm
                if key not in seen:
                    seen.add(key); urls.append(norm); new += 1

            if new == 0: break
            has_next = bool(soup.select_one("li.pager-next a") or
                           soup.select_one(".pager__item--next a"))
            if not has_next: break
            page += 1; time.sleep(1)

        time.sleep(1)

    return urls

# ════════════════════════════════════════════════════════════
# RSS FEED — supplemental source
# ════════════════════════════════════════════════════════════
def get_rss_pdf_links(date_from, date_to):
    log("📡 Fetching RSS feed…")
    urls = []
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
                    norm = normalise_url(full)  # convert PDF → HTML
                    if norm not in urls:
                        urls.append(norm)
            time.sleep(0.5)
    except Exception as e:
        log(f"  ⚠️  RSS: {e}")
    log(f"  📡 RSS: {len(urls)} links")
    return urls

# ════════════════════════════════════════════════════════════
# FETCH — HTML JSON-LD first, PDF fallback
# KEY CHANGE: Try HTML entry page ALWAYS (even for PDF URLs).
# HTML JSON-LD = clean structured text, never fails.
# PDF = fails on image-based, protected, or unusual encodings.
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

def extract_pdf_text(url):
    try:
        r = get_session().get(url, timeout=45, verify=False, allow_redirects=True,
                              headers={**HEADERS, "Accept": "application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 400: return ""
        if r.content[:4] != b"%PDF": return ""
        txt = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages[:15]:
                t = pg.extract_text()
                if t: txt += t + "\n"
        return txt[:15000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""

def fetch_announcement(url):
    """
    Returns (text, pdf_url, pub_date, doc_title).
    
    Priority:
    1. HTML entry page → JSON-LD (always clean, structured, never fails)
    2. PDF → pdfplumber (fallback for URLs without HTML equivalent)
    3. HTML body (last resort)
    
    ALL PDF/JSON URLs are first converted to HTML entry page URLs.
    """
    url_low  = url.lower()
    pdf_url  = None

    # ── Step 1: Always try HTML entry page first ────────────────────────────
    # Works for both HTML URLs and PDF URLs (via conversion)
    html_url = url
    if url_low.endswith(".pdf") or url_low.endswith(".json"):
        html_candidate = pdf_url_to_html_url(url)
        if html_candidate:
            html_url = html_candidate
            if url_low.endswith(".pdf"):
                pdf_url = url  # keep original PDF URL for reference

    r = safe_get(html_url, timeout=25)
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        jtext, jdate, jname, jpdf = extract_jsonld(soup)
        if jtext and len(jtext.strip()) > 100:
            text     = re.sub(r'\s+', ' ', jtext).strip()
            pub_date = jdate or extract_date_from_url(html_url)
            pdf_url  = pdf_url or jpdf
            return text, pdf_url, pub_date, jname or ""

    # ── Step 2: PDF fallback ─────────────────────────────────────────────────
    if url_low.endswith(".pdf"):
        text     = extract_pdf_text(url)
        pub_date = extract_date_from_url(url)
        if text and len(text.strip()) > 100:
            return text, url, pub_date, ""
        return "", None, pub_date, ""

    # ── Step 3: HTML body fallback ───────────────────────────────────────────
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
        ptext = extract_pdf_text(pdf_url)
        if ptext: parts.append(ptext)

    return re.sub(r'\s+', ' ', " ".join(parts)).strip(), pdf_url, pub_date, ""

# ════════════════════════════════════════════════════════════
# CLASSIFICATION  —  5-stage filter + ICIO fast-path
# ════════════════════════════════════════════════════════════

HARD_REJECT = [
    "subvención", "subvenciones para", "convocatoria de subvención",
    "bases reguladoras para la concesión de ayudas",
    "ayuda económica", "aportación dineraria",
    "modificación presupuestaria", "suplemento de crédito",
    "modificación del plan estratégico de subvenciones",
    "nombramiento funcionari", "personal laboral",
    "plantilla de personal", "oferta de empleo público",
    "convocatoria de proceso selectivo", "convocatoria de oposiciones",
    "bases de la convocatoria para la cobertura",
    "ordenanza fiscal reguladora",
    "impuesto sobre actividades económicas",
    "inicio del período voluntario de pago",
    "matrícula del impuesto",
    "festejos taurinos", "certamen de teatro", "certamen de",
    "convocatoria de premios", "actividades deportivas",
    "acción social en el ámbito del deporte",
    "actividades educativas", "proyectos educativos",
    "juez de paz", "comisión informativa permanente",
    "composición del pleno", "composición de las comisiones",
    "encomienda de gestión", "reglamento orgánico municipal",
    "reglamento de participación ciudadana",
    "eurotaxi", "autotaxi", "vehículos autotaxi",
    "normas subsidiarias de urbanismo",        # policy doc, no specific project
    "criterio interpretativo vinculante",
    "corrección de errores del bocm", "corrección de hipervínculo",
    "aprobación definitiva del plan estratégico de subvenciones",
    # Non-construction contracts (construction tenders are kept)
    "licitación de servicios de",
    "licitación de suministro de",
    "contrato de servicios de limpieza",
    "contrato de mantenimiento de",
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
    # Direct grant language
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
    # Declaración responsable (Ley 1/2020)
    "declaración responsable de obra mayor",
    "declaración responsable urbanística",
    "toma de conocimiento de la declaración responsable",
    # Budget-linked phrases (in urbanización docs)
    "con un presupuesto", "promovido por la junta de compensación",
    # Public construction contracts
    "licitación de obras", "contrato de obras",
    "adjudicación del contrato de obras", "se convoca licitación",
    "obras de construcción", "obras de urbanización",
    "obras de rehabilitación",
    # General approvals
    "se aprueba",
    # Reparcelación approvals
    "acuerdo de reparcelación", "aprobación del proyecto de reparcelación",
]

CONSTRUCTION_SIGNALS = [
    "obra mayor", "obras mayores", "licencia de obras",
    "licencia urbanística", "licencia de edificación",
    "declaración responsable",
    "nueva construcción", "nueva planta", "obra nueva",
    "edificio de nueva", "viviendas de nueva",
    "edificio plurifamiliar", "complejo residencial",
    "proyecto de urbanización", "obras de urbanización",
    "unidad de ejecución", "área de planeamiento específico",
    "junta de compensación", "reparcelación",
    "rehabilitación integral", "rehabilitación de edificio",
    "reforma integral", "reforma estructural",
    "demolición y construcción", "demolición y nueva planta",
    "ampliación de edificio",
    "nave industrial", "naves industriales",
    "almacén industrial", "almacén",
    "centro logístico", "plataforma logística",
    "parque empresarial", "instalación industrial",
    "hotel", "bloque de viviendas", "demolición", "derribo",
    "cambio de uso", "primera ocupación",
    "plan especial", "plan parcial", "estudio de detalle",
    "proyecto urbanístico",
    "presupuesto de ejecución material", "p.e.m",
    "base imponible del icio", "base imponible icio",
    "licitación de obras", "contrato de obras",
    "impuesto sobre construcciones",
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
    """
    5-stage classification + ICIO fast-path.
    Returns (is_lead: bool, reason: str, tier: int 1-5)
    """
    t = text.lower()

    # ── ICIO FAST-PATH ────────────────────────────────────────────────────────
    # ICIO notifications = confirmed approved construction with exact PEM.
    # They don't use grant language → fail Stage 4 without this fast-path.
    is_icio = (
        ("impuesto sobre construcciones" in t and
         any(s in t for s in ["notif", "liquid", "cuota", "base imponible"]))
        or "liquidación del icio" in t
        or ("base imponible" in t and "construccion" in t)
    )
    if is_icio:
        return True, "ICIO: confirmed approved construction (PEM = base imponible)", 4

    # ── Stage 1: Hard admin noise ─────────────────────────────────────────────
    for kw in HARD_REJECT:
        if kw in t:
            return False, f"Admin noise: '{kw}'", 0

    # ── Stage 2: Application phase (solicitud, NOT a grant) ───────────────────
    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 2:
        return False, f"Application phase (solicitud): {app_count} signals", 0

    # ── Stage 3: Denial ───────────────────────────────────────────────────────
    for kw in DENIAL_SIGNALS:
        if kw in t:
            return False, f"Denial: '{kw}'", 0

    # ── Stage 4: Grant + construction check ───────────────────────────────────
    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)

    if not has_grant:
        return False, "No grant language found", 0
    if not has_construction:
        return False, "Grant language but no construction content", 0

    # ── Stage 5: Filter small retail (only if no major construction signals) ──
    has_major = any(p in t for p in [
        "obra mayor", "nueva construcción", "nueva planta",
        "nave industrial", "proyecto de urbanización",
        "rehabilitación integral", "plan especial", "plan parcial",
        "bloque de viviendas", "junta de compensación",
        "licitación de obras", "contrato de obras",
        "reparcelación", "estudio de detalle",
    ])
    if not has_major:
        for kw in SMALL_ACTIVITY:
            if kw in t:
                return False, f"Small retail/service: '{kw}'", 0

    # ── Tier assignment ───────────────────────────────────────────────────────
    if any(p in t for p in ["proyecto de urbanización", "junta de compensación",
                             "plan parcial", "aprobación definitiva del plan",
                             "reparcelación"]):
        if any(p in t for p in ["aprobar definitivamente", "aprobación definitiva",
                                  "presupuesto", "acuerdo de reparcelación"]):
            return True, "Tier-1: Urbanismo definitivo (neighborhood-scale)", 1

    if any(p in t for p in ["plan especial", "reforma interior",
                             "área de planeamiento", "estudio de detalle"]):
        if any(p in t for p in ["definitiv", "presupuesto", "pem"]):
            return True, "Tier-2: Plan especial / PERI definitivo", 2

    if any(p in t for p in ["licitación de obras", "contrato de obras",
                             "adjudicación de obras", "obras de construcción"]):
        return True, "Tier-2: Contrato público de obras", 2

    if any(p in t for p in ["nueva construcción", "nueva planta",
                             "nave industrial", "bloque de viviendas",
                             "demolición y construcción", "rehabilitación integral"]):
        return True, "Tier-3: Obra mayor nueva construcción / industrial", 3

    if any(p in t for p in ["obra mayor", "reforma integral", "cambio de uso",
                             "ampliación de edificio", "declaración responsable"]):
        return True, "Tier-4: Obra mayor rehabilitación / cambio de uso", 4

    return True, "Tier-5: Licencia primera ocupación / actividad", 5


# ════════════════════════════════════════════════════════════
# LEAD SCORING — fixed to check permit_type directly
#
# BUG IN PREVIOUS VERSION: Checked for "proyecto de urbanización" in combined
# description+permit_type string. If permit_type = "urbanización" and description
# doesn't contain that exact phrase → 0 type points.
#
# FIX: Score permit_type field directly. Every classified lead gets type points.
# ════════════════════════════════════════════════════════════
def score_lead(p):
    score = 0
    pt   = (p.get("permit_type") or "").lower()
    desc = (p.get("description") or "").lower()

    # ── Type score (direct from permit_type field) ──
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
        # Fallback: check description
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
            score += 5   # any classified lead gets at least 5 pts

    # ── Budget score ──
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)) and val > 0:
        if val >= 10_000_000:  score += 35
        elif val >= 2_000_000: score += 28
        elif val >= 500_000:   score += 20
        elif val >= 100_000:   score += 12
        elif val >= 50_000:    score += 6

    # ── Data completeness ──
    if p.get("address"):    score += 8
    if p.get("applicant"):  score += 8
    if p.get("expediente"): score += 2
    if p.get("municipality") not in (None, "", "Madrid"):
        score += 2

    # ── AI bonus ──
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
        r'AYUNTAMIENTO\s+DE\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-]+?)(?:\n|\s{2,}|LICENCIAS|OTROS|CONTRATACIÓN|URBANISMO)',
        r'ayuntamiento de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:\.|,|\n)',
        r'(?:en|En)\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?),\s+a\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4}',
        r'Distrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:,|\.|$)',
    ]
    noise = {"null", "madrid", "comunidad", "boletín", "oficial", "administración", "spain", "españa"}
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            name = m.group(1).strip().rstrip(".,; ").strip()
            if name.lower() not in noise and 3 < len(name) < 60:
                return name.title()
    return "Madrid"

def extract_expediente(text):
    m = re.search(r'[Ee]xpediente[:\s]+(\d{2,6}/\d{4}/\d{3,8})', text)
    if m: return m.group(1)
    m = re.search(r'[Ee]xp\.\s*n[úu]?m\.?\s*([\d\-/]+)', text)
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
        # SANITY CAP: No Spanish construction project exceeds €3B PEM.
        # Values above = parsing error (land area m², cadastral refs, etc.)
        if v <= 0 or v > 3_000_000_000:
            return None
        return v
    except ValueError:
        return None

def extract_pem_value(text):
    """
    Extract PEM. Precedence:
    1. ICIO base imponible (most reliable — it IS the PEM by law)
    2. Explicit ETAPA rows summed (urbanización multi-etapa)
    3. PEM label
    4. Presupuesto base de licitación (public contracts)
    5. Generic presupuesto amount
    """
    c = text

    # Priority 1: ICIO base imponible = PEM by Spanish tax law
    for pat in [
        r'(?:base imponible(?:\s+del\s+ICIO)?|b\.i\.\s+del\s+icio)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'icio[^\n]{0,40}?([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 2: ETAPA rows (urbanización multi-stage)
    etapa_pems = re.findall(
        r'[Ee][Tt][Aa][Pp][Aa]\s*\d+[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*€',
        c)
    if etapa_pems:
        total = 0
        for vs in etapa_pems:
            v = _parse_euro(vs)
            if v and v >= 10000: total += v
        if total > 0: return round(total, 2)

    # Priority 3: Explicit PEM
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 4: IVA-inclusive total (urbanización)
    m = re.search(
        r'presupuesto,\s*\d+\s*%\s*IVA\s+incluido,\s*de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*euros',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    # Priority 5: Presupuesto base de licitación (public contracts)
    for pat in [
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'valor\s+estimado[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    # Priority 6: Generic presupuesto (strict context)
    m = re.search(
        r'(?:presupuesto|importe)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    return None

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
    }
    c = re.sub(r'\s+', ' ', text)

    # Address
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:paseo|po\.?|pso\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:camino|glorieta|ronda|travesía)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+n[úu]?[mº°]?\.?\s*(\d+)',
        r'Área de\s+[Pp]laneamiento\s+[A-Za-záéíóúñ\s]+[\"\']([^\"\']{3,80})[\"\']',
        r'[Uu]nidad de [Ee]jecución\s+(?:n[úu]?[mº°]\.?\s*)?(\w+)',
        r'[Uu]nidad de [Aa]ctuación\s+(?:n[úu]?[mº°]\.?\s*)?(\w+)',
        r'[Ss]ector\s+([A-ZÁÉÍÓÚÑ0-9][^,\n]{2,50})',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;"); break

    if not res["address"]:
        for pat in [
            r'[Dd]istrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\-\s]+?)(?:,|\.|$)',
            r'parcela\s+(?:situada\s+en\s+)?([A-Za-záéíóúñ\s,º]+\d+)',
            r'[Áá]rea de [Dd]esarrollo\s+([A-Za-záéíóúñ0-9\s\-]+?)(?:,|\.|$)',
        ]:
            m = re.search(pat, c, re.I)
            if m:
                res["address"] = m.group(0).strip().rstrip(".,;"); break

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
    if any(p in t for p in ["proyecto de urbanización", "obras de urbanización",
                             "junta de compensación", "reparcelación"]):
        res["permit_type"] = "urbanización"
    elif any(p in t for p in ["plan parcial", "plan especial de reforma interior", "peri"]):
        res["permit_type"] = "plan especial / parcial"
    elif any(p in t for p in ["estudio de detalle"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["plan especial de cambio de uso",
                               "cambio de uso de local a vivienda",
                               "cambio de uso de locales a vivienda"]):
        res["permit_type"] = "cambio de uso"
    elif any(p in t for p in ["plan especial para", "plan especial de"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["nave industrial", "almacén industrial", "plataforma logística",
                               "centro logístico", "naves industriales", "parque empresarial"]):
        res["permit_type"] = "obra mayor industrial"
    elif any(p in t for p in ["licitación de obras", "contrato de obras",
                               "adjudicación de obras", "concurso de obras",
                               "obras de construcción"]):
        res["permit_type"] = "licitación de obras"
    elif any(p in t for p in ["nueva construcción", "nueva planta", "obra nueva",
                               "edificio de nueva", "viviendas de nueva",
                               "edificio plurifamiliar"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral", "restauración de edificio",
                               "reforma integral", "reforma estructural"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma", "ampliación", "cambio de uso",
                               "modificación de edificio"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["demolición", "derribo"]):
        res["permit_type"] = "demolición y nueva planta"
    elif "primera ocupación" in t:
        res["permit_type"] = "licencia primera ocupación"
    elif any(p in t for p in ["declaración responsable"]):
        res["permit_type"] = "declaración responsable obra mayor"
    elif any(p in t for p in ["impuesto sobre construcciones", "liquidación del icio",
                               "base imponible"]):
        res["permit_type"] = "obra mayor"   # ICIO = confirmed approved obra
    elif any(p in t for p in ["actividad", "local comercial", "establecimiento"]):
        res["permit_type"] = "licencia de actividad"

    # Description
    desc = None
    m = re.search(r'(?:aprobar definitivamente|aprobación definitiva)\s+(?:el|del)\s+([^\.]{20,300})', c, re.I)
    if m: desc = "Aprobación definitiva: " + m.group(1).strip()[:250]
    if not desc:
        m = re.search(r'(?:licitación de obras|contrato de obras)\s+(?:de|para|del)?\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(r'licencia(?:\s+de\s+obra\s+mayor)?\s+para\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(
            r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de|'
            r'ampliación de|urbanización de|reparcelación de)\s+[^\.]{15,250}',
            c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        for gp in ["se concede", "se otorga", "se acuerda conceder",
                   "se aprueba definitivamente", "licitación de obras",
                   "acuerdo de reparcelación"]:
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
You read BOCM (Madrid regional bulletin) documents to extract actionable leads for construction companies.

CRITICAL RULES:
1. Return ONLY valid JSON — no markdown, no explanations.
2. If NOT a specific construction project → return: {"permit_type":"none","confidence":"low"}
3. Fields: applicant, address, municipality, permit_type, description, declared_value_eur, date_granted, confidence, lead_score, expediente.
4. permit_type values:
   "urbanización" | "plan especial" | "plan especial / parcial" |
   "obra mayor nueva construcción" | "obra mayor industrial" | "obra mayor rehabilitación" |
   "cambio de uso" | "declaración responsable obra mayor" | "licencia primera ocupación" |
   "licencia de actividad" | "licitación de obras" | "none"
5. declared_value_eur: PEM or ICIO base imponible (= PEM by law).
   For urbanización multi-stage: SUM all Etapa PEMs.
   For licitación: use presupuesto base de licitación.
   Hard cap: €3,000,000,000. Return NUMBER or null.
6. applicant: Who is building. For urbanización = "Junta de Compensación [NAME]".
   For licitación = Ayuntamiento (contracting) or company (adjudicatario).
7. municipality: Specific town (e.g. "Paracuellos de Jarama"), NOT "Comunidad de Madrid".
8. description: ONE commercial sentence — what, where, budget, why it matters commercially.
   Examples:
   "Urbanización 74ha AD-10 Paracuellos — 2.500 viviendas, €74M PEM, inicio obras 24m"
   "Nave industrial 8.500m² polígono Alcobendas, promotor Empresa SL"
   "Licitación obras reforma Ayuntamiento Getafe, presupuesto €1.2M"
9. lead_score: 0-100. urbanización/licitación grande = 60-80. Licencia sin PEM = 20-30.
10. confidence: "high" (grant confirmed + key data found), "medium", "low".

DOCUMENT TYPE DETECTION:
- "se ha SOLICITADO" + "plazo de veinte días" → APPLICATION not grant → permit_type:"none"
- "aprobar DEFINITIVAMENTE" → FINAL APPROVAL → high confidence
- "licitación de obras" → public construction tender → permit_type:"licitación de obras"
- "base imponible del ICIO" → confirmed obra mayor, declared_value_eur = base imponible value
- "Quinto.—Dejar sin efecto el Acuerdo" → CORRECTION of error → keep as valid lead
- "declaración responsable de obra mayor" → valid since Ley 1/2020 → same as licencia"""

        user_prompt = f"URL: {url}\n\nTexto BOCM:\n{text[:5500]}"

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": sys_prompt},
                      {"role": "user", "content": user_prompt}],
            temperature=0, max_tokens=700,
            response_format={"type": "json_object"})

        d = json.loads(resp.choices[0].message.content.strip())

        if str(d.get("permit_type", "")).lower() in ("none", "null", "", "otro", "n/a"):
            log("    AI: not a construction permit → skip")
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
                d["declared_value_eur"] = parsed if parsed and 0 < parsed <= 3_000_000_000 else None
            except:
                d["declared_value_eur"] = None
        elif isinstance(val, (int, float)):
            if val <= 0 or val > 3_000_000_000:
                d["declared_value_eur"] = None

        if not d.get("lead_score"):
            d["lead_score"] = score_lead(d)
        if not d.get("municipality"):
            d["municipality"] = extract_municipality(text)
        if not d.get("expediente"):
            d["expediente"] = extract_expediente(text)
        return d

    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def extract(text, url, pub_date):
    return ai_extract(text, url, pub_date) if USE_AI else keyword_extract(text, url, pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — dedup on BOCM document ID (not URL)
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted", "Municipality", "Full Address", "Applicant",
    "Permit Type", "Declared Value PEM (€)", "Est. Build Value (€)",
    "Maps Link", "Description", "Source URL", "PDF URL",
    "Mode", "Confidence", "Date Found", "Lead Score", "Expediente",
]
_ws = None
_seen_urls   = set()   # full source_url strings
_seen_bocm_ids = set() # BOCM-YYYYMMDD-NN canonical IDs for dedup

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
            ws = sh.add_worksheet("Permits", 1000, 20)
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
        log(f"✅ {len(_seen_urls)} existing URLs / {len(_seen_bocm_ids)} BOCM doc IDs loaded")
    except Exception as e:
        log(f"⚠️  load_seen: {e}")

def write_permit(p, pdf_url=""):
    ws  = get_sheet()
    url = p.get("source_url", "")

    # ── Dedup on BOCM document ID (prevents PDF + JSON + HTML triplicate) ──
    bocm_id = extract_bocm_id(url)
    if bocm_id and bocm_id in _seen_bocm_ids:
        log(f"  ⏭️  Dup BOCM-ID: {bocm_id}"); return False
    if url in _seen_urls:
        log(f"  ⏭️  Dup URL: {url[-60:]}"); return False

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
        log(f"  💾 [{p.get('lead_score',0):02d}pts] {muni} | "
            f"{addr[:35]} | {p.get('permit_type','?')[:20]} | {_dec_str}")
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
        total = sum(get_val(r) for r in recent)
        log(f"📧 Digest: {len(recent)} permits, €{int(total):,} total PEM")

        rhtml = ""
        for r in recent:
            raw_v = str(r[5]).strip() if len(r) > 5 and r[5] else ""
            if raw_v:
                _cv  = re.sub(r'[^\d.]', '', raw_v.replace('.','').replace(',','.'))
                dec  = f"€{int(float(_cv)):,}" if _cv else "—"
            else:
                dec = "—"
            sc    = get_score(r)
            sc_c  = "#1b5e20" if sc >= 65 else "#e65100" if sc >= 40 else "#b71c1c"
            sc_bg = "#e8f5e9" if sc >= 65 else "#fff3e0" if sc >= 40 else "#fce4ec"
            expd  = r[15] if len(r) > 15 and r[15] else ""
            maps_link = f"<a href='{r[7]}' style='color:#1565c0'>📍</a>&nbsp;" if (len(r)>7 and r[7]) else ""
            bocm_link = f"<a href='{r[9]}' style='color:#999;font-size:11px'>BOCM</a>" if (len(r)>9 and r[9]) else ""
            rhtml += f"""<tr style="border-bottom:1px solid #eee">
              <td style="padding:10px 7px;font-weight:600;font-size:13px">{r[1] or "—"}</td>
              <td style="padding:10px 7px;font-size:12px;color:#333">{r[2] or "—"}</td>
              <td style="padding:10px 7px;font-size:12px;color:#444">{r[3] or "—"}</td>
              <td style="padding:10px 7px"><span style="background:#e3f2fd;color:#0d47a1;padding:3px 7px;border-radius:10px;font-size:11px;white-space:nowrap">{r[4] or "—"}</span></td>
              <td style="padding:10px 7px;font-weight:700;color:#1565c0;font-size:14px">{dec}</td>
              <td style="padding:10px 7px;font-size:12px;color:#555">{(r[8] or "")[:140]}</td>
              <td style="padding:10px 7px;text-align:center"><span style="background:{sc_bg};color:{sc_c};padding:3px 8px;border-radius:10px;font-size:12px;font-weight:700">{sc}</span></td>
              <td style="padding:10px 7px;white-space:nowrap;font-size:11px;color:#888">{expd}</td>
              <td style="padding:10px 7px;white-space:nowrap">{maps_link}{bocm_link}</td>
            </tr>"""

        ws_d = (datetime.now() - timedelta(days=7)).strftime("%d %b")
        we_d = datetime.now().strftime("%d %b %Y")
        est_total = f"€{int(total/0.03):,}" if total > 0 else "N/D"
        html = f"""<html><body style="font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;color:#1a1a1a">
<div style="background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ PlanningScout — Oportunidades Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">Semana {ws_d} – {we_d} · Ordenado por puntuación de oportunidad</p>
</div>
<div style="display:flex;background:#e3f2fd;border-bottom:2px solid #bbdefb">
  <div style="flex:1;padding:16px 24px;border-right:1px solid #bbdefb">
    <div style="font-size:34px;font-weight:700;color:#1565c0">{len(recent)}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Proyectos detectados</div>
  </div>
  <div style="flex:1;padding:16px 24px;border-right:1px solid #bbdefb">
    <div style="font-size:34px;font-weight:700;color:#1565c0">€{int(total):,}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">PEM total</div>
  </div>
  <div style="flex:1;padding:16px 24px">
    <div style="font-size:34px;font-weight:700;color:#1565c0">{est_total}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Valor obra estimado</div>
  </div>
</div>
<div style="overflow-x:auto;padding:0 28px 24px">
<table style="width:100%;border-collapse:collapse;min-width:900px">
  <thead><tr style="background:#f5f5f5;text-align:left">
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Municipio</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Dirección/Área</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Promotor</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Tipo</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">PEM</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Descripción</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Score</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Expediente</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Links</th>
  </tr></thead>
  <tbody>{rhtml or '<tr><td colspan="9" style="padding:24px;text-align:center;color:#aaa">Sin proyectos esta semana</td></tr>'}</tbody>
</table></div>
<div style="padding:14px 28px;background:#f9f9f9;font-size:12px;color:#888;border-top:1px solid #e8e8e8">
  <strong>PlanningScout</strong> — Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales.<br>
  PEM = Presupuesto de Ejecución Material · Est. Obra = PEM / 0.03
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM", "")
        gp = os.environ.get("GMAIL_APP_PASSWORD", "")
        gt = os.environ.get(CLIENT_EMAIL_VAR, "")
        if not all([gf, gp, gt]): log("⚠️  Email vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = (f"🏗️ PlanningScout Madrid — {len(recent)} proyectos | "
                          f"€{int(total):,} PEM | {ws_d}–{we_d}")
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
    log(f"🏗️  PlanningScout Madrid — Engine v7")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction'}")
    log(f"💰  {'Min €' + f'{MIN_VALUE_EUR:,.0f}' if MIN_VALUE_EUR else 'No value filter'}")
    log("=" * 68)

    get_sheet(); load_seen()

    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming: {len(all_urls)} URLs from saved queue")
    else:
        all_urls = []
        seen_set = set()    # full URL dedup during collection
        seen_ids = set()    # BOCM-ID dedup during collection

        def add_url(u):
            """Add URL to collection with dedup on BOCM document ID."""
            norm = normalise_url(u)  # always convert to HTML entry page
            bid  = extract_bocm_id(norm)
            key  = bid if bid else norm
            if key in seen_ids: return False
            if is_bad_url(norm): return False
            if not url_date_ok(norm, date_from): return False
            if norm in _seen_urls: return False  # already in sheet
            if bid and bid in _seen_bocm_ids: return False  # already in sheet
            seen_ids.add(key)
            seen_set.add(norm)
            all_urls.append(norm)
            return True

        # ── SOURCE 1: Keyword search — Section III (Administración Local) ─────
        log(f"\n{'─'*50}")
        log(f"🔎 SOURCE 1: Keyword search — {len(SEARCH_KEYWORDS)} keywords")
        log(f"{'─'*50}")
        for kw in SEARCH_KEYWORDS:
            urls = search_keyword(kw, date_from, date_to, SECTION_LOCAL)
            added = sum(1 for u in urls if add_url(u))
            if added > 0:
                log(f"  +{added} | '{kw}' | total {len(all_urls)}")
            time.sleep(1.5)

        # ── SOURCE 2: Keyword search — Section II (CM regional) ──────────────
        # Plans especiales and major infrastructure sometimes in Section II
        log(f"\n{'─'*50}")
        log(f"🏛️  SOURCE 2: Regional section (Section II — CM plans especiales)")
        log(f"{'─'*50}")
        for kw in ["plan especial", "plan parcial", "proyecto de urbanización",
                   "junta de compensación", "reparcelación"]:
            urls = search_keyword(kw, date_from, date_to, SECTION_REGIONAL)
            added = sum(1 for u in urls if add_url(u))
            if added > 0:
                log(f"  +{added} [Sec.II] | '{kw}' | total {len(all_urls)}")
            time.sleep(1.5)

        # ── SOURCE 3: Per-day bulletin scan (the volume fix) ──────────────────
        # This scans ALL Section III announcements for each working day.
        # Catches what keyword search misses — individual licencias not in search index.
        log(f"\n{'─'*50}")
        log(f"📅 SOURCE 3: Per-day full scan of Section III announcements")
        log(f"{'─'*50}")
        working_days = []
        d = date_from
        while d <= date_to:
            if d.weekday() < 5:
                working_days.append(d)
            d += timedelta(days=1)
        log(f"  Scanning {len(working_days)} working days…")

        for day in working_days:
            day_urls = scrape_day_all_announcements(day)
            added    = sum(1 for u in day_urls if add_url(u))
            if added > 0:
                log(f"  📅 {day.strftime('%d/%m/%Y')}: +{added} | total {len(all_urls)}")
            time.sleep(0.5)

        # ── SOURCE 4: RSS (catches very recent bulletins not yet indexed) ─────
        log(f"\n{'─'*50}")
        log(f"📡 SOURCE 4: RSS bulletin feed")
        log(f"{'─'*50}")
        rss_urls = get_rss_pdf_links(date_from, date_to)
        added    = sum(1 for u in rss_urls if add_url(u))
        log(f"  RSS: +{added} | total {len(all_urls)}")

        log(f"\n📋 {len(all_urls)} unique URLs to process")
        with open(QUEUE_FILE, "w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume to restart if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new to process.")
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

            log(f"  ✅ Tier-{tier} — {doc_title[:60] if doc_title else '(extracting...)'}")
            p = extract(text, url, pub_date)

            if p is None:
                log("  ⏭️  AI rejected"); skipped += 1; continue

            log(f"  [{p.get('lead_score',0):02d}pts] "
                f"{p.get('municipality','?')} | "
                f"{p.get('permit_type','?')[:22]} | "
                f"€{p.get('declared_value_eur','?')}")

            dec = p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec, (int, float)) and dec < MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min €{MIN_VALUE_EUR:,.0f}")
                skipped += 1; continue

            if write_permit(p, pdf_url or ""):
                saved += 1
            else:
                skipped += 1

        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors += 1

        time.sleep(1)

    log(f"\n{'='*68}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log(f"📊 Acceptance: {saved}/{saved+skipped+errors} = "
        f"{100*saved/max(1,saved+skipped+errors):.0f}%")
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
