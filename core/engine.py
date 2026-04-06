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
parser.add_argument("--weeks",   type=int, default=2)
parser.add_argument("--digest",  action="store_true")
parser.add_argument("--resume",  action="store_true",
                    help="Skip collection phase, process saved queue from previous run")
args = parser.parse_args()

with open(args.client, "r", encoding="utf-8") as f:
    CFG = json.load(f)

SHEET_ID         = CFG["sheet_id"]
CLIENT_EMAIL_VAR = CFG["email_to_secret_name"]
MIN_VALUE_EUR    = CFG.get("min_declared_value_eur", 0)
WEEKS_BACK       = args.weeks
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "").strip()
USE_AI           = bool(OPENAI_API_KEY)

QUEUE_FILE = "/tmp/bocm_queue.json"   # URL queue persisted between runs

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ════════════════════════════════════════════════════════════
# HTTP SESSION
# One persistent session for the whole run.
# We rotate it if we hit repeated 502s.
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

_session = None
_consecutive_502s = 0
MAX_CONSECUTIVE_502S = 5

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    # Pre-set consent cookies so BOCM doesn't redirect to cookie banner
    for name in ["cookies-agreed", "cookie-agreed", "has_js",
                 "bocm_cookies", "cookie_accepted"]:
        s.cookies.set(name, "1", domain="www.bocm.es")
    return s

def get_session():
    global _session
    if _session is None:
        _session = make_session()
    return _session

def rotate_session():
    """Create a fresh session after repeated failures."""
    global _session, _consecutive_502s
    log("  🔄 Rotating session (too many failures)…")
    _session = make_session()
    _consecutive_502s = 0
    time.sleep(15)

def safe_get(url, timeout=30, retries=3, backoff_base=10):
    global _consecutive_502s
    for attempt in range(retries):
        try:
            r = get_session().get(url, timeout=timeout,
                                  verify=False, allow_redirects=True)

            if r.status_code == 200:
                _consecutive_502s = 0
                return r

            if r.status_code in (502, 503, 429):
                _consecutive_502s += 1
                wait = backoff_base * (3 ** attempt)   # 10s, 30s, 90s
                log(f"  ⚠️  HTTP {r.status_code} — waiting {wait}s "
                    f"(attempt {attempt+1}/{retries})")
                time.sleep(wait)
                if _consecutive_502s >= MAX_CONSECUTIVE_502S:
                    rotate_session()
                continue

            log(f"  HTTP {r.status_code}: {url[:80]}")
            return r

        except requests.exceptions.Timeout:
            wait = backoff_base * (2 ** attempt)
            log(f"  ⏱️  Timeout {attempt+1}/{retries} — waiting {wait}s")
            time.sleep(wait)
        except Exception as e:
            log(f"  ❌ {type(e).__name__}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff_base)
    return None

# ════════════════════════════════════════════════════════════
# BOCM SEARCH — EXACT URLs from Inga's browser research
#
# What we learned from Step 1 (cURL) and Step 4 (pagination URLs):
#
# INITIAL SEARCH (GET):
#   https://www.bocm.es/advanced-search?
#     search_api_views_fulltext_1=KEYWORD
#     &field_bulletin_field_date[date]=DD-MM-YYYY    ← dashes, not slashes
#     &field_bulletin_field_date_1[date]=DD-MM-YYYY
#     &field_orden_seccion=8387                       ← III. Administración Local
#     &field_orden_apartado_1=All
#     &field_orden_tipo_disposicin_1=All
#     &field_orden_organo_y_organismo_1_1=All
#     &field_orden_organo_y_organismo_1=All
#     &field_orden_organo_y_organismo_2=All
#     &field_orden_apartado_adm_local_3=All
#     &field_orden_organo_y_organismo_3=All
#     &field_orden_apartado_y_organo_4=All
#     &field_orden_organo_5=All
#
# PAGINATION (path-based, NOT query params):
#   /advanced-search/p/
#     field_bulletin_field_date/date__DD-MM-YYYY/
#     field_bulletin_field_date_1/date__DD-MM-YYYY/
#     .../busqueda/KEYWORD/seccion/8387/.../page/N
#
# Section ID:
#   8387 = "III. ADMINISTRACIÓN LOCAL AYUNTAMIENTOS"
# ════════════════════════════════════════════════════════════

SECTION_LOCAL = "8387"   # III. Administración Local Ayuntamientos

# Keywords confirmed to find GRANTED permits in Administración Local.
# Chosen to be grant-specific — not generic planning phrases.
# "se concede", "se otorga" appear only in resolution notices.
SEARCH_KEYWORDS = [
    "se concede licencia de obras",
    "se otorga licencia de obras",
    "licencia de obras mayor concedida",
    "concesión de licencia de obras mayor",
    "se concede licencia urbanística",
    "se otorga licencia urbanística",
    "licencia de obras mayor",
    "resolución favorable licencia obras",
]

def build_search_url(keyword, date_from, date_to):
    """Build the exact GET URL for the BOCM advanced search."""
    # Date format: DD-MM-YYYY (dashes, as confirmed from cURL)
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")

    # All the extra filter fields must be present (discovered from cURL)
    params = (
        f"search_api_views_fulltext_1={quote(keyword)}"
        f"&field_bulletin_field_date%5Bdate%5D={df}"
        f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
        f"&field_orden_seccion={SECTION_LOCAL}"
        f"&field_orden_apartado_1=All"
        f"&field_orden_tipo_disposicin_1=All"
        f"&field_orden_organo_y_organismo_1_1=All"
        f"&field_orden_organo_y_organismo_1=All"
        f"&field_orden_organo_y_organismo_2=All"
        f"&field_orden_apartado_adm_local_3=All"
        f"&field_orden_organo_y_organismo_3=All"
        f"&field_orden_apartado_y_organo_4=All"
        f"&field_orden_organo_5=All"
    )
    return f"{BOCM_BASE}/advanced-search?{params}"

def build_page_url(keyword, date_from, date_to, page):
    """
    Build pagination URL.  Format confirmed from Step 4:
    /advanced-search/p/
      field_bulletin_field_date/date__DD-MM-YYYY/
      field_bulletin_field_date_1/date__DD-MM-YYYY/
      field_orden_organo_y_organismo_1_1/All/
      field_orden_organo_y_organismo_1/All/
      field_orden_organo_y_organismo_2/All/
      field_orden_organo_y_organismo_3/All/
      field_orden_apartado_y_organo_4/All/
      busqueda/KEYWORD/
      seccion/8387/
      apartado/All/disposicion/All/administracion_local/All/organo_5/All/
      search_api_aggregation_2/KEYWORD/
      page/N
    """
    df  = date_from.strftime("%d-%m-%Y")
    dt  = date_to.strftime("%d-%m-%Y")
    kw  = quote(keyword)
    return (
        f"{BOCM_BASE}/advanced-search/p"
        f"/field_bulletin_field_date/date__{df}"
        f"/field_bulletin_field_date_1/date__{dt}"
        f"/field_orden_organo_y_organismo_1_1/All"
        f"/field_orden_organo_y_organismo_1/All"
        f"/field_orden_organo_y_organismo_2/All"
        f"/field_orden_organo_y_organismo_3/All"
        f"/field_orden_apartado_y_organo_4/All"
        f"/busqueda/{kw}"
        f"/seccion/{SECTION_LOCAL}"
        f"/apartado/All/disposicion/All/administracion_local/All/organo_5/All"
        f"/search_api_aggregation_2/{kw}"
        f"/page/{page}"
    )

def is_bad_url(url):
    """True for URLs we should never process."""
    if not url or "bocm.es" not in url:
        return True
    low = url.lower()
    bad_exts  = (".xml", ".css", ".js", ".png", ".jpg", ".gif", ".ico",
                 ".woff", ".svg", ".zip")
    bad_paths = ("/advanced-search", "/login", "/user", "/admin",
                 "/sites/", "/modules/", "#", "javascript:")
    if any(low.endswith(x) for x in bad_exts):
        return True
    if any(x in low for x in bad_paths):
        return True
    return False

def url_date_ok(url, date_from):
    """
    Fast pre-filter: if the URL embeds a date (e.g. BOCM-20110124-62.PDF)
    and that date is before our date_from, reject without fetching.
    """
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        try:
            url_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return url_date >= date_from - timedelta(days=1)
        except ValueError:
            pass
    return True  # No date in URL — let it through

def extract_result_links(soup):
    """Pull all result links from a BOCM search results page."""
    links = []
    for sel in [
        "a[href*='/boletin/']",
        "a[href*='/anuncio/']",
        ".view-content .views-row a",
        ".view-content a",
        "article h3 a",
        "article h2 a",
        ".field--name-title a",
        "h3.field-content a",
    ]:
        found = soup.select(sel)
        if found:
            for a in found:
                href = a.get("href", "")
                if href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    links.append(full)
            if links:
                break

    # Fallback: any internal link that looks like a bulletin entry
    if not links:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            if ("bocm.es" in full and
                    any(seg in full for seg in ["/boletin/", "/anuncio/", "/bocm/"])):
                links.append(full)

    return links

def search_keyword(keyword, date_from, date_to):
    """
    Search the BOCM for one keyword over the date range.
    Returns a deduplicated list of result page URLs.
    """
    log(f"  🔎 '{keyword}'")
    seen  = set()
    urls  = []
    page  = 0
    max_pages = 20   # safety cap per keyword

    while page < max_pages:
        if page == 0:
            url = build_search_url(keyword, date_from, date_to)
        else:
            url = build_page_url(keyword, date_from, date_to, page)

        r = safe_get(url, timeout=25, backoff_base=8)
        if not r:
            log(f"    No response on page {page} — stopping")
            break
        if r.status_code != 200:
            log(f"    HTTP {r.status_code} on page {page} — stopping")
            break

        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)
        new   = 0

        for link in links:
            if is_bad_url(link):
                continue
            if not url_date_ok(link, date_from):
                log(f"    ⏭️  Old URL skipped: {link[-30:]}")
                continue
            if link not in seen:
                seen.add(link)
                urls.append(link)
                new += 1

        log(f"    Page {page}: {new} new links (total {len(urls)})")

        if new == 0:
            break   # Empty page — end of results for this keyword

        # Check for next-page button
        has_next = bool(
            soup.select_one("li.pager-next a") or
            soup.select_one(".pager__item--next a") or
            soup.find("a", string=re.compile(r"Siguiente|siguiente|Next|»", re.I))
        )
        if not has_next:
            log(f"    No next-page button — done")
            break

        page += 1
        time.sleep(2)   # polite delay between pages

    return urls

# ════════════════════════════════════════════════════════════
# RSS FEED (daily bulletins — faster than keyword search)
# We also scrape recent bulletin editions directly.
# Each edition contains the full day's announcements.
# Section III = Administración Local
# ════════════════════════════════════════════════════════════
BOCM_RSS       = "https://www.bocm.es/boletines.rss"
BOCM_LAST_XML  = "https://www.bocm.es/ultimo-boletin.xml"

def get_rss_pdf_links(date_from, date_to):
    """
    Fetch the BOCM RSS feed and collect PDF links published
    within the date range. Each bulletin has one or more PDF
    sections — we collect the Section III (Ayuntamientos) PDFs.
    Returns list of PDF URLs.
    """
    log("📡 Fetching RSS feed for recent bulletins…")
    pdf_urls = []

    r = safe_get(BOCM_RSS, timeout=20)
    if not r:
        log("  ⚠️  RSS not available — skipping RSS collection")
        return pdf_urls

    # Parse RSS (it's XML)
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//entry")

        for item in items:
            # Get publication date
            pub = ""
            for tag in ["pubDate", "published", "updated", "date"]:
                el = item.find(tag)
                if el is not None and el.text:
                    pub = el.text; break

            # Parse date — RSS dates vary in format
            pub_date = None
            for fmt in ["%a, %d %b %Y %H:%M:%S %z",
                        "%a, %d %b %Y %H:%M:%S +0000",
                        "%Y-%m-%dT%H:%M:%S%z"]:
                try:
                    pub_date = datetime.strptime(pub[:30], fmt).replace(tzinfo=None)
                    break
                except ValueError:
                    pass
            if not pub_date:
                try:
                    # dateutil fallback
                    from dateutil import parser as dp
                    pub_date = dp.parse(pub).replace(tzinfo=None)
                except Exception:
                    pass

            if pub_date and (pub_date < date_from or pub_date > date_to):
                continue

            # Get the link to the bulletin HTML page
            link_el = item.find("link")
            link    = link_el.text if link_el is not None else ""
            if not link:
                continue

            # Fetch that bulletin page and collect its PDF links
            br = safe_get(link, timeout=20)
            if not br:
                continue

            bsoup = BeautifulSoup(br.text, "html.parser")
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                if ".PDF" in href.upper() or ".pdf" in href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    # Only Section III (local admin) PDFs
                    # Section III PDFs come from Ayuntamientos — they're mixed
                    # with other sections so we filter in the text extraction phase
                    if "bocm.es" in full and full not in pdf_urls:
                        pdf_urls.append(full)

            time.sleep(1)

    except Exception as e:
        log(f"  ⚠️  RSS parse error: {e}")

    log(f"  📡 RSS collected {len(pdf_urls)} PDF links")
    return pdf_urls

# ════════════════════════════════════════════════════════════
# FETCH ANNOUNCEMENT — HTML page or direct PDF
# ════════════════════════════════════════════════════════════
def fetch_announcement(url):
    """
    Fetch a BOCM result URL (could be HTML page or direct PDF).
    Returns (full_text, pdf_url_or_None, publication_date_str).
    """
    url_low = url.lower()

    # Direct PDF URL
    if url_low.endswith(".pdf"):
        text = extract_pdf_text(url)
        pub_date = extract_date_from_url(url)
        return text, url, pub_date

    # HTML announcement page
    r = safe_get(url, timeout=25)
    if not r or r.status_code != 200:
        return "", None, ""

    soup  = BeautifulSoup(r.text, "html.parser")
    parts = []

    for sel in [
        ".field--name-body", ".field-name-body",
        ".contenido-boletin", ".anuncio-texto", ".anuncio",
        "article .content", "article", "main", "#content",
    ]:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True))
            break

    if not parts:
        for tag in soup.find_all(["nav","header","footer","aside","script","style"]):
            tag.decompose()
        parts.append(soup.get_text(separator=" ", strip=True)[:8000])

    # Extract publication date from page text
    pub_date = ""
    page_text = " ".join(parts)
    m = re.search(r'\b(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})\b', page_text)
    if m:
        pub_date = m.group(0)

    # Find the PDF link on the page
    pdf_url = None
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if ".pdf" in h.lower() or "descargar-pdf" in h.lower() or ".PDF" in h:
            pdf_url = urljoin(BOCM_BASE, h) if h.startswith("/") else h
            break

    # Extract PDF text and append
    if pdf_url:
        pdf_text = extract_pdf_text(pdf_url)
        if pdf_text:
            parts.append(pdf_text)

    full_text = re.sub(r'\s+', ' ', " ".join(parts)).strip()
    return full_text, pdf_url, pub_date

def extract_pdf_text(url):
    """Download PDF and extract all text."""
    try:
        r = get_session().get(
            url, timeout=45, verify=False, allow_redirects=True,
            headers={**HEADERS, "Accept": "application/pdf,*/*"}
        )
        if r.status_code != 200 or len(r.content) < 400:
            return ""
        if r.content[:4] != b"%PDF":
            return ""
        txt = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages[:10]:
                t = pg.extract_text()
                if t:
                    txt += t + "\n"
        return txt[:12000]
    except Exception as e:
        log(f"    PDF error: {e}")
        return ""

def extract_date_from_url(url):
    """Extract publication date from BOCM URL like BOCM-20260327-44.PDF"""
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""

# ════════════════════════════════════════════════════════════
# GRANT / DENIAL CLASSIFICATION
#
# KEY INSIGHT from Inga's research:
#   "se ha SOLICITADO" = application submitted, NOT yet granted → NOT a lead
#   "se CONCEDE"       = permit granted → IS a lead
#
# The example PDF from Step 3 was a "solicitud" (application)
# with a 20-day public comment period. That's worthless to supply companies.
# We only want CONCESIÓN (grant) notices.
#
# ALSO: Filter out small activity licences (peluquería, pastelería, bar).
# Supply companies want CONSTRUCTION permits (obra mayor), not retail opening licences.
# ════════════════════════════════════════════════════════════
GRANT_PHRASES = [
    "se concede",
    "se otorga",
    "se autoriza",
    "concesión de licencia",
    "licencia concedida",
    "se resuelve favorablemente",
    "otorgamiento de licencia",
    "se acuerda conceder",
    "se acuerda otorgar",
    "resolución estimatoria",
    "expedición de licencia",
    "se aprueba",
]

# These MUST NOT appear if we want a construction-relevant permit
DENY_PHRASES = [
    "denegación", "se deniega", "inadmisión", "desestimación",
    "se desestima", "resolución denegatoria", "no se concede",
    "caducidad", "archivo del expediente",
]

# This phrase = APPLICATION phase, not granted. Discard.
APPLICATION_PHRASES = [
    "se ha solicitado",
    "ha solicitado licencia",
    "se encuentra en tramitación",
    "en período de información pública",
    "plazo de veinte días",
    "plazo de treinta días",
    "a fin de que quienes se consideren afectados",   # public comment period boilerplate
    "quienes se consideren afectados",
    "formular observaciones",
]

# Small activity licences that are NOT interesting for construction supply companies
SMALL_ACTIVITY_WORDS = [
    "peluquería", "peluquería y estética", "barbería", "salón de belleza",
    "pastelería", "panadería", "carnicería", "pescadería", "frutería",
    "estanco", "locutorio", "quiosco", "taller mecánico ligero",
    "academia de idiomas", "centro de yoga", "clínica dental", "farmacia",
    "bar", "café", "cafetería", "restaurante", "heladería",
    "lavandería", "tintorería", "zapatería",
]

# Keywords that indicate a MAJOR CONSTRUCTION project (what supply companies want)
CONSTRUCTION_INDICATORS = [
    "obra mayor", "obras mayores",
    "nueva construcción", "nueva planta", "edificio nuevo",
    "rehabilitación integral", "rehabilitación de edificio",
    "ampliación de edificio", "demolición y construcción",
    "reforma integral", "reforma estructural",
    "viviendas", "local comercial de gran superficie",
    "nave industrial", "almacén", "centro logístico",
    "hotel", "residencial", "bloque de viviendas",
    "presupuesto de ejecución material",
    "p.e.m", "base imponible",
]

def classify_permit(text):
    """
    Returns (is_useful, reject_reason) where:
    - is_useful=True means this is a granted CONSTRUCTION permit worth saving
    - reject_reason explains why it was rejected (for logging)
    """
    t = text.lower()

    # 1. Hard denial check
    if any(p in t for p in DENY_PHRASES):
        return False, "denial language"

    # 2. Application phase check — "se ha solicitado" = not yet granted
    if any(p in t for p in APPLICATION_PHRASES):
        return False, "solicitud (application, not grant)"

    # 3. Must have a grant phrase
    if not any(p in t for p in GRANT_PHRASES):
        return False, "no grant language found"

    # 4. Check if it's a small activity licence we don't care about
    is_small_activity = any(w in t for w in SMALL_ACTIVITY_WORDS)
    is_construction   = any(w in t for w in CONSTRUCTION_INDICATORS)

    if is_small_activity and not is_construction:
        return False, "small activity licence (not construction)"

    return True, ""

# ════════════════════════════════════════════════════════════
# DATA EXTRACTION
# Two modes: keyword regex (free) or GPT-4o-mini (€0.001/permit)
# ════════════════════════════════════════════════════════════
MONTHS_ES = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
}

def parse_spanish_date(s):
    if not s: return ""
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', s, re.I)
    if m:
        mo = MONTHS_ES.get(m.group(2).lower())
        if mo:
            try: return datetime(int(m.group(3)),mo,int(m.group(1))).strftime("%Y-%m-%d")
            except: pass
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', s)
    if m:
        try: return datetime(int(m.group(3)),int(m.group(2)),int(m.group(1))).strftime("%Y-%m-%d")
        except: pass
    return s[:10] if len(s) >= 10 else s

def keyword_extract(text, url, pub_date):
    res = {
        "address": None, "applicant": None, "permit_type": "obra mayor",
        "declared_value_eur": None,
        "date_granted": parse_spanish_date(pub_date) or extract_date_from_url(url),
        "description": None, "confidence": "medium",
        "source_url": url, "extraction_mode": "keyword",
    }
    c = re.sub(r'\s+', ' ', text)

    # Address
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*,?\s*n[úu]?[mº°]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:paseo|po\.?|pso\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:camino|glorieta|ronda|travesía|urbanización)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+n[úu]?[mº°]?\.?\s*(\d+)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;")
            break

    # Applicant
    for pat in [
        r'(?:a instancia de|solicitante|interesado[/a]*|promovido por|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:\bS\.?[AL]\.?U?\.?\b))',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            a = m.group(1).strip().rstrip(".,;")
            if 3 < len(a) < 80:
                res["applicant"] = a; break

    # Permit type
    t = c.lower()
    if any(p in t for p in ["nueva construcción","nueva planta","obra nueva","edificio de nueva"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral","restauración de edificio","reconstrucción"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma","ampliación","cambio de uso","modificación de edificio"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif "nave industrial" in t or "almacén" in t:
        res["permit_type"] = "obra mayor industrial"
    elif any(p in t for p in ["actividad","local comercial","establecimiento"]):
        res["permit_type"] = "licencia de actividad"

    # Declared value — search for PEM or ICIO base
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'(?:base imponible(?:\s+del\s+ICIO)?|cuota\s+ICIO)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
        r'(?:importe|presupuesto)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)?',
        r'([0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            vs = m.group(1).strip()
            if "," in vs and "." in vs:   vs = vs.replace(".","").replace(",",".")
            elif "," in vs:               vs = vs.replace(",",".")
            else:                         vs = vs.replace(".","")
            try:
                v = float(vs)
                if v >= 500:
                    res["declared_value_eur"] = round(v, 2); break
            except ValueError:
                pass

    # Description
    dm = re.search(
        r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de|ampliación de)\s+[^\.]{15,300}',
        c, re.I)
    res["description"] = dm.group(0).strip()[:300] if dm else c[:250].strip()
    return res

def ai_extract(text, url, pub_date):
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        prompt = f"""Eres un experto en boletines oficiales españoles (BOCM).

Dado el texto de una licencia de obras o urbanística CONCEDIDA, extrae:
1. Dirección completa del inmueble (calle, número, municipio)
2. Nombre del solicitante/promotor (empresa o persona)
3. Tipo de obra (uno de exactamente: "obra mayor nueva construcción", "obra mayor rehabilitación", "obra mayor industrial", "licencia de actividad", "otro")
4. Presupuesto de ejecución material o base imponible ICIO en euros (número, sin símbolo €)
5. Fecha de concesión (YYYY-MM-DD)
6. Descripción de las obras (máx. 200 caracteres)

Responde SOLO en JSON (sin markdown, sin texto previo o posterior):
{{"address":null,"applicant":null,"permit_type":"otro","declared_value_eur":null,"date_granted":null,"description":null,"confidence":"high/medium/low"}}

Reglas:
- Si falta un dato, usa null
- declared_value_eur debe ser número puro (ej. 450000.0), nunca string
- Para "se ha solicitado" (sólo aplicación, no concesión) → confidence="low"
- Para "se concede" o "se otorga" → confidence="high"

Texto:
{text[:4500]}"""

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=600
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r'^```(?:json)?\s*','',raw,flags=re.M)
        raw = re.sub(r'```\s*$','',raw,flags=re.M)
        d   = json.loads(raw.strip())
        d["source_url"]      = url
        d["extraction_mode"] = "ai"

        dg = d.get("date_granted") or pub_date or extract_date_from_url(url)
        d["date_granted"] = parse_spanish_date(dg)

        if isinstance(d.get("declared_value_eur"), str):
            try:
                v = d["declared_value_eur"].replace(".","").replace(",",".").replace("€","").strip()
                d["declared_value_eur"] = float(v)
            except: d["declared_value_eur"] = None

        return d
    except json.JSONDecodeError as e:
        log(f"    AI JSON error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)
    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def extract(text, url, pub_date):
    return ai_extract(text,url,pub_date) if USE_AI else keyword_extract(text,url,pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Notes",
]
_ws=None; _seen_urls=set()

def get_sheet():
    global _ws
    if _ws: return _ws
    sa = os.environ.get("GCP_SERVICE_ACCOUNT_JSON","").strip()
    if not sa:
        log("❌ GCP_SERVICE_ACCOUNT_JSON not set")
        return None
    try:
        info  = json.loads(sa)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try:    ws = sh.worksheet("Permits")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet("Permits",1000,20)
        if ws.row_values(1) != HDRS:
            ws.update(values=[HDRS], range_name="A1")
            log("✅ Sheet headers written")
        else:
            log("✅ Sheet connected")
        _ws = ws
        return _ws
    except Exception as e:
        log(f"❌ Sheet: {e}"); return None

def load_seen():
    global _seen_urls
    ws = get_sheet()
    if not ws: return
    try:
        _seen_urls = set(u.strip() for u in ws.col_values(10)[1:] if u.strip())
        log(f"✅ {len(_seen_urls)} existing URLs loaded")
    except Exception as e:
        log(f"⚠️  load_seen: {e}")

def write_permit(p, pdf_url=""):
    ws  = get_sheet()
    url = p.get("source_url","")
    if url in _seen_urls:
        log(f"  ⏭️  Dup: {url[-60:]}"); return False

    dec = p.get("declared_value_eur")
    est = round(dec/0.03) if dec and isinstance(dec,(int,float)) and dec>0 else ""
    addr = p.get("address") or ""
    maps = ""
    if addr:
        maps = ("https://www.google.com/maps/search/"
                + addr.replace(" ","+").replace(",","")
                + "+Madrid+España")

    row = [
        p.get("date_granted",""), "Madrid", addr,
        p.get("applicant") or "", p.get("permit_type") or "",
        dec or "", est, maps,
        (p.get("description") or "")[:300],
        url, pdf_url or "",
        p.get("extraction_mode","keyword"), p.get("confidence",""),
        datetime.now().strftime("%Y-%m-%d %H:%M"), "",
    ]
    try:
        if ws:
            ws.append_row(row, value_input_option="USER_ENTERED")
            _seen_urls.add(url)
            # Colour by confidence
            try:
                rn = len(ws.get_all_values())
                c  = p.get("confidence","")
                rb,gb,bb = (0.85,0.93,0.85) if c=="high" else \
                           (1.00,0.97,0.80) if c=="medium" else \
                           (0.98,0.91,0.91)
                ws.spreadsheet.batch_update({"requests":[{"repeatCell":{
                    "range":{"sheetId":ws.id,"startRowIndex":rn-1,"endRowIndex":rn},
                    "cell":{"userEnteredFormat":{"backgroundColor":{"red":rb,"green":gb,"blue":bb}}},
                    "fields":"userEnteredFormat.backgroundColor"}}]})
            except: pass
        log(f"  💾 {addr[:55]} | {p.get('permit_type','?')} | €{dec or '?'}")
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
        rows = ws.get_all_values()
        if len(rows)<2: log("⚠️  Sheet empty"); return
        cutoff = datetime.now()-timedelta(days=7)
        recent = []
        for row in rows[1:]:
            if len(row)<14: continue
            try:
                if datetime.strptime(row[13][:10],"%Y-%m-%d") >= cutoff:
                    recent.append(row)
            except: pass

        def val(r):
            try: return float(str(r[5]).replace(",",".")) if r[5] else 0
            except: return 0
        recent.sort(key=val, reverse=True)
        total = sum(val(r) for r in recent)
        log(f"📧 Digest: {len(recent)} permits, total €{int(total):,}")

        rhtml = ""
        for r in recent:
            dec = f"€{int(float(r[5])):,}" if r[5] else "—"
            est = f"€{int(float(r[6])):,}" if r[6] else "—"
            rhtml += f"""<tr style="border-bottom:1px solid #eee">
              <td style="padding:11px 8px;font-weight:600">{r[2] or "—"}</td>
              <td style="padding:11px 8px;color:#444;font-size:13px">{r[3] or "—"}</td>
              <td style="padding:11px 8px"><span style="background:#e8f5e9;color:#2e7d32;padding:3px 8px;border-radius:10px;font-size:11px;white-space:nowrap">{r[4] or "—"}</span></td>
              <td style="padding:11px 8px;font-weight:700;color:#1565c0;font-size:15px">{dec}</td>
              <td style="padding:11px 8px;color:#555;font-size:13px">{est}</td>
              <td style="padding:11px 8px;font-size:12px;color:#666">{(r[8] or "")[:100]}</td>
              <td style="padding:11px 8px;white-space:nowrap">{"<a href='"+r[7]+"' style='color:#1565c0;font-size:13px'>📍</a>&nbsp;" if r[7] else ""}{"<a href='"+r[9]+"' style='color:#999;font-size:11px'>BOCM</a>" if r[9] else ""}</td>
            </tr>"""

        ws_d = (datetime.now()-timedelta(days=7)).strftime("%d %b")
        we_d = datetime.now().strftime("%d %b %Y")
        html = f"""<html><body style="font-family:Arial,sans-serif;max-width:1100px;margin:20px auto;color:#1a1a1a">
<div style="background:#1565c0;color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ ConstructorScout — Licencias Concedidas Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">Semana {ws_d} – {we_d}</p>
</div>
<div style="display:flex;background:#e3f2fd;border-bottom:2px solid #bbdefb">
  <div style="flex:1;padding:18px 28px;border-right:1px solid #bbdefb">
    <div style="font-size:36px;font-weight:700;color:#1565c0">{len(recent)}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Nuevas licencias concedidas</div>
  </div>
  <div style="flex:1;padding:18px 28px">
    <div style="font-size:36px;font-weight:700;color:#1565c0">€{int(total):,}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Valor total declarado</div>
  </div>
</div>
<div style="padding:16px 28px 4px">
  <p style="color:#444;font-size:14px;margin:0">Estas licencias han sido <strong>concedidas</strong> esta semana en la Comunidad de Madrid.
  Contacta al promotor antes que tu competencia.</p>
</div>
<div style="overflow-x:auto;padding:0 28px 24px">
<table style="width:100%;border-collapse:collapse;min-width:700px">
  <thead><tr style="background:#f5f5f5;text-align:left">
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Dirección</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Solicitante</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Tipo</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Declarado</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Est. Obra</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Descripción</th>
    <th style="padding:9px 8px;font-size:11px;color:#777;text-transform:uppercase;border-bottom:2px solid #e0e0e0">Links</th>
  </tr></thead>
  <tbody>{rhtml or '<tr><td colspan="7" style="padding:24px;text-align:center;color:#aaa">Sin licencias esta semana</td></tr>'}</tbody>
</table></div>
<div style="padding:16px 28px;background:#f9f9f9;font-size:12px;color:#888;border-top:1px solid #e8e8e8">
  <strong>ConstructorScout</strong> — Datos extraídos del BOCM (registros públicos oficiales de la Comunidad de Madrid).<br>
  Cada licencia = un proyecto de construcción que comenzará pronto.
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM","")
        gp = os.environ.get("GMAIL_APP_PASSWORD","")
        gt = os.environ.get(CLIENT_EMAIL_VAR,"")
        if not all([gf,gp,gt]):
            log("⚠️  Email env vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🏗️ ConstructorScout Madrid — {len(recent)} licencias | {ws_d}–{we_d}"
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
def run():
    if args.digest:
        log("📧 Digest-only mode")
        get_sheet(); send_digest(); return

    today     = datetime.now()
    date_to   = today
    date_from = today - timedelta(weeks=WEEKS_BACK)

    log("="*64)
    log(f"🏗️  ConstructorScout — Madrid Building Permit Engine")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction (no API key)'}")
    log(f"💰  Min value: €{MIN_VALUE_EUR:,.0f}" if MIN_VALUE_EUR else "💰  No value filter")
    log("="*64)

    get_sheet(); load_seen()

    # ── COLLECTION PHASE ────────────────────────────────────
    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming from saved queue: {len(all_urls)} URLs")
    else:
        all_urls = []
        seen_set = set()

        # Primary: keyword search
        for kw in SEARCH_KEYWORDS:
            urls = search_keyword(kw, date_from, date_to)
            for u in urls:
                if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                    seen_set.add(u); all_urls.append(u)
            log(f"  → {len(urls)} from '{kw}' | {len(all_urls)} unique total")
            time.sleep(3)

        # Secondary: RSS feed
        rss_pdfs = get_rss_pdf_links(date_from, date_to)
        for u in rss_pdfs:
            if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                seen_set.add(u); all_urls.append(u)
        log(f"  → RSS added {len(rss_pdfs)} PDF links | {len(all_urls)} total")

        # Remove already-processed
        all_urls = [u for u in all_urls if u not in _seen_urls]
        log(f"\n📋 {len(all_urls)} new URLs to process")

        # Save queue to disk so we can resume if job is killed
        with open(QUEUE_FILE,"w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved to {QUEUE_FILE}")

    if not all_urls:
        log("ℹ️  Nothing new to process.")
        if today.weekday()==0:
            log("📧 Monday → digest"); send_digest()
        return

    # ── PROCESSING PHASE ────────────────────────────────────
    saved=skipped=errors=0

    for idx, url in enumerate(all_urls):
        log(f"\n[{idx+1}/{len(all_urls)}] {url}")
        try:
            text, pdf_url, pub_date = fetch_announcement(url)

            if not text or len(text.strip()) < 80:
                log("  ⚠️  Too little text — skip"); skipped+=1; continue

            useful, reason = classify_permit(text)
            if not useful:
                log(f"  ⏭️  {reason} — skip"); skipped+=1; continue

            log("  ✅ Qualifying permit — extracting data…")
            p = extract(text, url, pub_date)
            log(f"  addr='{(p.get('address') or '')[:50]}' "
                f"val=€{p.get('declared_value_eur','?')} "
                f"type='{p.get('permit_type','?')}' "
                f"conf='{p.get('confidence','?')}'")

            dec = p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec < MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min €{MIN_VALUE_EUR:,.0f}"); skipped+=1; continue

            if write_permit(p, pdf_url or ""): saved+=1
            else: skipped+=1

        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors+=1

        time.sleep(2)   # polite delay between document fetches

    log(f"\n{'='*64}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log(f"{'='*64}")

    # Clean up queue file on success
    if os.path.exists(QUEUE_FILE):
        os.remove(QUEUE_FILE)

    if today.weekday()==0:
        log("\n📧 Monday → digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
