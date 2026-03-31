import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials as SACredentials
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--client", required=True)
parser.add_argument("--weeks",  type=int, default=4)
parser.add_argument("--digest", action="store_true")
args = parser.parse_args()

with open(args.client, "r", encoding="utf-8") as f:
    CFG = json.load(f)

SHEET_ID         = CFG["sheet_id"]
CLIENT_EMAIL_VAR = CFG["email_to_secret_name"]
MIN_VALUE_EUR    = CFG.get("min_declared_value_eur", 0)
WEEKS_BACK       = args.weeks
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "").strip()
USE_AI           = bool(OPENAI_API_KEY)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

BOCM_BASE   = "https://www.bocm.es"
BOCM_SEARCH = "https://www.bocm.es/advanced-search/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def safe_get(url, timeout=30, retries=3):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout, verify=False, allow_redirects=True)
            log(f"    GET {r.status_code} → {r.url[:90]}")
            return r
        except requests.exceptions.Timeout:
            log(f"    Timeout {attempt+1}/{retries}")
            if attempt < retries-1: time.sleep(5)
        except Exception as e:
            log(f"    Error {attempt+1}/{retries}: {e}")
            if attempt < retries-1: time.sleep(5)
    return None

def safe_post(url, data, timeout=30):
    try:
        r = SESSION.post(url, data=data, timeout=timeout, verify=False, allow_redirects=True)
        log(f"    POST {r.status_code} → {url[:70]}")
        return r
    except Exception as e:
        log(f"    POST error: {e}")
        return None

# ══════════════════════════════════════════════════════
# SESSION WARMUP — visit homepage, set cookie consent
# ══════════════════════════════════════════════════════
def warmup_session():
    log("🔥 Warming up BOCM session…")

    # Visit homepage
    r = safe_get(BOCM_BASE + "/")
    if not r:
        log("❌ Cannot reach BOCM")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    time.sleep(1.5)

    # Accept cookie consent — try form submission
    for form in soup.find_all("form"):
        form_text = form.get_text().lower()
        if any(k in form_text for k in ["cookie", "acepto", "consent"]):
            fields = {i.get("name"): i.get("value","")
                      for i in form.find_all("input") if i.get("name")}
            action = form.get("action","")
            url    = urljoin(BOCM_BASE, action) if action else BOCM_BASE+"/"
            safe_post(url, fields)
            log("  ✅ Cookie consent form submitted")
            break

    # Force-set consent cookies (belt-and-braces)
    for name in ["cookies-agreed","cookie-agreed","bocm_cookies","has_js","cookie_accepted"]:
        SESSION.cookies.set(name, "1", domain="www.bocm.es")

    time.sleep(1)

    # Load advanced search page
    r = safe_get(BOCM_SEARCH)
    if not r:
        log("❌ Advanced search page unreachable")
        return None
    if r.status_code != 200:
        log(f"❌ Advanced search returned HTTP {r.status_code}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    log(f"  ✅ Page loaded: '{soup.title.get_text(strip=True) if soup.title else 'no title'}'")

    # Log all form fields for debugging
    for form in soup.find_all("form"):
        for el in form.find_all(["input","select"]):
            nm = el.get("name","")
            if not nm: continue
            if el.name == "select":
                opts = [(o.get("value",""), o.get_text(strip=True)[:40])
                        for o in el.find_all("option")]
                log(f"    SELECT '{nm}': {opts[:6]}")
            else:
                log(f"    {el.name.upper()} '{nm}' type={el.get('type','')} val='{el.get('value','')[:30]}'")

    return soup, r.url

# ══════════════════════════════════════════════════════
# FORM DISCOVERY — find field names dynamically
# ══════════════════════════════════════════════════════
def discover_form(soup):
    """Return dict of form parameters discovered from the page."""
    P = {
        "action": BOCM_SEARCH,
        "method": "get",
        "hidden": {},
        "text":   "text",          # defaults
        "dfrom":  "date_from",
        "dto":    "date_to",
        "section_field": None,
        "section_value": None,
    }

    form = None
    for f in soup.find_all("form"):
        txt = f.get_text().lower()
        if any(k in txt for k in ["buscar","search","texto","fecha"]):
            form = f; break
    if not form and soup.find_all("form"):
        form = soup.find_all("form")[0]
    if not form:
        log("  ⚠️  No form found — using defaults")
        return P

    action = form.get("action","")
    P["action"] = urljoin(BOCM_BASE, action) if action else BOCM_SEARCH
    P["method"] = form.get("method","get").lower()

    for el in form.find_all(["input","select"]):
        nm  = el.get("name","")
        if not nm: continue
        nml = nm.lower()
        lbl = ""
        if el.get("id"):
            lb = soup.find("label", {"for": el.get("id")})
            if lb: lbl = lb.get_text(strip=True).lower()
        ctx = f"{nml} {el.get('id','').lower()} {lbl}"

        if el.name == "input":
            t = el.get("type","text").lower()
            if t == "hidden":
                P["hidden"][nm] = el.get("value","")
            elif t in ("text","search"):
                if any(k in ctx for k in ["text","texto","buscar","search","q","keyword"]):
                    P["text"] = nm
                elif any(k in ctx for k in ["from","desde","inicio","start"]):
                    P["dfrom"] = nm
                elif any(k in ctx for k in ["to","hasta","fin","end"]):
                    P["dto"] = nm
            elif t == "date":
                if any(k in ctx for k in ["from","desde","start"]):
                    P["dfrom"] = nm
                elif any(k in ctx for k in ["to","hasta","end"]):
                    P["dto"] = nm

        elif el.name == "select":
            if any(k in ctx for k in ["secc","section","sección"]):
                P["section_field"] = nm
                for opt in el.find_all("option"):
                    v = opt.get("value","")
                    t2 = opt.get_text(strip=True).lower()
                    if any(k in t2 for k in ["ayuntamiento","local","municipal"]):
                        P["section_value"] = v
                        log(f"  ✅ Section 'Ayuntamientos' = '{v}'")
                        break

    log(f"  📋 text='{P['text']}' dfrom='{P['dfrom']}' dto='{P['dto']}' "
        f"section='{P['section_field']}'='{P['section_value']}'")
    return P

# ══════════════════════════════════════════════════════
# SEARCH KEYWORDS — broad coverage of granted permits
# ══════════════════════════════════════════════════════
KEYWORDS = [
    "licencia de obras mayor concedida",
    "se concede licencia de obras",
    "se otorga licencia urbanística",
    "licencia urbanística concedida",
    "concesión de licencia de obras",
    "obra mayor nueva construcción",
    "obra mayor rehabilitación",
    "licencia de actividad concedida",
    "se concede licencia de actividad",
    "licencia de primera ocupación",
    "declaración responsable de obras mayor",
    "resolución favorable licencia obras",
]

GRANT_PHRASES = [
    "se concede","se otorga","se autoriza","concesión de licencia",
    "licencia concedida","favorable","otorgamiento","se acuerda conceder",
    "resolución estimatoria","expedición de licencia","se acuerda otorgar",
]
DENY_PHRASES = [
    "denegación","se deniega","inadmisión","desestimación","se desestima",
    "resolución denegatoria","no se concede","caducidad","archivo",
]

def search_keyword(keyword, P, date_from, date_to):
    """Search BOCM using the exact hardcoded Drupal routing structure."""
    urls  = []
    seen  = set()
    page  = 0
    fmt   = "%d-%m-%Y"  # Fixed date format to match BOCM's exact requirement

    while True:
        start_str = date_from.strftime(fmt)
        end_str = date_to.strftime(fmt)

        # Using the exact URL routing you discovered
        base_url = f"{BOCM_BASE}/advanced-search/p/field_bulletin_field_date/date__{start_str}/field_bulletin_field_date_1/date__{end_str}/seccion/8387"

        params = {
            "search_api_views_fulltext_1": keyword
        }
        if page > 0:
            params["page"] = page

        req_url = base_url + "?" + urlencode(params)
        r = safe_get(req_url)

        if not r or r.status_code != 200:
            log(f"    Page {page} failed"); break

        soup = BeautifulSoup(r.text, "html.parser")
        new  = 0
        for sel in [
            "a[href*='/boletin/']","a[href*='/anuncio/']","a[href*='/bocm/']",
            ".view-content a",".views-row a",".resultado a",
            "article h3 a","article h2 a",".field--name-title a",
        ]:
            for a in soup.select(sel):
                href = a.get("href","")
                if not href: continue
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                if full in seen: continue
                if "bocm.es" not in full: continue
                if any(s in full for s in ["/advanced-search","/login","/user","#",".css",".js"]): continue
                seen.add(full); urls.append(full); new += 1

        log(f"    Page {page}: {new} new links")
        if new == 0: break

        nxt = soup.select_one("li.next a,.pager-next a,.pagination .next a")
        if not nxt:
            nxt = soup.find("a", string=re.compile(r"siguiente|next|»|›", re.I))
        if not nxt: break
        page += 1
        time.sleep(1.5)

    return urls

# ══════════════════════════════════════════════════════
# FETCH ANNOUNCEMENT + PDF
# ══════════════════════════════════════════════════════
def fetch_announcement(url):
    r = safe_get(url)
    if not r or r.status_code != 200:
        return "", None, ""

    soup  = BeautifulSoup(r.text, "html.parser")
    parts = []
    for sel in [".field--name-body",".field-name-body",".contenido-boletin",
                ".anuncio-texto",".anuncio","article .content","article","main"]:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True))
            break
    if not parts:
        for tag in soup.find_all(["nav","header","footer","aside"]):
            tag.decompose()
        parts.append(soup.get_text(separator=" ", strip=True)[:6000])

    pub_date = ""
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', " ".join(parts))
    if m: pub_date = m.group(0)

    pdf_url = None
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if ".pdf" in h.lower() or "descargar-pdf" in h.lower():
            pdf_url = urljoin(BOCM_BASE, h) if h.startswith("/") else h
            break

    if pdf_url:
        pt = extract_pdf_text(pdf_url)
        if pt: parts.append(pt)

    text = re.sub(r'\s+', ' ', " ".join(parts)).strip()
    return text, pdf_url, pub_date

def extract_pdf_text(url):
    try:
        r = SESSION.get(url, timeout=45, verify=False, allow_redirects=True,
                        headers={**HEADERS,"Accept":"application/pdf,*/*"})
        if r.status_code!=200 or len(r.content)<500: return ""
        if r.content[:4]!=b"%PDF": return ""
        txt = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages[:10]:
                t = pg.extract_text()
                if t: txt += t + "\n"
        return txt[:10000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""

def is_granted(text):
    t = text.lower()
    if any(p in t for p in DENY_PHRASES): return False
    return any(p in t for p in GRANT_PHRASES)

# ══════════════════════════════════════════════════════
# EXTRACTION
# ══════════════════════════════════════════════════════
MONTHS_ES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
             "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}

def parse_date(s):
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
    return s[:10]

def keyword_extract(text, url, pub_date):
    res = {"address":None,"applicant":None,"permit_type":"otro",
           "declared_value_eur":None,"date_granted":parse_date(pub_date),
           "description":None,"confidence":"medium",
           "source_url":url,"extraction_mode":"keyword"}
    c = re.sub(r'\s+',' ', text)

    # Address
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s+n[úu]?[mº]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s+n[úu]?[mº]\.?\s*(\d+)',
        r'(?:paseo|plaza|camino|glorieta)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s+n[úu]?[mº]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+(\d+)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;"); break

    # Applicant
    for pat in [
        r'(?:a instancia de|solicitante|interesado[/a]*|promovido por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:S\.?[AL]\.?[UPR]?\.?|SL|SA|SLU|SAU))',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            a = m.group(1).strip().rstrip(".,;")
            if 3<len(a)<80: res["applicant"]=a; break

    # Permit type
    t = c.lower()
    if any(p in t for p in ["nueva construcción","nueva planta","obra nueva"]):
        res["permit_type"]="obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación","reforma integral","restauración"]):
        res["permit_type"]="obra mayor rehabilitación"
    elif any(p in t for p in ["reforma","ampliación","cambio de uso","modificación"]):
        res["permit_type"]="obra mayor rehabilitación"
    elif "obra menor" in t: res["permit_type"]="obra menor"
    elif any(p in t for p in ["actividad","local comercial","apertura"]): res["permit_type"]="licencia de actividad comercial"

    # Value
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'(?:base imponible(?:\s+del\s+ICIO)?)[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
        r'([0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            vs = m.group(1)
            if "," in vs and "." in vs: vs=vs.replace(".","").replace(",",".")
            elif "," in vs: vs=vs.replace(",",".")
            else: vs=vs.replace(".","")
            try:
                v=float(vs)
                if v>=1000: res["declared_value_eur"]=round(v,2); break
            except: pass

    # Description
    dm = re.search(r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de)\s+[^\.]{15,300}', c, re.I)
    res["description"] = dm.group(0).strip()[:300] if dm else c[:250].strip()
    return res

def ai_extract(text, url, pub_date):
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"""Eres un experto en boletines oficiales españoles (BOCM).
Extrae del siguiente texto de una licencia de obras CONCEDIDA:
1. Dirección completa del inmueble
2. Nombre del solicitante (persona o empresa)
3. Tipo de licencia (uno de: "obra mayor nueva construcción","obra mayor rehabilitación","obra menor","licencia de actividad comercial","otro")
4. Valor declarado en euros (busca PEM, base imponible ICIO)
5. Fecha de concesión (YYYY-MM-DD)
6. Descripción breve (máx 200 chars)

Responde SOLO en JSON válido (sin markdown, sin texto extra):
{{"address":"","applicant":"","permit_type":"","declared_value_eur":null,"date_granted":"","description":"","confidence":"high/medium/low"}}

Si un campo falta usa null. declared_value_eur debe ser número, no string.

Texto: {text[:4000]}"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0, max_tokens=600)
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r'^```(?:json)?\s*','',raw,flags=re.M)
        raw = re.sub(r'```\s*$','',raw,flags=re.M)
        d = json.loads(raw.strip())
        d["source_url"]       = url
        d["extraction_mode"] = "ai"
        if not d.get("date_granted") and pub_date:
            d["date_granted"] = parse_date(pub_date)
        else:
            d["date_granted"] = parse_date(d.get("date_granted",""))
        if isinstance(d.get("declared_value_eur"),str):
            try:
                v=d["declared_value_eur"].replace(".","").replace(",",".").replace("€","").strip()
                d["declared_value_eur"]=float(v)
            except: d["declared_value_eur"]=None
        return d
    except json.JSONDecodeError as e:
        log(f"    AI JSON error ({e}) → keyword fallback")
        return keyword_extract(text,url,pub_date)
    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text,url,pub_date)

def extract(text,url,pub_date):
    return ai_extract(text,url,pub_date) if USE_AI else keyword_extract(text,url,pub_date)

# ══════════════════════════════════════════════════════
# GOOGLE SHEETS
# ══════════════════════════════════════════════════════
HDRS = ["Date Granted","Municipality","Full Address","Applicant",
        "Permit Type","Declared Value (€)","Est. Build Value (€)",
        "Maps Link","Description","Source URL","PDF URL",
        "Mode","Confidence","Date Found","Notes"]
_ws=None; _seen=set()

def get_sheet():
    global _ws
    if _ws: return _ws
    sa=os.environ.get("GCP_SERVICE_ACCOUNT_JSON","").strip()
    if not sa: log("❌ GCP_SERVICE_ACCOUNT_JSON not set"); return None
    try:
        info=json.loads(sa)
        creds=SACredentials.from_service_account_info(info,scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc=gspread.authorize(creds)
        sh=gc.open_by_key(SHEET_ID)
        try: ws=sh.worksheet("Permits")
        except gspread.WorksheetNotFound: ws=sh.add_worksheet("Permits",1000,20)
        if ws.row_values(1)!=HDRS:
            ws.update(values=[HDRS],range_name="A1"); log("✅ Headers written")
        else: log("✅ Sheet connected")
        _ws=ws; return _ws
    except Exception as e:
        log(f"❌ Sheet error: {e}"); return None

def load_seen():
    global _seen
    ws=get_sheet()
    if not ws: return
    try:
        _seen=set(u.strip() for u in ws.col_values(10)[1:] if u.strip())
        log(f"✅ {len(_seen)} existing URLs loaded")
    except Exception as e: log(f"⚠️  load_seen: {e}")

def write_permit(p, pdf_url=""):
    ws=get_sheet()
    url=p.get("source_url","")
    if url in _seen: log(f"  ⏭️  Dup: {url[:60]}"); return False

    dec=p.get("declared_value_eur")
    est=round(dec/0.03) if dec and isinstance(dec,(int,float)) and dec>0 else ""
    addr=p.get("address") or ""
    maps=""
    if addr:
        maps="https://www.google.com/maps/search/"+addr.replace(" ","+").replace(",","")+" Madrid España"

    row=[p.get("date_granted",""),"Madrid",addr,p.get("applicant") or "",
         p.get("permit_type") or "",dec or "",est,maps,
         (p.get("description") or "")[:300],url,pdf_url or "",
         p.get("extraction_mode","keyword"),p.get("confidence",""),
         datetime.now().strftime("%Y-%m-%d %H:%M"),""]
    try:
        if ws:
            ws.append_row(row,value_input_option="USER_ENTERED"); _seen.add(url)
            try:
                rn=len(ws.get_all_values())
                c=p.get("confidence","")
                rb,gb,bb=(0.85,0.93,0.85) if c=="high" else (1,0.97,0.80) if c=="medium" else (0.98,0.91,0.91)
                ws.spreadsheet.batch_update({"requests":[{"repeatCell":{
                    "range":{"sheetId":ws.id,"startRowIndex":rn-1,"endRowIndex":rn},
                    "cell":{"userEnteredFormat":{"backgroundColor":{"red":rb,"green":gb,"blue":bb}}},
                    "fields":"userEnteredFormat.backgroundColor"}}]})
            except: pass
        log(f"  💾 SAVED: {addr[:60]} | {p.get('permit_type','?')} | €{dec or '?'}")
        return True
    except Exception as e:
        log(f"  ❌ Write failed: {e}"); return False

# ══════════════════════════════════════════════════════
# EMAIL DIGEST
# ══════════════════════════════════════════════════════
def send_digest():
    ws=get_sheet()
    if not ws: log("❌ No sheet for digest"); return
    try:
        all_rows=ws.get_all_values()
        if len(all_rows)<2: log("⚠️  Sheet empty"); return
        cutoff=datetime.now()-timedelta(days=7)
        recent=[]
        for row in all_rows[1:]:
            if len(row)<14: continue
            try:
                d=datetime.strptime(row[13][:10],"%Y-%m-%d")
                if d>=cutoff: recent.append(row)
            except: pass

        def val(r):
            try: return float(str(r[5]).replace(",",".")) if r[5] else 0
            except: return 0
        recent.sort(key=val,reverse=True)
        total=sum(val(r) for r in recent)
        log(f"📧 Digest: {len(recent)} permits, €{int(total):,} total")

        rows_html=""
        for r in recent:
            declared=f"€{int(float(r[5])):,}" if r[5] else "—"
            est=f"€{int(float(r[6])):,}" if r[6] else "—"
            rows_html+=f"""<tr style="border-bottom:1px solid #eee;">
              <td style="padding:12px 8px;font-weight:600">{r[2] or "—"}</td>
              <td style="padding:12px 8px;color:#444">{r[3] or "—"}</td>
              <td style="padding:12px 8px"><span style="background:#e8f5e9;color:#2e7d32;padding:3px 8px;border-radius:10px;font-size:11px">{r[4] or "—"}</span></td>
              <td style="padding:12px 8px;font-weight:700;color:#1565c0">{declared}</td>
              <td style="padding:12px 8px;color:#555">{est}</td>
              <td style="padding:12px 8px;font-size:12px;color:#666">{(r[8] or "")[:100]}</td>
              <td style="padding:12px 8px">{"<a href='"+r[7]+"' style='color:#1565c0'>📍</a>" if r[7] else ""} {"<a href='"+r[9]+"' style='color:#999;font-size:11px'>BOCM</a>" if r[9] else ""}</td>
            </tr>"""

        ws_d=(datetime.now()-timedelta(days=7)).strftime("%d %b")
        we_d=datetime.now().strftime("%d %b %Y")
        html=f"""<html><body style="font-family:Arial,sans-serif;max-width:1100px;margin:20px auto">
        <div style="background:#1565c0;color:white;padding:24px;border-radius:8px 8px 0 0">
          <h1 style="margin:0;font-size:22px">🏗️ ConstructorScout — Licencias Madrid</h1>
          <p style="margin:8px 0 0;opacity:.85">Semana {ws_d} – {we_d}</p>
        </div>
        <div style="display:flex;border-bottom:1px solid #ddd">
          <div style="flex:1;padding:20px 24px;border-right:1px solid #ddd">
            <div style="font-size:36px;font-weight:700;color:#1565c0">{len(recent)}</div>
            <div style="color:#666;font-size:13px">Nuevas licencias</div>
          </div>
          <div style="flex:1;padding:20px 24px">
            <div style="font-size:36px;font-weight:700;color:#1565c0">€{int(total):,}</div>
            <div style="color:#666;font-size:13px">Valor total declarado</div>
          </div>
        </div>
        <div style="overflow-x:auto;padding:0 0 16px">
          <table style="width:100%;border-collapse:collapse;min-width:700px">
            <thead><tr style="background:#f5f5f5;text-align:left">
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Dirección</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Solicitante</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Tipo</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Declarado</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Est. Obra</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Descripción</th>
              <th style="padding:10px 8px;font-size:11px;color:#888;text-transform:uppercase">Links</th>
            </tr></thead>
            <tbody>{rows_html or '<tr><td colspan="7" style="padding:24px;text-align:center;color:#aaa">Sin licencias esta semana</td></tr>'}</tbody>
          </table>
        </div>
        <div style="padding:16px 24px;background:#f9f9f9;font-size:12px;color:#888;border-top:1px solid #eee">
          ConstructorScout — Datos extraídos del BOCM (registros públicos oficiales).
        </div></body></html>"""

        gf=os.environ.get("GMAIL_FROM","")
        gp=os.environ.get("GMAIL_APP_PASSWORD","")
        gt=os.environ.get(CLIENT_EMAIL_VAR,"")
        if not all([gf,gp,gt]):
            log("⚠️  Email creds missing — not sending"); return
        msg=MIMEMultipart("alternative")
        msg["Subject"]=f"🏗️ ConstructorScout Madrid — {len(recent)} licencias | {ws_d}–{we_d}"
        msg["From"]=gf; msg["To"]=gt
        msg.attach(MIMEText(html,"html","utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(gf,gp)
            s.sendmail(gf,[t.strip() for t in gt.split(",")],msg.as_string())
        log(f"✅ Digest sent to {gt}")
    except Exception as e:
        log(f"❌ Digest error: {e}"); import traceback; traceback.print_exc()

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def run():
    if args.digest:
        log("📧 Digest-only mode")
        get_sheet(); send_digest(); return

    today=datetime.now()
    date_to=today
    date_from=today-timedelta(weeks=WEEKS_BACK)
    log("="*64)
    log(f"🏗️  ConstructorScout — Madrid")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword mode (no API key)'}")
    log("="*64)

    get_sheet(); load_seen()

    result = warmup_session()
    if not result:
        log("❌ Session warmup failed — aborting"); return
    soup, _ = result

    P = discover_form(soup)

    all_urls=set()
    for kw in KEYWORDS:
        log(f"\n🔎 '{kw}'")
        urls=search_keyword(kw,P,date_from,date_to)
        for u in urls: all_urls.add(u)
        log(f"  → {len(urls)} found | {len(all_urls)} unique total")
        time.sleep(2)

    new=[u for u in all_urls if u not in _seen]
    log(f"\n📋 {len(all_urls)} total | {len(new)} new to process")
    if not new:
        log("ℹ️  Nothing new.")
        if today.weekday()==0: log("\n📧 Monday → digest"); send_digest()
        return

    saved=skipped=errors=0
    for idx,url in enumerate(new):
        log(f"\n[{idx+1}/{len(new)}] {url}")
        try:
            text,pdf_url,pub_date=fetch_announcement(url)
            if not text or len(text.strip())<80:
                log("  ⚠️  Too little text — skip"); skipped+=1; continue
            if not is_granted(text):
                log("  ⏭️  Not a granted permit — skip"); skipped+=1; continue
            log("  ✅ Grant confirmed — extracting…")
            p=extract(text,url,pub_date)
            log(f"  addr='{(p.get('address') or '')[:50]}' val=€{p.get('declared_value_eur','?')} type='{p.get('permit_type','?')}' conf='{p.get('confidence','?')}'")
            dec=p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec<MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min €{MIN_VALUE_EUR:,.0f}"); skipped+=1; continue
            if write_permit(p,pdf_url or ""): saved+=1
            else: skipped+=1
        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors+=1
        time.sleep(1.5)

    log(f"\n{'='*64}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log(f"{'='*64}")
    if today.weekday()==0:
        log("\n📧 Monday → sending digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
