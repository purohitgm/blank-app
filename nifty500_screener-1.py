"""
╔══════════════════════════════════════════════════════════════╗
║         NIFTY 500 PATTERN SCREENER  — Streamlit App         ║
║  Patterns: NR4 · NR7 · NR21 · Pocket Pivot · RS Lead · VCP ║
║  Dependencies: streamlit · pandas · numpy · requests        ║
╚══════════════════════════════════════════════════════════════╝

BUGS FIXED vs previous version:
  1. @st.cache_data in ThreadPoolExecutor threads → NoSessionContext crash
     FIX: module-level dict cache with threading.Lock (no Streamlit context needed)
  2. Yahoo Finance 401 / empty data — crumb token missing
     FIX: proper YF session init: fc.yahoo.com cookie → /v1/test/getcrumb → crumb param
  3. _parse_chart dropna() silently drops last bar when Volume is null (NSE common)
     FIX: dropna only on OHLC columns; fill null Volume with 0
  4. VCP requires 180 bars (20×3+120) but 3mo/6mo ≈ 65–130 bars → never fires
     FIX: reduced ut_lb 120→50 (min bars now 110, well within 6mo)

Run: streamlit run nifty500_screener.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import threading
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NIFTY 500 SCREENER",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────
#  CSS
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
html, body, [data-testid="stApp"] {
    background: #090909 !important;
    color: #e0e0e0 !important;
    font-family: 'IBM Plex Mono','Courier New',monospace !important;
}
#MainMenu, footer, header        { visibility: hidden; }
[data-testid="stDecoration"]     { display: none; }
[data-testid="stToolbar"]        { display: none; }
[data-testid="collapsedControl"] { display: none; }
section[data-testid="stSidebar"] { display: none; }

[data-testid="stSelectbox"] label {
    color: #e8a020 !important; font-size: 10px !important;
    font-family: 'IBM Plex Mono',monospace !important;
    letter-spacing: 1px !important; text-transform: uppercase !important;
}
[data-baseweb="select"] > div {
    background: #141414 !important; border: 1px solid #2a2a2a !important;
    border-radius: 0 !important; color: #e0e0e0 !important;
    font-family: 'IBM Plex Mono',monospace !important; font-size: 12px !important;
}
[data-baseweb="select"] > div:focus-within { border-color: #e8a020 !important; }

[data-testid="stCheckbox"] label {
    color: #bbb !important; font-size: 12px !important;
    font-family: 'IBM Plex Mono',monospace !important;
}

.stButton > button {
    background: #e8a020 !important; color: #000 !important;
    font-family: 'IBM Plex Mono',monospace !important;
    font-weight: 700 !important; font-size: 13px !important;
    letter-spacing: 1.5px !important; border: none !important;
    border-radius: 0 !important; width: 100% !important;
    padding: 10px 0 !important; margin-top: 18px !important;
}
.stButton > button:hover    { background: #ffb830 !important; }
.stButton > button:disabled { background: #2a2a2a !important; color: #555 !important; }

[data-testid="stProgressBar"] > div > div { background: #e8a020 !important; }

[data-testid="stTabs"] [role="tablist"] { border-bottom: 1px solid #1e1e1e !important; }
[data-testid="stTabs"] [role="tab"] {
    color: #444 !important; font-family: 'IBM Plex Mono',monospace !important;
    font-size: 11px !important; letter-spacing: 1px !important;
    text-transform: uppercase !important; padding: 8px 20px !important;
    border-radius: 0 !important; border: none !important; background: transparent !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #e8a020 !important; border-bottom: 2px solid #e8a020 !important;
    background: #0f0f0f !important;
}
[data-testid="stTabs"] [role="tab"]:hover { color: #aaa !important; }

[data-testid="stMetric"] {
    background: #0f0f0f !important; border: 1px solid #1a1a1a !important;
    border-left: 2px solid #e8a020 !important; padding: 8px 12px !important;
    border-radius: 0 !important;
}
[data-testid="stMetricLabel"] {
    color: #555 !important; font-size: 9px !important;
    font-family: 'IBM Plex Mono',monospace !important;
    letter-spacing: 1px !important; text-transform: uppercase !important;
}
[data-testid="stMetricValue"] {
    color: #e8a020 !important; font-size: 20px !important;
    font-weight: 700 !important; font-family: 'IBM Plex Mono',monospace !important;
}

[data-testid="stDownloadButton"] > button {
    background: transparent !important; color: #22d3ee !important;
    border: 1px solid #22d3ee !important;
    font-family: 'IBM Plex Mono',monospace !important;
    font-size: 11px !important; border-radius: 0 !important;
    padding: 6px 16px !important; margin-top: 0 !important; width: auto !important;
}
[data-testid="stDownloadButton"] > button:hover {
    background: rgba(34,211,238,0.08) !important;
}

hr { border-color: #1a1a1a !important; margin: 8px 0 !important; }
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #111; }
::-webkit-scrollbar-thumb { background: #2a2a2a; }
::-webkit-scrollbar-thumb:hover { background: #e8a020; }

.sc-tbl { width:100%; border-collapse:collapse;
    font-family:'IBM Plex Mono',monospace; font-size:11px; }
.sc-tbl th { padding:8px 10px; background:#0f0f0f; color:#e8a020;
    font-size:10px; letter-spacing:1px; text-transform:uppercase;
    border-bottom:2px solid #c8841a; white-space:nowrap; text-align:left; }
.sc-tbl td { padding:7px 10px; border-bottom:1px solid #131313; white-space:nowrap; }
.sc-tbl tr:hover td { background:rgba(232,160,32,.04); }
.sc-tbl tr.hi td { background:rgba(232,160,32,.06); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  UNIVERSE  — Full Nifty 500  (~500 NSE symbols)
# ─────────────────────────────────────────────────────────────
_RAW = [
    # ── Large-cap IT / Software ──
    "TCS","INFY","WIPRO","HCLTECH","TECHM","LTIM","OFSS","MPHASIS",
    "PERSISTENT","COFORGE","TATAELXSI","LTTS","KPITTECH","ZENSARTECH",
    "NIITTECH","MASTEK","HEXAWARE","SONATSOFTW","TANLA","RATEGAIN",
    "NEWGEN","INTELLECT","DATAMATICS","ZENSAR","TTML","VAKRANGEE",
    # ── Banking — Private ──
    "HDFCBANK","ICICIBANK","KOTAKBANK","AXISBANK","INDUSINDBK",
    "BANDHANBNK","FEDERALBNK","IDFCFIRSTB","RBLBANK","AUBANK",
    "DCBBANK","CSBBANK","KARNATAKABAN","LAKSHVILAS","TMBFINANCIAL",
    "EQUITASBNK","UJJIVANSFB","SURYODAY","ESAFSFB",
    # ── Banking — PSU ──
    "SBIN","CANBK","PNB","BANKBARODA","UNIONBANK","INDIANB",
    "BANKINDIA","CENTRALBK","IOB","MAHABANK","UCOBANK","PSBBANK",
    "JKBANK","KTKBANK","SOUTHBANK","KARURVYSYA","CITYUNIONBANK",
    # ── NBFC & Housing Finance ──
    "BAJFINANCE","BAJAJFINSV","CHOLAFIN","SHRIRAMFIN","M&MFIN",
    "LICHSGFIN","PNBHOUSING","CANFINHOME","REPCO","AAVAS",
    "HOMEFIRST","APTUS","MANAPPURAM","MUTHOOTFIN","IIFL",
    "ABCAPITAL","MOTILALOFS","ANGELONE","5PAISA","EDELWEISS",
    # ── Insurance ──
    "HDFCLIFE","SBILIFE","ICICIGI","NIACL","STARHEALTH",
    "MAXLIFE","GODIGIT","GICRE","POLICYBZR",
    # ── Capital Markets / Exchanges ──
    "SBICARD","BSE","CAMS","CDSL","MCXINDIA","IREDA",
    # ── Energy — Oil & Gas ──
    "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","GAIL",
    "PETRONET","IGL","MGL","ATGL","GULFOILLUB","MRPL",
    "CASTROLIND","CHENNPETRO","NOCIL","AEGISCHEM",
    # ── Power & Utilities ──
    "POWERGRID","NTPC","TATAPOWER","ADANIGREEN","ADANIPOWER",
    "TORNTPOWER","CESC","NHPC","SJVN","RECLTD","PFC",
    "JSWENERGY","GREENKO","KALPATPOWR","NAVA","RTNPOWER",
    "PGIL","INOXWIND","SUZLON","ORIENTELEC",
    # ── FMCG / Staples ──
    "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
    "GODREJCP","COLPAL","TATACONSUM","JYOTHYLAB","EMAMILTD",
    "VBL","RADICO","UNITDSPR","MCDOWELL-N","GLOBUSSPR",
    "PGHH","GILLETTE","ZYDUSWELL","BAJAJCON","HATSUN",
    "HERITAGE","KRBL","LTFOODS","AVANTI","VARUN",
    # ── Consumer Discretionary ──
    "ASIANPAINT","BERGEPAINT","PIDILITIND","HAVELLS","CROMPTON",
    "VOLTAS","TITAN","TRENT","BATAINDIA","PAGEIND","KAJARIACER",
    "SYMPHONY","VGUARD","RAJESHEXPO","WHIRLPOOL","BLUESTARCO",
    "ORIENTBELL","CERA","GREENPANEL","CENTURY","SUPREMEIND",
    "ASTRAL","FINOLEX","PRINCEPIPE","POLYPLEX","NILKAMAL",
    "RELAXO","CAMPUS","MIRZA","METRO","SPARC",
    # ── Auto & Auto-Ancillaries ──
    "MARUTI","TATAMOTORS","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
    "TVSMOTORS","M&M","MAHINDCIE","MOTHERSON","BALKRISIND",
    "APOLLOTYRE","BHARATFORG","ENDURANCE","BOSCHLTD","EXIDEIND",
    "AMARAJABAT","SCHAEFFLER","TIINDIA","SUNDRMFAST","MINDAIND",
    "SUPRAJIT","CRAFTSMAN","GABRIEL","SUBROS","WABCOINDIA",
    "JTEKTINDIA","SHRIRAMCIT","LUMAXIND","LUMAXTECH","SWARAJENG",
    "ESCORTS","TVSMOTOR","SMLISUZU","FORCEMOT","OLECTRA",
    # ── Pharma & Healthcare ──
    "SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","AUROPHARMA",
    "TORNTPHARM","LUPIN","ALKEM","ZYDUSLIFE","BIOCON",
    "NATCOPHARM","GRANULES","LAURUSLABS","SYNGENE","GLAND",
    "IPCA","AJANTPHARM","LALPATHLAB","METROPOLIS","KRSNAA",
    "APOLLOHOSP","FORTIS","MAXHEALTH","NARAYANA","ASTER",
    "RAINBOW","KIMS","YATHARTH","MEDANTA","HEALTHCARE",
    "PFIZER","GLAXO","ABBOTINDIA","SANOFI","NOVARTIS",
    "SUVEN","SOLARA","DRREDDYS","STRIDES","ERIS",
    "CAPLIPOINT","MARKSANS","SEQUENT","LAURUS","SHILPAMED",
    # ── Metals & Mining ──
    "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","HINDZINC",
    "NMDC","COALINDIA","NATIONALUM","MOIL","RATNAMANI",
    "WELCORP","APL","JINDALSAW","JINDALSTEL","SAILSTEEL",
    "GPIL","NMDC","KIOCL","MISRDHATU","RVNL",
    # ── Capital Goods / Engineering / Defence ──
    "LT","BHEL","ABB","SIEMENS","THERMAX","CUMMINSIND",
    "AIAENG","GRINDWELL","ELGIEQUIP","HAL","BEL","BEML",
    "BHARAT","COCHINSHIP","MAZAGON","DATPATTERN","PARAS",
    "TEXRAIL","TITAGARH","IRCON","NBCC","RITES",
    "POWERINDIA","TDPOWERSYS","VOLTAMP","JYOTISTRUC","KEI",
    "POLYCAB","FINOLEX","HAVELLS","INOXINDIA","ELECON",
    "KAYNES","SYRMA","AVALON","CENTUM","MTAR",
    # ── Cement & Construction Materials ──
    "ULTRACEMCO","SHREECEM","GRASIM","ACC","AMBUJACEM",
    "DALMIACEM","JKCEMENT","RAMCOCEM","HEIDELBERG","BIRLACORP",
    "STARCEMENT","NUVOCO","INDIACEM","ORIENTCEM","MANGCMCL",
    # ── Real Estate ──
    "DLF","GODREJPROP","PRESTIGE","OBEROIRLTY","PHOENIXLTD",
    "BRIGADE","SOBHA","MAHLIFE","KOLTEPATIL","SUNTECK",
    "LODHA","MACROTECH","EMAAR","KEYSTONE","ANANTRAJ",
    # ── Infrastructure / Logistics ──
    "ADANIPORTS","IRCTC","IRFC","CONCOR","MAHLOG",
    "BLUEDART","GATI","TCI","ALLCARGO","GATEWAY",
    "GMRINFRA","GVK","AIAENG","KNRCON","ASHOKA",
    "SADBHAV","HGINFRA","PNC","PNCINFRA","GPPL",
    # ── Telecom ──
    "BHARTIARTL","INDUSTOWER","TATACOMM","HFCL","RAILTEL",
    "MTNL","ITI","TEJASNET","STLTECH",
    # ── Media & Entertainment ──
    "ZEEL","SUNTV","PVRINOX","INOXLEISUR","NETWORK18",
    "TV18BRDCST","NDTV","SAREGAMA","TIPS","EROS",
    # ── Chemicals & Specialty ──
    "PIIND","DEEPAKFERT","COROMANDEL","TATACHEM","GNFC",
    "ALKYLAMINE","AARTIIND","NAVINFLUOR","FINEORG","VINATIORGA",
    "CLEAN","SUDARSCHEM","ATUL","GALAXYSURF","BASF",
    "NOCIL","VINATI","SRF","NEOGEN","ROSSARI",
    "ANUPAM","HOCL","PCBL","IGPL","TATACHEM",
    "DEEPAKNTR","DHARAMSI","HIKAL","LXCHEM","CHEMPLASTS",
    # ── Fertilisers & Agro ──
    "CHAMBLFERT","KSCL","RALLIS","BAYER","UPL",
    "DHANUKA","INSECTICID","DHANUKAS","PI","JUBLPHARMA",
    # ── Textiles & Apparel ──
    "RAYMOND","KPRMILL","TRIDENT","WELSPUNIND","VARDHMAN",
    "NITIN","AARVEE","GRASIM","ARVIND","MAFATLAL",
    "SIYARAM","RUPA","DOLLAR","LAXMIMACH","TEXINFRA",
    # ── Paper & Packaging ──
    "TNPL","WCPAPER","SATIA","ANDHRPAPER","ITC",
    "GPPL","MIRCELECTR","TCNSCLOTH","CANTABIL",
    # ── IT Services — Mid / Small ──
    "CYIENT","BIRLASOFT","HEXAWARE","SASKEN","OFSS",
    "ROUTE","FSL","ORACLE","INFOEDGE","JUSTDIAL",
    "POLICYBZR","NAUKRI","INDIAMART","CARTRADE","EASY",
    # ── New-age / Tech / Internet ──
    "ZOMATO","PAYTM","DELHIVERY","NYKAA","MAPMYINDIA",
    "IXIGO","FIRSTCRY","AWFIS","ZAGGLE","YATHARTH",
    # ── Diversified Conglomerates ──
    "ADANIENT","BAJAJHLDNG","TATAINVEST","CHOLAHLDNG",
    "RELI","HDFCAMC","UTIAMC","NIPPONLIFE","360ONE",
    # ── Hotels & Leisure ──
    "INDHOTEL","EIHOTEL","LEMONTREE","CHALET","MAHINDHOLIDAY",
    "THOMAS","MHRIL","WONDERLA",
    # ── Jewellery / Gems ──
    "TITAN","KALYAN","SENCO","PCJEWELLER","RAJESHEXPO",
    # ── Paints ──
    "ASIANPAINT","BERGEPAINT","KANSAINER","INDIGO","SHALPAINTS",
    # ── Gas / Pipeline ──
    "GAIL","IGL","MGL","ATGL","GSPL","GUJGAS",
    # ── Retail ──
    "DMART","VMART","SHOPERSTOP","TRENT","ABFRL",
    "VEDANT","BATA","RELAXO","METRO",
    # ── Food & Beverages ──
    "JUBLFOOD","DEVYANI","WESTLIFE","SAPPHIRE","BARBEQUE",
    "VARUN","KRBL","LTFOODS","USHAMART","PATANJALI",
]

def _build_universe(raw):
    seen, result = set(), []
    for s in raw:
        if s not in seen:
            seen.add(s)
            result.append({"display": s, "ticker": s.replace("&", "%26") + ".NS"})
    return result

UNIVERSE     = _build_universe(_RAW)
BENCH_TICKER = "%5ENSEI"   # ^NSEI


# ─────────────────────────────────────────────────────────────
#  YAHOO FINANCE SESSION  (FIX #2: crumb + cookie)
# ─────────────────────────────────────────────────────────────
_YF_LOCK    = threading.Lock()
_YF_SESSION = None
_YF_CRUMB   = None
_YF_HOSTS   = [
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
]
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin":          "https://finance.yahoo.com",
    "Referer":         "https://finance.yahoo.com/",
}


def _init_yf_session() -> tuple:
    """
    FIX #2 — Yahoo Finance now requires:
      1) A valid session cookie (fetched from https://fc.yahoo.com)
      2) A crumb token  (fetched from /v1/test/getcrumb with that cookie)
    Returns (session, crumb).
    """
    global _YF_SESSION, _YF_CRUMB
    with _YF_LOCK:
        if _YF_SESSION is not None and _YF_CRUMB:
            return _YF_SESSION, _YF_CRUMB

        sess = requests.Session()
        sess.headers.update(_HEADERS)

        # Step 1 — acquire cookie from fc.yahoo.com
        try:
            sess.get("https://fc.yahoo.com", timeout=8, allow_redirects=True)
        except Exception:
            pass

        # Also hit the main finance page to set cookies
        try:
            sess.get("https://finance.yahoo.com", timeout=8, allow_redirects=True)
        except Exception:
            pass

        # Step 2 — get crumb
        crumb = ""
        for host in _YF_HOSTS:
            try:
                r = sess.get(f"{host}/v1/test/getcrumb", timeout=8)
                if r.status_code == 200 and r.text.strip():
                    crumb = r.text.strip()
                    break
            except Exception:
                pass

        _YF_SESSION = sess
        _YF_CRUMB   = crumb
        return _YF_SESSION, _YF_CRUMB


def _reset_yf_session():
    global _YF_SESSION, _YF_CRUMB
    with _YF_LOCK:
        _YF_SESSION = None
        _YF_CRUMB   = None


def _fetch_chart(ticker: str, period: str, timeout: int = 15) -> dict | None:
    """Fetch Yahoo Finance v8 chart JSON with crumb."""
    sess, crumb = _init_yf_session()
    crumb_param = f"&crumb={crumb}" if crumb else ""
    path = (f"/v8/finance/chart/{ticker}"
            f"?interval=1d&range={period}&events=div{crumb_param}")

    for attempt in range(2):          # retry once on failure
        for host in _YF_HOSTS:
            try:
                r = sess.get(host + path, timeout=timeout)
                if r.status_code == 401:
                    # Crumb expired — reset and retry
                    _reset_yf_session()
                    sess, crumb = _init_yf_session()
                    crumb_param = f"&crumb={crumb}" if crumb else ""
                    path = (f"/v8/finance/chart/{ticker}"
                            f"?interval=1d&range={period}&events=div{crumb_param}")
                    continue
                if r.status_code == 200:
                    data = r.json()
                    # Validate that we got actual data
                    if (data.get("chart", {}).get("result") and
                            data["chart"]["result"][0].get("timestamp")):
                        return data
            except Exception:
                pass
            time.sleep(0.05)
        if attempt == 0:
            time.sleep(0.5)

    return None


# ─────────────────────────────────────────────────────────────
#  THREAD-SAFE OHLCV CACHE   (FIX #1: no @st.cache_data in threads)
# ─────────────────────────────────────────────────────────────
_OHLCV_CACHE: dict      = {}
_OHLCV_LOCK:  threading.Lock = threading.Lock()
_BENCH_CACHE: dict      = {}
_BENCH_LOCK:  threading.Lock = threading.Lock()


def clear_all_caches():
    """Call before each scan to force fresh data."""
    global _OHLCV_CACHE, _BENCH_CACHE
    with _OHLCV_LOCK:
        _OHLCV_CACHE = {}
    with _BENCH_LOCK:
        _BENCH_CACHE = {}
    _reset_yf_session()


def _parse_chart(data: dict) -> pd.DataFrame | None:
    """
    FIX #3 — dropna() only on OHLC columns.
    Volume null (common for last bar during/after session) is filled with 0,
    not used as a reason to drop the row.
    """
    try:
        res = data["chart"]["result"][0]
        ts  = res["timestamp"]
        q   = res["indicators"]["quote"][0]

        df = pd.DataFrame({
            "Open":   q.get("open",   [None] * len(ts)),
            "High":   q.get("high",   [None] * len(ts)),
            "Low":    q.get("low",    [None] * len(ts)),
            "Close":  q.get("close",  [None] * len(ts)),
            "Volume": q.get("volume", [0]    * len(ts)),
        }, index=(pd.to_datetime(ts, unit="s", utc=True)
                    .tz_convert("Asia/Kolkata")
                    .normalize()))
        df.index.name = "Date"

        # Convert to numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # FIX #3: drop only rows where OHLC price data is missing
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        # Fill null volume with 0 (don't drop the row)
        df["Volume"] = df["Volume"].fillna(0)

        return df if len(df) >= 25 else None
    except Exception:
        return None


def fetch_ohlcv(ticker: str, period: str) -> pd.DataFrame | None:
    """Thread-safe fetch with module-level dict cache."""
    key = f"{ticker}|{period}"
    with _OHLCV_LOCK:
        if key in _OHLCV_CACHE:
            return _OHLCV_CACHE[key]

    data   = _fetch_chart(ticker, period)
    result = _parse_chart(data) if data else None

    with _OHLCV_LOCK:
        _OHLCV_CACHE[key] = result
    return result


def fetch_benchmark(period: str) -> pd.DataFrame | None:
    """Cached benchmark fetch (called from main thread before pool starts)."""
    with _BENCH_LOCK:
        if period in _BENCH_CACHE:
            return _BENCH_CACHE[period]

    data = _fetch_chart(BENCH_TICKER, period)
    df   = _parse_chart(data) if data else None
    result = df[["Close"]].rename(columns={"Close": "Bench"}) if df is not None else None

    with _BENCH_LOCK:
        _BENCH_CACHE[period] = result
    return result


def _align_bench(stock_df: pd.DataFrame, bench_df: pd.DataFrame) -> np.ndarray:
    merged = stock_df[["Close"]].join(bench_df[["Bench"]], how="inner")
    return merged["Bench"].values


# ─────────────────────────────────────────────────────────────
#  PATTERN DETECTION
# ─────────────────────────────────────────────────────────────

def is_nr(H: np.ndarray, L: np.ndarray, n: int) -> bool:
    """Today's H-L range is strictly smallest of the last n sessions."""
    if len(H) < n:
        return False
    ranges = H[-n:] - L[-n:]
    today  = ranges[-1]
    # today must be positive and strictly less than every prior day in window
    return bool(today > 0 and today < ranges[:-1].min())


def pocket_pivot(C: np.ndarray, V: np.ndarray) -> bool:
    """
    Pocket Pivot (Kacher-Morales / Minervini):
    - Today is an up-day (close > prior close)
    - Today's volume > highest down-day volume of the PRIOR 10 sessions
    """
    if len(C) < 13:
        return False
    # Today must be an up day
    if C[-1] <= C[-2]:
        return False
    # Collect volumes of down days from sessions [-11] to [-2] (10 sessions before today)
    down_vols = []
    for i in range(1, 11):          # i=1 → yesterday, i=10 → 10 days ago
        idx_now  = -(i + 1)         # day being evaluated
        idx_prev = -(i + 2)         # prior day to compare against
        if abs(idx_prev) <= len(C): # bounds check
            if C[idx_now] < C[idx_prev]:
                down_vols.append(float(V[idx_now]))
    if not down_vols:
        return False
    return float(V[-1]) > max(down_vols)


def rs_leads_price(C: np.ndarray, B: np.ndarray,
                   lb: int = 63, rs_thr: float = 0.97, p_thr: float = 0.96) -> bool:
    """
    RS Leads Price High:
    - RS line (stock ÷ Nifty50) within rs_thr of its lb-day high
    - Price still p_thr or more below its own lb-day high
    """
    n = min(len(C), len(B))
    if n < lb + 2:
        return False
    c = C[-n:]
    b = B[-n:]
    safe_b = np.where(b > 0, b, np.nan)
    rs     = c / safe_b
    rs_w   = rs[-lb:]
    p_w    = c[-lb:]
    if np.any(np.isnan(rs_w)):
        return False
    rs_max    = rs_w.max()
    price_max = p_w.max()
    return bool(
        rs_w[-1]  >= rs_max    * rs_thr and
        p_w[-1]   <  price_max * p_thr
    )


def vcp(C: np.ndarray, H: np.ndarray, L: np.ndarray, V: np.ndarray,
        win: int = 20, nwin: int = 3, ut_lb: int = 50, min_up: float = 0.08) -> bool:
    """
    Volatility Contraction Pattern (Minervini):
    - Stock must be ≥8% above its ut_lb-day low (uptrend requirement)
    - Three successive win-day windows must show BOTH:
        * Contracting price range% (high-low / low × 100)
        * Declining average volume

    FIX #4: ut_lb reduced 120→50. Min bars = win*nwin + ut_lb = 60+50 = 110.
    Works with 6mo data (~130 bars). Previously needed 180 bars (never fired on 6mo).
    """
    min_bars = win * nwin + ut_lb
    if len(C) < min_bars:
        return False

    # Uptrend filter
    lo = L[-ut_lb:].min()
    if lo <= 0 or C[-1] < lo * (1 + min_up):
        return False

    n = len(C)
    rngs: list[float] = []
    vols: list[float] = []

    for w in range(nwin):
        # Window w=0 is oldest, w=nwin-1 is most recent (ends at today)
        s = n - win * (nwin - w)
        e = s + win
        if s < 0 or e > n:
            return False
        h_win = H[s:e].max()
        l_win = L[s:e].min()
        if l_win <= 0:
            return False
        rngs.append((h_win - l_win) / l_win * 100)
        vols.append(float(V[s:e].mean()))

    range_contracts = all(rngs[i] > rngs[i + 1] for i in range(nwin - 1))
    vol_contracts   = all(vols[i] > vols[i + 1] for i in range(nwin - 1))
    return bool(range_contracts and vol_contracts)


def calc_atr_pct(H: np.ndarray, L: np.ndarray, C: np.ndarray, p: int = 14) -> float:
    if len(C) < p + 1:
        return 0.0
    trs = []
    for i in range(-p, 0):
        tr = max(H[i] - L[i],
                 abs(H[i] - C[i - 1]),
                 abs(L[i] - C[i - 1]))
        trs.append(tr)
    return float(np.mean(trs) / C[-1] * 100) if C[-1] > 0 else 0.0


def calc_vol_ratio(V: np.ndarray, avg_p: int = 20) -> float:
    if len(V) < avg_p + 1:
        return 1.0
    avg = float(V[-(avg_p + 1):-1].mean())
    return float(V[-1] / avg) if avg > 0 else 1.0


def pct_from_52w(C: np.ndarray, p: int = 252) -> float:
    w  = C[-min(p, len(C)):]
    hi = w.max()
    return float((C[-1] - hi) / hi * 100) if hi > 0 else 0.0


# ─────────────────────────────────────────────────────────────
#  SINGLE STOCK ANALYSIS
# ─────────────────────────────────────────────────────────────
def _err_row(sym: str) -> dict:
    return {"symbol": sym, "error": True, "score": 0,
            "nr4": False, "nr7": False, "nr21": False,
            "pocket_pivot": False, "rs_leads": False, "vcp": False,
            "close": 0.0, "chg_pct": 0.0, "vol_ratio": 0.0,
            "atr_pct": 0.0, "from_52w": 0.0}


def analyse(entry: dict, period: str, bench_df) -> dict:
    sym = entry["display"]
    try:
        df = fetch_ohlcv(entry["ticker"], period)
        if df is None:
            return _err_row(sym)

        H = df["High"].values.astype(float)
        L = df["Low"].values.astype(float)
        C = df["Close"].values.astype(float)
        V = df["Volume"].values.astype(float)

        # Bench alignment
        bench_arr = None
        if bench_df is not None:
            try:
                bench_arr = _align_bench(df, bench_df)
            except Exception:
                bench_arr = None

        # ── Pattern checks ──
        nr4_h  = is_nr(H, L, 4)
        nr7_h  = is_nr(H, L, 7)
        nr21_h = is_nr(H, L, 21)
        pp_h   = pocket_pivot(C, V)
        rs_h   = (rs_leads_price(C, bench_arr)
                  if bench_arr is not None and len(bench_arr) > 65
                  else False)
        vcp_h  = vcp(C, H, L, V)
        score  = sum([nr4_h, nr7_h, nr21_h, pp_h, rs_h, vcp_h])

        c0 = float(C[-1])
        cp = float(C[-2]) if len(C) > 1 else c0
        chg = (c0 - cp) / cp * 100 if cp > 0 else 0.0

        return {
            "symbol":       sym,
            "close":        round(c0, 2),
            "chg_pct":      round(chg, 2),
            "vol_ratio":    round(calc_vol_ratio(V), 2),
            "atr_pct":      round(calc_atr_pct(H, L, C), 2),
            "from_52w":     round(pct_from_52w(C), 2),
            "nr4":          nr4_h,
            "nr7":          nr7_h,
            "nr21":         nr21_h,
            "pocket_pivot": pp_h,
            "rs_leads":     rs_h,
            "vcp":          vcp_h,
            "score":        score,
            "error":        False,
        }
    except Exception:
        return _err_row(sym)


# ─────────────────────────────────────────────────────────────
#  SCREENER RUNNER
# ─────────────────────────────────────────────────────────────
def run_screener(universe: list, period: str, workers: int,
                 pb, status_txt) -> tuple[list, dict]:
    """
    FIX #1: benchmark is fetched BEFORE the thread pool starts (main thread).
    Individual stocks are fetched inside threads using the module-level dict
    cache (no @st.cache_data — no Streamlit context required).
    """
    # Fetch benchmark in main thread first
    bench_df = fetch_benchmark(period)

    results: list[dict] = []
    ctr = dict(done=0, passed=0, errors=0,
               nr4=0, nr7=0, nr21=0, pp=0, rs=0, vcp=0)
    total = len(universe)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {ex.submit(analyse, entry, period, bench_df): entry
                for entry in universe}

        for fut in as_completed(fmap):
            ctr["done"] += 1
            try:
                r = fut.result(timeout=30)
            except Exception:
                r = None

            if r and not r["error"]:
                results.append(r)
                ctr["passed"] += 1
                if r["nr4"]:          ctr["nr4"]  += 1
                if r["nr7"]:          ctr["nr7"]  += 1
                if r["nr21"]:         ctr["nr21"] += 1
                if r["pocket_pivot"]: ctr["pp"]   += 1
                if r["rs_leads"]:     ctr["rs"]   += 1
                if r["vcp"]:          ctr["vcp"]  += 1
            else:
                ctr["errors"] += 1

            pb.progress(min(ctr["done"] / total, 1.0))
            signals_so_far = sum(1 for x in results if x["score"] > 0)
            status_txt.markdown(
                f'<span style="color:#e8a020;font-size:11px;font-family:monospace">'
                f'&#9679; SCANNING {ctr["done"]}/{total}'
                f'&nbsp;&nbsp;|&nbsp;&nbsp;VALID: {ctr["passed"]}'
                f'&nbsp;&nbsp;|&nbsp;&nbsp;SIGNALS: {signals_so_far}'
                f'&nbsp;&nbsp;|&nbsp;&nbsp;ERRORS: {ctr["errors"]}</span>',
                unsafe_allow_html=True,
            )

    return results, ctr


# ─────────────────────────────────────────────────────────────
#  RENDER HELPERS
# ─────────────────────────────────────────────────────────────
def badge(hit: bool, label: str) -> str:
    if hit:
        return (f'<span style="background:rgba(34,197,94,.15);color:#4ade80;'
                f'border:1px solid #22c55e44;padding:2px 6px;font-size:10px;'
                f'font-weight:700;font-family:monospace">{label}</span>')
    return '<span style="color:#252525;font-size:10px">&#183;</span>'


def score_pips(score: int) -> str:
    clr = {6:"#ffd700",5:"#f97316",4:"#22c55e",3:"#22d3ee",
           2:"#e0e0e0",1:"#666",0:"#222"}.get(score, "#222")
    pips = "".join(
        f'<span style="display:inline-block;width:7px;height:13px;margin-right:2px;'
        f'background:{clr if i < score else "#1a1a1a"};'
        f'border:1px solid {clr if i < score else "#2a2a2a"}"></span>'
        for i in range(6)
    )
    return (f'<span style="color:{clr};font-weight:700;font-size:14px;'
            f'font-family:monospace">{score}</span>&nbsp;{pips}')


def render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.markdown(
            '<div style="text-align:center;padding:50px;color:#333;'
            'font-family:monospace;font-size:12px;border:1px solid #141414;">'
            'NO STOCKS MATCH CURRENT FILTERS</div>',
            unsafe_allow_html=True,
        )
        return

    RC = {1: "#ffd700", 2: "#c0c0c0", 3: "#cd7f32"}
    rows_html = ""
    for i, r in enumerate(df.itertuples(), 1):
        rc  = RC.get(i, "#555")
        rsm = {1: "①", 2: "②", 3: "③"}.get(i, str(i))
        cc  = "#22c55e" if r.chg_pct >= 0 else "#ef4444"
        cs  = "+" if r.chg_pct >= 0 else ""
        fc  = "#22c55e" if r.from_52w >= -5 else ("#888" if r.from_52w >= -15 else "#444")
        hi  = "hi" if i <= 5 else ""
        rows_html += (
            f'<tr class="{hi}">'
            f'<td style="font-weight:700;color:{rc};font-size:13px;text-align:center;width:34px">{rsm}</td>'
            f'<td style="color:#22d3ee;font-weight:600;font-size:12px">{r.symbol}</td>'
            f'<td style="color:#e0e0e0;font-variant-numeric:tabular-nums">&#8377;{r.close:,.2f}</td>'
            f'<td style="color:{cc}">{cs}{r.chg_pct:.2f}%</td>'
            f'<td style="color:#888">{r.vol_ratio:.2f}&times;</td>'
            f'<td style="color:#666">{r.atr_pct:.2f}%</td>'
            f'<td style="color:{fc}">{r.from_52w:+.1f}%</td>'
            f'<td style="text-align:center">{badge(r.nr4,          "NR4"  )}</td>'
            f'<td style="text-align:center">{badge(r.nr7,          "NR7"  )}</td>'
            f'<td style="text-align:center">{badge(r.nr21,         "NR21" )}</td>'
            f'<td style="text-align:center">{badge(r.pocket_pivot, "PP"   )}</td>'
            f'<td style="text-align:center">{badge(r.rs_leads,     "RS&#8593;")}</td>'
            f'<td style="text-align:center">{badge(r.vcp,          "VCP"  )}</td>'
            f'<td>{score_pips(r.score)}</td>'
            f'</tr>'
        )

    st.markdown(
        f'<table class="sc-tbl"><thead><tr>'
        f'<th>#</th><th>SYMBOL</th><th>CLOSE</th><th>CHG%</th>'
        f'<th>VOL&times;</th><th>ATR%</th><th>52W</th>'
        f'<th style="text-align:center">NR4</th>'
        f'<th style="text-align:center">NR7</th>'
        f'<th style="text-align:center">NR21</th>'
        f'<th style="text-align:center">P.PIV</th>'
        f'<th style="text-align:center">RS&#8593;</th>'
        f'<th style="text-align:center">VCP</th>'
        f'<th>SCORE</th>'
        f'</tr></thead><tbody>{rows_html}</tbody></table>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────────────────────
for _k, _v in [("results_df", None), ("counters", {}),
               ("scan_time", None),  ("scan_done", False),
               ("bench_ok", False),  ("last_errors", [])]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─────────────────────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────────────────────
now_ist  = datetime.datetime.now(
    datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
mkt_min  = now_ist.hour * 60 + now_ist.minute
mkt_open = (now_ist.weekday() < 5) and (555 <= mkt_min < 930)

col_h, col_t = st.columns([5, 1])
with col_h:
    st.markdown(
        '<div style="padding:4px 0 10px">'
        '<span style="background:#e8a020;color:#000;font-weight:700;font-size:11px;'
        'padding:5px 9px;letter-spacing:1px;font-family:monospace">NSE</span>'
        '&nbsp;&nbsp;'
        '<span style="color:#e8a020;font-size:20px;font-weight:700;letter-spacing:3px;'
        'font-family:monospace">NIFTY 500 PATTERN SCREENER</span>'
        '<br><span style="color:#2a2a2a;font-size:10px;letter-spacing:2px;'
        'font-family:monospace">'
        'NR4 &#183; NR7 &#183; NR21 &#183; POCKET PIVOT &#183; RS LEAD &#183; VCP'
        '</span></div>',
        unsafe_allow_html=True,
    )
with col_t:
    mc = "#22c55e" if mkt_open else "#ef4444"
    st.markdown(
        f'<div style="text-align:right;padding-top:8px">'
        f'<div style="color:#22d3ee;font-family:monospace;font-size:13px;font-weight:600">'
        f'IST&nbsp;{now_ist.strftime("%H:%M")}</div>'
        f'<div style="color:{mc};font-family:monospace;font-size:10px;letter-spacing:1px">'
        f'{"&#11044; OPEN" if mkt_open else "&#9711; CLOSED"}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
st.markdown('<hr style="margin:0 0 10px">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  INLINE CONTROL PANEL  (all controls always visible)
# ─────────────────────────────────────────────────────────────
st.markdown(
    '<div style="color:#e8a020;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin-bottom:6px">&#9632; SCAN SETTINGS</div>',
    unsafe_allow_html=True,
)

# Row A — dropdowns
cA = st.columns(6)
with cA[0]:
    data_period = st.selectbox("Data Period",     ["3mo","6mo","1y"],  index=1, key="sp")
with cA[1]:
    n_choice    = st.selectbox("Universe Size",   ["Top 100","Top 200","Top 300","Top 400","All (~500)"], index=4, key="su")
with cA[2]:
    max_workers = st.selectbox("Workers",         [3, 5, 8, 10], index=1, key="sw")
with cA[3]:
    min_score   = st.selectbox("Min Score",       [0, 1, 2, 3, 4, 5, 6], index=0, key="sm")
with cA[4]:
    sort_by     = st.selectbox("Sort By",         ["score","chg_pct","vol_ratio","atr_pct","close","from_52w"], index=0, key="sb")
with cA[5]:
    sort_dir    = st.selectbox("Direction",       ["Descending ▼","Ascending ▲"], index=0, key="sd")

# Row B — pattern filters + RUN button
st.markdown(
    '<div style="color:#555;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin:8px 0 4px">&#9632; PATTERN FILTER</div>',
    unsafe_allow_html=True,
)
cB = st.columns([1.2, 1, 1, 1.4, 1.6, 1, 1.8])
with cB[0]: f_nr4  = st.checkbox("NR4",           value=False, key="fn4")
with cB[1]: f_nr7  = st.checkbox("NR7",           value=False, key="fn7")
with cB[2]: f_nr21 = st.checkbox("NR21",          value=False, key="fn21")
with cB[3]: f_pp   = st.checkbox("Pocket Pivot",  value=False, key="fpp")
with cB[4]: f_rs   = st.checkbox("RS Leads High", value=False, key="frs")
with cB[5]: f_vcp  = st.checkbox("VCP",           value=False, key="fvcp")
with cB[6]: run_btn = st.button("▶  RUN SCREENER", key="runbtn", use_container_width=True)

st.markdown('<hr style="margin:10px 0">', unsafe_allow_html=True)

# Universe slice
_n_map = {"Top 100": 100, "Top 200": 200, "Top 300": 300, "Top 400": 400, "All (~500)": len(UNIVERSE)}
scan_univ = UNIVERSE[: _n_map[n_choice]]


# ─────────────────────────────────────────────────────────────
#  SCAN EXECUTION
# ─────────────────────────────────────────────────────────────
if run_btn:
    clear_all_caches()   # flush stale data

    pb   = st.progress(0.0)
    stxt = st.empty()
    diag = st.empty()

    # Show benchmark status
    diag.markdown(
        '<span style="color:#555;font-size:10px;font-family:monospace">'
        'Initialising Yahoo Finance session + fetching benchmark…</span>',
        unsafe_allow_html=True,
    )

    raw, ctr = run_screener(scan_univ, data_period, max_workers, pb, stxt)

    pb.progress(1.0)
    bench_ok = _BENCH_CACHE.get(data_period) is not None
    bench_msg = ("&#10003; Benchmark OK" if bench_ok
                 else "&#9888; Benchmark failed — RS signal disabled")
    bench_col = "#22c55e" if bench_ok else "#f97316"

    stxt.markdown(
        f'<span style="color:#22c55e;font-size:11px;font-family:monospace">'
        f'&#10003; SCAN COMPLETE &nbsp;|&nbsp; '
        f'{ctr["passed"]} fetched &nbsp;|&nbsp; '
        f'{sum(1 for r in raw if r["score"] > 0)} signals &nbsp;|&nbsp; '
        f'{ctr["errors"]} errors</span>',
        unsafe_allow_html=True,
    )
    diag.markdown(
        f'<span style="color:{bench_col};font-size:10px;font-family:monospace">'
        f'{bench_msg}</span>',
        unsafe_allow_html=True,
    )

    if raw:
        good = [r for r in raw if not r["error"]]
        st.session_state["results_df"] = pd.DataFrame(good) if good else None
        st.session_state["counters"]   = ctr
        st.session_state["scan_time"]  = now_ist.strftime("%d-%b-%Y %H:%M IST")
        st.session_state["scan_done"]  = True
        st.session_state["bench_ok"]   = bench_ok


# ─────────────────────────────────────────────────────────────
#  RESULTS
# ─────────────────────────────────────────────────────────────
if st.session_state["scan_done"] and st.session_state["results_df"] is not None:
    df_all = st.session_state["results_df"].copy()
    ctr    = st.session_state["counters"]

    # ── Metric cards ──
    mc_pairs = [
        ("FETCHED",  "passed"), ("ERRORS",   "errors"),
        ("NR4",      "nr4"),    ("NR7",      "nr7"),
        ("NR21",     "nr21"),   ("P.PIVOT",  "pp"),
        ("RS LEADS", "rs"),     ("VCP",      "vcp"),
    ]
    for col, (lbl, key) in zip(st.columns(8), mc_pairs):
        col.metric(lbl, ctr.get(key, 0))

    if st.session_state["scan_time"]:
        st.markdown(
            f'<div style="color:#333;font-size:10px;font-family:monospace;'
            f'text-align:right;padding:4px 0">'
            f'Last scan: {st.session_state["scan_time"]}</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<hr style="margin:4px 0 0">', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "  RESULTS TABLE  ",
        "  PATTERN HEATMAP  ",
        "  EXPORT CSV  ",
    ])

    # ── Tab 1 : Results ──
    with tab1:
        df = df_all.copy()
        if f_nr4:  df = df[df["nr4"]          == True]
        if f_nr7:  df = df[df["nr7"]          == True]
        if f_nr21: df = df[df["nr21"]         == True]
        if f_pp:   df = df[df["pocket_pivot"] == True]
        if f_rs:   df = df[df["rs_leads"]     == True]
        if f_vcp:  df = df[df["vcp"]          == True]
        df = df[df["score"] >= min_score]
        df = df.sort_values(sort_by, ascending=(sort_dir == "Ascending ▲")).reset_index(drop=True)

        st.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:6px 0 8px;font-size:10px;font-family:monospace">'
            f'<span style="color:#e8a020;letter-spacing:1px">RESULTS</span>'
            f'<span style="color:#444">'
            f'showing {len(df)} / {len(df_all)} stocks</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        render_table(df)

    # ── Tab 2 : Co-occurrence heatmap ──
    with tab2:
        pkeys = ["nr4","nr7","nr21","pocket_pivot","rs_leads","vcp"]
        plbls = ["NR4","NR7","NR21","P.PIVOT","RS&#8593;","VCP"]

        st.markdown(
            '<div style="color:#444;font-size:10px;font-family:monospace;'
            'letter-spacing:1px;margin-bottom:10px">'
            'CO-OCCURRENCE — % of row-pattern stocks that also show column-pattern'
            '</div>',
            unsafe_allow_html=True,
        )
        hm = ('<table style="border-collapse:collapse;font-family:monospace;font-size:11px">'
              '<tr><th style="padding:8px 12px;color:#1e1e1e;border-bottom:1px solid #1a1a1a"></th>')
        for l in plbls:
            hm += (f'<th style="padding:8px 12px;color:#e8a020;'
                   f'border-bottom:1px solid #1a1a1a;text-align:center;'
                   f'font-size:10px;letter-spacing:1px">{l}</th>')
        hm += '</tr>'
        for l1, p1 in zip(plbls, pkeys):
            hm += (f'<tr><td style="padding:7px 12px;color:#e8a020;font-size:10px;'
                   f'letter-spacing:1px;border-right:1px solid #1a1a1a">{l1}</td>')
            t1 = int(df_all[p1].sum())
            for l2, p2 in zip(plbls, pkeys):
                if l1 == l2:
                    hm += '<td style="background:#111;text-align:center;border:1px solid #0d0d0d;color:#222;font-size:10px">&mdash;</td>'
                    continue
                both = int((df_all[p1] & df_all[p2]).sum())
                pct  = round(both / t1 * 100, 0) if t1 > 0 else 0
                a    = pct / 100 * 0.65
                fc   = "#22c55e" if pct > 25 else ("#888" if pct > 10 else "#333")
                hm  += (f'<td style="padding:7px 12px;background:rgba(34,197,94,{a:.2f});'
                        f'color:{fc};text-align:center;border:1px solid #0d0d0d">'
                        f'{int(pct)}%</td>')
            hm += '</tr>'
        hm += '</table>'
        st.markdown(hm, unsafe_allow_html=True)

        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(
            '<span style="color:#444;font-size:10px;font-family:monospace;'
            'letter-spacing:1px">TOP 5 STOCKS PER PATTERN</span>',
            unsafe_allow_html=True,
        )
        c6 = st.columns(6)
        for idx, (pk, pl) in enumerate(zip(pkeys, plbls)):
            sub  = df_all[df_all[pk] == True].nlargest(5, "score")
            body = (f'<div style="color:#e8a020;font-size:10px;font-weight:700;'
                    f'letter-spacing:1px;margin-bottom:6px;font-family:monospace">{pl}</div>')
            for _, row in sub.iterrows():
                body += (f'<div style="font-family:monospace;font-size:11px;'
                         f'color:#22d3ee;padding:3px 0;border-bottom:1px solid #111">'
                         f'{row["symbol"]} '
                         f'<span style="color:#333;font-size:10px">({int(row["score"])})</span>'
                         f'</div>')
            if sub.empty:
                body += '<div style="color:#222;font-size:10px;font-family:monospace">No hits</div>'
            with c6[idx]:
                st.markdown(
                    f'<div style="background:#0c0c0c;padding:10px;border:1px solid #181818">'
                    f'{body}</div>',
                    unsafe_allow_html=True,
                )

    # ── Tab 3 : Export ──
    with tab3:
        df_exp = df_all.sort_values("score", ascending=False).copy()
        df_exp.columns = [c.upper() for c in df_exp.columns]
        st.download_button(
            "&#11015;  DOWNLOAD CSV",
            data=df_exp.to_csv(index=False).encode(),
            file_name=f"nifty500_scan_{now_ist.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )
        st.markdown('<br>', unsafe_allow_html=True)
        st.dataframe(df_exp.head(30), use_container_width=True, hide_index=True)

else:
    st.markdown(
        '<div style="text-align:center;padding:60px 20px;border:1px solid #141414;margin:8px 0">'
        '<div style="font-size:40px;opacity:.06;margin-bottom:14px">&#9900;</div>'
        '<div style="color:#222;font-size:13px;letter-spacing:3px;font-family:monospace;margin-bottom:8px">'
        'AWAITING SCAN</div>'
        '<div style="color:#191919;font-size:11px;font-family:monospace">'
        'Set your preferences above &rarr; press &#9654; RUN SCREENER</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
#  PATTERN REFERENCE
# ─────────────────────────────────────────────────────────────
st.markdown('<hr style="margin:20px 0 10px">', unsafe_allow_html=True)
st.markdown(
    '<div style="color:#2a2a2a;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin-bottom:8px">PATTERN REFERENCE</div>',
    unsafe_allow_html=True,
)
_ref = [
    ("NR4",      "Narrow Range 4",   "Today's H-L is narrowest of last 4 sessions. First compression signal."),
    ("NR7",      "Narrow Range 7",   "Narrowest of last 7 sessions. Toby Crabel coil. Classic breakout setup."),
    ("NR21",     "Narrow Range 21",  "Narrowest in ~1 month. Extreme compression — major move imminent."),
    ("P.PIVOT",  "Pocket Pivot",     "Up-day vol > highest down-day vol of prior 10 sessions. Institutional accumulation."),
    ("RS&#8593;","RS Leads Price",   "RS line (stock÷Nifty50) within 3% of 63-day high; price still 4%+ below its own high."),
    ("VCP",      "Volatility Contr.","3×20-day windows: contracting range% AND declining avg volume on an uptrend."),
]
for col, (tag, name, desc) in zip(st.columns(6), _ref):
    with col:
        st.markdown(
            f'<div style="background:#0a0a0a;border:1px solid #141414;'
            f'border-top:2px solid #e8a020;padding:10px 12px">'
            f'<div style="color:#e8a020;font-size:10px;font-weight:700;letter-spacing:1px;'
            f'font-family:monospace;margin-bottom:3px">{tag}</div>'
            f'<div style="color:#888;font-size:10px;font-family:monospace;margin-bottom:4px">'
            f'{name}</div>'
            f'<div style="color:#2e2e2e;font-size:9px;line-height:1.6;font-family:monospace">'
            f'{desc}</div></div>',
            unsafe_allow_html=True,
        )
