"""
╔══════════════════════════════════════════════════════════════╗
║         NIFTY 500 PATTERN SCREENER  — Streamlit App         ║
║  Patterns: NR4 · NR7 · NR21 · Pocket Pivot · RS Lead · VCP ║
╚══════════════════════════════════════════════════════════════╝
Install:  pip install streamlit yfinance pandas numpy
Run:      streamlit run nifty500_screener.py
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
#  PAGE CONFIG  (must be first Streamlit call)
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="NIFTY 500 PATTERN SCREENER",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  CUSTOM CSS  — Bloomberg terminal aesthetic
# ─────────────────────────────────────────────
st.markdown("""
<style>
/* ── base ── */
html, body, [data-testid="stApp"] {
    background-color: #090909 !important;
    color: #e0e0e0 !important;
    font-family: 'IBM Plex Mono', 'Courier New', monospace !important;
}
/* hide default Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }
[data-testid="stToolbar"]    { display: none; }

/* ── sidebar ── */
[data-testid="stSidebar"] {
    background-color: #0f0f0f !important;
    border-right: 1px solid #1e1e1e !important;
}
[data-testid="stSidebar"] * { color: #c0c0c0 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label,
[data-testid="stSidebar"] .stMultiSelect label {
    color: #e8a020 !important;
    font-size: 11px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}

/* ── sidebar inputs ── */
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #1a1a1a !important;
    border: 1px solid #2a2a2a !important;
    color: #e0e0e0 !important;
    font-size: 12px !important;
}

/* ── metric cards ── */
[data-testid="stMetric"] {
    background: #111 !important;
    border: 1px solid #1e1e1e !important;
    padding: 10px 14px !important;
    border-left: 3px solid #e8a020 !important;
}
[data-testid="stMetricLabel"] { color: #888 !important; font-size: 10px !important; letter-spacing:1px; text-transform:uppercase; }
[data-testid="stMetricValue"] { color: #e8a020 !important; font-size: 22px !important; font-weight: 700 !important; }
[data-testid="stMetricDelta"] { font-size: 11px !important; }

/* ── dataframe ── */
[data-testid="stDataFrame"] { border: 1px solid #1e1e1e !important; }
.stDataFrame iframe { background: #0d0d0d !important; }

/* ── buttons ── */
.stButton > button {
    background-color: #e8a020 !important;
    color: #000 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    letter-spacing: 1px !important;
    border: none !important;
    border-radius: 0 !important;
    padding: 8px 20px !important;
    transition: background 0.15s !important;
}
.stButton > button:hover { background-color: #ffb830 !important; }
.stButton > button:disabled { background-color: #444 !important; color: #888 !important; }

/* ── progress ── */
[data-testid="stProgressBar"] > div > div {
    background-color: #e8a020 !important;
}

/* ── tabs ── */
[data-testid="stTabs"] [role="tab"] {
    color: #666 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 11px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    border-bottom: 2px solid transparent !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #e8a020 !important;
    border-bottom: 2px solid #e8a020 !important;
}

/* ── code/pre ── */
code, pre { background: #111 !important; color: #22d3ee !important; }

/* ── divider ── */
hr { border-color: #1e1e1e !important; }

/* ── scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #111; }
::-webkit-scrollbar-thumb { background: #2a2a2a; }
::-webkit-scrollbar-thumb:hover { background: #e8a020; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  NIFTY 500 UNIVERSE
# ─────────────────────────────────────────────
NIFTY500_SYMBOLS = [
    # IT
    "TCS","INFY","WIPRO","HCLTECH","TECHM","MPHASIS","LTIM","OFSS",
    "PERSISTENT","COFORGE","ZENSARTECH","KPITTECH","TATAELXSI","LTTS",
    # Banking
    "HDFCBANK","ICICIBANK","KOTAKBANK","AXISBANK","SBIN","INDUSINDBK",
    "BANDHANBNK","FEDERALBNK","IDFCFIRSTB","RBLBANK","CANBK","PNB",
    "BANKBARODA","UNIONBANK","INDIANB","AUBANK","DCBBANK","KARNATAKABAN",
    # NBFC / Fin
    "BAJFINANCE","BAJAJFINSV","CHOLAFIN","SBICARD","HDFCLIFE","SBILIFE",
    "ICICIGI","MANAPPURAM","MUTHOOTFIN","AAVAS","CANFINHOME","LICHSGFIN",
    "ABCAPITAL","IIFL","M&MFIN","SHRIRAMFIN",
    # Energy
    "RELIANCE","ONGC","BPCL","IOC","GAIL","PETRONET","IGL","MGL",
    "ATGL","HINDPETRO","MRPL","CASTROLIND",
    # Power
    "POWERGRID","NTPC","TATAPOWER","TORNTPOWER","CESC","ADANIGREEN",
    "ADANIPOWER","NHPC","SJVN","RECLTD","PFC","IREDA",
    # FMCG / Consumer Staples
    "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
    "GODREJCP","COLPAL","TATACONSUM","JYOTHYLAB","EMAMILTD","VBL",
    "RADICO","UNITDSPR","MCDOWELL-N",
    # Consumer Discretionary
    "ASIANPAINT","BERGEPAINT","PIDILITIND","HAVELLS","CROMPTON","VOLTAS",
    "TITAN","TRENT","BATAINDIA","PAGEIND","WHIRLPOOL","RAJESHEXPO",
    "KAJARIACER","CERA","ORIENTBELL","SYMPHONY","RELAXO",
    # Auto & Auto-ancillaries
    "MARUTI","TATAMOTORS","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
    "TVSMOTORS","MOTHERSON","BALKRISIND","APOLLOTYRE","BHARATFORG",
    "ENDURANCE","SCHAEFFLER","BOSCHLTD","EXIDEIND","AMARAJABAT",
    "MINDAIND","SUPRAJIT","SUNDARAMFAST",
    # Pharma & Healthcare
    "SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","AUROPHARMA","TORNTPHARM",
    "LUPIN","ALKEM","ZYDUSLIFE","BIOCON","LALPATHLAB","APOLLOHOSP",
    "FORTIS","MAXHEALTH","NATCOPHARM","GRANULES","LAURUSLABS","SYNGENE",
    "GLAND","IPCA","AJANTPHARM","PFIZER","GLAXO",
    # Metals & Mining
    "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","HINDZINC","NMDC",
    "COALINDIA","NATIONALUM","MOIL","WELCORP","APL","RATNAMANI",
    # Capital Goods / Engineering
    "LT","BHEL","ABB","SIEMENS","THERMAX","CUMMINSIND","AIAENG",
    "GRINDWELL","TIMKEN","SKF","KSB","ELGIEQUIP","KIRLOSKAR",
    "BHARAT","BEML","HAL","BEL","COCHINSHIP",
    # Cement
    "ULTRACEMCO","SHREECEM","GRASIM","ACC","AMBUJACEM",
    "DALMIACEM","JKCEMENT","RAMCOCEM","HEIDELBERG","BIRLACORP",
    # Telecom & Media
    "BHARTIARTL","INDUSTOWER","TATACOMM","HFCL",
    # Real Estate
    "DLF","GODREJPROP","PRESTIGE","OBEROIRLTY","PHOENIXLTD",
    "BRIGADE","SOBHA","MAHLIFE",
    # Chemicals
    "PIIND","DEEPAKFERT","COROMANDEL","TATACHEM","GNFC","ALKYLAMINE",
    "AARTIIND","VINATIORGA","NAVINFLUOR","CLEAN","FINEORG","SUDARSCHEM",
    # Logistics / Infra
    "ADANIPORTS","IRCTC","IRFC","CONCOR","TIINDIA","MAHLOG",
    # Textiles
    "RAYMOND","KPRMILL","TRIDENT","WELSPUNIND",
    # New-age / Tech
    "ZOMATO","NAUKRI","INDIAMART","PAYTM","POLICYBZR",
    # Diversified / PSU / Conglomerates
    "M&M","ADANIENT","TATACONSUM","MFSL","BAJAJHLDNG",
]
# Deduplicate preserving order
seen = set()
SYMBOLS = []
for s in NIFTY500_SYMBOLS:
    if s not in seen:
        seen.add(s)
        SYMBOLS.append(s)

BENCHMARK = "^NSEI"

# ─────────────────────────────────────────────
#  PATTERN DETECTION FUNCTIONS
# ─────────────────────────────────────────────

def is_nr(highs: np.ndarray, lows: np.ndarray, n: int) -> bool:
    """Narrow Range N: today's H-L range is smallest of last N sessions."""
    if len(highs) < n:
        return False
    ranges = highs[-n:] - lows[-n:]
    today_range = ranges[-1]
    if today_range <= 0:
        return False
    return bool(today_range < ranges[:-1].min())


def pocket_pivot(closes: np.ndarray, volumes: np.ndarray) -> bool:
    """
    Pocket Pivot (O'Neil / Kacher-Morales):
    - Today is an up-day (close > prev close)
    - Today's volume exceeds the highest down-day volume of the prior 10 sessions
    """
    if len(closes) < 12 or len(volumes) < 12:
        return False
    if closes[-1] <= closes[-2]:
        return False
    down_vols = [
        volumes[-1 - i]
        for i in range(1, 11)
        if closes[-1 - i] < closes[-2 - i]
    ]
    if not down_vols:
        return False
    return float(volumes[-1]) > max(down_vols)


def rs_leads_price(
    closes: np.ndarray,
    bench_closes: np.ndarray,
    lookback: int = 63,
    rs_threshold: float = 0.97,
    price_below_high: float = 0.96,
) -> bool:
    """
    RS Leads Price High:
    - RS line (stock / benchmark) is within `rs_threshold` of its `lookback`-day high
    - Price is still `price_below_high` or lower relative to its `lookback`-day high
    """
    n = min(len(closes), len(bench_closes))
    if n < lookback + 2:
        return False
    c = closes[-n:]
    b = bench_closes[-n:]
    rs = c / np.where(b > 0, b, np.nan)
    rs_window    = rs[-lookback:]
    price_window = c[-lookback:]
    if np.any(np.isnan(rs_window)):
        return False
    rs_max    = rs_window.max()
    price_max = price_window.max()
    rs_now    = rs_window[-1]
    price_now = price_window[-1]
    rs_near_high     = rs_now >= rs_max * rs_threshold
    price_not_at_top = price_now < price_max * price_below_high
    return bool(rs_near_high and price_not_at_top)


def vcp(
    closes: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    volumes: np.ndarray,
    window: int = 20,
    num_windows: int = 3,
    uptrend_lookback: int = 120,
    min_uptrend_pct: float = 0.08,
) -> bool:
    """
    Volatility Contraction Pattern (Minervini):
    - Stock is in an uptrend (close > 120-day low by min_uptrend_pct)
    - Three successive 20-day windows show BOTH:
        * Contracting price range (high - low) as % of low
        * Declining average volume
    """
    total_needed = window * num_windows + uptrend_lookback - window
    if len(closes) < total_needed:
        return False
    lo = lows[-uptrend_lookback:].min()
    if lo <= 0 or closes[-1] < lo * (1 + min_uptrend_pct):
        return False
    ranges_pct = []
    avg_vols   = []
    n = len(closes)
    for w in range(num_windows):
        s = n - window * (num_windows - w)
        e = s + window
        if s < 0 or e > n:
            return False
        h = highs[s:e].max()
        l = lows[s:e].min()
        rng_pct = (h - l) / l * 100 if l > 0 else 0
        avg_v   = volumes[s:e].mean()
        ranges_pct.append(rng_pct)
        avg_vols.append(avg_v)
    range_contracts = all(ranges_pct[i] > ranges_pct[i + 1] for i in range(num_windows - 1))
    vol_contracts   = all(avg_vols[i]   > avg_vols[i + 1]   for i in range(num_windows - 1))
    return bool(range_contracts and vol_contracts)


def atr_pct(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """ATR as % of last close."""
    if len(closes) < period + 1:
        return 0.0
    tr_list = []
    for i in range(-period, 0):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        tr_list.append(tr)
    atr_val = np.mean(tr_list)
    return (atr_val / closes[-1] * 100) if closes[-1] > 0 else 0.0


def volume_ratio(volumes: np.ndarray, avg_period: int = 20) -> float:
    """Today's volume / N-day average volume."""
    if len(volumes) < avg_period + 1:
        return 1.0
    avg = volumes[-(avg_period + 1):-1].mean()
    return float(volumes[-1] / avg) if avg > 0 else 1.0


def pct_from_52w_high(closes: np.ndarray, period: int = 252) -> float:
    """How far (%) current price is from 52-week high."""
    window = closes[-min(period, len(closes)):]
    if len(window) == 0:
        return 0.0
    high = window.max()
    return (closes[-1] - high) / high * 100 if high > 0 else 0.0


# ─────────────────────────────────────────────
#  DATA FETCHING
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ohlcv(symbol: str, period: str = "6mo") -> pd.DataFrame | None:
    """Fetch OHLCV for a single NSE symbol via yfinance."""
    ticker = symbol.replace("&", "%26") + ".NS"
    try:
        df = yf.download(
            ticker,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            actions=False,
        )
        if df is None or df.empty or len(df) < 25:
            return None
        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        return df
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark(period: str = "6mo") -> pd.DataFrame | None:
    """Fetch Nifty 50 as benchmark."""
    try:
        df = yf.download(
            BENCHMARK,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            actions=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df[["Close"]].dropna()
    except Exception:
        return None


def align_with_benchmark(stock_df: pd.DataFrame, bench_df: pd.DataFrame) -> np.ndarray:
    """Align stock closes with benchmark closes on common dates."""
    merged = stock_df[["Close"]].join(bench_df[["Close"]], how="inner", lsuffix="_s", rsuffix="_b")
    return merged["Close_b"].values


# ─────────────────────────────────────────────
#  SINGLE STOCK ANALYSIS
# ─────────────────────────────────────────────

def analyse_stock(symbol: str, period: str, bench_df: pd.DataFrame | None) -> dict | None:
    """Run all pattern checks on one stock. Returns result dict or None on failure."""
    df = fetch_ohlcv(symbol, period)
    if df is None:
        return {"symbol": symbol, "error": True}

    highs   = df["High"].values.astype(float)
    lows    = df["Low"].values.astype(float)
    closes  = df["Close"].values.astype(float)
    volumes = df["Volume"].values.astype(float)

    # Benchmark alignment for RS
    bench_closes = None
    if bench_df is not None:
        try:
            bench_closes = align_with_benchmark(df, bench_df)
        except Exception:
            bench_closes = None

    # ── Patterns ──
    nr4_hit  = is_nr(highs, lows, 4)
    nr7_hit  = is_nr(highs, lows, 7)
    nr21_hit = is_nr(highs, lows, 21)
    pp_hit   = pocket_pivot(closes, volumes)
    rs_hit   = rs_leads_price(closes, bench_closes) if bench_closes is not None else False
    vcp_hit  = vcp(closes, highs, lows, volumes)

    score = sum([nr4_hit, nr7_hit, nr21_hit, pp_hit, rs_hit, vcp_hit])

    # ── Price stats ──
    close_now = closes[-1]
    close_prev = closes[-2] if len(closes) > 1 else close_now
    chg_pct = (close_now - close_prev) / close_prev * 100 if close_prev > 0 else 0.0
    vr      = volume_ratio(volumes)
    atr_p   = atr_pct(highs, lows, closes)
    from52  = pct_from_52w_high(closes)

    # ── Today's range pct ──
    today_range_pct = (highs[-1] - lows[-1]) / lows[-1] * 100 if lows[-1] > 0 else 0.0

    return {
        "symbol":         symbol,
        "close":          round(float(close_now), 2),
        "chg_pct":        round(float(chg_pct), 2),
        "vol_ratio":      round(float(vr), 2),
        "atr_pct":        round(float(atr_p), 2),
        "from_52w_high":  round(float(from52), 2),
        "range_pct_today":round(float(today_range_pct), 2),
        "nr4":            nr4_hit,
        "nr7":            nr7_hit,
        "nr21":           nr21_hit,
        "pocket_pivot":   pp_hit,
        "rs_leads":       rs_hit,
        "vcp":            vcp_hit,
        "score":          score,
        "error":          False,
    }


# ─────────────────────────────────────────────
#  PARALLEL SCREENING
# ─────────────────────────────────────────────

def run_screener(
    symbols: list[str],
    period: str,
    max_workers: int,
    progress_bar,
    status_text,
) -> tuple[list[dict], dict]:
    """
    Screens all symbols in parallel. Returns (results, counters).
    """
    bench_df = fetch_benchmark(period)
    results  = []
    counters = {
        "total": len(symbols), "done": 0, "passed": 0, "errors": 0,
        "nr4": 0, "nr7": 0, "nr21": 0, "pp": 0, "rs": 0, "vcp": 0,
    }
    done_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(analyse_stock, sym, period, bench_df): sym for sym in symbols}
        for future in as_completed(future_map):
            sym = future_map[future]
            done_count += 1
            try:
                r = future.result(timeout=20)
            except Exception:
                r = {"symbol": sym, "error": True}

            if r and not r.get("error"):
                results.append(r)
                counters["passed"] += 1
                if r["nr4"]:          counters["nr4"]  += 1
                if r["nr7"]:          counters["nr7"]  += 1
                if r["nr21"]:         counters["nr21"] += 1
                if r["pocket_pivot"]: counters["pp"]   += 1
                if r["rs_leads"]:     counters["rs"]   += 1
                if r["vcp"]:          counters["vcp"]  += 1
            else:
                counters["errors"] += 1

            counters["done"] = done_count
            pct = done_count / len(symbols)
            progress_bar.progress(pct)
            status_text.markdown(
                f"<span style='color:#e8a020;font-size:11px;font-family:monospace'>"
                f"● SCANNING {done_count}/{len(symbols)}  —  "
                f"SIGNALS: {sum(1 for r in results if r['score']>0)}  —  "
                f"ERRORS: {counters['errors']}</span>",
                unsafe_allow_html=True,
            )

    return results, counters


# ─────────────────────────────────────────────
#  DISPLAY HELPERS
# ─────────────────────────────────────────────

def badge(hit: bool, label: str) -> str:
    if hit:
        return f'<span style="background:rgba(34,197,94,0.15);color:#4ade80;border:1px solid #22c55e55;padding:1px 6px;font-size:10px;font-weight:700;font-family:monospace">{label}</span>'
    return f'<span style="color:#2a2a2a;font-size:10px;font-family:monospace">·</span>'


def score_pips(score: int) -> str:
    color_map = {6:"#ffd700",5:"#f97316",4:"#22c55e",3:"#22d3ee",2:"#e0e0e0",1:"#666",0:"#222"}
    col = color_map.get(score, "#222")
    pips = ""
    for i in range(6):
        bg = col if i < score else "#1a1a1a"
        bd = col if i < score else "#2a2a2a"
        pips += f'<span style="display:inline-block;width:7px;height:13px;background:{bg};border:1px solid {bd};margin-right:2px"></span>'
    return f'<span style="color:{col};font-weight:700;font-size:15px;font-family:monospace">{score}</span>&nbsp;&nbsp;{pips}'


def render_results_table(df: pd.DataFrame) -> None:
    """Render styled HTML results table."""
    if df.empty:
        st.markdown(
            '<div style="text-align:center;padding:60px;color:#333;font-family:monospace;font-size:13px">'
            'NO STOCKS MATCH CURRENT FILTERS</div>',
            unsafe_allow_html=True,
        )
        return

    html = """
    <style>
    .screener-table { width:100%; border-collapse:collapse; font-family:'IBM Plex Mono',monospace; font-size:11px; }
    .screener-table th {
        padding:8px 10px; background:#111; color:#e8a020; font-size:10px;
        letter-spacing:1px; text-transform:uppercase; border-bottom:2px solid #c8841a;
        white-space:nowrap; text-align:left;
    }
    .screener-table td { padding:7px 10px; border-bottom:1px solid #141414; white-space:nowrap; }
    .screener-table tr:hover td { background:rgba(232,160,32,0.04); }
    .screener-table tr.top5 td { background:rgba(232,160,32,0.05); }
    </style>
    <table class="screener-table">
    <thead><tr>
        <th>#</th>
        <th>SYMBOL</th>
        <th>CLOSE ₹</th>
        <th>CHG %</th>
        <th>VOL ×</th>
        <th>ATR %</th>
        <th>52W HIGH</th>
        <th style="text-align:center">NR4</th>
        <th style="text-align:center">NR7</th>
        <th style="text-align:center">NR21</th>
        <th style="text-align:center">P.PIVOT</th>
        <th style="text-align:center">RS↑</th>
        <th style="text-align:center">VCP</th>
        <th>SCORE</th>
    </tr></thead>
    <tbody>
    """

    RANK_COLOR = {1:"#ffd700",2:"#c0c0c0",3:"#cd7f32"}

    for i, row in enumerate(df.itertuples(), 1):
        rank_color = RANK_COLOR.get(i, "#555")
        top_cls    = "top5" if i <= 5 else ""
        chg_color  = "#22c55e" if row.chg_pct >= 0 else "#ef4444"
        chg_sign   = "+" if row.chg_pct >= 0 else ""
        rank_sym   = {1:"①",2:"②",3:"③"}.get(i, i)
        from52_str = f"{row.from_52w_high:+.1f}%"
        from52_col = "#22c55e" if row.from_52w_high >= -5 else ("#aaa" if row.from_52w_high >= -15 else "#555")

        html += f"""
        <tr class="{top_cls}">
            <td style="font-weight:700;color:{rank_color};font-size:13px;text-align:center">{rank_sym}</td>
            <td style="color:#22d3ee;font-weight:600;font-size:12px;letter-spacing:.5px">{row.symbol}</td>
            <td style="color:#e0e0e0;font-variant-numeric:tabular-nums">₹{row.close:,.2f}</td>
            <td style="color:{chg_color}">{chg_sign}{row.chg_pct:.2f}%</td>
            <td style="color:#aaa">{row.vol_ratio:.2f}×</td>
            <td style="color:#888">{row.atr_pct:.2f}%</td>
            <td style="color:{from52_col}">{from52_str}</td>
            <td style="text-align:center">{badge(row.nr4,       'NR4'   )}</td>
            <td style="text-align:center">{badge(row.nr7,       'NR7'   )}</td>
            <td style="text-align:center">{badge(row.nr21,      'NR21'  )}</td>
            <td style="text-align:center">{badge(row.pocket_pivot,'PP' )}</td>
            <td style="text-align:center">{badge(row.rs_leads,  'RS↑'  )}</td>
            <td style="text-align:center">{badge(row.vcp,       'VCP'  )}</td>
            <td>{score_pips(row.score)}</td>
        </tr>"""

    html += "</tbody></table>"
    st.markdown(html, unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  SESSION STATE INIT
# ─────────────────────────────────────────────

if "results_df"  not in st.session_state: st.session_state["results_df"]  = None
if "counters"    not in st.session_state: st.session_state["counters"]    = {}
if "scan_time"   not in st.session_state: st.session_state["scan_time"]   = None
if "scan_done"   not in st.session_state: st.session_state["scan_done"]   = False


# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown(
        '<div style="border-left:3px solid #e8a020;padding:6px 10px;margin-bottom:16px">'
        '<span style="color:#e8a020;font-size:13px;font-weight:700;letter-spacing:2px">⬡ SCREENER CONTROLS</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    data_period = st.selectbox(
        "DATA PERIOD",
        ["3mo", "6mo", "1y"],
        index=1,
        help="OHLCV lookback for pattern calculations"
    )
    max_workers = st.selectbox(
        "PARALLEL WORKERS",
        [3, 5, 8, 10, 15],
        index=1,
        help="Higher = faster scan, but may trigger rate-limits"
    )
    min_score = st.selectbox(
        "MIN SIGNAL SCORE",
        [0, 1, 2, 3, 4, 5, 6],
        index=1,
        help="Only show stocks with at least this many patterns triggered"
    )

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)

    st.markdown('<span style="color:#888;font-size:10px;letter-spacing:1px">PATTERN FILTERS</span>', unsafe_allow_html=True)
    f_nr4  = st.checkbox("NR4  — Narrow Range 4",       value=False)
    f_nr7  = st.checkbox("NR7  — Narrow Range 7",       value=False)
    f_nr21 = st.checkbox("NR21 — Narrow Range 21",      value=False)
    f_pp   = st.checkbox("Pocket Pivot",                 value=False)
    f_rs   = st.checkbox("RS Leads Price High",          value=False)
    f_vcp  = st.checkbox("VCP — Volatility Contraction", value=False)

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)

    sort_col = st.selectbox(
        "SORT BY",
        ["score","chg_pct","vol_ratio","atr_pct","close","from_52w_high"],
        index=0,
    )
    sort_asc = st.checkbox("Sort Ascending", value=False)

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)

    n_stocks = st.selectbox(
        "UNIVERSE SIZE",
        options=["Top 50", "Top 100", "Top 150", "All (~180)"],
        index=3,
    )
    n_map = {"Top 50": 50, "Top 100": 100, "Top 150": 150, "All (~180)": len(SYMBOLS)}
    scan_universe = SYMBOLS[: n_map[n_stocks]]

    run_btn = st.button("▶  RUN SCREENER", use_container_width=True)

    if st.session_state.scan_time:
        st.markdown(
            f'<div style="color:#444;font-size:10px;text-align:center;margin-top:8px">'
            f'Last scan: {st.session_state.scan_time}</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────

col_logo, col_ist = st.columns([3, 1])
with col_logo:
    st.markdown(
        '<div style="display:flex;align-items:center;gap:12px;padding:4px 0 12px">'
        '<div style="background:#e8a020;color:#000;font-weight:700;font-size:11px;padding:4px 8px;letter-spacing:1px">NSE</div>'
        '<div>'
        '<span style="color:#e8a020;font-size:20px;font-weight:700;letter-spacing:3px;font-family:monospace">NIFTY 500 PATTERN SCREENER</span>'
        '<br><span style="color:#333;font-size:10px;letter-spacing:2px;font-family:monospace">NR4 · NR7 · NR21 · POCKET PIVOT · RS LEAD · VCP</span>'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )
with col_ist:
    now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    mkt_hour  = now_ist.hour * 60 + now_ist.minute
    mkt_open  = (now_ist.weekday() < 5) and (555 <= mkt_hour < 930)
    mkt_label = "● MARKET OPEN" if mkt_open else "○ MARKET CLOSED"
    mkt_color = "#22c55e" if mkt_open else "#ef4444"
    st.markdown(
        f'<div style="text-align:right;padding-top:8px">'
        f'<div style="color:#22d3ee;font-family:monospace;font-size:12px">IST {now_ist.strftime("%H:%M:%S")}</div>'
        f'<div style="color:{mkt_color};font-family:monospace;font-size:10px;letter-spacing:1px">{mkt_label}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

st.markdown('<hr style="border-color:#1e1e1e;margin:0">', unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  SCAN EXECUTION
# ─────────────────────────────────────────────

if run_btn:
    # Clear cache so fresh data is fetched
    fetch_ohlcv.clear()
    fetch_benchmark.clear()

    progress_bar = st.progress(0.0)
    status_text  = st.empty()

    with st.spinner(""):
        raw_results, counters = run_screener(
            symbols    = scan_universe,
            period     = data_period,
            max_workers= max_workers,
            progress_bar= progress_bar,
            status_text = status_text,
        )

    progress_bar.progress(1.0)
    status_text.markdown(
        f'<span style="color:#22c55e;font-size:11px;font-family:monospace">'
        f'✓ SCAN COMPLETE — {counters["passed"]} stocks analysed, '
        f'{sum(1 for r in raw_results if r["score"]>0)} signals detected, '
        f'{counters["errors"]} errors</span>',
        unsafe_allow_html=True,
    )

    if raw_results:
        df = pd.DataFrame(raw_results)
        df = df[df["error"] == False].drop(columns=["error"])
        st.session_state["results_df"]  = df
        st.session_state["counters"]    = counters
        st.session_state["scan_time"]   = now_ist.strftime("%d-%b %H:%M IST")
        st.session_state["scan_done"]   = True


# ─────────────────────────────────────────────
#  RESULTS DISPLAY
# ─────────────────────────────────────────────

if st.session_state["scan_done"] and st.session_state["results_df"] is not None:
    df_all = st.session_state["results_df"].copy()
    cntr   = st.session_state["counters"]

    # ── Metric cards ──
    m1, m2, m3, m4, m5, m6, m7, m8 = st.columns(8)
    m1.metric("SCANNED",       cntr.get("passed", 0))
    m2.metric("ERRORS",        cntr.get("errors", 0))
    m3.metric("NR4",           cntr.get("nr4",    0))
    m4.metric("NR7",           cntr.get("nr7",    0))
    m5.metric("NR21",          cntr.get("nr21",   0))
    m6.metric("POCKET PIVOT",  cntr.get("pp",     0))
    m7.metric("RS LEAD",       cntr.get("rs",     0))
    m8.metric("VCP",           cntr.get("vcp",    0))

    st.markdown('<hr style="border-color:#1e1e1e;margin:4px 0">', unsafe_allow_html=True)

    # ── Tabs ──
    tab_main, tab_heatmap, tab_export = st.tabs(["📋  RESULTS TABLE", "🔥  PATTERN HEATMAP", "⬇  EXPORT"])

    with tab_main:
        # Apply sidebar filters
        df = df_all.copy()
        if f_nr4:  df = df[df["nr4"]           == True]
        if f_nr7:  df = df[df["nr7"]           == True]
        if f_nr21: df = df[df["nr21"]          == True]
        if f_pp:   df = df[df["pocket_pivot"]  == True]
        if f_rs:   df = df[df["rs_leads"]      == True]
        if f_vcp:  df = df[df["vcp"]           == True]
        df = df[df["score"] >= min_score]
        df = df.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)

        st.markdown(
            f'<div style="display:flex;justify-content:space-between;padding:6px 0;font-size:10px;font-family:monospace">'
            f'<span style="color:#e8a020;letter-spacing:1px">SCAN RESULTS</span>'
            f'<span style="color:#555">{len(df)} STOCKS SHOWN</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        render_results_table(df)

    with tab_heatmap:
        st.markdown(
            '<div style="color:#888;font-size:11px;font-family:monospace;margin-bottom:12px;letter-spacing:1px">'
            'PATTERN CO-OCCURRENCE MATRIX — how often each pattern fires with others</div>',
            unsafe_allow_html=True,
        )
        patterns = ["nr4","nr7","nr21","pocket_pivot","rs_leads","vcp"]
        labels   = ["NR4","NR7","NR21","P.PIVOT","RS↑","VCP"]

        df_hits = df_all[df_all["score"] > 0]
        if not df_hits.empty:
            heatmap_data = {}
            for p, l in zip(patterns, labels):
                heatmap_data[l] = df_hits[p].astype(int).values

            hm_df = pd.DataFrame(heatmap_data, index=df_hits["symbol"].values)

            # Co-occurrence
            co = pd.DataFrame(index=labels, columns=labels, dtype=float)
            for l1 in labels:
                for l2 in labels:
                    p1 = patterns[labels.index(l1)]
                    p2 = patterns[labels.index(l2)]
                    both = ((df_all[p1]) & (df_all[p2])).sum()
                    total = df_all[p1].sum()
                    co.loc[l1, l2] = round(both / total * 100, 1) if total > 0 else 0.0

            co = co.astype(float)

            # Render heatmap as HTML
            hm_html = '<table style="border-collapse:collapse;font-family:monospace;font-size:11px">'
            hm_html += '<tr><th style="padding:8px 12px;color:#555;border-bottom:1px solid #1e1e1e"></th>'
            for l in labels:
                hm_html += f'<th style="padding:8px 12px;color:#e8a020;border-bottom:1px solid #1e1e1e;text-align:center;font-size:10px;letter-spacing:1px">{l}</th>'
            hm_html += '</tr>'

            for l1 in labels:
                hm_html += f'<tr><td style="padding:7px 12px;color:#e8a020;font-size:10px;letter-spacing:1px;border-right:1px solid #1e1e1e">{l1}</td>'
                for l2 in labels:
                    val = co.loc[l1, l2]
                    intensity = int(min(val / 100 * 255, 255))
                    if l1 == l2:
                        bg = "#1a1a0a"
                        fc = "#e8a020"
                    else:
                        alpha = val / 100 * 0.6
                        bg = f"rgba(34,197,94,{alpha:.2f})"
                        fc = "#22c55e" if val > 20 else ("#666" if val > 5 else "#333")
                    hm_html += (
                        f'<td style="padding:7px 12px;background:{bg};color:{fc};'
                        f'text-align:center;border:1px solid #111">'
                        f'{"—" if l1 == l2 else f"{val:.0f}%"}</td>'
                    )
                hm_html += '</tr>'
            hm_html += '</table>'
            st.markdown(hm_html, unsafe_allow_html=True)

            st.markdown('<br>', unsafe_allow_html=True)

            # Top stocks per pattern
            st.markdown(
                '<span style="color:#888;font-size:10px;font-family:monospace;letter-spacing:1px">'
                'TOP 5 STOCKS PER PATTERN</span>',
                unsafe_allow_html=True,
            )
            cols = st.columns(6)
            for idx, (p, l) in enumerate(zip(patterns, labels)):
                with cols[idx]:
                    sub = df_all[df_all[p] == True].nlargest(5, "score")
                    content = f'<div style="color:#e8a020;font-size:10px;font-weight:700;letter-spacing:1px;margin-bottom:6px;font-family:monospace">{l}</div>'
                    for _, row in sub.iterrows():
                        content += (
                            f'<div style="font-family:monospace;font-size:11px;color:#22d3ee;'
                            f'padding:3px 0;border-bottom:1px solid #111">'
                            f'{row["symbol"]} '
                            f'<span style="color:#666;font-size:10px">({row["score"]}✓)</span></div>'
                        )
                    if sub.empty:
                        content += '<div style="color:#333;font-size:10px;font-family:monospace">No hits</div>'
                    st.markdown(
                        f'<div style="background:#111;padding:10px;border:1px solid #1e1e1e">{content}</div>',
                        unsafe_allow_html=True,
                    )
        else:
            st.markdown(
                '<div style="color:#333;font-family:monospace;padding:40px;text-align:center">NO PATTERN HITS YET</div>',
                unsafe_allow_html=True,
            )

    with tab_export:
        df_export = df_all.sort_values("score", ascending=False).copy()
        df_export.columns = [c.upper() for c in df_export.columns]

        st.markdown(
            f'<div style="color:#888;font-size:11px;font-family:monospace;margin-bottom:12px">'
            f'{len(df_export)} ROWS READY FOR EXPORT</div>',
            unsafe_allow_html=True,
        )
        csv_bytes = df_export.to_csv(index=False).encode()
        st.download_button(
            label="⬇  DOWNLOAD CSV",
            data=csv_bytes,
            file_name=f"nifty500_scan_{now_ist.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(
            '<span style="color:#555;font-size:10px;font-family:monospace;letter-spacing:1px">DATA PREVIEW</span>',
            unsafe_allow_html=True,
        )
        st.dataframe(
            df_export.head(30),
            use_container_width=True,
            hide_index=True,
        )

else:
    # ── Empty state ──
    st.markdown(
        """
        <div style="text-align:center;padding:80px 20px">
            <div style="font-size:50px;margin-bottom:20px;opacity:0.2">⬡</div>
            <div style="color:#333;font-size:14px;letter-spacing:3px;font-family:monospace;margin-bottom:10px">
                AWAITING SCAN
            </div>
            <div style="color:#2a2a2a;font-size:11px;font-family:monospace">
                Configure settings in the sidebar, then press  ▶ RUN SCREENER
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
#  PATTERN LEGEND  (always visible)
# ─────────────────────────────────────────────

st.markdown('<hr style="border-color:#1e1e1e;margin:20px 0 10px">', unsafe_allow_html=True)
st.markdown(
    '<span style="color:#444;font-size:10px;font-family:monospace;letter-spacing:2px">PATTERN REFERENCE</span>',
    unsafe_allow_html=True,
)

legend_items = [
    ("NR4",         "Narrow Range 4",         "Today's H-L is the narrowest of the last 4 sessions. First sign of volatility compression."),
    ("NR7",         "Narrow Range 7",          "Today's H-L is the narrowest of the last 7 sessions. Classic Toby Crabel coil setup."),
    ("NR21",        "Narrow Range 21",         "Today's H-L is the narrowest of the last 21 sessions (~1 month). Extreme compression — major move imminent."),
    ("P.PIVOT",     "Pocket Pivot",            "Up-day volume exceeds the highest down-day volume of prior 10 sessions. Minervini / O'Neil accumulation signal."),
    ("RS↑",         "RS Leads Price",          "RS line (stock ÷ Nifty50) within 3% of its 63-day high, while price is still 4%+ below its own high. Institutional loading signal."),
    ("VCP",         "Volatility Contraction",  "Three successive 20-day windows with contracting range% AND declining volume — on top of an 8%+ uptrend. Minervini Stage 2 base."),
]

cols = st.columns(6)
for i, (tag, name, desc) in enumerate(legend_items):
    with cols[i]:
        st.markdown(
            f'<div style="background:#0e0e0e;border:1px solid #1a1a1a;border-top:2px solid #e8a020;'
            f'padding:10px;height:100%">'
            f'<div style="color:#e8a020;font-size:10px;font-weight:700;letter-spacing:1px;'
            f'font-family:monospace;margin-bottom:5px">{tag} — {name}</div>'
            f'<div style="color:#444;font-size:10px;line-height:1.6;font-family:monospace">{desc}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
