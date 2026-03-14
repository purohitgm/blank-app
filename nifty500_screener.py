"""
╔══════════════════════════════════════════════════════════════╗
║         NIFTY 500 PATTERN SCREENER  — Streamlit App         ║
║  Patterns: NR4 · NR7 · NR21 · Pocket Pivot · RS Lead · VCP ║
║  Dependencies: streamlit · pandas · numpy · requests        ║
║  (all pre-installed on Streamlit Cloud — no requirements.txt)║
╚══════════════════════════════════════════════════════════════╝
Run: streamlit run nifty500_screener.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="NIFTY 500 PATTERN SCREENER",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
#  CSS — Bloomberg terminal aesthetic
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap');
html, body, [data-testid="stApp"] {
    background-color: #090909 !important;
    color: #e0e0e0 !important;
    font-family: 'IBM Plex Mono', 'Courier New', monospace !important;
}
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }
[data-testid="stToolbar"]    { display: none; }

[data-testid="stSidebar"] {
    background-color: #0f0f0f !important;
    border-right: 1px solid #1e1e1e !important;
}
[data-testid="stSidebar"] * { color: #c0c0c0 !important; }
[data-testid="stSidebar"] label {
    color: #e8a020 !important;
    font-size: 11px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}
[data-testid="stSidebar"] [data-baseweb="select"] > div {
    background-color: #1a1a1a !important;
    border: 1px solid #2a2a2a !important;
    color: #e0e0e0 !important;
    font-size: 12px !important;
}
[data-testid="stMetric"] {
    background: #111 !important;
    border: 1px solid #1e1e1e !important;
    padding: 10px 14px !important;
    border-left: 3px solid #e8a020 !important;
}
[data-testid="stMetricLabel"] {
    color: #888 !important; font-size: 10px !important;
    letter-spacing:1px; text-transform:uppercase;
}
[data-testid="stMetricValue"] {
    color: #e8a020 !important; font-size: 22px !important; font-weight: 700 !important;
}
.stButton > button {
    background-color: #e8a020 !important;
    color: #000 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 700 !important; font-size: 12px !important;
    letter-spacing: 1px !important; border: none !important;
    border-radius: 0 !important; padding: 8px 20px !important;
}
.stButton > button:hover  { background-color: #ffb830 !important; }
.stButton > button:disabled { background-color: #444 !important; color: #888 !important; }
[data-testid="stProgressBar"] > div > div { background-color: #e8a020 !important; }
[data-testid="stTabs"] [role="tab"] {
    color: #555 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 11px !important; letter-spacing: 1px !important;
    text-transform: uppercase !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #e8a020 !important;
    border-bottom: 2px solid #e8a020 !important;
}
hr { border-color: #1e1e1e !important; }
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #111; }
::-webkit-scrollbar-thumb { background: #2a2a2a; }
::-webkit-scrollbar-thumb:hover { background: #e8a020; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  UNIVERSE  (display name → Yahoo ticker)
# ─────────────────────────────────────────────
_RAW = [
    # IT
    "TCS","INFY","WIPRO","HCLTECH","TECHM","MPHASIS","LTIM","OFSS",
    "PERSISTENT","COFORGE","ZENSARTECH","KPITTECH","TATAELXSI","LTTS",
    # Banking
    "HDFCBANK","ICICIBANK","KOTAKBANK","AXISBANK","SBIN","INDUSINDBK",
    "BANDHANBNK","FEDERALBNK","IDFCFIRSTB","RBLBANK","CANBK","PNB",
    "BANKBARODA","UNIONBANK","INDIANB","AUBANK","DCBBANK",
    # NBFC / Fin
    "BAJFINANCE","BAJAJFINSV","CHOLAFIN","SBICARD","HDFCLIFE","SBILIFE",
    "ICICIGI","MANAPPURAM","MUTHOOTFIN","AAVAS","CANFINHOME","LICHSGFIN",
    "ABCAPITAL","SHRIRAMFIN",
    # Energy
    "RELIANCE","ONGC","BPCL","IOC","GAIL","PETRONET","IGL","MGL",
    "HINDPETRO","MRPL","CASTROLIND",
    # Power
    "POWERGRID","NTPC","TATAPOWER","TORNTPOWER","CESC","ADANIGREEN",
    "ADANIPOWER","NHPC","SJVN","RECLTD","PFC",
    # FMCG
    "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
    "GODREJCP","COLPAL","TATACONSUM","JYOTHYLAB","EMAMILTD","VBL",
    "RADICO",
    # Consumer Discretionary
    "ASIANPAINT","BERGEPAINT","PIDILITIND","HAVELLS","CROMPTON","VOLTAS",
    "TITAN","TRENT","BATAINDIA","PAGEIND","KAJARIACER","SYMPHONY",
    # Auto
    "MARUTI","TATAMOTORS","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT",
    "TVSMOTORS","MOTHERSON","BALKRISIND","APOLLOTYRE","BHARATFORG",
    "ENDURANCE","BOSCHLTD","EXIDEIND","AMARAJABAT",
    # Pharma
    "SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","AUROPHARMA","TORNTPHARM",
    "LUPIN","ALKEM","ZYDUSLIFE","BIOCON","LALPATHLAB","APOLLOHOSP",
    "FORTIS","MAXHEALTH","NATCOPHARM","GRANULES","LAURUSLABS","SYNGENE",
    "GLAND","IPCA","AJANTPHARM",
    # Metals
    "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","HINDZINC","NMDC",
    "COALINDIA","NATIONALUM","MOIL","RATNAMANI",
    # Capital Goods
    "LT","BHEL","ABB","SIEMENS","THERMAX","CUMMINSIND","AIAENG",
    "GRINDWELL","ELGIEQUIP","HAL","BEL","BEML",
    # Cement
    "ULTRACEMCO","SHREECEM","GRASIM","ACC","AMBUJACEM",
    "DALMIACEM","JKCEMENT","RAMCOCEM",
    # Telecom / Infra
    "BHARTIARTL","INDUSTOWER","ADANIPORTS","IRCTC","IRFC","CONCOR",
    # Real Estate
    "DLF","GODREJPROP","PRESTIGE","OBEROIRLTY","PHOENIXLTD","BRIGADE",
    # Chemicals
    "PIIND","DEEPAKFERT","COROMANDEL","TATACHEM","GNFC","ALKYLAMINE",
    "AARTIIND","NAVINFLUOR","FINEORG",
    # Textiles
    "RAYMOND","KPRMILL","TRIDENT",
    # New-age
    "ZOMATO","NAUKRI","INDIAMART",
    # Diversified
    "M&M","ADANIENT","BAJAJHLDNG",
]

# Build deduplicated list; handle special chars for Yahoo ticker
def _build_universe(raw):
    seen, result = set(), []
    for s in raw:
        if s not in seen:
            seen.add(s)
            # M&M → M%26M.NS for Yahoo URL
            yf_ticker = s.replace("&", "%26") + ".NS"
            result.append({"display": s, "ticker": yf_ticker})
    return result

UNIVERSE = _build_universe(_RAW)
BENCH_TICKER = "%5ENSEI"   # ^NSEI

# ─────────────────────────────────────────────
#  YAHOO FINANCE HTTP  (pure requests)
# ─────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

_YF_HOSTS = [
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
]


def _fetch_chart(ticker: str, period: str, timeout: int = 12) -> dict | None:
    """Try both Yahoo Finance query hosts."""
    path = f"/v8/finance/chart/{ticker}?interval=1d&range={period}&events=div"
    for host in _YF_HOSTS:
        try:
            r = requests.get(host + path, headers=_HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(0.1)
    return None


def _parse_chart(data: dict) -> pd.DataFrame | None:
    """Yahoo v8 chart JSON → OHLCV DataFrame indexed by IST date."""
    try:
        res = data["chart"]["result"][0]
        ts  = res["timestamp"]
        q   = res["indicators"]["quote"][0]
        df  = pd.DataFrame({
            "Open":   q.get("open",   [None]*len(ts)),
            "High":   q.get("high",   [None]*len(ts)),
            "Low":    q.get("low",    [None]*len(ts)),
            "Close":  q.get("close",  [None]*len(ts)),
            "Volume": q.get("volume", [None]*len(ts)),
        }, index=pd.to_datetime(ts, unit="s", utc=True)
                          .tz_convert("Asia/Kolkata")
                          .normalize())
        df.index.name = "Date"
        df = df.apply(pd.to_numeric, errors="coerce").dropna()
        return df if len(df) >= 25 else None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ohlcv(ticker: str, period: str) -> pd.DataFrame | None:
    data = _fetch_chart(ticker, period)
    return _parse_chart(data) if data else None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark(period: str) -> pd.DataFrame | None:
    data = _fetch_chart(BENCH_TICKER, period)
    if data is None:
        return None
    df = _parse_chart(data)
    if df is None:
        return None
    return df[["Close"]].rename(columns={"Close": "Bench"})


def _align_bench(stock_df: pd.DataFrame, bench_df: pd.DataFrame) -> np.ndarray:
    merged = stock_df[["Close"]].join(bench_df[["Bench"]], how="inner")
    return merged["Bench"].values

# ─────────────────────────────────────────────
#  PATTERN DETECTION
# ─────────────────────────────────────────────

def is_nr(H: np.ndarray, L: np.ndarray, n: int) -> bool:
    if len(H) < n:
        return False
    ranges = H[-n:] - L[-n:]
    today  = ranges[-1]
    return bool(today > 0 and today < ranges[:-1].min())


def pocket_pivot(C: np.ndarray, V: np.ndarray) -> bool:
    if len(C) < 12:
        return False
    if C[-1] <= C[-2]:
        return False
    down_vols = [
        float(V[-1 - i])
        for i in range(1, 11)
        if len(C) > i + 1 and C[-1 - i] < C[-2 - i]
    ]
    return bool(down_vols and float(V[-1]) > max(down_vols))


def rs_leads_price(C: np.ndarray, B: np.ndarray,
                   lb: int = 63, rs_thr: float = 0.97, p_thr: float = 0.96) -> bool:
    n = min(len(C), len(B))
    if n < lb + 2:
        return False
    c = C[-n:];  b = B[-n:]
    safe_b = np.where(b > 0, b, np.nan)
    rs = c / safe_b
    rs_w = rs[-lb:];  p_w = c[-lb:]
    if np.any(np.isnan(rs_w)):
        return False
    return bool(rs_w[-1] >= rs_w.max() * rs_thr and p_w[-1] < p_w.max() * p_thr)


def vcp(C: np.ndarray, H: np.ndarray, L: np.ndarray, V: np.ndarray,
        win: int = 20, nwin: int = 3, ut_lb: int = 120, min_up: float = 0.08) -> bool:
    if len(C) < win * nwin + ut_lb:
        return False
    lo = L[-ut_lb:].min()
    if lo <= 0 or C[-1] < lo * (1 + min_up):
        return False
    n = len(C)
    rngs, vols = [], []
    for w in range(nwin):
        s = n - win * (nwin - w);  e = s + win
        if s < 0 or e > n:
            return False
        h = H[s:e].max();  l = L[s:e].min()
        rngs.append((h - l) / l * 100 if l > 0 else 0.0)
        vols.append(float(V[s:e].mean()))
    rc = all(rngs[i] > rngs[i + 1] for i in range(nwin - 1))
    vc = all(vols[i] > vols[i + 1] for i in range(nwin - 1))
    return bool(rc and vc)


def calc_atr_pct(H, L, C, p=14) -> float:
    if len(C) < p + 1:
        return 0.0
    trs = [max(H[-p+i]-L[-p+i],
               abs(H[-p+i]-C[-p+i-1]),
               abs(L[-p+i]-C[-p+i-1])) for i in range(p)]
    return float(np.mean(trs) / C[-1] * 100) if C[-1] > 0 else 0.0


def calc_vol_ratio(V, avg_p=20) -> float:
    if len(V) < avg_p + 1:
        return 1.0
    avg = float(V[-(avg_p+1):-1].mean())
    return float(V[-1] / avg) if avg > 0 else 1.0


def pct_from_52w(C, p=252) -> float:
    w  = C[-min(p, len(C)):]
    hi = w.max()
    return float((C[-1] - hi) / hi * 100) if hi > 0 else 0.0

# ─────────────────────────────────────────────
#  SINGLE STOCK ANALYSIS
# ─────────────────────────────────────────────

_ERR = lambda sym: {"symbol":sym,"error":True,"score":0,
                    "nr4":False,"nr7":False,"nr21":False,
                    "pocket_pivot":False,"rs_leads":False,"vcp":False}

def analyse(entry: dict, period: str, bench_df) -> dict:
    sym    = entry["display"]
    ticker = entry["ticker"]
    try:
        df = fetch_ohlcv(ticker, period)
        if df is None:
            return _ERR(sym)

        H = df["High"].values.astype(float)
        L = df["Low"].values.astype(float)
        C = df["Close"].values.astype(float)
        V = df["Volume"].values.astype(float)

        bench_arr = None
        if bench_df is not None:
            try:
                bench_arr = _align_bench(df, bench_df)
            except Exception:
                bench_arr = None

        nr4_h  = is_nr(H, L, 4)
        nr7_h  = is_nr(H, L, 7)
        nr21_h = is_nr(H, L, 21)
        pp_h   = pocket_pivot(C, V)
        rs_h   = rs_leads_price(C, bench_arr) if (bench_arr is not None and len(bench_arr) > 65) else False
        vcp_h  = vcp(C, H, L, V)
        score  = sum([nr4_h, nr7_h, nr21_h, pp_h, rs_h, vcp_h])

        c_now  = float(C[-1])
        c_prev = float(C[-2]) if len(C) > 1 else c_now
        chg    = (c_now - c_prev) / c_prev * 100 if c_prev > 0 else 0.0

        return {
            "symbol":       sym,
            "close":        round(c_now, 2),
            "chg_pct":      round(chg, 2),
            "vol_ratio":    round(calc_vol_ratio(V), 2),
            "atr_pct":      round(calc_atr_pct(H, L, C), 2),
            "from_52w":     round(pct_from_52w(C), 2),
            "range_pct":    round((H[-1]-L[-1])/L[-1]*100 if L[-1]>0 else 0, 2),
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
        return _ERR(sym)

# ─────────────────────────────────────────────
#  SCREENER RUNNER
# ─────────────────────────────────────────────

def run_screener(universe, period, workers, progress_bar, status_txt):
    bench_df = fetch_benchmark(period)
    results  = []
    ctr      = dict(done=0, passed=0, errors=0,
                    nr4=0, nr7=0, nr21=0, pp=0, rs=0, vcp=0)
    total    = len(universe)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {ex.submit(analyse, entry, period, bench_df): entry for entry in universe}
        for fut in as_completed(fmap):
            ctr["done"] += 1
            try:
                r = fut.result(timeout=25)
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

            progress_bar.progress(min(ctr["done"] / total, 1.0))
            status_txt.markdown(
                f'<span style="color:#e8a020;font-size:11px;font-family:monospace">'
                f'● SCANNING {ctr["done"]}/{total}  ·  '
                f'SIGNALS: {sum(1 for x in results if x["score"]>0)}  ·  '
                f'ERRORS: {ctr["errors"]}</span>',
                unsafe_allow_html=True,
            )

    return results, ctr

# ─────────────────────────────────────────────
#  RENDER HELPERS
# ─────────────────────────────────────────────

def badge(hit: bool, label: str) -> str:
    if hit:
        return (
            f'<span style="background:rgba(34,197,94,.15);color:#4ade80;'
            f'border:1px solid #22c55e55;padding:2px 6px;'
            f'font-size:10px;font-weight:700;font-family:monospace">{label}</span>'
        )
    return '<span style="color:#252525;font-size:10px;font-family:monospace">·</span>'


def score_pips(score: int) -> str:
    clr = {6:"#ffd700",5:"#f97316",4:"#22c55e",3:"#22d3ee",
           2:"#e0e0e0",1:"#666",0:"#222"}.get(score, "#222")
    pips = "".join(
        f'<span style="display:inline-block;width:7px;height:13px;'
        f'background:{clr if i < score else "#1a1a1a"};'
        f'border:1px solid {clr if i < score else "#2a2a2a"};margin-right:2px"></span>'
        for i in range(6)
    )
    return (
        f'<span style="color:{clr};font-weight:700;font-size:15px;'
        f'font-family:monospace">{score}</span>&nbsp;&nbsp;{pips}'
    )


def render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.markdown(
            '<div style="text-align:center;padding:60px;color:#333;'
            'font-family:monospace;font-size:13px">'
            'NO STOCKS MATCH CURRENT FILTERS</div>',
            unsafe_allow_html=True,
        )
        return

    RANK_CLR = {1:"#ffd700", 2:"#c0c0c0", 3:"#cd7f32"}
    rows_html = ""
    for i, row in enumerate(df.itertuples(), 1):
        rc  = RANK_CLR.get(i, "#555")
        rsm = {1:"①", 2:"②", 3:"③"}.get(i, str(i))
        cc  = "#22c55e" if row.chg_pct >= 0 else "#ef4444"
        cs  = "+" if row.chg_pct >= 0 else ""
        fc  = "#22c55e" if row.from_52w >= -5 else ("#aaa" if row.from_52w >= -15 else "#555")
        tc  = "top5" if i <= 5 else ""
        rows_html += f"""
        <tr class="{tc}">
          <td style="font-weight:700;color:{rc};font-size:13px;text-align:center;width:36px">{rsm}</td>
          <td style="color:#22d3ee;font-weight:600;font-size:12px;letter-spacing:.5px">{row.symbol}</td>
          <td style="color:#e0e0e0;font-variant-numeric:tabular-nums">&#8377;{row.close:,.2f}</td>
          <td style="color:{cc}">{cs}{row.chg_pct:.2f}%</td>
          <td style="color:#aaa">{row.vol_ratio:.2f}&times;</td>
          <td style="color:#888">{row.atr_pct:.2f}%</td>
          <td style="color:{fc}">{row.from_52w:+.1f}%</td>
          <td style="text-align:center">{badge(row.nr4,         'NR4'  )}</td>
          <td style="text-align:center">{badge(row.nr7,         'NR7'  )}</td>
          <td style="text-align:center">{badge(row.nr21,        'NR21' )}</td>
          <td style="text-align:center">{badge(row.pocket_pivot,'PP'   )}</td>
          <td style="text-align:center">{badge(row.rs_leads,    'RS&#8593;')}</td>
          <td style="text-align:center">{badge(row.vcp,         'VCP'  )}</td>
          <td>{score_pips(row.score)}</td>
        </tr>"""

    html = f"""
    <style>
    .screener-table{{width:100%;border-collapse:collapse;
        font-family:'IBM Plex Mono',monospace;font-size:11px}}
    .screener-table th{{padding:8px 10px;background:#111;color:#e8a020;
        font-size:10px;letter-spacing:1px;text-transform:uppercase;
        border-bottom:2px solid #c8841a;white-space:nowrap;text-align:left}}
    .screener-table td{{padding:7px 10px;border-bottom:1px solid #131313;white-space:nowrap}}
    .screener-table tr:hover td{{background:rgba(232,160,32,.04)}}
    .screener-table tr.top5 td{{background:rgba(232,160,32,.05)}}
    </style>
    <table class="screener-table">
    <thead><tr>
      <th>#</th><th>SYMBOL</th><th>CLOSE</th><th>CHG%</th>
      <th>VOL&times;</th><th>ATR%</th><th>52W</th>
      <th style="text-align:center">NR4</th>
      <th style="text-align:center">NR7</th>
      <th style="text-align:center">NR21</th>
      <th style="text-align:center">P.PIVOT</th>
      <th style="text-align:center">RS&#8593;</th>
      <th style="text-align:center">VCP</th>
      <th>SCORE</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
    </table>"""
    st.markdown(html, unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  SESSION STATE INIT
# ─────────────────────────────────────────────
for k, v in [("results_df", None), ("counters", {}),
             ("scan_time", None),  ("scan_done", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="border-left:3px solid #e8a020;padding:6px 10px;margin-bottom:16px">'
        '<span style="color:#e8a020;font-size:13px;font-weight:700;letter-spacing:2px">'
        'SCREENER CONTROLS</span></div>',
        unsafe_allow_html=True,
    )
    data_period = st.selectbox("DATA PERIOD",       ["3mo","6mo","1y"],  index=1)
    max_workers = st.selectbox("PARALLEL WORKERS",  [3, 5, 8, 10],       index=1)
    min_score   = st.selectbox("MIN SIGNAL SCORE",  [0,1,2,3,4,5,6],     index=1)

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)
    st.markdown(
        '<span style="color:#888;font-size:10px;letter-spacing:1px">PATTERN FILTERS</span>',
        unsafe_allow_html=True,
    )
    f_nr4  = st.checkbox("NR4  — Narrow Range 4",         value=False)
    f_nr7  = st.checkbox("NR7  — Narrow Range 7",         value=False)
    f_nr21 = st.checkbox("NR21 — Narrow Range 21",        value=False)
    f_pp   = st.checkbox("Pocket Pivot",                   value=False)
    f_rs   = st.checkbox("RS Leads Price High",            value=False)
    f_vcp  = st.checkbox("VCP — Volatility Contraction",  value=False)

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)
    sort_col = st.selectbox(
        "SORT BY",
        ["score","chg_pct","vol_ratio","atr_pct","close","from_52w"],
        index=0,
    )
    sort_asc = st.checkbox("Sort Ascending", value=False)

    st.markdown('<hr style="border-color:#1e1e1e">', unsafe_allow_html=True)
    n_choice  = st.selectbox("UNIVERSE SIZE", ["Top 50","Top 100","Top 150","All"], index=3)
    n_map     = {"Top 50":50,"Top 100":100,"Top 150":150,"All":len(UNIVERSE)}
    scan_univ = UNIVERSE[:n_map[n_choice]]

    run_btn = st.button("RUN SCREENER", use_container_width=True)

    if st.session_state["scan_time"]:
        st.markdown(
            f'<div style="color:#444;font-size:10px;text-align:center;margin-top:8px">'
            f'Last scan: {st.session_state["scan_time"]}</div>',
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────
#  HEADER
# ─────────────────────────────────────────────
now_ist  = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
mkt_min  = now_ist.hour * 60 + now_ist.minute
mkt_open = (now_ist.weekday() < 5) and (555 <= mkt_min < 930)

col_t, col_s = st.columns([4, 1])
with col_t:
    st.markdown(
        '<div style="padding:4px 0 12px">'
        '<span style="background:#e8a020;color:#000;font-weight:700;font-size:11px;'
        'padding:4px 8px;letter-spacing:1px;font-family:monospace">NSE</span>'
        '&nbsp;&nbsp;'
        '<span style="color:#e8a020;font-size:20px;font-weight:700;letter-spacing:3px;'
        'font-family:monospace">NIFTY 500 PATTERN SCREENER</span>'
        '<br><span style="color:#333;font-size:10px;letter-spacing:2px;font-family:monospace">'
        'NR4 &middot; NR7 &middot; NR21 &middot; POCKET PIVOT &middot; RS LEAD &middot; VCP'
        '</span></div>',
        unsafe_allow_html=True,
    )
with col_s:
    mc = "#22c55e" if mkt_open else "#ef4444"
    ml = "MARKET OPEN" if mkt_open else "MARKET CLOSED"
    st.markdown(
        f'<div style="text-align:right;padding-top:8px">'
        f'<div style="color:#22d3ee;font-family:monospace;font-size:12px">'
        f'IST {now_ist.strftime("%H:%M:%S")}</div>'
        f'<div style="color:{mc};font-family:monospace;font-size:10px;letter-spacing:1px">{ml}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
st.markdown('<hr style="border-color:#1e1e1e;margin:0 0 8px">', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  SCAN EXECUTION
# ─────────────────────────────────────────────
if run_btn:
    fetch_ohlcv.clear()
    fetch_benchmark.clear()

    pb   = st.progress(0.0)
    stxt = st.empty()

    raw, ctr = run_screener(scan_univ, data_period, max_workers, pb, stxt)

    pb.progress(1.0)
    stxt.markdown(
        f'<span style="color:#22c55e;font-size:11px;font-family:monospace">'
        f'SCAN COMPLETE &mdash; {ctr["passed"]} stocks analysed &middot; '
        f'{sum(1 for r in raw if r["score"]>0)} signals &middot; '
        f'{ctr["errors"]} errors</span>',
        unsafe_allow_html=True,
    )

    if raw:
        df_raw = pd.DataFrame([r for r in raw if not r["error"]])
        st.session_state["results_df"] = df_raw
        st.session_state["counters"]   = ctr
        st.session_state["scan_time"]  = now_ist.strftime("%d-%b %H:%M IST")
        st.session_state["scan_done"]  = True

# ─────────────────────────────────────────────
#  RESULTS DISPLAY
# ─────────────────────────────────────────────
if st.session_state["scan_done"] and st.session_state["results_df"] is not None:
    df_all = st.session_state["results_df"].copy()
    ctr    = st.session_state["counters"]

    # Metrics
    m_cols = st.columns(8)
    for col, (lbl, key) in zip(m_cols, [
        ("SCANNED","passed"), ("ERRORS","errors"),
        ("NR4","nr4"), ("NR7","nr7"), ("NR21","nr21"),
        ("POCKET PIVOT","pp"), ("RS LEAD","rs"), ("VCP","vcp"),
    ]):
        col.metric(lbl, ctr.get(key, 0))

    st.markdown('<hr style="border-color:#1e1e1e;margin:6px 0">', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["RESULTS TABLE", "PATTERN HEATMAP", "EXPORT"])

    # ── Tab 1 ──
    with tab1:
        df = df_all.copy()
        if f_nr4:  df = df[df["nr4"]          == True]
        if f_nr7:  df = df[df["nr7"]          == True]
        if f_nr21: df = df[df["nr21"]         == True]
        if f_pp:   df = df[df["pocket_pivot"] == True]
        if f_rs:   df = df[df["rs_leads"]     == True]
        if f_vcp:  df = df[df["vcp"]          == True]
        df = df[df["score"] >= min_score]
        df = df.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)

        st.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:4px 0 6px;font-size:10px;font-family:monospace">'
            f'<span style="color:#e8a020;letter-spacing:1px">SCAN RESULTS</span>'
            f'<span style="color:#555">{len(df)} STOCKS</span></div>',
            unsafe_allow_html=True,
        )
        render_table(df)

    # ── Tab 2 : Heatmap ──
    with tab2:
        patterns = ["nr4","nr7","nr21","pocket_pivot","rs_leads","vcp"]
        labels   = ["NR4","NR7","NR21","P.PIVOT","RS","VCP"]

        st.markdown(
            '<div style="color:#555;font-size:10px;font-family:monospace;'
            'margin-bottom:10px;letter-spacing:1px">'
            'CO-OCCURRENCE: % of row-pattern stocks that also show column-pattern'
            '</div>',
            unsafe_allow_html=True,
        )

        hm_html = (
            '<table style="border-collapse:collapse;font-family:monospace;font-size:11px">'
            '<tr><th style="padding:8px 12px;color:#333;border-bottom:1px solid #1e1e1e"></th>'
        )
        for l in labels:
            hm_html += (
                f'<th style="padding:8px 12px;color:#e8a020;'
                f'border-bottom:1px solid #1e1e1e;text-align:center;'
                f'font-size:10px;letter-spacing:1px">{l}</th>'
            )
        hm_html += '</tr>'

        for l1, p1 in zip(labels, patterns):
            hm_html += (
                f'<tr><td style="padding:7px 12px;color:#e8a020;'
                f'font-size:10px;letter-spacing:1px;'
                f'border-right:1px solid #1e1e1e">{l1}</td>'
            )
            total1 = int(df_all[p1].sum())
            for l2, p2 in zip(labels, patterns):
                if l1 == l2:
                    hm_html += (
                        '<td style="background:#1a1a0a;text-align:center;'
                        'border:1px solid #111;color:#333;font-size:10px">&mdash;</td>'
                    )
                    continue
                both  = int((df_all[p1] & df_all[p2]).sum())
                pct   = round(both / total1 * 100, 0) if total1 > 0 else 0
                alpha = pct / 100 * 0.65
                fc    = "#22c55e" if pct > 25 else ("#aaa" if pct > 10 else "#444")
                hm_html += (
                    f'<td style="padding:7px 12px;'
                    f'background:rgba(34,197,94,{alpha:.2f});'
                    f'color:{fc};text-align:center;border:1px solid #111">'
                    f'{int(pct)}%</td>'
                )
            hm_html += '</tr>'
        hm_html += '</table>'
        st.markdown(hm_html, unsafe_allow_html=True)

        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(
            '<span style="color:#555;font-size:10px;font-family:monospace;letter-spacing:1px">'
            'TOP 5 STOCKS PER PATTERN</span>',
            unsafe_allow_html=True,
        )
        cols6 = st.columns(6)
        for idx, (p, l) in enumerate(zip(patterns, labels)):
            sub = df_all[df_all[p] == True].nlargest(5, "score")
            content = (
                f'<div style="color:#e8a020;font-size:10px;font-weight:700;'
                f'letter-spacing:1px;margin-bottom:6px;font-family:monospace">{l}</div>'
            )
            for _, row in sub.iterrows():
                content += (
                    f'<div style="font-family:monospace;font-size:11px;color:#22d3ee;'
                    f'padding:3px 0;border-bottom:1px solid #111">'
                    f'{row["symbol"]} '
                    f'<span style="color:#444;font-size:10px">({int(row["score"])})</span></div>'
                )
            if sub.empty:
                content += (
                    '<div style="color:#2a2a2a;font-size:10px;font-family:monospace">'
                    'No hits</div>'
                )
            with cols6[idx]:
                st.markdown(
                    f'<div style="background:#0e0e0e;padding:10px;'
                    f'border:1px solid #1a1a1a">{content}</div>',
                    unsafe_allow_html=True,
                )

    # ── Tab 3 : Export ──
    with tab3:
        df_exp = df_all.sort_values("score", ascending=False).copy()
        df_exp.columns = [c.upper() for c in df_exp.columns]
        csv_b  = df_exp.to_csv(index=False).encode()
        st.download_button(
            "DOWNLOAD CSV",
            data=csv_b,
            file_name=f"nifty500_{now_ist.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )
        st.markdown('<br>', unsafe_allow_html=True)
        st.dataframe(df_exp.head(30), use_container_width=True, hide_index=True)

else:
    # Empty state
    st.markdown(
        '<div style="text-align:center;padding:80px 20px">'
        '<div style="font-size:50px;opacity:.08;margin-bottom:20px">&#11042;</div>'
        '<div style="color:#2a2a2a;font-size:14px;letter-spacing:3px;'
        'font-family:monospace;margin-bottom:10px">AWAITING SCAN</div>'
        '<div style="color:#1e1e1e;font-size:11px;font-family:monospace">'
        'Configure settings in the sidebar &rarr; press RUN SCREENER'
        '</div></div>',
        unsafe_allow_html=True,
    )

# ─────────────────────────────────────────────
#  PATTERN LEGEND
# ─────────────────────────────────────────────
st.markdown('<hr style="border-color:#1e1e1e;margin:20px 0 10px">', unsafe_allow_html=True)
st.markdown(
    '<span style="color:#2a2a2a;font-size:10px;font-family:monospace;letter-spacing:2px">'
    'PATTERN REFERENCE</span>',
    unsafe_allow_html=True,
)
_legend = [
    ("NR4",     "Narrow Range 4",    "Today H-L is the narrowest of the last 4 sessions. First compression sign."),
    ("NR7",     "Narrow Range 7",    "Today H-L is the narrowest of the last 7 sessions. Classic Toby Crabel coil."),
    ("NR21",    "Narrow Range 21",   "Smallest range in ~1 month. Extreme compression — breakout imminent."),
    ("P.PIVOT", "Pocket Pivot",      "Up-day volume exceeds highest down-day volume of prior 10 sessions."),
    ("RS",      "RS Leads Price",    "RS line (stock/Nifty) near 63-day high while price is still below its own high."),
    ("VCP",     "Volatility Contr.", "Three 20-day windows: contracting range% AND declining volume on an uptrend."),
]
lc = st.columns(6)
for i, (tag, name, desc) in enumerate(_legend):
    with lc[i]:
        st.markdown(
            f'<div style="background:#0c0c0c;border:1px solid #181818;'
            f'border-top:2px solid #e8a020;padding:10px">'
            f'<div style="color:#e8a020;font-size:10px;font-weight:700;'
            f'letter-spacing:1px;font-family:monospace;margin-bottom:5px">'
            f'{tag} &mdash; {name}</div>'
            f'<div style="color:#333;font-size:10px;line-height:1.6;font-family:monospace">'
            f'{desc}</div></div>',
            unsafe_allow_html=True,
        )
