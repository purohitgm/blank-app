"""
╔══════════════════════════════════════════════════════════════╗
║         NIFTY 500 PATTERN SCREENER  — Streamlit App         ║
║  Patterns: NR4 · NR7 · NR21 · Pocket Pivot · RS Lead · VCP ║
║  Dependencies: streamlit · pandas · numpy · requests        ║
║  All controls are inline — no sidebar needed                ║
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

# ─────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NIFTY 500 SCREENER",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",   # sidebar hidden by default
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
    font-family: 'IBM Plex Mono', 'Courier New', monospace !important;
}
#MainMenu, footer, header           { visibility: hidden; }
[data-testid="stDecoration"]        { display: none; }
[data-testid="stToolbar"]           { display: none; }
[data-testid="collapsedControl"]    { display: none; }   /* hide sidebar toggle */
section[data-testid="stSidebar"]    { display: none; }   /* remove sidebar space */

/* ── control panel ── */
.ctrl-panel {
    background: #0f0f0f;
    border: 1px solid #1e1e1e;
    border-top: 2px solid #e8a020;
    padding: 14px 16px 10px;
    margin-bottom: 10px;
}
/* ── selectbox labels ── */
[data-testid="stSelectbox"] label,
[data-testid="stNumberInput"] label {
    color: #e8a020 !important;
    font-size: 10px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    margin-bottom: 2px !important;
}
/* ── selectbox widget ── */
[data-baseweb="select"] > div {
    background: #141414 !important;
    border: 1px solid #2a2a2a !important;
    border-radius: 0 !important;
    color: #e0e0e0 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 12px !important;
}
[data-baseweb="select"] > div:focus-within {
    border-color: #e8a020 !important;
}
/* ── checkbox labels ── */
[data-testid="stCheckbox"] label {
    color: #aaa !important;
    font-size: 11px !important;
    font-family: 'IBM Plex Mono', monospace !important;
}
[data-testid="stCheckbox"] input:checked + div { background: #e8a020 !important; }

/* ── RUN button ── */
.stButton > button {
    background: #e8a020 !important;
    color: #000 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-weight: 700 !important;
    font-size: 13px !important;
    letter-spacing: 1.5px !important;
    border: none !important;
    border-radius: 0 !important;
    width: 100% !important;
    padding: 10px 0 !important;
    margin-top: 20px !important;
    transition: background 0.15s !important;
}
.stButton > button:hover    { background: #ffb830 !important; }
.stButton > button:disabled { background: #2a2a2a !important; color: #555 !important; }

/* ── progress bar ── */
[data-testid="stProgressBar"] > div > div { background: #e8a020 !important; }

/* ── tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    border-bottom: 1px solid #1e1e1e !important;
    gap: 0 !important;
}
[data-testid="stTabs"] [role="tab"] {
    color: #444 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 11px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    padding: 8px 18px !important;
    border-radius: 0 !important;
    border: none !important;
    background: transparent !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #e8a020 !important;
    border-bottom: 2px solid #e8a020 !important;
    background: #0f0f0f !important;
}
[data-testid="stTabs"] [role="tab"]:hover { color: #aaa !important; }

/* ── metrics ── */
[data-testid="stMetric"] {
    background: #0f0f0f !important;
    border: 1px solid #1a1a1a !important;
    border-left: 2px solid #e8a020 !important;
    padding: 8px 12px !important;
    border-radius: 0 !important;
}
[data-testid="stMetricLabel"] {
    color: #555 !important;
    font-size: 9px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}
[data-testid="stMetricValue"] {
    color: #e8a020 !important;
    font-size: 20px !important;
    font-weight: 700 !important;
    font-family: 'IBM Plex Mono', monospace !important;
}

/* ── download button ── */
[data-testid="stDownloadButton"] > button {
    background: transparent !important;
    color: #22d3ee !important;
    border: 1px solid #22d3ee !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 11px !important;
    border-radius: 0 !important;
    padding: 6px 16px !important;
    margin-top: 0 !important;
    width: auto !important;
}
[data-testid="stDownloadButton"] > button:hover {
    background: rgba(34,211,238,0.08) !important;
}

/* ── filter chip row ── */
.chip-row {
    display: flex; flex-wrap: wrap; gap: 6px;
    padding: 8px 0 6px;
    align-items: center;
}
.chip {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px; font-weight: 600;
    padding: 4px 12px;
    border: 1px solid #2a2a2a;
    color: #555; background: transparent;
    cursor: pointer; letter-spacing: 0.5px;
    transition: all 0.12s; user-select: none;
}
.chip.on  { border-color: #e8a020; color: #e8a020; background: rgba(232,160,32,.08); }
.chip.on:hover { background: rgba(232,160,32,.14); }
.chip:hover { border-color: #555; color: #aaa; }

/* ── dividers ── */
hr { border-color: #1a1a1a !important; margin: 8px 0 !important; }

/* ── scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: #111; }
::-webkit-scrollbar-thumb { background: #2a2a2a; }
::-webkit-scrollbar-thumb:hover { background: #e8a020; }

/* ── screener table ── */
.st-table { width:100%; border-collapse:collapse;
    font-family:'IBM Plex Mono',monospace; font-size:11px; }
.st-table th { padding:8px 10px; background:#0f0f0f; color:#e8a020;
    font-size:10px; letter-spacing:1px; text-transform:uppercase;
    border-bottom:2px solid #c8841a; white-space:nowrap; text-align:left; }
.st-table td { padding:7px 10px; border-bottom:1px solid #131313; white-space:nowrap; }
.st-table tr:hover td { background:rgba(232,160,32,.04); }
.st-table tr.top5 td { background:rgba(232,160,32,.05); }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  UNIVERSE
# ─────────────────────────────────────────────────────────────
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
    "GODREJCP","COLPAL","TATACONSUM","JYOTHYLAB","EMAMILTD","VBL","RADICO",
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

def _build_universe(raw):
    seen, result = set(), []
    for s in raw:
        if s not in seen:
            seen.add(s)
            result.append({"display": s, "ticker": s.replace("&", "%26") + ".NS"})
    return result

UNIVERSE     = _build_universe(_RAW)
BENCH_TICKER = "%5ENSEI"


# ─────────────────────────────────────────────────────────────
#  YAHOO FINANCE HTTP  (pure requests — no yfinance)
# ─────────────────────────────────────────────────────────────
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

def _fetch_chart(ticker: str, period: str, timeout: int = 12):
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

def _parse_chart(data):
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
        }, index=(pd.to_datetime(ts, unit="s", utc=True)
                    .tz_convert("Asia/Kolkata")
                    .normalize()))
        df.index.name = "Date"
        df = df.apply(pd.to_numeric, errors="coerce").dropna()
        return df if len(df) >= 25 else None
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ohlcv(ticker: str, period: str):
    data = _fetch_chart(ticker, period)
    return _parse_chart(data) if data else None

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_benchmark(period: str):
    data = _fetch_chart(BENCH_TICKER, period)
    if data is None:
        return None
    df = _parse_chart(data)
    return df[["Close"]].rename(columns={"Close": "Bench"}) if df is not None else None

def _align_bench(stock_df, bench_df):
    return stock_df[["Close"]].join(bench_df[["Bench"]], how="inner")["Bench"].values


# ─────────────────────────────────────────────────────────────
#  PATTERN DETECTION
# ─────────────────────────────────────────────────────────────
def is_nr(H, L, n):
    if len(H) < n: return False
    r = H[-n:] - L[-n:]
    return bool(r[-1] > 0 and r[-1] < r[:-1].min())

def pocket_pivot(C, V):
    if len(C) < 12: return False
    if C[-1] <= C[-2]: return False
    dv = [float(V[-1-i]) for i in range(1,11)
          if len(C) > i+1 and C[-1-i] < C[-2-i]]
    return bool(dv and float(V[-1]) > max(dv))

def rs_leads_price(C, B, lb=63, rs_thr=0.97, p_thr=0.96):
    n = min(len(C), len(B))
    if n < lb+2: return False
    c = C[-n:]; b = B[-n:]
    rs = c / np.where(b > 0, b, np.nan)
    rw = rs[-lb:]; pw = c[-lb:]
    if np.any(np.isnan(rw)): return False
    return bool(rw[-1] >= rw.max()*rs_thr and pw[-1] < pw.max()*p_thr)

def vcp(C, H, L, V, win=20, nwin=3, ut_lb=120, min_up=0.08):
    if len(C) < win*nwin + ut_lb: return False
    lo = L[-ut_lb:].min()
    if lo <= 0 or C[-1] < lo*(1+min_up): return False
    n = len(C); rngs, vols = [], []
    for w in range(nwin):
        s = n - win*(nwin-w); e = s+win
        if s < 0 or e > n: return False
        rngs.append((H[s:e].max()-L[s:e].min())/L[s:e].min()*100)
        vols.append(float(V[s:e].mean()))
    return bool(all(rngs[i]>rngs[i+1] for i in range(nwin-1)) and
                all(vols[i]>vols[i+1] for i in range(nwin-1)))

def calc_atr_pct(H, L, C, p=14):
    if len(C) < p+1: return 0.0
    trs = [max(H[-p+i]-L[-p+i], abs(H[-p+i]-C[-p+i-1]),
               abs(L[-p+i]-C[-p+i-1])) for i in range(p)]
    return float(np.mean(trs)/C[-1]*100) if C[-1] > 0 else 0.0

def calc_vol_ratio(V, avg_p=20):
    if len(V) < avg_p+1: return 1.0
    avg = float(V[-(avg_p+1):-1].mean())
    return float(V[-1]/avg) if avg > 0 else 1.0

def pct_from_52w(C, p=252):
    w = C[-min(p, len(C)):]
    hi = w.max()
    return float((C[-1]-hi)/hi*100) if hi > 0 else 0.0


# ─────────────────────────────────────────────────────────────
#  ANALYSIS
# ─────────────────────────────────────────────────────────────
_ERR = lambda s: {"symbol":s,"error":True,"score":0,
                  "nr4":False,"nr7":False,"nr21":False,
                  "pocket_pivot":False,"rs_leads":False,"vcp":False}

def analyse(entry, period, bench_df):
    sym = entry["display"]
    try:
        df = fetch_ohlcv(entry["ticker"], period)
        if df is None: return _ERR(sym)
        H = df["High"].values.astype(float)
        L = df["Low"].values.astype(float)
        C = df["Close"].values.astype(float)
        V = df["Volume"].values.astype(float)
        BA = None
        if bench_df is not None:
            try: BA = _align_bench(df, bench_df)
            except Exception: pass
        nr4  = is_nr(H, L, 4);  nr7 = is_nr(H, L, 7);  nr21 = is_nr(H, L, 21)
        pp   = pocket_pivot(C, V)
        rs   = rs_leads_price(C, BA) if (BA is not None and len(BA) > 65) else False
        vc   = vcp(C, H, L, V)
        score = sum([nr4, nr7, nr21, pp, rs, vc])
        c0 = float(C[-1]); cp = float(C[-2]) if len(C)>1 else c0
        return {
            "symbol": sym, "close": round(c0, 2),
            "chg_pct": round((c0-cp)/cp*100 if cp>0 else 0, 2),
            "vol_ratio": round(calc_vol_ratio(V), 2),
            "atr_pct":   round(calc_atr_pct(H, L, C), 2),
            "from_52w":  round(pct_from_52w(C), 2),
            "nr4": nr4, "nr7": nr7, "nr21": nr21,
            "pocket_pivot": pp, "rs_leads": rs, "vcp": vc,
            "score": score, "error": False,
        }
    except Exception:
        return _ERR(sym)


# ─────────────────────────────────────────────────────────────
#  SCREENER
# ─────────────────────────────────────────────────────────────
def run_screener(universe, period, workers, pb, status):
    bench   = fetch_benchmark(period)
    results = []
    ctr     = dict(done=0, passed=0, errors=0,
                   nr4=0, nr7=0, nr21=0, pp=0, rs=0, vcp=0)
    total   = len(universe)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fmap = {ex.submit(analyse, e, period, bench): e for e in universe}
        for fut in as_completed(fmap):
            ctr["done"] += 1
            try:
                r = fut.result(timeout=25)
            except Exception:
                r = None
            if r and not r["error"]:
                results.append(r)
                ctr["passed"] += 1
                for k in ("nr4","nr7","nr21"):
                    if r[k]: ctr[k] += 1
                if r["pocket_pivot"]: ctr["pp"]  += 1
                if r["rs_leads"]:     ctr["rs"]  += 1
                if r["vcp"]:          ctr["vcp"] += 1
            else:
                ctr["errors"] += 1
            pb.progress(min(ctr["done"]/total, 1.0))
            status.markdown(
                f'<span style="color:#e8a020;font-size:11px;font-family:monospace">'
                f'&#9679; SCANNING {ctr["done"]}/{total} &nbsp;&#183;&nbsp; '
                f'SIGNALS: {sum(1 for x in results if x["score"]>0)} &nbsp;&#183;&nbsp; '
                f'ERRORS: {ctr["errors"]}</span>',
                unsafe_allow_html=True,
            )
    return results, ctr


# ─────────────────────────────────────────────────────────────
#  RENDER HELPERS
# ─────────────────────────────────────────────────────────────
def badge(hit, label):
    if hit:
        return (f'<span style="background:rgba(34,197,94,.15);color:#4ade80;'
                f'border:1px solid #22c55e44;padding:2px 6px;font-size:10px;'
                f'font-weight:700;font-family:monospace">{label}</span>')
    return '<span style="color:#252525;font-size:10px">&#183;</span>'

def score_pips(score):
    clr = {6:"#ffd700",5:"#f97316",4:"#22c55e",3:"#22d3ee",
           2:"#e0e0e0",1:"#666",0:"#222"}.get(score,"#222")
    pips = "".join(
        f'<span style="display:inline-block;width:7px;height:13px;margin-right:2px;'
        f'background:{clr if i<score else "#1a1a1a"};'
        f'border:1px solid {clr if i<score else "#2a2a2a"}"></span>'
        for i in range(6))
    return (f'<span style="color:{clr};font-weight:700;font-size:14px;'
            f'font-family:monospace">{score}</span>&nbsp;{pips}')

def render_table(df):
    if df.empty:
        st.markdown(
            '<div style="text-align:center;padding:50px;color:#333;'
            'font-family:monospace;font-size:12px">NO STOCKS MATCH FILTERS</div>',
            unsafe_allow_html=True)
        return
    RC = {1:"#ffd700",2:"#c0c0c0",3:"#cd7f32"}
    rows = ""
    for i, r in enumerate(df.itertuples(), 1):
        rc  = RC.get(i,"#555")
        rsm = {1:"①",2:"②",3:"③"}.get(i,str(i))
        cc  = "#22c55e" if r.chg_pct>=0 else "#ef4444"
        cs  = "+" if r.chg_pct>=0 else ""
        fc  = "#22c55e" if r.from_52w>=-5 else ("#888" if r.from_52w>=-15 else "#444")
        tc  = "top5" if i<=5 else ""
        rows += (
            f'<tr class="{tc}">'
            f'<td style="font-weight:700;color:{rc};font-size:13px;text-align:center;width:34px">{rsm}</td>'
            f'<td style="color:#22d3ee;font-weight:600;font-size:12px">{r.symbol}</td>'
            f'<td style="color:#e0e0e0;font-variant-numeric:tabular-nums">&#8377;{r.close:,.2f}</td>'
            f'<td style="color:{cc}">{cs}{r.chg_pct:.2f}%</td>'
            f'<td style="color:#888">{r.vol_ratio:.2f}&times;</td>'
            f'<td style="color:#666">{r.atr_pct:.2f}%</td>'
            f'<td style="color:{fc}">{r.from_52w:+.1f}%</td>'
            f'<td style="text-align:center">{badge(r.nr4,         "NR4" )}</td>'
            f'<td style="text-align:center">{badge(r.nr7,         "NR7" )}</td>'
            f'<td style="text-align:center">{badge(r.nr21,        "NR21")}</td>'
            f'<td style="text-align:center">{badge(r.pocket_pivot,"PP"  )}</td>'
            f'<td style="text-align:center">{badge(r.rs_leads,    "RS&#8593;")}</td>'
            f'<td style="text-align:center">{badge(r.vcp,         "VCP" )}</td>'
            f'<td>{score_pips(r.score)}</td>'
            f'</tr>'
        )
    st.markdown(
        f'<table class="st-table"><thead><tr>'
        f'<th>#</th><th>SYMBOL</th><th>CLOSE</th><th>CHG%</th>'
        f'<th>VOL&times;</th><th>ATR%</th><th>52W</th>'
        f'<th style="text-align:center">NR4</th>'
        f'<th style="text-align:center">NR7</th>'
        f'<th style="text-align:center">NR21</th>'
        f'<th style="text-align:center">P.PIV</th>'
        f'<th style="text-align:center">RS&#8593;</th>'
        f'<th style="text-align:center">VCP</th>'
        f'<th>SCORE</th>'
        f'</tr></thead><tbody>{rows}</tbody></table>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────────────────────────
_defaults = {
    "results_df": None, "counters": {}, "scan_time": None, "scan_done": False,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────────────────────
#  ████████████████  TOP HEADER BAR  ████████████████
# ─────────────────────────────────────────────────────────────
now_ist  = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
mkt_min  = now_ist.hour * 60 + now_ist.minute
mkt_open = (now_ist.weekday() < 5) and (555 <= mkt_min < 930)
mc = "#22c55e" if mkt_open else "#ef4444"
ml = "&#11044; OPEN" if mkt_open else "&#9711; CLOSED"

st.markdown(
    f'<div style="display:flex;align-items:center;justify-content:space-between;'
    f'padding:10px 0 6px">'
    f'<div style="display:flex;align-items:center;gap:12px">'
    f'<span style="background:#e8a020;color:#000;font-weight:700;font-size:11px;'
    f'padding:5px 9px;letter-spacing:1px;font-family:monospace">NSE</span>'
    f'<div>'
    f'<div style="color:#e8a020;font-size:18px;font-weight:700;letter-spacing:3px;'
    f'font-family:monospace;line-height:1.1">NIFTY 500 PATTERN SCREENER</div>'
    f'<div style="color:#2a2a2a;font-size:10px;letter-spacing:2px;font-family:monospace">'
    f'NR4 &#183; NR7 &#183; NR21 &#183; POCKET PIVOT &#183; RS LEAD &#183; VCP</div>'
    f'</div></div>'
    f'<div style="text-align:right">'
    f'<div style="color:#22d3ee;font-family:monospace;font-size:13px;font-weight:600">'
    f'IST&nbsp;{now_ist.strftime("%H:%M:%S")}</div>'
    f'<div style="color:{mc};font-family:monospace;font-size:10px;letter-spacing:1px">{ml}</div>'
    f'</div></div>',
    unsafe_allow_html=True,
)
st.markdown('<hr style="margin:0 0 10px">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  ████████████  INLINE CONTROL PANEL  ████████████
#  Always visible — no sidebar needed
# ─────────────────────────────────────────────────────────────
st.markdown(
    '<div style="color:#e8a020;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin-bottom:8px">&#9632; SCAN CONTROLS</div>',
    unsafe_allow_html=True,
)

# ── Row A: main settings + RUN button ──
cA1, cA2, cA3, cA4, cA5, cA6 = st.columns([2, 2, 2, 2, 2, 2])

with cA1:
    data_period = st.selectbox(
        "Data Period",
        options=["3mo", "6mo", "1y"],
        index=1,
        key="sel_period",
    )
with cA2:
    n_choice = st.selectbox(
        "Universe Size",
        options=["Top 50", "Top 100", "Top 150", "All"],
        index=3,
        key="sel_universe",
    )
with cA3:
    max_workers = st.selectbox(
        "Parallel Workers",
        options=[3, 5, 8, 10],
        index=1,
        key="sel_workers",
    )
with cA4:
    min_score = st.selectbox(
        "Min Signal Score",
        options=[0, 1, 2, 3, 4, 5, 6],
        index=1,
        key="sel_minscore",
    )
with cA5:
    sort_by = st.selectbox(
        "Sort Results By",
        options=["score", "chg_pct", "vol_ratio", "atr_pct", "close", "from_52w"],
        index=0,
        key="sel_sortby",
    )
with cA6:
    sort_asc = st.selectbox(
        "Sort Direction",
        options=["Descending ▼", "Ascending ▲"],
        index=0,
        key="sel_sortdir",
    )

# ── Row B: pattern quick-filters ──
st.markdown(
    '<div style="color:#555;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin:10px 0 4px">&#9632; PATTERN QUICK-FILTER</div>',
    unsafe_allow_html=True,
)
fB1, fB2, fB3, fB4, fB5, fB6, fB7 = st.columns([1.2, 1, 1, 1, 1.4, 1.2, 1])
with fB1: f_nr4  = st.checkbox("NR4",          value=False, key="chk_nr4")
with fB2: f_nr7  = st.checkbox("NR7",          value=False, key="chk_nr7")
with fB3: f_nr21 = st.checkbox("NR21",         value=False, key="chk_nr21")
with fB4: f_pp   = st.checkbox("Pocket Pivot", value=False, key="chk_pp")
with fB5: f_rs   = st.checkbox("RS Leads High",value=False, key="chk_rs")
with fB6: f_vcp  = st.checkbox("VCP",          value=False, key="chk_vcp")
with fB7: run_btn = st.button("▶ RUN", key="run_btn", use_container_width=True)

st.markdown('<hr style="margin:10px 0">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
#  BUILD SCAN UNIVERSE
# ─────────────────────────────────────────────────────────────
n_map     = {"Top 50": 50, "Top 100": 100, "Top 150": 150, "All": len(UNIVERSE)}
scan_univ = UNIVERSE[: n_map[n_choice]]


# ─────────────────────────────────────────────────────────────
#  SCAN EXECUTION
# ─────────────────────────────────────────────────────────────
if run_btn:
    fetch_ohlcv.clear()
    fetch_benchmark.clear()

    pb   = st.progress(0.0)
    stxt = st.empty()

    raw, ctr = run_screener(scan_univ, data_period, max_workers, pb, stxt)
    pb.progress(1.0)
    stxt.markdown(
        f'<span style="color:#22c55e;font-size:11px;font-family:monospace">'
        f'&#10003; COMPLETE &mdash; {ctr["passed"]} stocks &nbsp;&#183;&nbsp; '
        f'{sum(1 for r in raw if r["score"]>0)} signals &nbsp;&#183;&nbsp; '
        f'{ctr["errors"]} errors</span>',
        unsafe_allow_html=True,
    )
    if raw:
        st.session_state["results_df"] = pd.DataFrame([r for r in raw if not r["error"]])
        st.session_state["counters"]   = ctr
        st.session_state["scan_time"]  = now_ist.strftime("%d-%b %H:%M IST")
        st.session_state["scan_done"]  = True


# ─────────────────────────────────────────────────────────────
#  RESULTS
# ─────────────────────────────────────────────────────────────
if st.session_state["scan_done"] and st.session_state["results_df"] is not None:
    df_all = st.session_state["results_df"].copy()
    ctr    = st.session_state["counters"]

    # ── Summary metrics ──
    mc_list = [
        ("SCANNED",      ctr.get("passed", 0)),
        ("ERRORS",       ctr.get("errors", 0)),
        ("NR4 HITS",     ctr.get("nr4",    0)),
        ("NR7 HITS",     ctr.get("nr7",    0)),
        ("NR21 HITS",    ctr.get("nr21",   0)),
        ("POCKET PIVOT", ctr.get("pp",     0)),
        ("RS LEADS",     ctr.get("rs",     0)),
        ("VCP",          ctr.get("vcp",    0)),
    ]
    for col, (lbl, val) in zip(st.columns(8), mc_list):
        col.metric(lbl, val)

    st.markdown('<hr style="margin:8px 0">', unsafe_allow_html=True)

    # ── Last scan timestamp ──
    if st.session_state["scan_time"]:
        st.markdown(
            f'<div style="color:#333;font-size:10px;font-family:monospace;'
            f'text-align:right;margin-bottom:4px">'
            f'Last scan: {st.session_state["scan_time"]}</div>',
            unsafe_allow_html=True,
        )

    tab1, tab2, tab3 = st.tabs(["  RESULTS TABLE  ", "  PATTERN HEATMAP  ", "  EXPORT CSV  "])

    # ── Tab 1: Results ──
    with tab1:
        df = df_all.copy()
        if f_nr4:  df = df[df["nr4"]          == True]
        if f_nr7:  df = df[df["nr7"]          == True]
        if f_nr21: df = df[df["nr21"]         == True]
        if f_pp:   df = df[df["pocket_pivot"] == True]
        if f_rs:   df = df[df["rs_leads"]     == True]
        if f_vcp:  df = df[df["vcp"]          == True]
        df = df[df["score"] >= min_score]
        df = df.sort_values(sort_by, ascending=(sort_asc == "Ascending ▲")).reset_index(drop=True)

        st.markdown(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:4px 0 8px;font-size:10px;font-family:monospace">'
            f'<span style="color:#e8a020;letter-spacing:1px">SCAN RESULTS</span>'
            f'<span style="color:#444">{len(df)} of {len(df_all)} stocks shown</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        render_table(df)

    # ── Tab 2: Heatmap ──
    with tab2:
        pkeys  = ["nr4","nr7","nr21","pocket_pivot","rs_leads","vcp"]
        plbls  = ["NR4","NR7","NR21","P.PIVOT","RS&#8593;","VCP"]

        st.markdown(
            '<div style="color:#444;font-size:10px;font-family:monospace;'
            'letter-spacing:1px;margin-bottom:10px">'
            'CO-OCCURRENCE: % of row-pattern stocks that also fire the column-pattern'
            '</div>',
            unsafe_allow_html=True,
        )
        hm = ('<table style="border-collapse:collapse;font-family:monospace;font-size:11px">'
              '<tr><th style="padding:8px 12px;color:#2a2a2a;border-bottom:1px solid #1e1e1e"></th>')
        for l in plbls:
            hm += (f'<th style="padding:8px 12px;color:#e8a020;border-bottom:1px solid '
                   f'#1e1e1e;text-align:center;font-size:10px;letter-spacing:1px">{l}</th>')
        hm += '</tr>'
        for l1, p1 in zip(plbls, pkeys):
            hm += (f'<tr><td style="padding:7px 12px;color:#e8a020;font-size:10px;'
                   f'letter-spacing:1px;border-right:1px solid #1e1e1e">{l1}</td>')
            t1 = int(df_all[p1].sum())
            for l2, p2 in zip(plbls, pkeys):
                if l1 == l2:
                    hm += '<td style="background:#111;text-align:center;border:1px solid #0d0d0d;color:#2a2a2a;font-size:10px">&mdash;</td>'
                    continue
                both = int((df_all[p1] & df_all[p2]).sum())
                pct  = round(both/t1*100, 0) if t1 > 0 else 0
                a    = pct/100*0.65
                fc   = "#22c55e" if pct>25 else ("#888" if pct>10 else "#333")
                hm  += (f'<td style="padding:7px 12px;background:rgba(34,197,94,{a:.2f});'
                        f'color:{fc};text-align:center;border:1px solid #0d0d0d">{int(pct)}%</td>')
            hm += '</tr>'
        hm += '</table>'
        st.markdown(hm, unsafe_allow_html=True)

        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown(
            '<span style="color:#444;font-size:10px;font-family:monospace;letter-spacing:1px">'
            'TOP 5 PER PATTERN</span>',
            unsafe_allow_html=True,
        )
        cols6 = st.columns(6)
        for idx, (pk, pl) in enumerate(zip(pkeys, plbls)):
            sub  = df_all[df_all[pk] == True].nlargest(5, "score")
            body = f'<div style="color:#e8a020;font-size:10px;font-weight:700;letter-spacing:1px;margin-bottom:6px;font-family:monospace">{pl}</div>'
            for _, row in sub.iterrows():
                body += (f'<div style="font-family:monospace;font-size:11px;color:#22d3ee;'
                         f'padding:3px 0;border-bottom:1px solid #111">{row["symbol"]} '
                         f'<span style="color:#333;font-size:10px">({int(row["score"])})</span></div>')
            if sub.empty:
                body += '<div style="color:#2a2a2a;font-size:10px;font-family:monospace">No hits</div>'
            with cols6[idx]:
                st.markdown(
                    f'<div style="background:#0d0d0d;padding:10px;border:1px solid #181818">{body}</div>',
                    unsafe_allow_html=True,
                )

    # ── Tab 3: Export ──
    with tab3:
        df_exp = df_all.sort_values("score", ascending=False).copy()
        df_exp.columns = [c.upper() for c in df_exp.columns]
        st.download_button(
            "&#11015;  DOWNLOAD CSV",
            data=df_exp.to_csv(index=False).encode(),
            file_name=f"nifty500_{now_ist.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )
        st.markdown('<br>', unsafe_allow_html=True)
        st.dataframe(df_exp.head(30), use_container_width=True, hide_index=True)

else:
    # ── Empty state ──
    st.markdown(
        '<div style="text-align:center;padding:60px 20px;border:1px solid #141414;'
        'margin:10px 0">'
        '<div style="font-size:40px;opacity:.07;margin-bottom:16px">&#9900;</div>'
        '<div style="color:#222;font-size:13px;letter-spacing:3px;font-family:monospace;'
        'margin-bottom:8px">AWAITING SCAN</div>'
        '<div style="color:#1a1a1a;font-size:11px;font-family:monospace">'
        'Set your preferences above &rarr; press &#9654; RUN</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
#  PATTERN REFERENCE (always visible at bottom)
# ─────────────────────────────────────────────────────────────
st.markdown('<hr style="margin:20px 0 10px">', unsafe_allow_html=True)
st.markdown(
    '<div style="color:#2a2a2a;font-size:10px;font-family:monospace;'
    'letter-spacing:2px;margin-bottom:8px">PATTERN REFERENCE</div>',
    unsafe_allow_html=True,
)
_ref = [
    ("NR4",     "Narrow Range 4",   "Today H-L is the narrowest of last 4 sessions."),
    ("NR7",     "Narrow Range 7",   "Today H-L is the narrowest of last 7 sessions. Toby Crabel coil."),
    ("NR21",    "Narrow Range 21",  "Smallest range in ~1 month. Extreme compression."),
    ("P.PIVOT", "Pocket Pivot",     "Up-day volume > highest down-day vol of prior 10 sessions."),
    ("RS&#8593;","RS Leads Price",  "RS line near 63-day high while price is still below its own high."),
    ("VCP",     "Volatility Contr.","3 x 20-day windows: contracting range% AND volume. Minervini."),
]
for col, (tag, name, desc) in zip(st.columns(6), _ref):
    with col:
        st.markdown(
            f'<div style="background:#0a0a0a;border:1px solid #151515;'
            f'border-top:2px solid #e8a020;padding:10px 12px">'
            f'<div style="color:#e8a020;font-size:10px;font-weight:700;letter-spacing:1px;'
            f'font-family:monospace;margin-bottom:4px">{tag}</div>'
            f'<div style="color:#c0c0c0;font-size:10px;font-family:monospace;'
            f'margin-bottom:4px">{name}</div>'
            f'<div style="color:#333;font-size:9px;line-height:1.6;font-family:monospace">'
            f'{desc}</div></div>',
            unsafe_allow_html=True,
        )
