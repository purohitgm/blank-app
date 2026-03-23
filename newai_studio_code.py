import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.express as px

# Set up the Streamlit page configuration
st.set_page_config(page_title="Multi-Asset Live Dashboard", layout="wide")

st.title("📈 Multi-Asset Live Correlation & Performance Dashboard")
st.markdown("Compare **Nifty 50, Crude Oil, Gold, Silver, and Bitcoin (BTC)** using live data from Yahoo Finance.")

# Sidebar for user inputs
st.sidebar.header("Settings")

period = st.sidebar.selectbox(
    "Select Time Period", 
    options=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], 
    index=5 
)

# REFRESH BUTTON
if st.sidebar.button('🔄 Refresh Live Data'):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared! Fetching latest prices...")

TICKERS = {
    'Nifty 50': '^NSEI',
    'Crude Oil (WTI)': 'CL=F',
    'Gold': 'GC=F',
    'Silver': 'SI=F',
    'Bitcoin': 'BTC-USD'
}

@st.cache_data(ttl=60) 
def load_data(period):
    if period == "1d": interval = "1m"
    elif period == "5d": interval = "5m"
    elif period == "1mo": interval = "15m"
    else: interval = "1d"
        
    series_list =[]
    
    for name, ticker in TICKERS.items():
        try:
            ticker_data = yf.Ticker(ticker)
            hist = ticker_data.history(period=period, interval=interval)
            
            # Ensure data was returned and contains the 'Close' column
            if not hist.empty and 'Close' in hist.columns:
                s = hist['Close'].copy()
                s.name = name
                
                # BUG FIX 1: Force every series index to strict UTC Datetime BEFORE merging. 
                # This prevents Pandas from turning the index into an unusable 'Object' type.
                s.index = pd.to_datetime(s.index, utc=True)
                series_list.append(s)
        except Exception:
            # If Yahoo Finance fails for one ticker, skip it instead of crashing the app
            continue
            
    # If all fetches failed, return empty dataframe
    if not series_list:
        return pd.DataFrame()
        
    # Combine all assets into a single dataframe
    df = pd.concat(series_list, axis=1)
    
    # Sort chronologically just in case
    df.sort_index(inplace=True)
    
    # Forward fill missing minutes (e.g. when traditional markets are closed but Crypto is open)
    df = df.ffill().bfill()
    
    # BUG FIX 2: Convert to Indian Standard Time, then remove timezone for Plotly
    try:
        df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None) 
    except Exception:
        pass # Fallback in case tz_convert fails unexpectedly
    
    return df

# Load the data
with st.spinner('Fetching live market data... (This takes a few seconds)'):
    df = load_data(period)

if df.empty:
    st.error("⚠️ Failed to fetch data. Yahoo Finance might be blocking requests or the market data is currently unavailable. Please try again in a few minutes.")
    st.stop()

st.success(f'Data loaded successfully!')

# --- LINE CHART SECTION ---
st.subheader("1. Relative Performance Line Chart (Normalized)")
st.markdown("*This chart normalizes all assets to **100** at the start of the selected period. This allows you to compare the pure percentage growth of Bitcoin alongside Silver and Nifty.*")

# Normalize data to base 100
normalized_df = (df / df.iloc[0]) * 100

fig_line = px.line(
    normalized_df, 
    x=normalized_df.index, 
    y=normalized_df.columns,
    labels={'value': 'Normalized Price (Base 100)', 'index': 'Time / Date', 'variable': 'Asset'},
    template="plotly_dark" 
)
fig_line.update_layout(hovermode="x unified")
st.plotly_chart(fig_line, use_container_width=True)


# --- CORRELATION CHART SECTION ---
st.subheader("2. Correlation Heatmap")

corr_type = st.radio(
    "Select Correlation Method:", 
    ["Returns Correlation (Recommended)", "Absolute Price Correlation"],
    horizontal=True
)

if corr_type == "Returns Correlation (Recommended)":
    # BUG FIX 3: .fillna(0) prevents Plotly from crashing if the market is closed 
    # causing price flatlines (which result in mathematical NaN correlations).
    corr_matrix = df.pct_change().corr().fillna(0)
else:
    corr_matrix = df.corr().fillna(0)

fig_corr = px.imshow(
    corr_matrix, 
    text_auto=".2f", 
    aspect="auto",
    color_continuous_scale='RdBu_r', 
    zmin=-1, zmax=1,
    template="plotly_dark"
)
st.plotly_chart(fig_corr, use_container_width=True)


# --- RAW DATA SECTION ---
st.subheader("3. Latest Live Market Prices")
with st.expander("Click to view the raw data table"):
    st.dataframe(df.tail(15).iloc[::-1].round(2), use_container_width=True)

st.markdown("---")
st.caption("Data provided by [Yahoo Finance](https://finance.yahoo.com/). Note: Different markets have different trading hours. When a market (like Nifty) is closed but Crypto is open, the chart carries forward the last traded price to keep the visualization intact.")
