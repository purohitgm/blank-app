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

# Added shorter periods ("1d", "5d") for live intraday tracking
period = st.sidebar.selectbox(
    "Select Time Period", 
    options=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], 
    index=5 # Default to 1 year
)

# REFRESH BUTTON: Clears the cache to force a new download of live data
if st.sidebar.button('🔄 Refresh Live Data'):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared! Fetching latest prices...")

# Define the Yahoo Finance ticker symbols
TICKERS = {
    'Nifty 50': '^NSEI',
    'Crude Oil (WTI)': 'CL=F',
    'Gold': 'GC=F',
    'Silver': 'SI=F',
    'Bitcoin': 'BTC-USD'
}

# Cache is now set to 60 seconds so it refreshes frequently
@st.cache_data(ttl=60) 
def load_data(period):
    # Determine the best interval based on the selected period to get "live" feeling data
    if period == "1d":
        interval = "1m"   # 1-minute intervals for today
    elif period == "5d":
        interval = "5m"   # 5-minute intervals for the last 5 days
    elif period == "1mo":
        interval = "15m"  # 15-minute intervals for the last month
    else:
        interval = "1d"   # Daily intervals for anything longer
        
    series_list =[]
    
    for name, ticker in TICKERS.items():
        ticker_data = yf.Ticker(ticker)
        hist = ticker_data.history(period=period, interval=interval)
        
        if not hist.empty:
            # Convert timezone to UTC strictly for perfect alignment across different global markets
            if hist.index.tz is None:
                hist.index = hist.index.tz_localize('UTC')
            else:
                hist.index = hist.index.tz_convert('UTC')
                
            s = hist['Close']
            s.name = name
            series_list.append(s)
            
    # Combine all assets into a single dataframe
    if series_list:
        df = pd.concat(series_list, axis=1)
    else:
        return pd.DataFrame()
        
    # Forward fill (carry last traded price forward) and backward fill
    df = df.ffill().bfill()
    
    # Convert index back to local time (IST) for display, then remove tz for Plotly
    df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None) 
    
    return df

# Load the data
with st.spinner('Fetching live market data...'):
    df = load_data(period)

if df.empty:
    st.error("Failed to fetch data. Please try again later.")
    st.stop()

st.success(f'Data loaded successfully! (Interval based on period selected)')

# --- LINE CHART SECTION ---
st.subheader("1. Relative Performance Line Chart (Normalized)")
st.markdown("""
*This chart normalizes all assets to **100** at the start of the selected period. This allows you to compare the percentage growth of Bitcoin alongside Silver and Nifty.*
""")

# Normalize data to base 100
normalized_df = (df / df.iloc[0]) * 100

# Plotly Line Chart
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
    st.caption("Calculates correlation based on period-to-period percentage changes.")
    corr_matrix = df.pct_change().corr()
else:
    st.caption("Calculates correlation based on raw asset prices.")
    corr_matrix = df.corr()

# Plotly Heatmap
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
    # Show the last 10 rows (most recent minutes/days) reversed so newest is on top
    st.dataframe(df.tail(15).iloc[::-1].round(2), use_container_width=True)

st.markdown("---")
st.caption("Data provided by [Yahoo Finance](https://finance.yahoo.com/). Note: Different markets have different trading hours. When a market (like Nifty) is closed but Crypto is open, the chart carries forward the last traded price to keep the visualization intact.")