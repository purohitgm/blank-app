import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.express as px

# Set up the Streamlit page configuration
st.set_page_config(page_title="Multi-Asset Dashboard", layout="wide")

st.title("📈 Multi-Asset Correlation & Performance Dashboard")
st.markdown("Compare **Nifty 50, Crude Oil, Gold, Silver, and Bitcoin (BTC)** using live data from Yahoo Finance.")

# Sidebar for user inputs
st.sidebar.header("Settings")
# Time periods accepted by yfinance
period = st.sidebar.selectbox(
    "Select Time Period", 
    options=["1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], 
    index=3 # Default to 1 year
)

# Define the Yahoo Finance ticker symbols
TICKERS = {
    'Nifty 50': '^NSEI',
    'Crude Oil (WTI)': 'CL=F',
    'Gold': 'GC=F',
    'Silver': 'SI=F',
    'Bitcoin': 'BTC-USD'
}

# Function to fetch data (cached so we don't spam the API on every UI update)
@st.cache_data(ttl=3600) # Data caches for 1 hour
def load_data(period):
    df = pd.DataFrame()
    for name, ticker in TICKERS.items():
        # Fetch historical data
        ticker_data = yf.Ticker(ticker)
        hist = ticker_data.history(period=period)
        if not hist.empty:
            df[name] = hist['Close']
            
    # Forward fill and backward fill to handle missing dates (like weekends/holidays)
    df = df.ffill().bfill()
    
    # Remove timezone information to make plotting cleaner
    df.index = df.index.tz_localize(None) 
    return df

# Load the data
with st.spinner('Fetching live market data...'):
    df = load_data(period)

st.success('Data loaded successfully!')

# --- LINE CHART SECTION ---
st.subheader("1. Relative Performance Line Chart (Normalized)")
st.markdown("""
*Since BTC is traded in thousands of dollars and Silver in tens, absolute prices cannot be compared on a single chart.* 
*This chart normalizes all assets to **100** at the start of the selected period to show pure relative growth.*
""")

# Normalize data to base 100
normalized_df = (df / df.iloc[0]) * 100

# Plotly Line Chart
fig_line = px.line(
    normalized_df, 
    x=normalized_df.index, 
    y=normalized_df.columns,
    labels={'value': 'Normalized Price (Base 100)', 'Date': 'Date', 'variable': 'Asset'},
    template="plotly_dark" # Change to "plotly_white" if you prefer a light theme
)
fig_line.update_layout(hovermode="x unified")
st.plotly_chart(fig_line, use_container_width=True)


# --- CORRELATION CHART SECTION ---
st.subheader("2. Correlation Heatmap")

# Radio button to toggle between Return-based correlation and Price-based correlation
corr_type = st.radio(
    "Select Correlation Method:", 
    ["Daily Returns Correlation (Recommended)", "Absolute Price Correlation"]
)

if corr_type == "Daily Returns Correlation (Recommended)":
    st.caption("Calculates correlation based on daily percentage changes. Best for finding true relationships between financial assets.")
    # Calculate percentage change first, then correlation
    corr_matrix = df.pct_change().corr()
else:
    st.caption("Calculates correlation based on raw asset prices. (Warning: Can sometimes show spurious correlations due to market trends).")
    corr_matrix = df.corr()

# Plotly Heatmap
fig_corr = px.imshow(
    corr_matrix, 
    text_auto=".2f", # Show 2 decimal places
    aspect="auto",
    color_continuous_scale='RdBu_r', # Red to Blue colormap
    zmin=-1, zmax=1,
    template="plotly_dark"
)
st.plotly_chart(fig_corr, use_container_width=True)


# --- RAW DATA SECTION ---
st.subheader("3. Recent Market Prices (Raw Data)")
with st.expander("Click to view the raw historical data table"):
    # Show the last 10 days of the dataset and round to 2 decimal places
    st.dataframe(df.tail(10).round(2), use_container_width=True)

st.markdown("---")
st.caption("Data provided by [Yahoo Finance](https://finance.yahoo.com/). Note: Cryptocurrency markets are 24/7, while traditional markets observe weekends and holidays. Missing traditional market data on weekends is forward-filled automatically by this script.")