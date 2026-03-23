import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf
import pandas as pd
import plotly.express as px
import json

# Set up the Streamlit page configuration
st.set_page_config(page_title="Multi-Asset Live Dashboard", layout="wide")

st.title("📈 Multi-Asset Live Dashboard (TradingView Edition)")
st.markdown("Compare **Nifty 50, Crude Oil, Gold, Silver, and Bitcoin (BTC)** using live data from Yahoo Finance.")

# Sidebar for user inputs
st.sidebar.header("Settings")

period = st.sidebar.selectbox(
    "Select Time Period", 
    options=["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"], 
    index=5 
)

show_ema = st.sidebar.checkbox("📉 Show EMA 21", value=False, help="Plots the 21-period Exponential Moving Average for all assets.")

# REFRESH BUTTON
if st.sidebar.button('🔄 Refresh Live Data'):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared! Fetching latest prices...")

TICKERS = {
    'Nifty 50': '^NSEI',
    'Crude Oil': 'CL=F',
    'Gold': 'GC=F',
    'Silver': 'SI=F',
    'Bitcoin': 'BTC-USD'
}

# TV Chart Colors
COLORS = {
    'Nifty 50': '#2962FF',     
    'Crude Oil': '#E91E63',    
    'Gold': '#FF9800',         
    'Silver': '#9C27B0',       
    'Bitcoin': '#00BCD4'       
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
            
            if not hist.empty and 'Close' in hist.columns:
                s = hist['Close'].copy()
                s.name = name
                # Force index to strictly UTC Datetime BEFORE merging
                s.index = pd.to_datetime(s.index, utc=True)
                series_list.append(s)
        except Exception:
            continue
            
    if not series_list:
        return pd.DataFrame()
        
    df = pd.concat(series_list, axis=1)
    
    # Clean duplicates, sort chronologically, drop purely empty cols, and fill missing limits safely
    df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df = df.ffill().bfill()
    
    # We DO NOT remove the timezone here anymore. TradingView requires UTC Datetime 
    # to perfectly calculate UNIX timestamps. TV will convert to Local Time automatically!
    return df

# Load the data
with st.spinner('Fetching live market data... (This takes a few seconds)'):
    df = load_data(period)

if df.empty:
    st.error("⚠️ Failed to fetch data. Yahoo Finance might be blocking requests or the market data is currently unavailable.")
    st.stop()

st.success('Data loaded successfully!')

# --- PREPARE TRADINGVIEW DATA ---
st.subheader("1. Relative Performance Chart (TradingView Lightweight)")
st.markdown("*This chart normalizes all assets to **100** at the start. The dashed lines represent the EMA 21.*")

# Normalize data to base 100
normalized_df = (df / df.iloc[0]) * 100

# Calculate EMA 21
ema_df = normalized_df.ewm(span=21, adjust=False).mean()

# Format Data for TradingView JS
tv_data = {}
tv_ema_data = {}

for col in normalized_df.columns:
    price_series =[]
    ema_series = []
    
    for ts, val in normalized_df[col].items():
        # Prevent inf/NaN from breaking Javascript JSON parsing
        if pd.notna(val) and val != float('inf') and val != float('-inf'):
            price_series.append({"time": int(ts.timestamp()), "value": float(val)})
            
    for ts, val in ema_df[col].items():
        if pd.notna(val) and val != float('inf') and val != float('-inf'):
            ema_series.append({"time": int(ts.timestamp()), "value": float(val)})
            
    tv_data[col] = price_series
    tv_ema_data[col] = ema_series

chart_data_json = json.dumps(tv_data)
ema_data_json = json.dumps(tv_ema_data)
colors_json = json.dumps(COLORS)
show_ema_js = "true" if show_ema else "false"

# Generate HTML/JS
html_string = f"""
<!DOCTYPE html>
<html>
<head>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body {{ margin: 0; padding: 0; background-color: #131722; color: white; font-family: sans-serif; }}
        #container {{ position: relative; width: 100%; height: 500px; }}
        #tvchart {{ position: absolute; width: 100%; height: 100%; }}
        .legend {{ position: absolute; top: 10px; left: 10px; z-index: 10; font-size: 14px; background: rgba(19, 23, 34, 0.8); padding: 5px; border-radius: 5px; pointer-events: none; }}
        .legend-item {{ display: flex; align-items: center; margin-bottom: 4px; }}
        .color-box {{ width: 12px; height: 12px; margin-right: 8px; border-radius: 2px; }}
        #error-box {{ color: #ff5252; padding: 20px; display: none; font-size: 16px; }}
    </style>
</head>
<body>
    <div id="error-box"></div>
    <div id="container">
        <div id="tvchart"></div>
        <div class="legend" id="legend"></div>
    </div>
    
    <script>
        // Global Error handler to visibly show if something breaks
        window.onerror = function(message) {{
            document.getElementById('container').style.display = 'none';
            const errBox = document.getElementById('error-box');
            errBox.style.display = 'block';
            errBox.innerHTML = "<b>Chart Render Error:</b> " + message + "<br><br>Try clicking 'Refresh Live Data' on the sidebar.";
        }};

        const chartData = {chart_data_json};
        const emaData = {ema_data_json};
        const colors = {colors_json};
        const showEma = {show_ema_js};

        const chartProperties = {{
            layout: {{ background: {{ type: 'solid', color: '#131722' }}, textColor: '#d1d4dc' }},
            grid: {{ vertLines: {{ color: '#2B2B43' }}, horzLines: {{ color: '#2B2B43' }} }},
            timeScale: {{ timeVisible: true, secondsVisible: false }},
            rightPriceScale: {{ scaleMargins: {{ top: 0.1, bottom: 0.1 }} }}
        }};

        const chart = LightweightCharts.createChart(document.getElementById('tvchart'), chartProperties);
        const legend = document.getElementById('legend');
        
        Object.keys(chartData).forEach(asset => {{
            // ONLY plot if the asset successfully downloaded data
            if (chartData[asset].length > 0) {{
                const lineSeries = chart.addLineSeries({{
                    color: colors[asset],
                    lineWidth: 2,
                    title: asset
                }});
                lineSeries.setData(chartData[asset]);
                
                // Add EMA if toggled
                if (showEma && emaData[asset] && emaData[asset].length > 0) {{
                    const emaSeries = chart.addLineSeries({{
                        color: colors[asset],
                        lineWidth: 1,
                        lineStyle: 2, // 2 = Dashed line (Hardcoded integer prevents undefined enum errors)
                        title: asset + ' EMA 21'
                    }});
                    emaSeries.setData(emaData[asset]);
                }}

                // Add to custom HTML legend
                legend.innerHTML += `
                    <div class="legend-item">
                        <div class="color-box" style="background-color: ${{colors[asset]}}"></div>
                        ${{asset}}
                    </div>
                `;
            }}
        }});

        chart.timeScale().fitContent();

        // Handle window resize dynamically
        window.addEventListener('resize', () => {{
            chart.applyOptions({{ width: document.getElementById('container').clientWidth }});
        }});
    </script>
</body>
</html>
"""

# Render the TradingView Chart component in Streamlit
components.html(html_string, height=500)


# --- CORRELATION CHART SECTION ---
st.subheader("2. Correlation Heatmap")

corr_type = st.radio(
    "Select Correlation Method:",["Returns Correlation (Recommended)", "Absolute Price Correlation"],
    horizontal=True
)

# Temporarily remove timezones just for the Plotly Heatmap so it doesn't complain
df_plotly = df.copy()
df_plotly.index = df_plotly.index.tz_convert('Asia/Kolkata').tz_localize(None)

if corr_type == "Returns Correlation (Recommended)":
    corr_matrix = df_plotly.pct_change().corr().fillna(0)
else:
    corr_matrix = df_plotly.corr().fillna(0)

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
st.subheader("3. Latest Live Market Prices (IST)")
with st.expander("Click to view the raw data table"):
    st.dataframe(df_plotly.tail(15).iloc[::-1].round(2), use_container_width=True)

st.markdown("---")
st.caption("Data provided by [Yahoo Finance](https://finance.yahoo.com/). Note: Different markets have different trading hours. When a market (like Nifty) is closed but Crypto is open, the script carries forward the last traded price to keep the correlation math intact.")