import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
import cachetools.func

# Thông tin kết nối
conn_info = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'stock',
    'user': 'admin',
    'password': 'admin123'
}

def get_data(query, conn_info):
    try:
        conn = psycopg2.connect(**conn_info)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f'Error: {e}')
        return pd.DataFrame()

# Caching geocoding results to avoid repeated API calls
@cachetools.func.ttl_cache(maxsize=100, ttl=86400)  # Cache for 1 day
def get_lat_lon(location):
    geolocator = Nominatim(user_agent="stock_dashboard")
    try:
        loc = geolocator.geocode(location, timeout=10)
        if loc:
            return loc.latitude, loc.longitude
        else:
            return None, None
    except GeocoderTimedOut:
        return get_lat_lon(location)
    except Exception as e:
        st.error(f"Geocoding error: {e}")
        return None, None

# Truy vấn dữ liệu từ các bảng
marketcap_query = """
SELECT "GICS_Sector", ticker, "marketCap"
FROM dim_companies;
"""

bid_ask_query = """
SELECT ticker, bid, ask
FROM dim_companies;
"""

recommendation_query = """
SELECT ticker, "recommendationMean"
FROM dim_companies
ORDER BY "recommendationMean" DESC
LIMIT 10;
"""

location_query = """
SELECT ticker, headquarters_location
FROM dim_companies;
"""

marketcap_data = get_data(marketcap_query, conn_info)
bid_ask_data = get_data(bid_ask_query, conn_info)
recommendation_data = get_data(recommendation_query, conn_info)
location_data = get_data(location_query, conn_info)

# Thiết kế dashboard trên Streamlit
st.title('Stock Dashboard')

# Chia cột Top Market Cap by Sector và Bid and Ask Prices of All Tickers
col1, col2 = st.columns(2)

with col1:
    st.header('Top Market Cap by Sector')
    if not marketcap_data.empty:
        fig_marketcap_sector = px.bar(marketcap_data, x='GICS_Sector', y='marketCap', color='ticker', title='Top Market Cap by Sector', barmode='group')
        st.plotly_chart(fig_marketcap_sector)
    else:
        st.write("No data available for Market Cap by Sector")

with col2:
    st.header('Bid and Ask Prices of All Tickers')
    if not bid_ask_data.empty:
        fig_bid_ask = go.Figure()
        for ticker in bid_ask_data['ticker'].unique():
            df = bid_ask_data[bid_ask_data['ticker'] == ticker]
            fig_bid_ask.add_trace(go.Scatter(
                x=[df['bid'].values[0], df['ask'].values[0]],
                y=[ticker, ticker],
                mode='lines+markers',
                name=ticker
            ))

        fig_bid_ask.update_layout(
            title='Bid and Ask Prices of All Tickers',
            xaxis_title='Price',
            yaxis_title='Ticker',
            showlegend=True
        )
        st.plotly_chart(fig_bid_ask)
    else:
        st.write("No data available for Bid and Ask Prices of All Tickers")

# Biểu đồ cloud word của top 10 ticker có recommendationMean lớn nhất
st.header('Ticker Word Cloud (Top 10 by Recommendation Mean)')
if not recommendation_data.empty:
    wordcloud_data = ' '.join([f"{ticker} ({rec})" for ticker, rec in zip(recommendation_data['ticker'], recommendation_data['recommendationMean'])])
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(wordcloud_data)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt)
else:
    st.write("No data available for Ticker Word Cloud")

# Heat map của các ticker trên bản đồ nước mỹ
st.header('Ticker Headquarters Heat Map')
if not location_data.empty:
    geolocator = Nominatim(user_agent="stock_dashboard")
    lat_lon_list = []
    with st.spinner('Geocoding locations...'):
        for loc in location_data['headquarters_location']:
            lat, lon = get_lat_lon(loc)
            lat_lon_list.append((lat, lon))
            time.sleep(1)  # Avoid hitting the API rate limit
            st.write(f'Geocoded: {loc} -> ({lat}, {lon})')

    location_data[['latitude', 'longitude']] = pd.DataFrame(lat_lon_list, columns=['latitude', 'longitude'])
    location_data = location_data.dropna(subset=['latitude', 'longitude'])

    if not location_data.empty:
        st.map(location_data[['latitude', 'longitude']])
    else:
        st.write("No valid location data available for heat map")
else:
    st.write("No data available for Headquarters Locations")
