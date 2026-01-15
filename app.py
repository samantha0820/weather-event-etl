# app.py

import streamlit as st
import pandas as pd
import os
from bigquery_utils import query_bigquery

# --- Page Config ---
st.set_page_config(
    page_title="5-Day Weather & Event Recommendations",
    layout="wide"
)

# --- Data Source Selection ---
data_source = st.sidebar.radio(
    "Data Source:",
    ["CSV Files", "BigQuery"],
    help="Choose where to load data from"
)

# --- Clear Cache Button (for BigQuery) ---
if data_source == "BigQuery":
    if st.sidebar.button("🔄 Refresh Data", help="Clear cache and reload from BigQuery"):
        st.cache_data.clear()
        st.rerun()

# --- Load Data from BigQuery ---
@st.cache_data(ttl=300)  # cache for 5 minutes (shorter cache for fresher data)
def load_data_from_bigquery():
    """Load data from BigQuery"""
    try:
        # Query weather data (latest 5 days) - use full project ID
        weather_query = """
        SELECT 
            date,
            temperature_celsius,
            feels_like,
            temp_min,
            temp_max,
            humidity,
            pressure,
            wind_speed,
            cloudiness,
            precipitation_chance,
            weather_main,
            weather_description
        FROM `ds5500-459222.weather_events.weather_forecast`
        WHERE date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY)
        ORDER BY date ASC
        """
        
        # Query events data (latest 5 days) - use full project ID
        events_query = """
        SELECT 
            event_name,
            event_date,
            event_time,
            venue,
            address,
            city,
            price_min,
            price_max,
            category,
            free_or_paid,
            status,
            event_url,
            image_url,
            recommendation
        FROM `ds5500-459222.weather_events.events_forecast`
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY)
        ORDER BY event_date ASC, event_time ASC
        """
        
        weather_df = query_bigquery(weather_query)
        events_df = query_bigquery(events_query)
        
        # Convert date columns
        weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date
        events_df["event_date"] = pd.to_datetime(events_df["event_date"]).dt.date
        
        return weather_df, events_df, "bigquery"
    except Exception as e:
        st.error(f"Error loading data from BigQuery: {str(e)}")
        st.info("Make sure you have set up BigQuery credentials and the tables exist.")
        return None, None, "error"

# --- Load Data from CSV ---
@st.cache_data(ttl=3600)  # cache for 1 hr
def load_data_from_csv():
    """Load data from CSV files"""
    # Try to load from local files first (for local development)
    try:
        weather = pd.read_csv("output/weather_forecast.csv")
        event = pd.read_csv("output/events_forecast.csv")
        weather["date"] = pd.to_datetime(weather["date"]).dt.date
        event["event_date"] = pd.to_datetime(event["event_date"]).dt.date
        return weather, event, "local"
    except FileNotFoundError:
        # If local files don't exist, load from GitHub (for Streamlit Cloud deployment)
        try:
            github_base_url = "https://raw.githubusercontent.com/samantha0820/weather-event-etl/main/output"
            weather = pd.read_csv(f"{github_base_url}/weather_forecast.csv")
            event = pd.read_csv(f"{github_base_url}/events_forecast.csv")
            weather["date"] = pd.to_datetime(weather["date"]).dt.date
            event["event_date"] = pd.to_datetime(event["event_date"]).dt.date
            return weather, event, "github"
        except Exception as e:
            st.error(f"Error loading data from CSV: {str(e)}")
            return None, None, "error"

# --- Load Data Based on Selection ---
if data_source == "BigQuery":
    weather_df, event_df, data_source_name = load_data_from_bigquery()
    if data_source_name == "error":
        st.stop()
else:
    weather_df, event_df, data_source_name = load_data_from_csv()
    if data_source_name == "error":
        st.stop()

# --- Sidebar: Show Data Source Info ---
st.sidebar.divider()
st.sidebar.write(f"**Data Source:** {data_source_name.upper()}")
if data_source_name == "local":
    try:
        last_update_time = max(
            os.path.getmtime("output/weather_forecast.csv"),
            os.path.getmtime("output/events_forecast.csv")
        )
        st.sidebar.write(f"Last updated: {pd.to_datetime(last_update_time, unit='s')}")
    except FileNotFoundError:
        st.sidebar.write("Data not found.")
elif data_source_name == "github":
    st.sidebar.write("Data loaded from GitHub")
    st.sidebar.write("Auto-updates when ETL pipeline runs")
elif data_source_name == "bigquery":
    st.sidebar.write("Data loaded from BigQuery")
    st.sidebar.write(f"Records: {len(weather_df)} weather, {len(event_df)} events")
    try:
        # Get latest date from data
        if not weather_df.empty:
            latest_weather_date = weather_df["date"].max()
            st.sidebar.write(f"Latest weather: {latest_weather_date}")
        if not event_df.empty:
            latest_event_date = event_df["event_date"].max()
            st.sidebar.write(f"Latest events: {latest_event_date}")
    except Exception:
        pass

# --- App Title ---
st.title("5-Day Weather & Event Recommendations")

# --- Weather Section ---
st.header("Weather Summary")

# Ensure date is in the correct format
if isinstance(weather_df["date"].iloc[0], str):
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date
available_weather_dates = sorted(weather_df["date"].unique())

selected_weather_date = st.selectbox(
    "Select a Date to View Weather:",
    available_weather_dates,
    key="weather_select"
)

selected_weather = weather_df[weather_df["date"] == selected_weather_date].iloc[0]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Temperature (°C)", f"{selected_weather['temperature_celsius']} (Feels like {selected_weather['feels_like']})")
    st.metric("Humidity (%)", selected_weather['humidity'])
with col2:
    st.metric("Pressure (hPa)", selected_weather['pressure'])
    st.metric("Wind Speed (m/s)", selected_weather['wind_speed'])
with col3:
    st.metric("Cloudiness (%)", selected_weather['cloudiness'])
    st.metric("Chance of Rain", f"{selected_weather['precipitation_chance'] * 100:.0f}%")

st.write(f"**Weather:** {selected_weather['weather_main']} - {selected_weather['weather_description']}")

st.divider()

# --- Events Section ---
st.header("5-Day Events")

# Ensure event_date is in the correct format
if isinstance(event_df["event_date"].iloc[0], str):
    event_df["event_date"] = pd.to_datetime(event_df["event_date"]).dt.date
elif hasattr(event_df["event_date"].iloc[0], 'date'):
    event_df["event_date"] = event_df["event_date"].dt.date
available_dates = sorted(event_df["event_date"].unique())

selected_date = st.selectbox("Select a Date for Events:", available_dates)
recommendation_filter = st.selectbox(
    "Filter by Recommendation:",
    ["All", "Recommended (Indoor)", "Recommended (Outdoor)", "Recommended (Indoor OK)", "Not Recommended (Outdoor)"]
)

filtered_df = event_df[event_df["event_date"] == selected_date]

if recommendation_filter != "All":
    filtered_df = filtered_df[filtered_df["recommendation"] == recommendation_filter]

filtered_df = filtered_df.sort_values(by="event_time")

st.subheader(f"Events on {selected_date.strftime('%B %d, %Y')}")

if filtered_df.empty:
    st.info("No events available for this date.")
else:
    for _, row in filtered_df.iterrows():
        with st.container():
            image_col, detail_col = st.columns([1, 2])
            with image_col:
                if pd.notna(row.get("image_url")):
                    st.image(row["image_url"], width=180)
                else:
                    st.write("(No Image Available)")
            with detail_col:
                st.markdown(f"### [{row['event_name']}]({row['event_url']})")
                st.write(f"**Venue:** {row['venue']}, {row['city']}")
                st.write(f"**Time:** {row['event_time']}")
                price_text = (
                    f"${row['price_min']:.2f} - ${row['price_max']:.2f}"
                    if pd.notna(row['price_min']) and pd.notna(row['price_max']) else "N/A"
                )
                st.write(f"**Price:** {price_text}")
                st.write(f"**Recommendation:** {row['recommendation']}")
            st.divider()

# --- Footer ---
st.caption("\u00a9 Generated by Samantha Wang")