from prefect import flow, task
import pandas as pd
import os
from weather_api import fetch_weather_forecast
from event_api import fetch_events_forecast_daily
from transform import validate_weather, validate_events
from load import save_to_csv
from recommendation import generate_recommendation
from upload_github import upload_to_github
from bigquery_utils import update_bigquery_data

@task
def extract():
    weather_api_key = os.getenv("WEATHER_API_KEY")
    event_api_key = os.getenv("EVENT_API_KEY")

    if not weather_api_key or not event_api_key:
        raise ValueError("Missing WEATHER_API_KEY or EVENT_API_KEY in environment variables.")

    weather_data = fetch_weather_forecast(weather_api_key)
    event_data = fetch_events_forecast_daily(event_api_key)
    return weather_data, event_data

@task
def transform(weather_data: list, event_data: list):
    weather_df = pd.DataFrame(weather_data)
    event_df = pd.DataFrame(event_data)
    weather_df["date"] = pd.to_datetime(weather_df["date"])
    event_df["event_date"] = pd.to_datetime(event_df["event_date"])

    float_columns = [
        "temperature_celsius", "feels_like", "temp_min", "temp_max",
        "humidity", "pressure", "wind_speed", "cloudiness", "precipitation_chance"
    ]
    weather_df[float_columns] = weather_df[float_columns].astype(float)
    weather_lookup = weather_df.set_index(weather_df["date"].dt.date)

    recommendations = []
    for _, row in event_df.iterrows():
        event_day = row["event_date"].date()
        today_weather = weather_lookup.loc[event_day] if event_day in weather_lookup.index else None
        if isinstance(today_weather, pd.DataFrame):
            today_weather = today_weather.dropna(subset=["weather_main"]).iloc[0] if not today_weather.dropna(subset=["weather_main"]).empty else None
        elif today_weather is not None and pd.isna(today_weather.get("weather_main", None)):
            today_weather = None

        if today_weather is not None:
            rec = generate_recommendation(
                today_weather["temperature_celsius"],
                today_weather["feels_like"],
                today_weather["humidity"],
                today_weather["wind_speed"],
                today_weather["weather_main"],
                today_weather["precipitation_chance"],
                row["venue"]
            )
        else:
            rec = "No Recommendation"

        recommendations.append(rec)

    # Add recommendation
    event_df["recommendation"] = recommendations
    
    weather_df = validate_weather(weather_df)
    event_df = validate_events(event_df)
    
    return weather_df.reset_index(drop=True), event_df.reset_index(drop=True)

@task
def load(weather_df: pd.DataFrame, event_df: pd.DataFrame):
    # Save to CSV for Streamlit
    save_to_csv(weather_df, event_df)
    
    # Update BigQuery data
    try:
        print(f"Starting BigQuery update...")
        print(f"Weather data: {len(weather_df)} rows")
        print(f"Event data: {len(event_df)} rows")
        update_bigquery_data(weather_df, event_df)
        print("✅ BigQuery update completed successfully")
    except Exception as e:
        error_msg = f"❌ Error updating BigQuery: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        # Re-raise the exception so Prefect marks the task as failed
        raise Exception(error_msg) from e

@task
def github_push():
    upload_to_github("output/weather_forecast.csv", "samantha0820/weather-event-etl", "output/weather_forecast.csv")
    upload_to_github("output/events_forecast.csv", "samantha0820/weather-event-etl", "output/events_forecast.csv")

@flow(name="Daily ETL Pipeline")
def etl_pipeline():
    weather_data, event_data = extract()
    weather_df, event_df = transform(weather_data, event_data)
    load(weather_df, event_df)
    github_push()

if __name__ == "__main__":
    etl_pipeline()