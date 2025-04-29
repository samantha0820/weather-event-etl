# etl_pipeline.py

from prefect import flow, task
import pandas as pd
from weather_api import fetch_weather_forecast
from event_api import fetch_events_forecast_daily
from transform import validate_weather, validate_events
from load import save_to_csv
from recommendation import generate_recommendation
import subprocess


@task
def extract(api_keys: dict):
    weather_data = fetch_weather_forecast(api_keys["weather"])
    event_data = fetch_events_forecast_daily(api_keys["event"])
    return weather_data, event_data

@task
def transform(weather_data: list, event_data: list):
    weather_df = pd.DataFrame(weather_data)
    event_df = pd.DataFrame(event_data)

    weather_df["date"] = pd.to_datetime(weather_df["date"])
    event_df["event_date"] = pd.to_datetime(event_df["event_date"])

    float_columns = ["temperature_celsius", "feels_like", "temp_min", "temp_max", "humidity", "pressure", "wind_speed", "cloudiness", "precipitation_chance"]
    weather_df[float_columns] = weather_df[float_columns].astype(float)

    weather_lookup = weather_df.set_index(weather_df["date"].dt.date)

    recommendations = []
    for _, row in event_df.iterrows():
        event_day = row["event_date"].date()

        if event_day in weather_lookup.index:
            today_weather = weather_lookup.loc[event_day]

            # In case multiple rows per day, pick first valid
            if isinstance(today_weather, pd.DataFrame):
                today_weather = today_weather.dropna(subset=["weather_main"]).iloc[0] if not today_weather.dropna(subset=["weather_main"]).empty else None
            else:
                today_weather = today_weather if pd.notna(today_weather.get("weather_main", None)) else None

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
        else:
            rec = "No Recommendation"
        recommendations.append(rec)

    event_df["recommendation"] = recommendations

    weather_df = validate_weather(weather_df)
    event_df = validate_events(event_df)

    return weather_df.reset_index(drop=True), event_df.reset_index(drop=True)

@task
def load(weather_df: pd.DataFrame, event_df: pd.DataFrame):
    save_to_csv(weather_df, event_df)


@task
def git_push():
    try:
        # Add updated files
        subprocess.run(["git", "add", "output/weather_forecast.csv", "output/events_forecast.csv"], check=True)

        # Commit with a standard message
        subprocess.run(["git", "commit", "-m", "Daily ETL update - auto commit"], check=True)

        # Pull remote updates first (rebase) and auto-resolve trivial conflicts
        subprocess.run(["git", "pull", "--rebase", "--strategy-option=theirs"], check=True)

        # Push to remote
        subprocess.run(["git", "push"], check=True)

    except subprocess.CalledProcessError as e:
        print(f"Git operation failed: {e}. Maybe nothing to commit or conflict too complex.")

@flow(name="Daily ETL Pipeline")
def etl_pipeline(api_keys):
    weather_data, event_data = extract(api_keys)
    weather_df, event_df = transform(weather_data, event_data)
    load(weather_df, event_df)
    git_push()

if __name__ == "__main__":
    keys = {
        "weather": "e5b41a5fc18b2a3b2cb1e0bdc638ee14",
        "event": "OXzVpMzUq7Dn8JtVnysPuAGNFMNmdYtF"
    }
    etl_pipeline(keys)