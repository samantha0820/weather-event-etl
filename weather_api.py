import requests
import pandas as pd
import os

def fetch_weather_forecast(api_key, city="New York"):
    if api_key.startswith("${") and api_key.endswith("}"):
        env_var_name = api_key[2:-1]
        api_key = os.getenv(env_var_name)
        if not api_key:
            raise ValueError(f"Missing environment variable: {env_var_name}")
    # Fetch current weather data for today
    current_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    current_response = requests.get(current_url)
    current_response.raise_for_status()
    current_data = current_response.json()

    # Create a dictionary for current weather metrics
    current_weather = {
        "date": pd.to_datetime("today").normalize(),  # Today's date at midnight
        "temperature_celsius": current_data["main"]["temp"],
        "feels_like": current_data["main"]["feels_like"],
        "temp_min": current_data["main"]["temp_min"],
        "temp_max": current_data["main"]["temp_max"],
        "humidity": current_data["main"]["humidity"],
        "pressure": current_data["main"]["pressure"],
        "wind_speed": current_data["wind"]["speed"],
        "cloudiness": current_data["clouds"]["all"],
        "precipitation_chance": current_data.get("rain", {}).get("1h", 0) / 100,
        "weather_main": current_data["weather"][0]["main"],
        "weather_description": current_data["weather"][0]["description"]
    }

    # Fetch 5-day forecast data (3-hour intervals)
    forecast_url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric"
    forecast_response = requests.get(forecast_url)
    forecast_response.raise_for_status()
    forecast_data = forecast_response.json()

    # Select only the forecast entries at 12:00 PM each day
    forecast_list = forecast_data["list"]
    daily_forecast = []
    for entry in forecast_list:
        timestamp = pd.to_datetime(entry["dt_txt"])
        if timestamp.hour == 12:
            daily_forecast.append({
                "date": timestamp.normalize(),  # Forecast date at midnight
                "temperature_celsius": entry["main"]["temp"],
                "feels_like": entry["main"]["feels_like"],
                "temp_min": entry["main"]["temp_min"],
                "temp_max": entry["main"]["temp_max"],
                "humidity": entry["main"]["humidity"],
                "pressure": entry["main"]["pressure"],
                "wind_speed": entry["wind"]["speed"],
                "cloudiness": entry["clouds"]["all"],
                "precipitation_chance": entry.get("rain", {}).get("3h", 0) / 100,
                "weather_main": entry["weather"][0]["main"],
                "weather_description": entry["weather"][0]["description"]
            })

    # Combine current weather with daily forecasts
    weather_df = pd.DataFrame([current_weather] + daily_forecast)

    # Filter to only include today through 4 days ahead
    today = pd.to_datetime("today").normalize()
    end_date = today + pd.Timedelta(days=4)
    weather_df = weather_df[
        (weather_df["date"] >= today) & (weather_df["date"] <= end_date)
    ].reset_index(drop=True)

    return weather_df