import requests
import pandas as pd

def fetch_weather_forecast(api_key, city="New York"):
    # --- 抓今天 (current weather)
    current_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    current_response = requests.get(current_url)
    current_response.raise_for_status()
    current_data = current_response.json()

    current_weather = {
        "date": pd.to_datetime("today").normalize(),
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

    # --- 抓未來5天 (forecast)
    forecast_url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric"
    forecast_response = requests.get(forecast_url)
    forecast_response.raise_for_status()
    forecast_data = forecast_response.json()

    # forecast每3小時一筆 → 取每天中午12:00的預測
    forecast_list = forecast_data["list"]
    daily_forecast = []

    for entry in forecast_list:
        timestamp = pd.to_datetime(entry["dt_txt"])
        if timestamp.hour == 12:  # 選中午12點的預報（比較穩）
            daily_forecast.append({
                "date": timestamp.normalize(),
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

    # --- 組成 DataFrame
    weather_df = pd.DataFrame([current_weather] + daily_forecast)

    today = pd.to_datetime("today").normalize()
    start_date = today
    end_date = today + pd.Timedelta(days=4)
    weather_df = weather_df[
        (weather_df["date"] >= start_date) & (weather_df["date"] <= end_date)
    ].reset_index(drop=True)

    return weather_df