import pandas as pd
import os

def save_to_csv(weather_df, event_df, path_prefix="weather-event-etl/output"):
    os.makedirs(path_prefix, exist_ok=True)
    weather_df.to_csv(f"{path_prefix}/weather_forecast.csv", index=False)
    event_df.to_csv(f"{path_prefix}/events_forecast.csv", index=False)