import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema

weather_schema = DataFrameSchema({
    "date": Column(pa.DateTime),
    "temperature_celsius": Column(pa.Float),
    "feels_like": Column(pa.Float),
    "temp_min": Column(pa.Float),
    "temp_max": Column(pa.Float),
    "humidity": Column(pa.Float),
    "pressure": Column(pa.Float),
    "wind_speed": Column(pa.Float),
    "cloudiness": Column(pa.Float),
    "precipitation_chance": Column(pa.Float, checks=pa.Check.greater_than_or_equal_to(0)),
    "weather_main": Column(pa.String),
    "weather_description": Column(pa.String)
})

event_schema = DataFrameSchema({
    "event_name": Column(pa.String),
    "event_date": Column(pa.DateTime),
    "event_time": Column(pa.String),
    "event_url": Column(pa.String, nullable=True),
    "image_url": Column(pa.String, nullable=True),
    "venue": Column(pa.String),
    "address": Column(pa.String),
    "city": Column(pa.String),
    "price_min": Column(pa.Float, nullable=True),
    "price_max": Column(pa.Float, nullable=True),
    "category": Column(pa.String),
    "free_or_paid": Column(pa.String, checks=pa.Check.isin(["Free", "Paid"])),
    "status": Column(pa.String, checks=pa.Check.isin([
        "scheduled", "cancelled", "postponed", "onsale", "offsale", "rescheduled", "closed", "moved"
    ]))
})

def validate_weather(df):
    return weather_schema.validate(df)

def validate_events(df):
    return event_schema.validate(df)