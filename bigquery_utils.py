from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

def get_bigquery_client():
    """
    Create and return a BigQuery client.
    Credentials are loaded from the .env file or Prefect deployment.
    """
    # Check if credentials file exists
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set."
        )
    
    # Try to read the credentials file
    try:
        with open(creds_path, 'r') as f:
            credentials = json.load(f)
            return bigquery.Client.from_service_account_info(credentials)
    except FileNotFoundError:
        # If file not found, try to read from Prefect's file deployment
        try:
            with open('bq_service_account.json', 'r') as f:
                credentials = json.load(f)
                return bigquery.Client.from_service_account_info(credentials)
        except FileNotFoundError:
            raise ValueError(
                f"BigQuery credentials file not found at {creds_path} or bq_service_account.json. "
                "Please make sure the credentials file exists and GOOGLE_APPLICATION_CREDENTIALS "
                "is set correctly."
            )

def get_weather_schema():
    """
    Returns the schema for the weather data table.
    """
    return [
        bigquery.SchemaField("date", "TIMESTAMP"),
        bigquery.SchemaField("temperature_celsius", "FLOAT64"),
        bigquery.SchemaField("feels_like", "FLOAT64"),
        bigquery.SchemaField("temp_min", "FLOAT64"),
        bigquery.SchemaField("temp_max", "FLOAT64"),
        bigquery.SchemaField("humidity", "FLOAT64"),
        bigquery.SchemaField("pressure", "FLOAT64"),
        bigquery.SchemaField("wind_speed", "FLOAT64"),
        bigquery.SchemaField("cloudiness", "FLOAT64"),
        bigquery.SchemaField("precipitation_chance", "FLOAT64"),
        bigquery.SchemaField("weather_main", "STRING"),
        bigquery.SchemaField("weather_description", "STRING"),
    ]

def get_events_schema():
    """
    Returns the schema for the events data table.
    """
    return [
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_date", "DATE"),
        bigquery.SchemaField("event_time", "STRING"),
        bigquery.SchemaField("venue", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("price_min", "FLOAT64"),
        bigquery.SchemaField("price_max", "FLOAT64"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("free_or_paid", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("event_url", "STRING"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("recommendation", "STRING"),
    ]

def update_bigquery_data(weather_df: pd.DataFrame, event_df: pd.DataFrame, dataset_id: str = "weather_events"):
    """
    Update BigQuery tables with new data. This function will:
    1. Create the dataset and tables if they don't exist
    2. Append new data to the existing tables
    
    Args:
        weather_df: DataFrame containing weather data
        event_df: DataFrame containing event data
        dataset_id: ID of the BigQuery dataset
    """
    client = get_bigquery_client()
    
    # Create dataset if it doesn't exist
    try:
        dataset_ref = client.dataset(dataset_id)
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    
    # Create tables if they don't exist
    for table_id, schema in [
        ("weather_forecast", get_weather_schema()),
        ("events_forecast", get_events_schema())
    ]:
        table_ref = dataset_ref.table(table_id)
        try:
            client.get_table(table_ref)
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {dataset_id}.{table_id}")
    
    # Update data
    for df, table_id, schema in [
        (weather_df, "weather_forecast", get_weather_schema()),
        (event_df, "events_forecast", get_events_schema())
    ]:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        
        job = client.load_table_from_dataframe(
            df, 
            f"{dataset_id}.{table_id}", 
            job_config=job_config
        )
        job.result()  # Wait for the job to complete
        print(f"Updated {len(df)} rows in {dataset_id}.{table_id}")

def query_bigquery(query: str):
    """
    Execute a query on BigQuery and return results as a pandas DataFrame.
    
    Args:
        query: SQL query string
    
    Returns:
        pandas DataFrame containing query results
    """
    client = get_bigquery_client()
    query_job = client.query(query)
    return query_job.result().to_dataframe()

# Example usage:
if __name__ == "__main__":
    # Example query to get the latest weather and events
    query = """
    WITH latest_weather AS (
        SELECT *
        FROM `weather_events.weather_forecast`
        WHERE date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    ),
    latest_events AS (
        SELECT *
        FROM `weather_events.events_forecast`
        WHERE event_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    )
    SELECT 
        w.date,
        w.temperature_celsius,
        w.weather_main,
        e.event_name,
        e.venue,
        e.recommendation
    FROM latest_weather w
    JOIN latest_events e
    ON DATE(w.date) = DATE(e.event_date)
    ORDER BY w.date DESC
    """
    
    results = query_bigquery(query)
    print(results) 