from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import os

def init_bigquery_tables():
    """
    Initialize BigQuery tables for weather and events data.
    This script will create the dataset and tables if they don't exist.
    """
    # Initialize BigQuery client
    client = bigquery.Client()
    dataset_id = "weather_events"
    
    # Create dataset if it doesn't exist
    try:
        dataset_ref = client.dataset(dataset_id)
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")
    
    # Define schemas
    weather_schema = [
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
    
    events_schema = [
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
    
    # Create tables
    tables = [
        ("weather_forecast", weather_schema),
        ("events_forecast", events_schema)
    ]
    
    for table_id, schema in tables:
        table_ref = dataset_ref.table(table_id)
        try:
            client.get_table(table_ref)
            print(f"Table {dataset_id}.{table_id} already exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {dataset_id}.{table_id}")

if __name__ == "__main__":
    # Check if Google Cloud credentials are set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("Example: export GOOGLE_APPLICATION_CREDENTIALS=\"/path/to/your/service-account-key.json\"")
        exit(1)
    
    try:
        init_bigquery_tables()
        print("\nBigQuery tables initialized successfully!")
        print("\nYou can now run your ETL pipeline to start loading data.")
    except Exception as e:
        print(f"Error initializing BigQuery tables: {str(e)}") 