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
    Credentials are loaded from environment variables (Prefect), Streamlit secrets, or files.
    """
    # Priority 1: Try to get credentials from environment variable (for Prefect/local)
    # This should be checked FIRST to avoid triggering Streamlit secrets loading
    credentials_json = os.getenv("BQ_SERVICE_ACCOUNT_JSON")
    if credentials_json:
        try:
            # Try parsing directly first
            try:
                credentials = json.loads(credentials_json)
                return bigquery.Client.from_service_account_info(credentials)
            except json.JSONDecodeError:
                # If direct parsing fails, the \n might be literal (two chars) from YAML
                # Convert literal \n (backslash + n) to escaped \\n (backslash + backslash + n)
                # This is needed when YAML stores \n as two characters in env var
                if isinstance(credentials_json, str):
                    # Replace literal \n with escaped \\n for JSON parsing
                    credentials_json = credentials_json.replace('\\n', '\\\\n')
                    # Also handle any actual newline characters
                    credentials_json = credentials_json.replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')
                
                # Try parsing again after conversion
                credentials = json.loads(credentials_json)
                return bigquery.Client.from_service_account_info(credentials)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error parsing BQ_SERVICE_ACCOUNT_JSON: {e}")
            print(f"JSON preview (first 200 chars): {credentials_json[:200] if credentials_json else 'None'}")
    
    # Priority 2: If environment variable fails, try to read from file
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path:
        # Try different possible paths
        possible_paths = [
            creds_path,
            os.path.join(os.getcwd(), creds_path),
            os.path.join(os.getcwd(), "keys", creds_path)
        ]
        
        for path in possible_paths:
            try:
                with open(path, 'r') as f:
                    credentials = json.load(f)
                    return bigquery.Client.from_service_account_info(credentials)
            except (FileNotFoundError, json.JSONDecodeError):
                continue
    
    # Priority 3: Try to get credentials from Streamlit secrets (for Streamlit Cloud only)
    # Only try this if environment variable and file both failed
    # Check if we're actually in a Streamlit runtime before importing
    try:
        import sys
        # Only try Streamlit if we're clearly in a Streamlit context
        is_streamlit_runtime = (
            'streamlit' in sys.modules or 
            any('streamlit' in str(arg).lower() for arg in sys.argv) or
            os.getenv('STREAMLIT_SERVER_PORT') is not None
        )
        
        if is_streamlit_runtime:
            try:
                import streamlit as st
                # Use get() method instead of 'in' operator to avoid triggering secrets.toml loading
                if hasattr(st, 'secrets'):
                    try:
                        credentials_json = st.secrets.get('BQ_SERVICE_ACCOUNT_JSON')
                        if credentials_json:
                            try:
                                # Handle both string and dict formats
                                if isinstance(credentials_json, str):
                                    # Replace actual newlines with escaped newlines for JSON parsing
                                    credentials_json = credentials_json.replace('\r\n', '\\n').replace('\n', '\\n').replace('\r', '\\n')
                                    credentials = json.loads(credentials_json)
                                else:
                                    credentials = credentials_json
                                return bigquery.Client.from_service_account_info(credentials)
                            except (json.JSONDecodeError, TypeError) as e:
                                print(f"Error parsing BQ_SERVICE_ACCOUNT_JSON from Streamlit secrets: {e}")
                    except Exception:
                        # Streamlit secrets not available or error accessing, continue
                        pass
            except (ImportError, AttributeError):
                # Not running in Streamlit, continue
                pass
    except Exception:
        # Any error accessing Streamlit, continue to error message
        pass
    
    raise ValueError(
        "BigQuery credentials not found. Please make sure:\n"
        "1. In Prefect: Set BQ_SERVICE_ACCOUNT_JSON environment variable in prefect.yaml\n"
        "2. In Streamlit Cloud: Set BQ_SERVICE_ACCOUNT_JSON in Secrets\n"
        "3. Locally: Set BQ_SERVICE_ACCOUNT_JSON environment variable or GOOGLE_APPLICATION_CREDENTIALS file path"
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
    
    # Update data - delete existing records for the same dates first to avoid duplicates
    for df, table_id, schema in [
        (weather_df, "weather_forecast", get_weather_schema()),
        (event_df, "events_forecast", get_events_schema())
    ]:
        if df.empty:
            print(f"⚠️  {table_id} DataFrame is empty, skipping update")
            continue
            
        print(f"📊 Updating {table_id} with {len(df)} rows...")
        print(f"   Columns: {list(df.columns)}")
        
        try:
            # Step 1: Filter out duplicates by checking existing data in BigQuery
            # This avoids DML queries which require billing
            if table_id == "weather_forecast":
                # For weather data, filter by date
                if 'date' in df.columns:
                    # Get unique dates from new data
                    if pd.api.types.is_datetime64_any_dtype(df['date']):
                        new_dates = set(df['date'].dt.date.unique())
                    else:
                        new_dates = set(pd.to_datetime(df['date']).dt.date.unique())
                    
                    if len(new_dates) > 0:
                        # Query existing dates from BigQuery
                        date_list = ', '.join([f"DATE('{d}')" for d in sorted(new_dates)])
                        existing_query = f"""
                        SELECT DISTINCT DATE(date) as date
                        FROM `{dataset_id}.{table_id}`
                        WHERE DATE(date) IN ({date_list})
                        """
                        print(f"   🔍 Checking for existing records for {len(new_dates)} date(s)...")
                        existing_df = client.query(existing_query).result().to_dataframe()
                        
                        if not existing_df.empty:
                            # Convert existing dates to date objects
                            if pd.api.types.is_datetime64_any_dtype(existing_df['date']):
                                existing_dates = set(existing_df['date'].dt.date.unique())
                            else:
                                existing_dates = set(pd.to_datetime(existing_df['date']).dt.date.unique())
                            
                            # Filter out rows with dates that already exist
                            if pd.api.types.is_datetime64_any_dtype(df['date']):
                                df = df[~df['date'].dt.date.isin(existing_dates)]
                            else:
                                df = df[~pd.to_datetime(df['date']).dt.date.isin(existing_dates)]
                            
                            filtered_count = len(new_dates) - len(existing_dates)
                            if filtered_count > 0:
                                print(f"   ✅ Filtered out {len(existing_dates)} duplicate date(s), {filtered_count} new date(s) to insert")
                            else:
                                print(f"   ⚠️  All {len(new_dates)} date(s) already exist, skipping insert")
                                continue
            
            elif table_id == "events_forecast":
                # For events data, filter by event_date
                if 'event_date' in df.columns:
                    # Get unique dates from new data
                    if pd.api.types.is_datetime64_any_dtype(df['event_date']):
                        new_dates = set(df['event_date'].dt.date.unique())
                    else:
                        new_dates = set(pd.to_datetime(df['event_date']).dt.date.unique())
                    
                    if len(new_dates) > 0:
                        # Query existing dates from BigQuery
                        date_list = ', '.join([f"DATE('{d}')" for d in sorted(new_dates)])
                        existing_query = f"""
                        SELECT DISTINCT event_date
                        FROM `{dataset_id}.{table_id}`
                        WHERE event_date IN ({date_list})
                        """
                        print(f"   🔍 Checking for existing records for {len(new_dates)} date(s)...")
                        existing_df = client.query(existing_query).result().to_dataframe()
                        
                        if not existing_df.empty:
                            # Convert existing dates to date objects
                            if pd.api.types.is_datetime64_any_dtype(existing_df['event_date']):
                                existing_dates = set(existing_df['event_date'].dt.date.unique())
                            else:
                                existing_dates = set(pd.to_datetime(existing_df['event_date']).dt.date.unique())
                            
                            # Filter out rows with dates that already exist
                            if pd.api.types.is_datetime64_any_dtype(df['event_date']):
                                df = df[~df['event_date'].dt.date.isin(existing_dates)]
                            else:
                                df = df[~pd.to_datetime(df['event_date']).dt.date.isin(existing_dates)]
                            
                            filtered_count = len(new_dates) - len(existing_dates)
                            if filtered_count > 0:
                                print(f"   ✅ Filtered out {len(existing_dates)} duplicate date(s), {filtered_count} new date(s) to insert")
                            else:
                                print(f"   ⚠️  All {len(new_dates)} date(s) already exist, skipping insert")
                                continue
            
            # Step 2: Insert new data (only non-duplicates)
            if df.empty:
                print(f"   ⚠️  No new data to insert after filtering duplicates")
                continue
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
            
            # Verify the update
            table = client.get_table(f"{dataset_id}.{table_id}")
            print(f"✅ Inserted {len(df)} new row(s) into {dataset_id}.{table_id}")
            print(f"   Total rows in table: {table.num_rows}")
            
        except Exception as e:
            error_msg = f"❌ Error updating {dataset_id}.{table_id}: {str(e)}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            raise Exception(error_msg) from e

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