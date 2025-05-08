# Weather & Event ETL Pipeline

This project demonstrates an automated ETL pipeline that fetches weather forecasts and event listings, matches them by date, and generates event recommendations based on weather comfort. Final results are saved as CSV files, uploaded to GitHub, and stored in Google BigQuery.

## Features

- **Weather Forecast Fetching**: Uses OpenWeatherMap API for current and 5-day forecasts.
- **Event Data Fetching**: Uses Ticketmaster API to retrieve upcoming events in New York City.
- **Data Validation**: Ensures data integrity using Pandera.
- **Recommendation Logic**: Matches events with weather forecasts and generates textual recommendations.
- **Automation**: Orchestrated via Prefect Cloud with daily scheduled runs.
- **Auto GitHub Upload**: Uploads latest CSV outputs directly to a GitHub repository via GitHub API.
- **BigQuery Integration**: Stores all data in Google BigQuery for analytics and long-term storage.

## Directory Structure

```
weather-event-etl/
├── etl_pipeline.py             # Main Prefect flow
├── event_api.py                # Ticketmaster data extraction
├── weather_api.py              # Weather API extraction
├── recommendation.py          # Comfort scoring and recommendation
├── transform.py               # Pandera data validation
├── load.py                    # Save output CSVs
├── upload_github.py          # Upload to GitHub using API
├── bigquery_utils.py         # BigQuery utilities and schema definitions
├── init_bigquery.py          # BigQuery table initialization
├── output/
│   ├── events_forecast.csv
│   └── weather_forecast.csv
├── .gitignore
├── prefect.yaml               # Prefect deployment spec
└── README.md
```

## Setup

### 1. Environment Setup

```bash
conda create -n mynewenv python=3.12
conda activate mynewenv
pip install -r requirements.txt
```

### 2. Environment Variables

Create a `.env` file or export manually:

```bash
export WEATHER_API_KEY="your_openweather_key"
export EVENT_API_KEY="your_ticketmaster_key"
export GITHUB_TOKEN="your_github_pat"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

### 3. Prefect Setup

```bash
prefect cloud login
prefect profile use cloud
prefect work-pool create default-agent-pool --type process
prefect deploy --all
```

Set environment variables for deployments inside `prefect.yaml`:

```yaml
job_variables:
  env:
    WEATHER_API_KEY: ${WEATHER_API_KEY}
    EVENT_API_KEY: ${EVENT_API_KEY}
    GITHUB_TOKEN: ${GITHUB_TOKEN}
    GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
```

### 4. BigQuery Setup

1. Create a Google Cloud project and enable BigQuery API
2. Create a service account and download the JSON key file
3. Initialize BigQuery tables:
```bash
python init_bigquery.py
```

## Data Flow

1. **Extract**: Fetches weather and event data from respective APIs
2. **Transform**: 
   - Validates data using Pandera schemas
   - Generates weather-based recommendations for events
   - Prepares data for both CSV and BigQuery storage
3. **Load**:
   - Saves data to CSV files for Streamlit app
   - Uploads CSV files to GitHub
   - Stores data in BigQuery tables

## BigQuery Schema

### Weather Forecast Table
- date (TIMESTAMP)
- temperature_celsius (FLOAT64)
- feels_like (FLOAT64)
- temp_min (FLOAT64)
- temp_max (FLOAT64)
- humidity (FLOAT64)
- pressure (FLOAT64)
- wind_speed (FLOAT64)
- cloudiness (FLOAT64)
- precipitation_chance (FLOAT64)
- weather_main (STRING)
- weather_description (STRING)

### Events Forecast Table
- event_name (STRING)
- event_date (DATE)
- event_time (STRING)
- venue (STRING)
- address (STRING)
- city (STRING)
- price_min (FLOAT64)
- price_max (FLOAT64)
- category (STRING)
- free_or_paid (STRING)
- status (STRING)
- event_url (STRING)
- image_url (STRING)
- recommendation (STRING)

## Running the Pipeline

```bash
python etl_pipeline.py
```

This will:
1. Fetch latest weather and event data
2. Generate recommendations
3. Save data to CSV files
4. Upload to GitHub
5. Update BigQuery tables

## Deployment via Prefect

To deploy the pipeline:

1. Register your flow:

```bash
prefect deployment build etl_pipeline.py:etl_pipeline -n daily-weather-event-pipeline
```

2. Deploy and schedule:

```bash
prefect deploy --all
```

3. Run via Prefect Cloud or manually:

```bash
prefect deployment run 'Daily ETL Pipeline/daily-weather-event-pipeline'
```

## Recommendation Logic

Events are categorized based on daily weather:

- **Comfortable** → Recommended (Outdoor)
- **Moderate** → Recommended (Indoor OK)
- **Uncomfortable** → Recommended (Indoor) or Not Recommended

## Streamlit Dashboard

A Streamlit app is deployed to visualize event recommendations and weather summaries interactively.

**Access the app here:** [https://samantha0820-weather-etl.streamlit.app/](https://samantha0820-weather-etl.streamlit.app/)

The app displays:
- Top events in New York City for the next 5 days
- Associated weather conditions
- Recommendation tag (Outdoor / Indoor / Not Recommended)

## GitHub Upload

Files are uploaded to GitHub using the [Contents API](https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28):

- `output/weather_forecast.csv`
- `output/events_forecast.csv`

## Notes

- All credentials are securely passed as environment variables.
- You must **not commit** `.env` or `prefect.yaml` with real secrets.
- `prefect.yaml` is `.gitignore` after deployment.

## License

MIT License © Samantha Wang