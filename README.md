# Weather & Event ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline that collects weather forecasts and event data for New York City, processes them, and stores them in both CSV files and BigQuery tables.

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

1. Clone the repository:
```bash
git clone https://github.com/samantha0820/weather-event-etl.git
cd weather-event-etl
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file in the project root with the following variables:
```env
WEATHER_API_KEY=your_weather_api_key
EVENT_API_KEY=your_event_api_key
GITHUB_TOKEN=your_github_token
GOOGLE_APPLICATION_CREDENTIALS=keys/bq_service_account.json
BQ_SERVICE_ACCOUNT_JSON=your_bigquery_service_account_json
```

## BigQuery Setup

1. Create a BigQuery dataset named `weather_events` in your Google Cloud project.

2. The pipeline will automatically create two tables in the dataset:
   - `weather_forecast`: Stores weather forecast data
   - `events_forecast`: Stores event data

3. Table Schemas:

   **Weather Forecast Table:**
   ```sql
   date TIMESTAMP
   temperature_celsius FLOAT64
   feels_like FLOAT64
   temp_min FLOAT64
   temp_max FLOAT64
   humidity FLOAT64
   pressure FLOAT64
   wind_speed FLOAT64
   cloudiness FLOAT64
   precipitation_chance FLOAT64
   weather_main STRING
   weather_description STRING
   ```

   **Events Forecast Table:**
   ```sql
   event_name STRING
   event_date DATE
   event_time STRING
   venue STRING
   address STRING
   city STRING
   price_min FLOAT64
   price_max FLOAT64
   category STRING
   free_or_paid STRING
   status STRING
   event_url STRING
   image_url STRING
   recommendation STRING
   ```

## Prefect Setup

1. Install Prefect:
```bash
pip install prefect
```

2. Start the Prefect server:
```bash
prefect server start
```

3. Create a work pool:
```bash
prefect work-pool create default-agent-pool --type process
```

4. Start a worker:
```bash
prefect worker start -p default-agent-pool
```

5. Deploy the flow:
```bash
prefect deploy --all
```

6. Run the deployment:
```bash
prefect deployment run 'Daily ETL Pipeline/daily-weather-event-pipeline'
```

## Data Flow

1. **Extract:**
   - Weather data is fetched from OpenWeatherMap API
   - Event data is fetched from SeatGeek API

2. **Transform:**
   - Weather data is processed to extract relevant fields
   - Event data is processed and enriched with weather information
   - Data is formatted according to BigQuery schema

3. **Load:**
   - Data is saved to CSV files in the `output` directory
   - Data is loaded into BigQuery tables
   - Tables are updated with new data daily

## Scheduling

The pipeline is scheduled to run daily at 2:10 AM Eastern Time. The schedule is configured in the `prefect.yaml` file.

## Monitoring

You can monitor the pipeline runs in the Prefect UI at `http://localhost:4200`.

## Error Handling

The pipeline includes error handling for:
- API request failures
- Data validation errors
- BigQuery connection issues
- File system errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.