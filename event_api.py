import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_session_with_retry():
    """
    Create a requests session with retry mechanism
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # number of retries
        backoff_factor=1,  # wait 1, 2, 4 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504]  # HTTP status codes to retry on
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_events_forecast_daily(api_key, city="New York"):
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    
    # List of event classifications to fetch
    classification_list = [
        "Music",
        "Sports",
        "Arts & Theatre",
        "Miscellaneous",
        "Fairs & Festivals"
    ]
    
    # Start from today's date
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    start_datetime_ny = ny_now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Use utc time
    start_datetime_utc = start_datetime_ny.astimezone(ZoneInfo("UTC"))

    all_events = []
    session = create_session_with_retry()

    # Fetch events for each classification separately
    for classification in classification_list:
        params = {
            "apikey": api_key,
            "city": city,
            "countryCode": "US",
            "classificationName": classification,
            "startDateTime": start_datetime_utc.isoformat().replace("+00:00", "Z"),
            "sort": "date,asc",
            "size": 200
        }
        
        try:
            # Add delay between requests
            time.sleep(1)  # Wait 1 second between requests
            
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            events = []
            for event in data.get('_embedded', {}).get('events', []):
                events.append({
                    "event_name": event["name"],
                    "event_date": event["dates"]["start"]["localDate"],
                    "event_time": event["dates"]["start"].get("localTime", "Unknown"),
                    "venue": event["_embedded"]["venues"][0]["name"],
                    "address": event["_embedded"]["venues"][0].get("address", {}).get("line1", "Unknown"),
                    "city": event["_embedded"]["venues"][0]["city"]["name"],
                    "price_min": event.get("priceRanges", [{}])[0].get("min", None),
                    "price_max": event.get("priceRanges", [{}])[0].get("max", None),
                    "category": event["classifications"][0]["segment"]["name"],
                    "free_or_paid": "Paid" if event.get("priceRanges") else "Free",
                    "status": event["dates"]["status"]["code"],
                    "event_url": event.get("url", None),
                    "image_url": event.get("images", [{}])[0].get("url", None)
                })
            
            all_events.extend(events)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {classification} events: {str(e)}")
            continue

    # Combine all events from different classifications
    events_df = pd.DataFrame(all_events)

    today_ny = ny_now.date()
    valid_days = [(today_ny + timedelta(days=i)) for i in range(5)]

    events_df["event_date"] = pd.to_datetime(events_df["event_date"]).dt.date

    daily_events = []
    for day in valid_days:
        day_events = events_df.query("event_date == @day").head(50)
        daily_events.append(day_events)

    final_df = pd.concat(daily_events, ignore_index=True)
    return final_df.to_dict(orient="records")