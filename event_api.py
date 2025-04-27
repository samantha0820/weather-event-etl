import requests
import pandas as pd
from datetime import datetime, timedelta

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
    
    # Start from today's date at midnight UTC
    start_datetime = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    all_events = []

    # Fetch events for each classification separately
    for classification in classification_list:
        params = {
            "apikey": api_key,
            "city": city,
            "countryCode": "US",
            "classificationName": classification,
            "startDateTime": start_datetime.isoformat() + "Z",
            "sort": "date,asc",
            "size": 200
        }
        
        response = requests.get(url, params=params)
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

    # Combine all events from different classifications
    events_df = pd.DataFrame(all_events)

    # Split events by day and limit to 50 per day
    daily_events = []
    for i in range(5):  # Next 5 days
        day = (start_datetime + timedelta(days=i)).date()
        day_events = events_df.query("event_date == @day.isoformat()").head(50)
        daily_events.append(day_events)

    # Concatenate daily event dataframes into a final dataframe
    final_df = pd.concat(daily_events, ignore_index=True)
    return final_df.to_dict(orient="records")