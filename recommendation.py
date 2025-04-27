# recommendation.py

def calculate_comfort_level(temp, humidity, wind_speed, precipitation_chance):
    """
    Calculate comfort level based on temperature, humidity, wind speed, and rain chance.
    """
    # Temperature score
    if temp < 10 or temp > 30:
        temp_score = -1
    elif 15 <= temp <= 25:
        temp_score = 1
    else:
        temp_score = 0

    # Rain score
    rain_score = -1 if precipitation_chance > 0.5 else 0

    # Wind score
    wind_score = -1 if wind_speed > 10 else 0

    # Total comfort score
    total_score = temp_score + rain_score + wind_score

    if total_score >= 1:
        return "Comfortable"
    elif total_score == 0:
        return "Moderate"
    else:
        return "Uncomfortable"

def generate_recommendation(temp, feels_like, humidity, wind_speed, weather_main, precipitation_chance, venue_name):
    """
    Generate event recommendation based on today's weather conditions.
    """
    comfort_level = calculate_comfort_level(temp, humidity, wind_speed, precipitation_chance)

    if comfort_level == "Comfortable":
        return "Recommended (Outdoor)"
    elif comfort_level == "Moderate":
        return "Recommended (Indoor OK)"
    else:
        if any(keyword in venue_name for keyword in ["Indoor", "Club", "Theater", "Theatre", "Center", "Auditorium", "Hall"]):
            return "Recommended (Indoor)"
        else:
            return "Not Recommended (Outdoor)"