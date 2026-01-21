import requests
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY")
LAT = os.getenv("LAT")
LON = os.getenv("LON")

DATA_DIR = "data/raw"
FILE_PATH = f"{DATA_DIR}/weather.csv"

def fetch_weather():
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    )

    response = requests.get(url).json()

    rain = response.get("rain", {}).get("1h", 0.0)

    return {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": response["main"]["temp"],
        "humidity": response["main"]["humidity"],
        "pressure": response["main"]["pressure"],
        "visibility": response.get("visibility"),
        "wind_speed": response["wind"]["speed"],
        "rain": rain,
        "city": CITY
    }

def append_to_csv(data):
    os.makedirs(DATA_DIR, exist_ok=True)

    file_exists = os.path.isfile(FILE_PATH)

    with open(FILE_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

if __name__ == "__main__":
    weather_data = fetch_weather()
    append_to_csv(weather_data)
    print("Weather data appended:", weather_data)
