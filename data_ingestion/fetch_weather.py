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

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = f"{OUTPUT_DIR}/weather.csv"


def fetch_weather():
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    )

    try:
        response = requests.get(url, timeout=10).json()
    except Exception as e:
        print("Weather request failed:", e)
        return None

    # 1️⃣ Validate response
    if "main" not in response:
        print("Invalid weather response:", response)
        return None

    rain = response.get("rain", {}).get("1h", 0.0)

    return {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": response["main"].get("temp"),
        "humidity": response["main"].get("humidity"),
        "pressure": response["main"].get("pressure"),
        "visibility": response.get("visibility"),
        "wind_speed": response.get("wind", {}).get("speed"),
        "rain": rain,
        "city": CITY,
    }


def append_weather_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    record = fetch_weather()

    # 2️⃣ Skip if no data (DO NOT FAIL)
    if record is None:
        print("No weather data available for this run. Skipping.")
        return

    file_exists = os.path.isfile(OUTPUT_FILE)

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=record.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)

    print("Weather data appended successfully")


if __name__ == "__main__":
    append_weather_data()
