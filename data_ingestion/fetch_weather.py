import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY")

OUTPUT_FILE = "weather_data.csv"

def fetch_weather_at_time(unix_ts):
    url = (
        "https://api.openweathermap.org/data/2.5/onecall/timemachine"
        f"?lat=24.8607&lon=67.0011"
        f"&dt={unix_ts}&appid={API_KEY}&units=metric"
    )

    response = requests.get(url).json()
    current = response.get("data", [{}])[0]

    return {
        "timestamp": datetime.utcfromtimestamp(unix_ts),
        "temperature": current.get("temp"),
        "humidity": current.get("humidity"),
        "pressure": current.get("pressure"),
        "visibility": current.get("visibility"),
        "wind_speed": current.get("wind_speed"),
        "rain": current.get("rain", {}).get("1h", 0.0),
        "city": CITY
    }

def backfill_weather(days=30):
    os.makedirs("data", exist_ok=True)

    records = []
    now = datetime.utcnow()

    for d in range(days):
        ts = int((now - timedelta(days=d)).timestamp())
        try:
            record = fetch_weather_at_time(ts)
            records.append(record)
            time.sleep(1)
        except Exception as e:
            print("Skipped:", e)

    df = pd.DataFrame(records)

    if os.path.exists(OUTPUT_FILE):
        df_old = pd.read_csv(OUTPUT_FILE)
        df = pd.concat([df_old, df], ignore_index=True)

    df.drop_duplicates(subset=["timestamp"], inplace=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("Weather backfill completed")

if __name__ == "__main__":
    backfill_weather(days=30)
