import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY = os.getenv("AQICN_API_KEY")
CITY = os.getenv("CITY")

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = f"{OUTPUT_DIR}/aqi.csv"


def fetch_current_aqi():
    url = f"https://api.waqi.info/feed/{CITY}/?token={API_KEY}"

    try:
        response = requests.get(url, timeout=10).json()
    except Exception as e:
        print("Request failed:", e)
        return None

    # 1️⃣ Check API status
    if response.get("status") != "ok":
        print("AQICN API status not ok:", response)
        return None

    data = response.get("data")
    if not data:
        print("AQICN returned empty data:", response)
        return None

    iaqi = data.get("iaqi", {})
    geo = data.get("city", {}).get("geo", [None, None])

    return {
        "timestamp": data.get("time", {}).get("s"),
        "aqi": data.get("aqi"),
        "pm25": iaqi.get("pm25", {}).get("v"),
        "pm10": iaqi.get("pm10", {}).get("v"),
        "no2": iaqi.get("no2", {}).get("v"),
        "o3": iaqi.get("o3", {}).get("v"),
        "so2": iaqi.get("so2", {}).get("v"),
        "dominant_pollutant": data.get("dominentpol"),
        "lat": geo[0],
        "lon": geo[1],
        "city": CITY,
    }


def append_aqi_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    record = fetch_current_aqi()

    # 2️⃣ If no data, DO NOT FAIL PIPELINE
    if record is None:
        print("No AQI data available for this run. Skipping.")
        return

    df_new = pd.DataFrame([record])

    if os.path.exists(OUTPUT_FILE):
        df_old = pd.read_csv(OUTPUT_FILE)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    # 3️⃣ Avoid duplicates
    df.drop_duplicates(subset=["timestamp"], inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)
    print("AQI data appended successfully")


if __name__ == "__main__":
    append_aqi_data()
