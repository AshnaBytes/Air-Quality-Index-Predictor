import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

API_KEY = os.getenv("AQICN_API_KEY")
CITY = os.getenv("CITY")

OUTPUT_FILE = "aqi_data.csv"

def fetch_current_aqi():
    url = f"https://api.waqi.info/feed/{CITY}/?token={API_KEY}"
    response = requests.get(url).json()

    if response.get("status") != "ok":
        raise ValueError(response.get("data"))

    data = response["data"]
    iaqi = data.get("iaqi", {})

    return {
        "timestamp": data.get("time", {}).get("s"),
        "aqi": data.get("aqi"),
        "pm25": iaqi.get("pm25", {}).get("v"),
        "pm10": iaqi.get("pm10", {}).get("v"),
        "no2": iaqi.get("no2", {}).get("v"),
        "o3": iaqi.get("o3", {}).get("v"),
        "so2": iaqi.get("so2", {}).get("v"),
        "dominant_pollutant": data.get("dominentpol"),
        "lat": data.get("city", {}).get("geo", [None, None])[0],
        "lon": data.get("city", {}).get("geo", [None, None])[1],
        "city": CITY
    }

def append_aqi_data():
    os.makedirs("data", exist_ok=True)

    record = fetch_current_aqi()
    df_new = pd.DataFrame([record])

    if os.path.exists(OUTPUT_FILE):
        df_old = pd.read_csv(OUTPUT_FILE)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df.drop_duplicates(subset=["timestamp"], inplace=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("AQI data appended")

if __name__ == "__main__":
    append_aqi_data()
