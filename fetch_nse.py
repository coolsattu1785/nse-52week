import requests
import pandas as pd
from datetime import datetime
import os

def fetch_nse_52week_high():
    session = requests.Session()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }

    # Step 1: Get cookies
    session.get("https://www.nseindia.com", headers=headers)

    # Step 2: Hit API
    url = "https://www.nseindia.com/api/live-analysis-data-52weekhighstock"
    response = session.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Failed: {response.status_code}")

    data = response.json()

    # Extract proper list (depends on NSE format)
    if "data" in data:
        df = pd.DataFrame(data["data"])
    else:
        df = pd.DataFrame(data)

    # Save with date
    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs("data", exist_ok=True)
    filepath = f"data/52week_high_{today}.csv"
    df.to_csv(filepath, index=False)

    print(f"✅ Data saved: {filepath}")

if __name__ == "__main__":
    fetch_nse_52week_high()
