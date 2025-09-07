#!/usr/bin/env python3
"""
fetch_nse.py
Fetch JSON from NSE XHR endpoint, convert to CSV, save to nse_csv/52week_YYYY-MM-DD.csv
"""

import requests
import pandas as pd
import datetime
import os
import time
import sys

# ---------- CONFIG ----------
# Replace this with the exact Request URL you saw in browser Network tab for the data XHR
API_URL = "REPLACE_WITH_REQUEST_URL_FROM_YOUR_BROWSER"
HOMEPAGE = "https://www.nseindia.com"
OUT_DIR = "nse_csv"
# ----------------------------

os.makedirs(OUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": HOMEPAGE,
}

def get_session_with_cookies():
    s = requests.Session()
    s.headers.update(HEADERS)
    # preflight to get cookies and avoid some basic bot blocks
    try:
        r = s.get(HOMEPAGE, timeout=15)
        # tiny delay to look more like a normal browser
        time.sleep(0.8)
    except Exception as e:
        print("Warning: Preflight to homepage failed:", e)
    return s

def fetch_json(session, url, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                # return parsed JSON
                return r.json()
            else:
                print(f"Attempt {attempt}: Status {r.status_code} - {r.text[:200]}")
        except Exception as e:
            print(f"Attempt {attempt} exception:", e)
        # exponential backoff
        time.sleep(2 ** attempt)
    raise SystemExit("Failed to fetch JSON after retries")

def find_list_in_json(j):
    # common keys -> data, result, rows, items
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        for k in ("data", "result", "rows", "items"):
            if k in j and isinstance(j[k], list):
                return j[k]
        # fallback: first list value found
        for v in j.values():
            if isinstance(v, list):
                return v
    raise ValueError("Could not find a list of records in JSON response. Inspect structure.")

def main():
    if "REPLACE_WITH_REQUEST_URL_FROM_YOUR_BROWSER" in API_URL:
        print("ERROR: Please edit fetch_nse.py and set API_URL to the Request URL you saw in browser Network tab.")
        sys.exit(1)

    session = get_session_with_cookies()
    data_json = fetch_json(session, API_URL)

    try:
        rows = find_list_in_json(data_json)
    except Exception as e:
        print("Error reading JSON:", e)
        # print snippet to help debugging
        import json
        print(json.dumps(data_json, indent=2)[:2000])
        sys.exit(1)

    # Normalize into a clean table
    df = pd.json_normalize(rows)
    # optionally rename columns or format here if you like

    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    out_path = os.path.join(OUT_DIR, f"52week_{date_str}.csv")
    df.to_csv(out_path, index=False)
    print("Saved:", out_path)

if __name__ == "__main__":
    main()
