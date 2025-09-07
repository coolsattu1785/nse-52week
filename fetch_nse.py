#!/usr/bin/env python3
"""
fetch_nse.py
Robust fetcher for NSE 52-week-high JSON -> CSV.
- Does browser-like preflight requests to obtain cookies
- Retries on transient errors
- Prints helpful debug info when 401/403 occurs
"""

import requests
import pandas as pd
import time
import os
import sys
import json
from datetime import datetime

# ---------- EDIT THIS IF NEEDED ----------
API_URL = "https://www.nseindia.com/api/live-analysis-data-52weekhighstock"
HOMEPAGE = "https://www.nseindia.com"
OUT_DIR = "data"
# -----------------------------------------

os.makedirs(OUT_DIR, exist_ok=True)

# Browser-like headers
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": HOMEPAGE,
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.nseindia.com",
    # sec-fetch headers sometimes help mimic real browser
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

def preflight_session(session, verbose=False):
    """
    Hit a few NSE pages to make server set cookies (homepage, market-data path).
    This often resolves 401/403 from API endpoints.
    """
    pref_urls = [
        HOMEPAGE,
        HOMEPAGE + "/market-data",
        HOMEPAGE + "/market-data/live-analysis",
        HOMEPAGE + "/live-analysis",
    ]
    for u in pref_urls:
        try:
            if verbose:
                print("Preflight GET:", u)
            session.get(u, headers=HEADERS, timeout=12)
            # small sleep to mimic human/browser
            time.sleep(0.4)
        except Exception as e:
            if verbose:
                print("Preflight failed for", u, ":", e)
            # keep trying other preflight URLs

def find_list_in_json(j):
    """Return the main list of records from typical NSE JSON shapes"""
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        for k in ("data", "result", "rows", "items", "records"):
            if k in j and isinstance(j[k], list):
                return j[k]
        # fallback: first list value in the dict
        for v in j.values():
            if isinstance(v, list):
                return v
    raise ValueError("Could not find a list of records in JSON response. JSON keys: " + ", ".join(map(str, j.keys())) if isinstance(j, dict) else "not a dict")

def fetch_once(session, url):
    """Single try to fetch and return response object"""
    return session.get(url, headers=HEADERS, timeout=25, allow_redirects=True)

def fetch_with_retries(url, max_attempts=4, verbose=False):
    session = requests.Session()
    session.headers.update(HEADERS)

    # 1) initial preflight
    preflight_session(session, verbose=verbose)

    for attempt in range(1, max_attempts + 1):
        if verbose:
            print(f"[Attempt {attempt}] Fetching {url}")
        try:
            resp = fetch_once(session, url)
        except Exception as e:
            print(f"Request exception: {e} (attempt {attempt})")
            time.sleep(2 ** attempt)
            # do additional preflight before next attempt
            preflight_session(session, verbose=verbose)
            continue

        if resp.status_code == 200:
            return resp, session
        elif resp.status_code in (401, 403):
            # Probably cookie/headers issue. Try a more aggressive preflight and retry.
            print(f"Warning: status {resp.status_code}. Trying extra preflight & alternate referer...")
            # try hitting some pages again and update referer
            HEADERS["Referer"] = HOMEPAGE + "/market-data/live-analysis"
            session.headers.update(HEADERS)
            preflight_session(session, verbose=verbose)
            time.sleep(1.2)
            continue
        else:
            print(f"Non-200 status: {resp.status_code}. Response snippet: {resp.text[:300]}")
            time.sleep(2 ** attempt)
            preflight_session(session, verbose=verbose)
            continue

    # If we reach here, all attempts failed
    return None, session

def save_json_to_csv(data_obj, out_path):
    # find records list
    rows = find_list_in_json(data_obj)
    df = pd.json_normalize(rows)
    df.to_csv(out_path, index=False)
    return df

def main():
    if "live-analysis" not in API_URL and "52week" not in API_URL:
        print("Note: make sure API_URL matches the XHR you saw in browser Network tab.")
    resp, session = fetch_with_retries(API_URL, max_attempts=5, verbose=True)
    if resp is None:
        print("ERROR: All attempts failed. Dumping session cookies for debugging:")
        for c in session.cookies:
            print(c)
        sys.exit(2)

    print("Status code:", resp.status_code)
    ct = resp.headers.get("Content-Type", "")
    print("Content-Type:", ct)

    # debug: print cookies present
    print("Cookies:", {c.name: c.value for c in session.cookies})

    # parse JSON
    try:
        data_json = resp.json()
    except Exception as e:
        print("Failed to parse JSON:", e)
        print("Response text (first 2000 chars):")
        print(resp.text[:2000])
        sys.exit(3)

    # Save CSV
    try:
        date_str = datetime.now().strftime("%Y-%m-%d")
        out_file = os.path.join(OUT_DIR, f"52week_high_{date_str}.csv")
        df = save_json_to_csv(data_json, out_file)
        print("Saved CSV:", out_file)
        print("Rows:", len(df))
    except Exception as e:
        print("Error saving CSV:", e)
        # helpful dump for debugging
        try:
            print(json.dumps(data_json, indent=2)[:2000])
        except:
            pass
        sys.exit(4)

if __name__ == "__main__":
    main()
