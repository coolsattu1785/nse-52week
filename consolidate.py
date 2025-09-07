#!/usr/bin/env python3
"""
consolidate_weekly.py
Concatenate daily CSVs in data/ into one weekly consolidated CSV in weekly/
"""
import pandas as pd
import glob
import os
from datetime import datetime

IN_DIR = "data"
OUT_DIR = "weekly"
os.makedirs(OUT_DIR, exist_ok=True)

pattern = os.path.join(IN_DIR, "52week_high_*.csv")
files = sorted(glob.glob(pattern))

if not files:
    print("No daily files found in", IN_DIR)
    exit(0)

dfs = []
for f in files:
    try:
        df = pd.read_csv(f)
    except Exception as e:
        print("Skipping file (read error):", f, e)
        continue
    base = os.path.basename(f)
    date_part = base.replace("52week_high_", "").replace(".csv", "")
    df["fetch_date"] = date_part
    df["source_file"] = base
    dfs.append(df)

if not dfs:
    print("No readable CSVs found.")
    exit(0)

combined = pd.concat(dfs, ignore_index=True)
week_date = datetime.now().strftime("%Y-%m-%d")
out_file = os.path.join(OUT_DIR, f"weekly_consolidated_{week_date}.csv")
combined.to_csv(out_file, index=False)
print("Saved weekly consolidated file:", out_file)
