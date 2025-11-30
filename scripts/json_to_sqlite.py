#!/usr/bin/env python3
"""
scripts/json_to_sqlite.py

Creates/updates `data/geocoded_hosts.db` from `data/geocoded_hosts.json`.

Usage:
  python3 scripts/json_to_sqlite.py
"""
import json
import sqlite3
import os
import datetime
import sys

JSON_PATH = os.path.join("data", "geocoded_hosts.json")
DB_PATH = os.path.join("data", "geocoded_hosts.db")

def load_json(path):
    if not os.path.exists(path):
        print(f"Error: JSON file not found: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS geocoded_hosts (
        id INTEGER PRIMARY KEY,
        year TEXT,
        city TEXT,
        country TEXT,
        lat REAL,
        lon REAL,
        geocode_provider TEXT,
        geocode_status TEXT,
        geocode_ts TEXT,
        address TEXT,
        raw_json TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT
    );
    """)
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_geocoded_hosts_year_city_country
    ON geocoded_hosts(year, city, country);
    """)
    conn.commit()

def upsert_record(conn, rec):
    year = rec.get("year") or rec.get("Year") or None
    city = rec.get("city") or rec.get("City") or None
    country = rec.get("country") or rec.get("Country") or None
    lat = rec.get("lat")
    lon = rec.get("lon")
    provider = rec.get("provider") or rec.get("geocode_provider") or None
    status = rec.get("status") or rec.get("geocode_status") or None
    ts = rec.get("timestamp") or rec.get("geocode_ts") or None
    address = rec.get("address") or rec.get("display_name") or None
    raw_json = json.dumps(rec, ensure_ascii=False)

    cur = conn.cursor()
    now = datetime.datetime.utcnow().isoformat() + "Z"

    cur.execute("""
    INSERT INTO geocoded_hosts (year, city, country, lat, lon, geocode_provider, geocode_status, geocode_ts, address, raw_json, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year, city, country) DO UPDATE SET
      lat=excluded.lat,
      lon=excluded.lon,
      geocode_provider=excluded.geocode_provider,
      geocode_status=excluded.geocode_status,
      geocode_ts=excluded.geocode_ts,
      address=excluded.address,
      raw_json=excluded.raw_json,
      updated_at=?
    ;
    """, (year, city, country, lat, lon, provider, status, ts, address, raw_json, now, now))
    conn.commit()

def main():
    print("Loading JSON:", JSON_PATH)
    data = load_json(JSON_PATH)
    if not isinstance(data, list):
        print("Warning: JSON root is not a list. Attempting to interpret as dict -> list of values.")
        if isinstance(data, dict):
            data = [data]
        else:
            print("Error: Unrecognized JSON structure.")
            sys.exit(1)

    print(f"Loaded {len(data)} records from JSON.")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    count = 0
    for rec in data:
        upsert_record(conn, rec)
        count += 1

    conn.close()
    print(f"Done: inserted/updated {count} records into {DB_PATH}")

if __name__ == "__main__":
    main()
