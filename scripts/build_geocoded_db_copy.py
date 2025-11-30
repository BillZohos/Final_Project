#!/usr/bin/env python3
"""
scripts/build_geocoded_db_copy.py

Create safe copies of `data/olympic_host_cities_normalized_clean.csv` and
`data/geocoded_hosts.json`, then merge those copies into a new SQLite DB
`data/geocoded_hosts_merged.db` so originals remain untouched.

Usage:
  python3 scripts/build_geocoded_db_copy.py
"""
import shutil
import os
import sys
import json
import csv
import sqlite3
import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(ROOT, 'data')
CSV_ORIG = os.path.join(DATA_DIR, 'olympic_host_cities_normalized_clean.csv')
JSON_ORIG = os.path.join(DATA_DIR, 'geocoded_hosts.json')

CSV_COPY = os.path.join(DATA_DIR, 'olympic_host_cities_normalized_clean_copy.csv')
JSON_COPY = os.path.join(DATA_DIR, 'geocoded_hosts_copy.json')
DB_OUT = os.path.join(DATA_DIR, 'geocoded_hosts_merged.db')


def copy_inputs():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CSV_ORIG):
        print(f"Missing CSV input: {CSV_ORIG}")
        sys.exit(1)
    if not os.path.exists(JSON_ORIG):
        print(f"Missing JSON input: {JSON_ORIG}")
        sys.exit(1)
    # Copy but do not overwrite existing copies
    if not os.path.exists(CSV_COPY):
        shutil.copy2(CSV_ORIG, CSV_COPY)
        print(f"Copied CSV -> {CSV_COPY}")
    else:
        print(f"CSV copy already exists: {CSV_COPY}")
    if not os.path.exists(JSON_COPY):
        shutil.copy2(JSON_ORIG, JSON_COPY)
        print(f"Copied JSON -> {JSON_COPY}")
    else:
        print(f"JSON copy already exists: {JSON_COPY}")


def load_cache(path):
    with open(path, 'r', encoding='utf-8') as fh:
        return json.load(fh)


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS geocoded_hosts (
        id INTEGER PRIMARY KEY,
        year TEXT,
        city TEXT,
        country TEXT,
        season TEXT,
        notes TEXT,
        source_table TEXT,
        raw_source TEXT,
        lat REAL,
        lon REAL,
        geocode_status TEXT,
        geocode_provider TEXT,
        geocode_ts TEXT,
        address TEXT,
        cache_key TEXT,
        raw_cache_json TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT
    );
    ''')
    cur.execute('''
    CREATE UNIQUE INDEX IF NOT EXISTS ux_geocoded_hosts_year_city_country
    ON geocoded_hosts(year, city, country);
    ''')
    conn.commit()


def upsert(conn, rec):
    cur = conn.cursor()
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    cur.execute('''
    INSERT INTO geocoded_hosts (
      year, city, country, season, notes, source_table, raw_source,
      lat, lon, geocode_status, geocode_provider, geocode_ts, address, cache_key, raw_cache_json, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(year, city, country) DO UPDATE SET
      season=excluded.season,
      notes=excluded.notes,
      source_table=excluded.source_table,
      raw_source=excluded.raw_source,
      lat=COALESCE(excluded.lat, geocoded_hosts.lat),
      lon=COALESCE(excluded.lon, geocoded_hosts.lon),
      geocode_status=COALESCE(excluded.geocode_status, geocoded_hosts.geocode_status),
      geocode_provider=COALESCE(excluded.geocode_provider, geocoded_hosts.geocode_provider),
      geocode_ts=COALESCE(excluded.geocode_ts, geocoded_hosts.geocode_ts),
      address=COALESCE(excluded.address, geocoded_hosts.address),
      cache_key=COALESCE(excluded.cache_key, geocoded_hosts.cache_key),
      raw_cache_json=COALESCE(excluded.raw_cache_json, geocoded_hosts.raw_cache_json),
      updated_at=?
    ;
    ''', (
        rec.get('year'), rec.get('city'), rec.get('country'), rec.get('season'), rec.get('notes'), rec.get('source_table'), rec.get('raw_source'),
        rec.get('lat'), rec.get('lon'), rec.get('geocode_status'), rec.get('geocode_provider'), rec.get('geocode_ts'), rec.get('address'), rec.get('cache_key'), rec.get('raw_cache_json'), now, now
    ))
    conn.commit()


def find_cache_entry(cache, year, city, country):
    key = f"{year}|{city}|{country}"
    if key in cache:
        return key, cache[key]
    for k, v in cache.items():
        if str(v.get('year') or '').strip().lower() == str(year).strip().lower() and \
           str(v.get('city') or '').strip().lower() == str(city).strip().lower():
            return k, v
    for k, v in cache.items():
        if str(v.get('city') or '').strip().lower() == str(city).strip().lower():
            return k, v
    return None, None


def merge():
    copy_inputs()
    cache = load_cache(JSON_COPY)
    print(f"Loaded {len(cache)} cache entries from copy")

    rows = []
    with open(CSV_COPY, newline='', encoding='utf-8') as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({
                'year': (r.get('Year') or '').strip(),
                'city': (r.get('City') or '').strip(),
                'country': (r.get('Country') or '').strip(),
                'season': (r.get('Season') or '').strip(),
                'notes': (r.get('Notes') or '').strip(),
                'source_table': (r.get('Source_table') or '').strip(),
                'raw_source': (r.get('Raw') or '').strip()
            })

    if os.path.exists(DB_OUT):
        print(f"Removing existing output DB: {DB_OUT}")
        os.remove(DB_OUT)

    conn = sqlite3.connect(DB_OUT)
    ensure_schema(conn)

    total = 0
    with conn:
        for r in rows:
            key, entry = find_cache_entry(cache, r['year'], r['city'], r['country'])
            rec = dict(r)
            if entry:
                rec['lat'] = entry.get('lat')
                rec['lon'] = entry.get('lon')
                rec['geocode_status'] = entry.get('status')
                rec['geocode_provider'] = entry.get('provider')
                rec['geocode_ts'] = entry.get('timestamp') or entry.get('ts')
                rec['address'] = None
                if isinstance(entry.get('raw'), dict):
                    rec['address'] = entry['raw'].get('display_name')
                rec['cache_key'] = key
                rec['raw_cache_json'] = json.dumps(entry, ensure_ascii=False)
            else:
                rec['lat'] = None
                rec['lon'] = None
                rec['geocode_status'] = None
                rec['geocode_provider'] = None
                rec['geocode_ts'] = None
                rec['address'] = None
                rec['cache_key'] = None
                rec['raw_cache_json'] = None
            upsert(conn, rec)
            total += 1

    conn.close()
    print(f"Merged {total} rows into {DB_OUT}")


if __name__ == '__main__':
    merge()
