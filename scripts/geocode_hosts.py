#!/usr/bin/env python3
"""Geocode cleaned Olympic host cities and cache results.

Reads `data/olympic_host_cities_normalized_clean.csv`, queries Nominatim
via `geopy`, and writes a JSON cache to `data/geocoded_hosts.json` and a
CSV summary to `data/geocoded_hosts.csv`.

The script is safe to re-run: it will load any existing cache and skip
already-geocoded queries. It respects Nominatim's polite rate of 1s
between requests and uses a descriptive User-Agent.
"""
from __future__ import annotations

import csv
import datetime
import json
import os
import time
from typing import Dict, Any

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

INPUT_CSV = "data/olympic_host_cities_normalized_clean.csv"
CACHE_JSON = "data/geocoded_hosts.json"
OUT_CSV = "data/geocoded_hosts.csv"

USER_AGENT = "MyOlympicsScraper/1.0 (+https://your-website.example/info; you@example.com)"


def make_key(row: Dict[str, str]) -> str:
    # Use Year|City|Country as a stable cache key
    return f"{row.get('Year','').strip()}|{row.get('City','').strip()}|{row.get('Country','').strip()}"


def load_cache(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(path: str, cache: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def geocode_rows():
    if not os.path.exists(INPUT_CSV):
        raise SystemExit(f"Input CSV not found: {INPUT_CSV}")

    cache = load_cache(CACHE_JSON)

    geolocator = Nominatim(user_agent=USER_AGENT)
    # RateLimiter ensures we wait at least min_delay_seconds between calls
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2, error_wait_seconds=5.0)

    rows = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = make_key(r)
            if key in cache:
                rows.append(cache[key])
                continue

            query = ", ".join([p for p in (r.get("City",""), r.get("Country","")) if p])
            entry: Dict[str, Any] = {
                "key": key,
                "year": r.get("Year",""),
                "city": r.get("City",""),
                "country": r.get("Country",""),
                "query": query,
                "lat": None,
                "lon": None,
                "provider": "nominatim",
                "status": "not_found",
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "raw": None,
            }

            if not query:
                entry["status"] = "no_query"
                cache[key] = entry
                rows.append(entry)
                continue

            try:
                loc = geocode(query)
                if loc:
                    entry["lat"] = loc.latitude
                    entry["lon"] = loc.longitude
                    entry["status"] = "ok"
                    # Some geocoders expose raw, geopy Location has `raw` attr
                    try:
                        entry["raw"] = getattr(loc, "raw", None)
                    except Exception:
                        entry["raw"] = None
                else:
                    entry["status"] = "not_found"
            except Exception as e:
                entry["status"] = "error"
                entry["error"] = str(e)

            cache[key] = entry
            rows.append(entry)
            # save cache incrementally so we can resume safely
            save_cache(CACHE_JSON, cache)

    # final save & produce CSV summary
    save_cache(CACHE_JSON, cache)

    fieldnames = ["key", "year", "city", "country", "query", "lat", "lon", "provider", "status", "timestamp"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for k in cache:
            item = cache[k]
            writer.writerow({k: item.get(k) for k in fieldnames})

    print(f"Geocoding complete. Cached {len(cache)} entries to {CACHE_JSON} and {OUT_CSV}.")


if __name__ == "__main__":
    geocode_rows()
