#!/usr/bin/env python3
"""Ensure `data/geocoded_hosts.json` has an entry for every row in the cleaned CSV.

This creates placeholder entries (lat/lon null, status "pending") for any rows
not already present in the JSON cache so a geocoding run can resume or be
performed with a different backend (e.g. `tidygeocoder`).
"""
from __future__ import annotations

import csv
import datetime
import json
import os
from typing import Dict, Any

INPUT_CSV = "data/olympic_host_cities_normalized_clean.csv"
CACHE_JSON = "data/geocoded_hosts.json"


def make_key(row: Dict[str, str]) -> str:
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


def bootstrap():
    if not os.path.exists(INPUT_CSV):
        raise SystemExit(f"Input CSV not found: {INPUT_CSV}")

    cache = load_cache(CACHE_JSON)

    added = 0
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            key = make_key(r)
            if key in cache:
                continue

            city = r.get("City", "").strip()
            country = r.get("Country", "").strip()
            query = ", ".join([p for p in (city, country) if p])

            entry: Dict[str, Any] = {
                "key": key,
                "year": r.get("Year", "").strip(),
                "city": city,
                "country": country,
                "query": query,
                "lat": None,
                "lon": None,
                "provider": "nominatim",
                "status": "pending",
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "raw": None,
            }

            cache[key] = entry
            added += 1

    if added:
        save_cache(CACHE_JSON, cache)

    print(f"Bootstrapped {added} missing entries into {CACHE_JSON} (total {len(cache)} entries).")


if __name__ == "__main__":
    bootstrap()
