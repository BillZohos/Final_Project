import csv
import json
from pathlib import Path

CSV_IN = Path(__file__).resolve().parent.parent / "data" / "olympics_games_updated.csv"
JSON_CACHE = Path(__file__).resolve().parent.parent / "data" / "geocoded_hosts.json"
CSV_OUT = Path(__file__).resolve().parent.parent / "data" / "olympics_games_geocoded.csv"

PRIMARY_FIELDS = ["Year", "City", "Country"]


def load_geocode_cache(path: Path):
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    # Normalize cache into key -> (lat, lon, provider)
    norm = {}
    for key, record in data.items():
        year = str(record.get("year", "")).strip()
        city = str(record.get("city", "")).strip()
        country = str(record.get("country", "")).strip()
        lat = record.get("lat")
        lon = record.get("lon")
        provider = record.get("provider") or record.get("geocode_provider") or record.get("geocode_provider")
        cache_key_exact = f"{year}|{city}|{country}"
        norm[cache_key_exact] = (lat, lon, provider)
        # Also add relaxed key without spaces/slashes for fallback
        relaxed = f"{year}|{city.replace(' ', '')}|{country.replace(' ', '')}"
        norm.setdefault(relaxed, (lat, lon, provider))
    return norm


def build_row_key(row: dict):
    year = str(row.get("Year", "")).strip()
    city = str(row.get("City", "")).strip()
    country = str(row.get("Country", "")).strip()
    return f"{year}|{city}|{country}", f"{year}|{city.replace(' ', '')}|{country.replace(' ', '')}"


def augment():
    if not CSV_IN.exists():
        raise FileNotFoundError(f"Input CSV not found: {CSV_IN}")
    if not JSON_CACHE.exists():
        raise FileNotFoundError(f"Geocode cache not found: {JSON_CACHE}")

    cache = load_geocode_cache(JSON_CACHE)
    augmented_rows = []
    matched = 0
    unmatched = 0

    with CSV_IN.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        # Add lat/lon/provider fields if not present
        out_fields = fieldnames + ["lat", "lon", "geocode_provider"]
        for row in reader:
            key_exact, key_relaxed = build_row_key(row)
            geo = cache.get(key_exact) or cache.get(key_relaxed)
            if geo and geo[0] is not None and geo[1] is not None:
                row["lat"], row["lon"], row["geocode_provider"] = geo
                matched += 1
            else:
                row["lat"], row["lon"], row["geocode_provider"] = "", "", ""
                unmatched += 1
            augmented_rows.append(row)

    with CSV_OUT.open('w', newline='', encoding='utf-8') as out:
        writer = csv.DictWriter(out, fieldnames=out_fields)
        writer.writeheader()
        for r in augmented_rows:
            writer.writerow(r)

    print(f"Augmented file written: {CSV_OUT}")
    print(f"Matched geocodes: {matched}; Unmatched: {unmatched}; Total rows: {len(augmented_rows)}")


if __name__ == "__main__":
    augment()
