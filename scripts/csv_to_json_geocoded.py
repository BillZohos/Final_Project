import csv
import json
from pathlib import Path

CSV_IN = Path(__file__).resolve().parent.parent / "data" / "olympics_games_geocoded.csv"
JSON_OUT = Path(__file__).resolve().parent.parent / "data" / "olympics_games_geocoded.json"

KEY_FIELDS = ["Year", "City", "Country"]


def main():
    if not CSV_IN.exists():
        raise FileNotFoundError(f"Input CSV not found: {CSV_IN}")

    with CSV_IN.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Build dict keyed by Year|City|Country
    out = {}
    for r in rows:
        key = "|".join(r[k] for k in KEY_FIELDS)
        # Normalize numeric fields
        year_int = int(r["Year"]) if r["Year"].isdigit() else None
        lat_val = float(r["lat"]) if r.get("lat") not in (None, "") else None
        lon_val = float(r["lon"]) if r.get("lon") not in (None, "") else None
        out[key] = {
            "year": year_int,
            "city": r["City"],
            "country": r["Country"],
            "season": r["Season"],
            "notes": r.get("Notes", ""),
            "lat": lat_val,
            "lon": lon_val,
            "geocode_provider": r.get("geocode_provider", ""),
        }

    with JSON_OUT.open('w', encoding='utf-8') as jf:
        json.dump(out, jf, ensure_ascii=False, indent=2)

    print(f"Wrote {len(out)} records to {JSON_OUT}")


if __name__ == "__main__":
    main()
