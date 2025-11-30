import csv
import re
from pathlib import Path

SOURCE = Path(__file__).resolve().parent.parent / "app" / "hosts_full.csv"
OUTPUT = Path(__file__).resolve().parent.parent / "app" / "hosts_clean.csv"

SUMMER_YEARS = {
    1896, 1900, 1904, 1908, 1912,
    1920, 1924, 1928, 1932, 1936,
    1948, 1952, 1956, 1960, 1964,
    1968, 1972, 1976, 1980, 1984,
    1988, 1992, 1996, 2000, 2004,
    2008, 2012, 2016, 2020, 2024,
}
WINTER_YEARS = {
    1924, 1928, 1932, 1936,
    1948, 1952, 1956, 1960,
    1964, 1968, 1972, 1976,
    1980, 1984, 1988, 1992,
    1994, 1998, 2002, 2006,
    2010, 2014, 2018, 2022,
}
CANCELLED_YEARS = {1916, 1940, 1944}
FUTURE_CUTOFF = 2024  # exclude > 2024

# Mapping of code prefixes to year where year missing
CODE_YEAR_MAP = {
    "008": 1924,
    "009": 1928,
    "010": 1932,
    "011": 1936,
    "014": 1948,
    "015": 1952,
    "025": 1992,
}

FOOTNOTE_PATTERN = re.compile(r"\[[^\]]*\]||||||\u2020|\u00a7")  # remove bracketed and dagger/section markers
TRIM_MARKERS = ["†", "§"]
MULTI_YEAR_SUMMARY = re.compile(r"\b\d+\s*\(\d{4}.*\d{4}.*\)")  # e.g., "2 (1896,2004)"
YEAR_IN_PARENS = re.compile(r"(\d{4})")
CAPITAL_RUN = re.compile(r"([A-Z][a-z]+)([A-Z][a-z]+)")  # MelbourneStockholm

def clean_text(s: str) -> str:
    if not s:
        return ""
    # Remove footnote brackets and markers
    s = FOOTNOTE_PATTERN.sub("", s)
    for m in TRIM_MARKERS:
        s = s.replace(m, "")
    return s.strip().strip(',')

def split_merged_city(city: str) -> str:
    if city == "MelbourneStockholm":
        return "Melbourne / Stockholm"
    match = CAPITAL_RUN.search(city)
    if match and city in {"MelbourneStockholm"}:  # already handled
        pass
    return city

def infer_year(row_year: str, notes: str, country: str) -> int | None:
    # Direct numeric year
    if row_year and row_year.isdigit():
        return int(row_year)
    # Year inside notes if notes is just a year or starts with one
    if notes:
        m = re.match(r"^(\d{4})(?:$|[^0-9])", notes.strip())
        if m:
            return int(m.group(1))
    # Code-based year from country field (e.g., S008VIII)
    if country and re.match(r"^S\d{3}", country):
        code = country[1:4]
        if code in CODE_YEAR_MAP:
            return CODE_YEAR_MAP[code]
    return None

def determine_season(year: int) -> str | None:
    if year in WINTER_YEARS:
        return "Winter"
    if year in SUMMER_YEARS:
        return "Summer"
    return None

def is_summary_row(year_val: str, season: str, city: str, notes: str) -> bool:
    # Rows with non-numeric year but season present and notes referencing multiple years
    if (not year_val or not year_val.isdigit()) and season in {"Summer", "Winter"} and MULTI_YEAR_SUMMARY.search(notes or ""):
        return True
    # Rows where first column holds a city name (e.g., 'Athens,Winter,...')
    if year_val and not year_val.isdigit() and season in {"Summer", "Winter"} and not notes:
        return True
    return False

def main():
    if not SOURCE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE}")

    kept = []
    seen = set()

    with SOURCE.open(newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for raw in reader:
            if not raw or len(raw) < 5:
                continue
            year_val, season_raw, city_raw, country_raw, notes_raw = raw[:5]

            # Initial summary detection
            if is_summary_row(year_val, season_raw, city_raw, notes_raw):
                continue

            # Clean fields
            city = clean_text(city_raw)
            country = clean_text(country_raw)
            notes = clean_text(notes_raw)

            # Normalize merged city names
            city = split_merged_city(city)
            if city == "Melbourne / Stockholm":
                country = "Australia / Sweden"

            # Infer year
            year = infer_year(year_val.strip(), notes, country_raw.strip())
            if year is None:
                continue
            if year in CANCELLED_YEARS or year > FUTURE_CUTOFF:
                continue

            season = determine_season(year)
            if season is None:
                continue

            key = (year, season, city)
            if key in seen:
                continue
            seen.add(key)

            kept.append({
                "year": year,
                "season": season,
                "city": city,
                "country": country,
                "notes": notes,
            })

    # Sort by year then season (Summer before Winter for same year if both exist)
    order = {"Summer": 0, "Winter": 1}
    kept.sort(key=lambda r: (r["year"], order[r["season"]]))

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open('w', newline='', encoding='utf-8') as out:
        w = csv.writer(out)
        w.writerow(["year", "season", "city", "country", "notes"])
        for r in kept:
            w.writerow([r["year"], r["season"], r["city"], r["country"], r["notes"]])

    print(f"Wrote {len(kept)} cleaned rows to {OUTPUT}")

if __name__ == "__main__":
    main()
