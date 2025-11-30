#!/usr/bin/env python3
"""Normalized scraper for Olympic host cities using the MediaWiki API.

Produces a CSV with columns: Year, City, Country, Season, Notes, Source_table, Raw

Cleaning goals (refactor):
- Keep only real Games rows; drop aggregated summaries.
- Strip footnotes ([a], [b], [c], †, §) and suffixes.
- Assign Season using known IOC years if captions are unreliable.
- Parse Year as a numeric year; cancelled years appear with Notes="Cancelled due to war".
- Split merged city names (e.g., MelbourneStockholm → Melbourne / Stockholm).
- Output clean CSV to data/olympic_host_cities_clean.csv.

Heuristics:
- Uses the API to get parsed HTML (less brittle than scraping the page HTML directly).
- Finds `wikitable` tables and associates them with the nearest preceding heading
  to infer Season (Summer/Winter/Other).
- Extracts Year by regex, City as the first non-year text cell, Country as the cell
  directly after City when available. Falls back to raw row content when uncertain.
"""
from __future__ import annotations

import csv
import os
import re
import time
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup, Tag

API_URL = "https://en.wikipedia.org/w/api.php"
PAGE = "List_of_Olympic_Games_host_cities"
HEADERS = {"User-Agent": "MyOlympicsScraper/1.0 (+https://your-website.example/info; you@example.com)"}

YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b")
FOOTNOTE_BRACKETS = re.compile(r"\[[^\]]*\]")
FOOTNOTE_MARKERS = re.compile(r"[\u2020\u00a7]|[†§]")

# Known IOC years for season mapping
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
FUTURE_CUTOFF = 2024


def fetch_parsed_html(page: str, delay: float = 0.5) -> str:
    params = {
        "action": "parse",
        "page": page,
        "prop": "text",
        "format": "json",
    }
    resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    html = data["parse"]["text"]["*"]
    time.sleep(delay)
    return html


def find_nearest_heading(elem: Tag) -> Optional[str]:
    # Walk previous siblings to find a heading tag (h2/h3/h4) for context
    sib = elem.previous_sibling
    while sib:
        if isinstance(sib, Tag) and sib.name in ("h2", "h3", "h4", "h5"):
            return sib.get_text(" ", strip=True)
        sib = sib.previous_sibling
    return None


def extract_row_normalized(cells: List[Tag], caption: str | None) -> Dict[str, str]:
    def strip_footnotes(s: str) -> str:
        s = FOOTNOTE_BRACKETS.sub("", s)
        s = FOOTNOTE_MARKERS.sub("", s)
        return s.strip()

    texts = [strip_footnotes(c.get_text(" ", strip=True)) for c in cells]
    raw = " | ".join(texts)
    year: Optional[str] = None
    for t in texts:
        m = YEAR_RE.search(t)
        if m:
            year = m.group(0)
            break

    # City: prefer cell containing an <a> link, otherwise first non-year non-empty cell
    city = None
    country = None
    for c in cells:
        if c.find("a"):
            txt = strip_footnotes(c.get_text(" ", strip=True))
            if not YEAR_RE.search(txt):
                city = txt
                break
    if not city:
        for t in texts:
            if t and not YEAR_RE.search(t):
                city = t
                break

    # Country: heuristically the next cell after city if available and not a year
    if city:
        try:
            idx = texts.index(city)
            if idx + 1 < len(texts) and not YEAR_RE.search(texts[idx + 1]):
                country = texts[idx + 1]
        except ValueError:
            country = None

    season = "Other"
    cap = (caption or "").lower()
    if "summer" in cap:
        season = "Summer"
    elif "winter" in cap:
        season = "Winter"
    else:
        # fallback: check raw row for indicators
        if any("summer" in t.lower() for t in texts):
            season = "Summer"
        elif any("winter" in t.lower() for t in texts):
            season = "Winter"

    return {
        "Year": year or "",
        "City": city or "",
        "Country": country or "",
        "Season": season,
        "Notes": "",
        "Source_table": caption or "",
        "Raw": raw,
    }


def parse_and_normalize(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, str]] = []
    tables = soup.find_all("table", class_=lambda c: c and "wikitable" in c)
    for table in tables:
        caption_tag = table.find("caption")
        caption_text = caption_tag.get_text(" ", strip=True) if caption_tag else None
        # try to infer a nearby heading for better season context
        heading = find_nearest_heading(table)
        if heading and not caption_text:
            caption_text = heading

        # header detection: skip if no rows
        trs = table.find_all("tr")
        if len(trs) <= 1:
            continue
        # find the row elements after header rows
        # many wikitables have a header row with th; skip any initial th-only rows
        start_idx = 0
        for i, tr in enumerate(trs):
            # choose first row that has td elements
            if tr.find_all("td"):
                start_idx = i
                break

        for tr in trs[start_idx:]:
            cells = tr.find_all(["td", "th"])  # some tables put data in th
            if not cells:
                continue
            norm = extract_row_normalized(cells, caption_text)
            # don't include totally empty rows
            if not (norm.get("Year") or norm.get("City") or norm.get("Raw")):
                continue
            rows.append(norm)
    return rows

def is_summary_row(row: Dict[str, str]) -> bool:
    # Aggregated summaries typically have non-numeric Year and Notes with multiple years pattern
    year = row.get("Year", "")
    notes = row.get("Notes", "")
    raw = row.get("Raw", "")
    if not year or not year.isdigit():
        # detect patterns like "2 (1896, 2004)" or comma-separated multiple years
        if re.search(r"\(\s*\d{4}[^)]*\d{4}[^)]*\)", raw) or re.search(r"\b\d\s*\(\d{4}.*\)", raw):
            return True
    return False

def determine_season_by_year(year: int) -> Optional[str]:
    if year in WINTER_YEARS:
        return "Winter"
    if year in SUMMER_YEARS:
        return "Summer"
    return None

def clean_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    cleaned: List[Dict[str, str]] = []
    seen = set()

    for r in rows:
        if is_summary_row(r):
            continue

        # Footnote-stripped city/country
        city = r.get("City", "").strip()
        country = r.get("Country", "").strip()

        # Fix merged city names
        if city == "MelbourneStockholm":
            city = "Melbourne / Stockholm"
            if country in ("AustraliaSweden", "OceaniaEurope", ""):
                country = "Australia / Sweden"

        # Parse numeric year
        year_str = r.get("Year", "")
        year: Optional[int] = None
        if year_str.isdigit():
            year = int(year_str)
        else:
            # try to extract from Raw
            m = YEAR_RE.search(r.get("Raw", ""))
            if m:
                year = int(m.group(0))

        if year is None:
            continue

        notes = r.get("Notes", "").strip()

        # Cancelled games retained with note
        if year in CANCELLED_YEARS:
            notes = "Cancelled due to war"

        # Exclude future beyond cutoff
        if year > FUTURE_CUTOFF:
            continue

        # Season by year authoritative
        season = determine_season_by_year(year) or r.get("Season", "Other")
        if season == "Other":
            # If still unknown, skip row
            continue

        key = (year, season, city)
        if key in seen:
            continue
        seen.add(key)

        cleaned.append({
            "Year": str(year),
            "City": city,
            "Country": country,
            "Season": season,
            "Notes": notes,
            "Source_table": r.get("Source_table", ""),
            "Raw": r.get("Raw", ""),
        })

    # Sort by Year, then Season (Summer before Winter)
    order = {"Summer": 0, "Winter": 1}
    cleaned.sort(key=lambda x: (int(x["Year"]), order.get(x["Season"], 2)))
    return cleaned


def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fieldnames = ["Year", "City", "Country", "Season", "Notes", "Source_table", "Raw"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    out_norm = "data/olympic_host_cities_normalized.csv"
    out_clean = "data/olympic_host_cities_clean.csv"
    print("Fetching parsed HTML via MediaWiki API...")
    html = fetch_parsed_html(PAGE)
    print("Parsing and normalizing tables...")
    rows = parse_and_normalize(html)
    print(f"Found {len(rows)} normalized rows. Writing to {out_norm}")
    write_csv(rows, out_norm)
    print("Applying cleaning rules and writing clean output...")
    cleaned = clean_rows(rows)
    write_csv(cleaned, out_clean)
    print(f"Wrote {len(cleaned)} cleaned rows to {out_clean}")
    print("Done.")


if __name__ == "__main__":
    main()
