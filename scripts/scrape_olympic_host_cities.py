#!/usr/bin/env python3
"""Scrape Olympic host cities from the Wikipedia page and write CSV output.

This script uses a polite User-Agent and basic rate-limiting. It parses
`wikitable` tables on the page and heuristically selects those that look
like lists of host cities (headers containing Year/City/Host).

Usage:
  python3 scripts/scrape_olympic_host_cities.py --output data/olympic_host_cities.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import time
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

URL = "https://en.wikipedia.org/wiki/List_of_Olympic_Games_host_cities"
HEADERS = {
    "User-Agent": "MyOlympicsScraper/1.0 (+https://your-website.example/info; you@example.com)"
}


def fetch_page(url: str, session: requests.Session | None = None, delay: float = 1.0) -> str:
    s = session or requests.Session()
    resp = s.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(delay)
    return resp.text


def table_header_texts(table) -> List[str]:
    header = table.find("tr")
    if not header:
        return []
    ths = header.find_all("th")
    if ths:
        return [th.get_text(strip=True) for th in ths]
    # fallback: use first row tds and generate generic column names
    tds = header.find_all("td")
    return [f"col_{i}" for i in range(len(tds))]


def looks_like_host_table(headers: List[str]) -> bool:
    lowered = [h.lower() for h in headers]
    keywords = ("year", "city", "host", "games", "country", "location")
    return any(k in h for h in lowered for k in keywords)


def parse_tables(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_=("wikitable", "sortable"))
    all_rows: List[Dict[str, str]] = []
    for table in tables:
        caption = table.find("caption")
        caption_text = caption.get_text(strip=True) if caption else ""
        headers = table_header_texts(table)
        if not headers:
            continue
        if not looks_like_host_table(headers):
            # skip tables that don't look relevant
            continue
        # normalize headers to unique keys
        header_keys = []
        seen = {}
        for h in headers:
            key = h if h else ""
            key = key or "unnamed"
            base = key
            i = 1
            while key in seen:
                i += 1
                key = f"{base}_{i}"
            seen[key] = True
            header_keys.append(key)

        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all(["td", "th"])
            if not cols:
                continue
            row: Dict[str, str] = {}
            for i, cell in enumerate(cols):
                text = cell.get_text(" ", strip=True)
                if i < len(header_keys):
                    row[header_keys[i]] = text
                else:
                    row[f"extra_{i}"] = text
            row["source_table"] = caption_text or "wikitable"
            all_rows.append(row)
    return all_rows


def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    # collect fieldnames
    fieldnames = set()
    for r in rows:
        fieldnames.update(r.keys())
    # keep order: try to prefer common columns
    preferred = ["Year", "City", "Country", "Location", "Host city", "source_table"]
    # case-insensitive mapping to existing names
    ordered = []
    for p in preferred:
        for f in list(fieldnames):
            if f.lower() == p.lower():
                ordered.append(f)
                fieldnames.discard(f)
    ordered.extend(sorted(fieldnames))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Olympic host cities and write CSV")
    parser.add_argument("--output", "-o", default="data/olympic_host_cities.csv")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    args = parser.parse_args()

    print(f"Fetching {URL} ...")
    html = fetch_page(URL, delay=args.delay)
    print("Parsing tables...")
    rows = parse_tables(html)
    if not rows:
        print("No relevant tables found. Consider using the MediaWiki API instead.")
    else:
        print(f"Found {len(rows)} rows. Writing CSV to {args.output}")
        write_csv(rows, args.output)
        print("Done.")


if __name__ == "__main__":
    main()
