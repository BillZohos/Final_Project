#!/usr/bin/env python3
"""
Scrape the official Olympics site for the list of Olympic Games and their season (Summer/Winter).

Usage:
  python scripts/scrape_official_olympics.py --output data/olympics_games_official.csv

The script writes a CSV with columns: year,season,name,url
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from typing import List, Dict

import requests
from bs4 import BeautifulSoup


def fetch_and_parse(url: str) -> List[Dict[str, str]]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Find section headings that mention Summer or Winter Olympic Games
    headings = soup.find_all(lambda tag: tag.name in ["h2", "h3", "h4"] and re.search(r"(Summer|Winter)\s+Olympic Games", tag.get_text(), re.I))
    results = []

    if headings:
        for h in headings:
            season = "Summer" if re.search(r"Summer", h.get_text(), re.I) else "Winter"
            node = h.next_sibling
            collected = []
            # gather anchors until the next heading of the same kind
            while node:
                # skip navigable strings
                if getattr(node, "name", None) in ["h2", "h3", "h4"] and re.search(r"(Summer|Winter)\s+Olympic Games", node.get_text() if node.get_text else "", re.I):
                    break
                if hasattr(node, "find_all"):
                    for a in node.find_all("a", href=True):
                        href = a["href"]
                        if "/en/olympic-games/" in href:
                            text = a.get_text(strip=True)
                            collected.append((text, href))
                node = node.next_sibling

            for text, href in collected:
                row = parse_anchor_text(text, href, season)
                if row:
                    results.append(row)

    # Fallback: collect anchors from the whole page if headings strategy failed
    if not results:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/en/olympic-games/" in href:
                text = a.get_text(strip=True)
                season = infer_season_from_text(text)
                row = parse_anchor_text(text, href, season)
                if row:
                    results.append(row)

    # Deduplicate by (year, season)
    seen = set()
    out = []
    for r in results:
        key = (r.get("year", ""), r.get("season", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    # Sort by year (numeric when possible)
    def sort_key(x):
        y = x.get("year", "")
        return int(y) if y.isdigit() else 9999

    out.sort(key=sort_key)
    return out


def parse_anchor_text(text: str, href: str, season: str) -> Dict[str, str] | None:
    # Extract a 4-digit year if present
    m = re.search(r"(\d{4})", text)
    year = m.group(1) if m else ""
    name = text.replace(year, "").strip()
    if not name:
        seg = href.rstrip("/").split("/")[-1]
        name = seg.replace("-", " ").title()
    url = href if href.startswith("http") else "https://www.olympics.com" + href
    return {"year": year, "season": season, "name": name, "url": url}


def infer_season_from_text(text: str) -> str:
    if re.search(r"Winter", text, re.I):
        return "Winter"
    if re.search(r"Summer", text, re.I):
        return "Summer"
    return ""


def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["year", "season", "name", "url"])
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in ["year", "season", "name", "url"]})


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape olympics.com list of games and seasons")
    parser.add_argument("--output", "-o", default="data/olympics_games_official.csv", help="Output CSV path")
    parser.add_argument("--url", default="https://www.olympics.com/en/olympic-games", help="Page to scrape")
    args = parser.parse_args()

    try:
        rows = fetch_and_parse(args.url)
    except Exception as e:
        print("Error fetching/parsing:", e, file=sys.stderr)
        sys.exit(1)

    write_csv(rows, args.output)
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
