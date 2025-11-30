#!/usr/bin/env python3
"""Clean normalized Olympic host cities CSV.

This script removes bracketed citations (e.g. "[ a ]", "[11]") and
footnote markers (e.g. "†") from the `City` and `Country` columns and
writes a cleaned CSV to `data/olympic_host_cities_normalized_clean.csv` by default.
"""
from __future__ import annotations

import csv
import os
import re
from typing import Dict

INPUT = "data/olympic_host_cities_normalized.csv"
OUTPUT = "data/olympic_host_cities_normalized_clean.csv"


def clean_text(s: str) -> str:
    if s is None:
        return ""
    # remove bracketed citations like [ a ] or [11]
    s = re.sub(r"\[\s*[^\]]+\s*\]", "", s)
    # remove dagger and other common footnote symbols
    s = s.replace("†", "")
    # normalize whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # strip surrounding punctuation
    s = s.strip(" ,;:\n\t\r")
    return s


def clean_row(row: Dict[str, str]) -> Dict[str, str]:
    # Clean City and Country in-place if present
    if "City" in row:
        row["City"] = clean_text(row.get("City", ""))
    if "Country" in row:
        row["Country"] = clean_text(row.get("Country", ""))
    # Optionally clean Notes
    if "Notes" in row:
        row["Notes"] = clean_text(row.get("Notes", ""))
    return row


def main(in_path: str = INPUT, out_path: str = OUTPUT) -> None:
    if not os.path.exists(in_path):
        raise SystemExit(f"Input file not found: {in_path}")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    with open(in_path, newline="", encoding="utf-8") as inf:
        reader = csv.DictReader(inf)
        rows = [clean_row(dict(r)) for r in reader]
        fieldnames = reader.fieldnames or []

    with open(out_path, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


if __name__ == "__main__":
    main()
