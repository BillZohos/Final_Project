Scraper: Olympic host cities

This repository now contains a small scraper script at:

  `scripts/scrape_olympic_host_cities.py`

Usage:

1. Install dependencies (recommended to use a virtualenv):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run the scraper to produce a CSV:

```bash
python3 scripts/scrape_olympic_host_cities.py --output data/olympic_host_cities.csv
```

Notes:
- The script uses a polite User-Agent and a 1s delay by default.
- For Wikipedia it's preferable to use the MediaWiki API for structured data.
- If no tables are found, try using the API instead.
