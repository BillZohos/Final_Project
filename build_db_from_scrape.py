import requests
from bs4 import BeautifulSoup
import sqlite3

URL = "https://en.wikipedia.org/wiki/List_of_Olympic_Games_host_cities"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}

def scrape_hosts():
    resp = requests.get(URL, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    tables = soup.find_all("table", class_="wikitable")
    summer_table = tables[0]
    winter_table = tables[1]

    def parse_table(table, season_label):
        rows = []
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all(["th", "td"])
            cols = [td.get_text(strip=True) for td in tds]
            if len(cols) < 3:
                continue

            year = cols[0]
            city = cols[1]
            country = cols[2]
            notes = cols[3] if len(cols) > 3 else ""

            rows.append({
                "year": year,
                "city": city,
                "country": country,
                "season": season_label,
                "notes": notes
            })
        return rows

    data = []
    data += parse_table(summer_table, "Summer")
    data += parse_table(winter_table, "Winter")
    return data


def build_db(db_path="olympics.db"):
    data = scrape_hosts()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Simple schema focused on what you scraped
    cur.execute("""
    CREATE TABLE IF NOT EXISTS olympics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year TEXT,
        city TEXT,
        country TEXT,
        season TEXT,
        notes TEXT
    );
    """)

    # Clear old rows if you want a clean rebuild
    cur.execute("DELETE FROM olympics;")

    cur.executemany("""
    INSERT INTO olympics (year, city, country, season, notes)
    VALUES (:year, :city, :country, :season, :notes);
    """, data)

    conn.commit()
    conn.close()
    print(f"Inserted {len(data)} rows into olympics table.")


if __name__ == "__main__":
    build_db()