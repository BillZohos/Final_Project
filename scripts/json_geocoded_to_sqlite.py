import json
import os
import sqlite3

DATA_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "olympics_games_geocoded.json")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "olympics_geocoded.db")
TABLE_NAME = "olympics_geocoded"


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def init_db(conn):
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        year INTEGER NOT NULL,
        city TEXT NOT NULL,
        country TEXT NOT NULL,
        season TEXT NOT NULL,
        notes TEXT,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        geocode_provider TEXT,
        PRIMARY KEY (year, city, country)
    )"""
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_season ON {TABLE_NAME}(season)")


def insert_rows(conn, records):
    rows = []
    for key, rec in records.items():
        rows.append(
            (
                rec.get("year"),
                rec.get("city"),
                rec.get("country"),
                rec.get("season"),
                rec.get("notes", ""),
                rec.get("lat"),
                rec.get("lon"),
                rec.get("geocode_provider"),
            )
        )
    conn.executemany(
        f"INSERT OR REPLACE INTO {TABLE_NAME} (year, city, country, season, notes, lat, lon, geocode_provider) VALUES (?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return len(rows)


def main():
    if not os.path.isfile(DATA_JSON_PATH):
        raise FileNotFoundError(f"JSON data file not found at {DATA_JSON_PATH}")
    data = load_json(DATA_JSON_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        init_db(conn)
        count = insert_rows(conn, data)
        print(f"Inserted {count} rows into {DB_PATH} table {TABLE_NAME}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
