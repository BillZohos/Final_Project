import sqlite3
from pathlib import Path

# Path to the SQLite database
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "olympics.db"

def ensure_columns(conn: sqlite3.Connection) -> None:
    """Ensure required columns (season, games_code) exist."""
    cur = conn.execute("PRAGMA table_info(olympics)")
    existing_cols = [row[1] for row in cur.fetchall()]
    if "season" not in existing_cols:
        conn.execute("ALTER TABLE olympics ADD COLUMN season TEXT")
    if "games_code" not in existing_cols:
        conn.execute("ALTER TABLE olympics ADD COLUMN games_code TEXT")
    conn.commit()

def update_season(conn: sqlite3.Connection) -> None:
    """Update season based on games_code prefixes and print counts."""
    cur = conn.cursor()
    summer_count = cur.execute("UPDATE olympics SET season='Summer' WHERE games_code LIKE 'S%'").rowcount
    winter_count = cur.execute("UPDATE olympics SET season='Winter' WHERE games_code LIKE 'W%'").rowcount
    conn.commit()
    total = summer_count + winter_count
    print(f"Updated rows: Summer={summer_count}, Winter={winter_count}, Total={total}")

if __name__ == "__main__":
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    try:
        ensure_columns(conn)
        update_season(conn)
    finally:
        conn.close()
