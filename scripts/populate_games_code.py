import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "olympics.db"

# Known Winter Olympic years (including post-1994 staggered schedule)
WINTER_YEARS = {
    1924, 1928, 1932, 1936,
    1948, 1952, 1956, 1960,
    1964, 1968, 1972, 1976,
    1980, 1984, 1988, 1992,
    1994, 1998, 2002, 2006,
    2010, 2014, 2018, 2022,
}

if __name__ == "__main__":
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    try:
        cur = conn.cursor()
        # Ensure games_code column exists
        cols = [row[1] for row in cur.execute("PRAGMA table_info(olympics)").fetchall()]
        if "games_code" not in cols:
            cur.execute("ALTER TABLE olympics ADD COLUMN games_code TEXT")

        # Set default Summer code: SYYYY for all rows with a numeric year
        cur.execute("UPDATE olympics SET games_code = 'S' || year WHERE year GLOB '[0-9]*'")
        summer_assigned = cur.rowcount

        # Overwrite Winter years to WYYYY where year matches known winter set
        # Use parameterized IN via temporary table approach for portability
        cur.execute("CREATE TEMP TABLE tmp_w(year TEXT)")
        cur.executemany("INSERT INTO tmp_w(year) VALUES (?)", [(str(y),) for y in sorted(WINTER_YEARS)])
        cur.execute(
            "UPDATE olympics SET games_code = 'W' || year WHERE year IN (SELECT year FROM tmp_w)"
        )
        winter_assigned = cur.rowcount

        conn.commit()
        print(f"games_code populated: Summer set={summer_assigned}, Winter set={winter_assigned}")
    finally:
        conn.close()
