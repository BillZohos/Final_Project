import sqlite3
import pandas as pd

DB_PATH = "olympics.db"

def export_csv():
    conn = sqlite3.connect(DB_PATH)

    # 1) Full data set for filtering / tables in the app
    hosts_full = pd.read_sql_query("""
        SELECT year, season, city, country, notes
        FROM olympics
        ORDER BY year;
    """, conn)
    hosts_full.to_csv("app/hosts_full.csv", index=False)

    # 2) Summary by country for a bar chart
    hosts_by_country = pd.read_sql_query("""
        SELECT country, COUNT(*) AS times_hosted
        FROM olympics
        GROUP BY country
        ORDER BY times_hosted DESC;
    """, conn)
    hosts_by_country.to_csv("app/hosts_by_country.csv", index=False)

    # 3) Optional: summary by season
    hosts_by_season = pd.read_sql_query("""
        SELECT season, COUNT(*) AS total_games
        FROM olympics
        GROUP BY season;
    """, conn)
    hosts_by_season.to_csv("app/hosts_by_season.csv", index=False)

    conn.close()
    print("CSV exports written to app/.")

if __name__ == "__main__":
    export_csv()
