#!/usr/bin/env python3
"""
Run a .sql file against the Olympics SQLite database.

Usage:
  python3 scripts/run_sql.py Queries.sql            # uses default DB path
  python3 scripts/run_sql.py Queries.sql data/olympics.db

Prints each statement and any resulting rows; for scalar aggregates like COUNT(*) prints the value.
"""
import sys
import os
import sqlite3

DEFAULT_DB = os.path.join('data', 'olympics.db')

def read_sql_file(path: str) -> str:
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def split_statements(sql_text: str):
    # Simple splitter on semicolons; ignores semicolons inside strings minimally.
    statements = []
    current = []
    in_single = False
    in_double = False
    for ch in sql_text:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if ch == ';' and not in_single and not in_double:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
    tail = ''.join(current).strip()
    if tail:
        statements.append(tail)
    return statements

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/run_sql.py <file.sql> [db_path]", file=sys.stderr)
        sys.exit(1)
    sql_file = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DB
    if not os.path.exists(sql_file):
        print(f"SQL file not found: {sql_file}", file=sys.stderr)
        sys.exit(2)
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}", file=sys.stderr)
        sys.exit(3)
    sql_text = read_sql_file(sql_file)
    statements = split_statements(sql_text)
    if not statements:
        print("No SQL statements found.")
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    for i, stmt in enumerate(statements, start=1):
        print(f"-- Statement {i} --\n{stmt}")
        try:
            cur.execute(stmt)
            if cur.description:  # query with result set
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                print('Columns:', ', '.join(cols))
                if rows:
                    for r in rows:
                        print('| ' + ' | '.join(str(c) for c in r))
                else:
                    print('(no rows)')
            else:
                print(f"(OK, {cur.rowcount} rows affected)")
        except sqlite3.Error as e:
            print(f"ERROR executing statement {i}: {e}", file=sys.stderr)
    conn.close()

if __name__ == '__main__':
    main()
