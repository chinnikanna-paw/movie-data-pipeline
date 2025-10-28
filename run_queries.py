# run_queries.py
import sqlite3
import os

DB_FILE = "movies_pipeline.db"
QUERIES_FILE = "queries.sql"

if not os.path.exists(QUERIES_FILE):
    print(f"ERROR: {QUERIES_FILE} not found!")
    exit()

# Read queries
with open(QUERIES_FILE, 'r') as f:
    sql_script = f.read()

# Connect and run
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Split and execute each statement
for statement in sql_script.split(';'):
    stmt = statement.strip()
    if stmt and not stmt.startswith('--') and not stmt.startswith('.'):
        print("\n" + "="*50)
        print(stmt.split('\n')[0])  # Print query title
        print("="*50)
        try:
            cursor.execute(stmt)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            # Print header
            print(" | ".join(cols))
            print("-" * 50)
            # Print rows
            for row in rows:
                print(" | ".join(str(x) for x in row))
        except Exception as e:
            print(f"Error: {e}")

conn.close()
print("\nALL QUERIES EXECUTED!")