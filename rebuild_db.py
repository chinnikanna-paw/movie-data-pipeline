# rebuild_db.py
from sqlalchemy import create_engine, text
import sqlite3
import os

DB_FILE = "movies_pipeline.db"

# Read schema.sql
with open("schema.sql", "r") as f:
    sql_script = f.read()

# Use raw sqlite3 to run script
conn = sqlite3.connect(DB_FILE)
conn.executescript(sql_script)
conn.close()

print("Database rebuilt using schema.sql!")
print("Tables: movies, ratings, movie_details, temp_links")
print("TASK 3 SCHEMA APPLIED SUCCESSFULLY!")