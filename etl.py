# etl.py
import pandas as pd
import requests
import sqlite3
import json
import time
import os
from tqdm import tqdm

# CONFIG
DB_FILE = "movies_pipeline.db"
DATA_DIR = "data"
OMDB_API_KEY = "addc3d09"  # ← YOUR KEY
OMDB_URL = "http://www.omdbapi.com/"
ENRICH_LIMIT = 100
PROGRESS_FILE = "etl_progress.json"

print("STARTING ETL PIPELINE...")

# 1. EXTRACT
print("\n1. EXTRACT: Reading CSVs...")
movies_df = pd.read_csv(os.path.join(DATA_DIR, "movies.csv"))
ratings_df = pd.read_csv(os.path.join(DATA_DIR, "ratings.csv"))
links_df = pd.read_csv(os.path.join(DATA_DIR, "links.csv"))

movie_links = movies_df.merge(links_df, on='movieId', how='left')
movie_links['imdbId'] = 'tt' + movie_links['imdbId'].astype(str).str.zfill(7)

rating_counts = ratings_df['movieId'].value_counts()
movie_links['rating_count'] = movie_links['movieId'].map(rating_counts)
movie_links = movie_links.sort_values('rating_count', ascending=False)

if ENRICH_LIMIT:
    movie_links = movie_links.head(ENRICH_LIMIT)
    print(f"   • Enriching TOP {ENRICH_LIMIT} most-rated movies")

start_idx = 0
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        progress = json.load(f)
        start_idx = progress.get('last_index', 0) + 1
    print(f"   • Resuming from movie #{start_idx}")
else:
    print(f"   • Starting from scratch")

# 2. ENRICH
print("\n2. ENRICH: Calling OMDB API...")
enriched_data = []

for idx, row in tqdm(movie_links.iloc[start_idx:].iterrows(), 
                     total=len(movie_links)-start_idx, 
                     desc="OMDB API"):
    
    imdb_id = row['imdbId']
    api_url = f"{OMDB_URL}?i={imdb_id}&apikey={OMDB_API_KEY}"
    
    try:
        response = requests.get(api_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'True':
                enriched_data.append({
                    'movieId': row['movieId'],
                    'imdbId': imdb_id,
                    'director': data.get('Director'),
                    'plot': data.get('Plot'),
                    'box_office': data.get('BoxOffice'),
                    'runtime': data.get('Runtime')
                })
            else:
                enriched_data.append({'movieId': row['movieId'], 'imdbId': imdb_id, 'director': None, 'plot': None, 'box_office': None, 'runtime': None})
        else:
            enriched_data.append({'movieId': row['movieId'], 'imdbId': imdb_id, 'director': None, 'plot': None, 'box_office': None, 'runtime': None})
    except:
        enriched_data.append({'movieId': row['movieId'], 'imdbId': imdb_id, 'director': None, 'plot': None, 'box_office': None, 'runtime': None})
    
    if (idx + 1) % 10 == 0:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({'last_index': idx}, f)
    
    time.sleep(0.1)

with open(PROGRESS_FILE, 'w') as f:
    json.dump({'last_index': len(movie_links)-1, 'completed': True}, f)

enriched_df = pd.DataFrame(enriched_data)

# 2. TRANSFORM: SAFE
print("\n2. TRANSFORM: Cleaning & Feature Engineering...")
if not enriched_df.empty:
    enriched_df['box_office'] = enriched_df['box_office'].str.replace(r'[\$,]', '', regex=True)
    enriched_df['box_office'] = pd.to_numeric(enriched_df['box_office'], errors='coerce')
    enriched_df['runtime_min'] = enriched_df['runtime'].fillna('').str.extract(r'(\d+)').astype(float)
else:
    print("   • No new movies to transform")

movies_df['year'] = movies_df['title'].str.extract(r'\((\d{4})\)').astype(float)
movies_df['decade'] = (movies_df['year'].fillna(0) // 10 * 10).astype(int)

if not enriched_df.empty:
    final_movies = movies_df.merge(enriched_df, on='movieId', how='left')
else:
    final_movies = movies_df.copy()
    print("   • No new enriched data to merge")

# 3. LOAD (USING INSERT OR REPLACE)
print("\n3. LOAD: Saving to database...")
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")

# Load movies
for _, row in final_movies.iterrows():
    cursor.execute("""
    INSERT OR REPLACE INTO movies (movieId, title, year, genres)
    VALUES (?, ?, ?, ?)
    """, (row['movieId'], row['title'], row['year'], row['genres']))

# Load ratings
for _, row in ratings_df.iterrows():
    cursor.execute("""
    INSERT OR IGNORE INTO ratings (userId, movieId, rating, timestamp)
    VALUES (?, ?, ?, ?)
    """, (row['userId'], row['movieId'], row['rating'], row['timestamp']))

# Load movie_details
if not enriched_df.empty:
    for _, row in enriched_df.iterrows():
        cursor.execute("""
        INSERT OR REPLACE INTO movie_details (movieId, imdbId, director, plot, box_office)
        VALUES (?, ?, ?, ?, ?)
        """, (row['movieId'], row['imdbId'], row['director'], row['plot'], row['box_office']))

conn.commit()
conn.close()

print(f"\nETL COMPLETE! Enriched {len(enriched_df)} movies")
if os.path.exists(PROGRESS_FILE):
    os.remove(PROGRESS_FILE)