import pandas as pd
import sqlite3
import requests
import re
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Load environment variables from .env file ---
load_dotenv()

# --- Configuration ---
DATABASE_NAME = os.getenv('DATABASE_NAME')
MOVIES_CSV = os.getenv('MOVIES_CSV')
RATINGS_CSV = os.getenv('RATINGS_CSV')
OMDB_API_KEY = os.getenv('OMDB_API_KEY')
OMDB_URL = os.getenv('OMDB_URL')
CACHE_FILE = os.getenv('CACHE_FILE')
API_LIMIT = int(os.getenv('API_LIMIT', 1000))  # Ensure integer type

# -------------------------------------------------------------
#                  UTILITY FUNCTIONS
# -------------------------------------------------------------

def extract_year(title):
    """Extract the 4-digit year at the end of a movie title like 'Toy Story (1995)'."""
    match = re.search(r'\((\d{4})\)$', str(title))
    return int(match.group(1)) if match else None


def fetch_omdb_data(title, year):
    """Fetch movie details from OMDb API using title and year."""
    clean_title = re.sub(r'\s*\(\d{4}\)\s*$', '', str(title)).strip()

    params = {
        't': clean_title,
        'y': int(year) if pd.notna(year) else '',
        'plot': 'short',
        'r': 'json',
        'apikey': OMDB_API_KEY
    }

    try:
        response = requests.get('http://www.omdbapi.com/', params=params, timeout=5)
        response.raise_for_status()
        data = response.json()

        if data.get('Response') == 'True':
            return {
                'imdb_id': data.get('imdbID'),
                'director': data.get('Director'),
                'plot': data.get('Plot'),
                'box_office': data.get('BoxOffice'),
                'poster_url': data.get('Poster'),
                'runtime_minutes': data.get('Runtime'),
                'metascore': int(data.get('Metascore')) if data.get('Metascore') not in [None, 'N/A'] else None,
                'imdb_rating': float(data.get('imdbRating')) if data.get('imdbRating') not in [None, 'N/A'] else None  # ✅ NEW
            }
        else:
            print(f"No OMDb data found for: {clean_title} ({year}) - {data.get('Error')}")
            return None

    except requests.exceptions.RequestException as e:
        print(f" API Error for {clean_title}: {e}")
        return None


# -------------------------------------------------------------
#                     MAIN ETL PIPELINE
# -------------------------------------------------------------

def etl_pipeline():
    """Executes the complete ETL pipeline."""
    print("--- Starting ETL Pipeline ---")

    # ------------------ 1️⃣ Extraction ------------------
    print("1. Extracting data from CSV files...")
    df_movies = pd.read_csv(MOVIES_CSV)
    df_ratings = pd.read_csv(RATINGS_CSV)

    # ------------------ 2️⃣ Transformation + Enrichment ------------------
    print("2. Transforming and Enriching Movie data...")

    df_movies['release_year'] = df_movies['title'].apply(extract_year)
    df_movies['title_cleaned'] = df_movies.apply(
        lambda row: row['title'].replace(f" ({row['release_year']})", "") if row['release_year'] else row['title'],
        axis=1
    )

    # Load cache
    if os.path.exists(CACHE_FILE):
        df_cache = pd.read_csv(CACHE_FILE)
        print(f"  > Loaded {len(df_cache)} cached OMDb records.")
    else:
        df_cache = pd.DataFrame(columns=[
            'movieId', 'title', 'release_year', 'status', 'imdb_id',
            'director', 'plot', 'box_office', 'poster_url',
            'runtime_minutes', 'metascore', 'imdb_rating'  # ✅ Added imdb_rating
        ])
        print("  > No existing cache found. Starting fresh.")

    processed_ids = set(df_cache['movieId'])
    movies_to_enrich = df_movies[df_movies['release_year'].notna()]
    movies_to_enrich = movies_to_enrich[~movies_to_enrich['movieId'].isin(processed_ids)]
    total_new = len(movies_to_enrich)
    print(f"  > {total_new} movies to fetch from OMDb (not in cache).")

    omdb_data_list = []
    failed_data_list = []
    processed_count = 0

    for index, row in movies_to_enrich.iterrows():
        if processed_count >= API_LIMIT:
            print(f"\n Reached daily API limit ({API_LIMIT}). Stopping enrichment for today.\n")
            break

        if processed_count % 100 == 0:
            print(f"  > Processing movie {processed_count + 1}/{min(total_new, API_LIMIT)}...")

        omdb_result = fetch_omdb_data(row['title_cleaned'], row['release_year'])
        processed_count += 1

        if omdb_result:
            omdb_data_list.append({
                **omdb_result,
                'movieId': row['movieId'],
                'title': row['title_cleaned'],
                'release_year': row['release_year'],
                'status': 'success'
            })
        else:
            failed_data_list.append({
                'movieId': row['movieId'],
                'title': row['title_cleaned'],
                'release_year': row['release_year'],
                'status': 'failed'
            })

    # Update cache (skip empty frames)
    if omdb_data_list or failed_data_list:
        df_new_success = pd.DataFrame(omdb_data_list)
        df_new_failed = pd.DataFrame(failed_data_list)
        df_new_combined = pd.concat([df_new_success, df_new_failed], ignore_index=True)

        frames_to_concat = [df for df in [df_cache, df_new_combined] if not df.empty]
        df_cache = pd.concat(frames_to_concat, ignore_index=True) if frames_to_concat else df_cache

        df_cache.to_csv(CACHE_FILE, index=False)
        print(f"  > Cache updated with {len(df_new_combined)} new records (total: {len(df_cache)}).")

    # Use only successfully enriched movies for DB insertion
    df_omdb_success = df_cache[df_cache['status'] == 'success'].drop_duplicates(subset=['movieId'])
    df_movies_enriched = df_movies.merge(df_omdb_success, on='movieId', how='inner')

    # --- FIX: Normalize column names & drop duplicates ---
    df_movies_enriched.rename(columns={'title_x': 'title', 'release_year_x': 'release_year'}, inplace=True)
    if 'release_year_y' in df_movies_enriched.columns:
        df_movies_enriched.drop(columns=['release_year_y'], inplace=True)

    expected_cols = [
        'movieId', 'title', 'release_year', 'imdb_id', 'director',
        'plot', 'box_office', 'poster_url', 'runtime_minutes', 'metascore', 'imdb_rating'  # ✅ Added imdb_rating
    ]
    available_cols = [c for c in expected_cols if c in df_movies_enriched.columns]
    df_movies_load = df_movies_enriched[available_cols].rename(columns={'movieId': 'movie_id'})

    # ------------------ Genre processing ------------------
    print("  > Processing genres...")
    all_genres = set()
    for genres_str in df_movies['genres'].dropna():
        for genre in genres_str.split('|'):
            if genre and genre != '(no genres listed)':
                all_genres.add(genre)

    df_genres = pd.DataFrame(sorted(list(all_genres)), columns=['genre_name'])
    df_genres['genre_id'] = df_genres.index + 1

    movie_genre_mappings = []
    genre_to_id = df_genres.set_index('genre_name')['genre_id'].to_dict()

    for index, row in df_movies.iterrows():
        if pd.notna(row['genres']) and row['genres'] != '(no genres listed)':
            for genre in row['genres'].split('|'):
                if genre in genre_to_id:
                    movie_genre_mappings.append({'movie_id': row['movieId'], 'genre_id': genre_to_id[genre]})
    df_movie_genres = pd.DataFrame(movie_genre_mappings)

    df_ratings_load = df_ratings[['userId', 'movieId', 'rating', 'timestamp']].rename(
        columns={'userId': 'user_id', 'movieId': 'movie_id'}
    )

    # ------------------ 3️⃣ Loading into SQLite ------------------
    print("3. Loading data into SQLite database...")

    engine = create_engine(f'sqlite:///{DATABASE_NAME}')

    with engine.connect() as conn:
        print("  > Creating/Recreating Tables (Idempotency Step)...")
        with open('schema.sql', 'r') as f:
            sql_script = f.read()

        for statement in sql_script.strip().split(';'):
            if statement.strip():
                conn.exec_driver_sql(statement + ';')

    print("  > Loading Genres...")
    df_genres.to_sql('genres', engine, if_exists='replace', index=False)

    print("  > Loading Movies (only successfully enriched)...")
    df_movies_load.to_sql('movies', engine, if_exists='replace', index=False)

    print("  > Loading Movie Genres Junction Table...")
    df_movie_genres.to_sql('movie_genres', engine, if_exists='replace', index=False)

    print("  > Loading Ratings...")
    df_ratings_load.to_sql('ratings', engine, if_exists='replace', index=False)

    print("--- ETL Pipeline Completed Successfully! ---")


# -------------------------------------------------------------
#                     ENTRY POINT
# -------------------------------------------------------------
if __name__ == '__main__':
    if not OMDB_API_KEY or OMDB_API_KEY == 'YOUR_OMDB_API_KEY':
        print("!!! ERROR: Please set a valid OMDb API key in your .env file !!!")
    else:
        etl_pipeline()
