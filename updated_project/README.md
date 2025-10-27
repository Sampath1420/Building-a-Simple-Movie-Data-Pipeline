# Movie Analytics ETL Pipeline

## Overview

This project implements a complete **ETL (Extract, Transform, Load) pipeline** for movie analytics. It ingests raw movie and rating data, enriches it with external information from the **OMDb API**, processes genres, and stores the cleaned data in a **SQLite database** for analysis.  

The pipeline is designed to be **idempotent**, **cache-aware**, and **scalable**, with clear separation of extraction, transformation, and loading steps.

---

## Features

- Extracts movie and rating data from CSV files.
- Cleans and transforms movie data, including:
  - Extracting release year from movie titles.
  - Normalizing box office and runtime.
- Enriches movie data via **OMDb API** with details such as:
  - IMDb ID, director, plot, poster URL, runtime, box office, metascore.
- Implements a **cache mechanism** (`omdb_cache.csv`) to prevent redundant API calls.
- Handles many-to-many **movie-genre relationships**.
- Loads cleaned and enriched data into SQLite with proper relational schema:
  - Movies, Genres, Movie-Genres, Ratings.
- Executes predefined SQL queries for analytics:
  - Top-rated movies
  - Highest-rated genres
  - Most prolific directors
  - Year-wise average ratings

---

## Prerequisites

- Python 3.10+
- Packages:
  ```bash
  pip install pandas sqlalchemy requests python-dotenv
  ```
- SQLite (bundled with Python)
- A valid OMDb API key

---

## Project Structure

```
.
├── .env                  # Environment variables
├── movies.csv            # Sample movie data
├── ratings.csv           # Sample rating data
├── omdb_cache.csv        # OMDb API cache
├── schema.sql            # Database schema definition
├── queries.sql           # SQL queries for analysis
├── etl.py                # ETL pipeline script
├── sql.py                # Executes queries on the database
└── README.md             # Project documentation
```

---

## Configuration (.env)

```env
DATABASE_NAME = 'movie_analytics.db'
MOVIES_CSV = 'movies.csv'
RATINGS_CSV = 'ratings.csv'
OMDB_API_KEY = 'YOUR_OMDB_API_KEY'
OMDB_URL = 'http://www.omdbapi.com/?apikey={OMDB_API_KEY}&'
CACHE_FILE = 'omdb_cache.csv'
API_LIMIT = 1000
```

- `API_LIMIT`: Maximum number of daily OMDb API requests.
- `CACHE_FILE`: Stores previously fetched OMDb data to reduce API calls.

---

## How It Works

### 1️⃣ Extraction

- Reads raw movie and rating data from CSV files.
- Extracts release year and cleans movie titles.

### 2️⃣ Transformation & Enrichment

- Cleans OMDb API data.
- Enriches movies not yet in the cache.
- Updates cache file after successful/failed API calls.
- Processes genres into a lookup table and creates a many-to-many junction table.

### 3️⃣ Loading

- Loads data into SQLite database:
  - `movies`
  - `genres`
  - `movie_genres`
  - `ratings`
- Ensures idempotency using `IF NOT EXISTS` and `replace` strategies.

---

## Running the Pipeline

1. Add your **OMDb API key** to `.env`.
2. Run the ETL script:

```bash
python etl.py
```

- The script prints progress logs for extraction, transformation, enrichment, and loading.
- OMDb API requests respect the daily limit defined in `.env`.

---

## Running SQL Queries

- Execute predefined analytical queries:

```bash
python sql.py
```

- Queries included:
  1. Movie with the highest average rating
  2. Top 5 genres by average rating
  3. Director with the most movies
  4. Year-wise average rating of movies

---

## Database Schema

**Movies Table (`movies`)**: Stores core and enriched movie data.  

**Genres Table (`genres`)**: Lookup table for all genres.  

**Movie-Genres (`movie_genres`)**: Junction table for many-to-many relationships.  

**Ratings Table (`ratings`)**: Stores user ratings per movie.

---

## Notes

- API enrichment is **limited per day** to avoid exceeding OMDb free tier limits.
- Cache file (`omdb_cache.csv`) ensures that repeated runs do not redundantly fetch API data.
- Pipeline handles missing or malformed data gracefully (e.g., missing box office or runtime).

---

## 🚧 5. Challenges Faced

| **Challenge** | **Solution** |
|----------------|--------------|
| OMDb API rate limit (1000/day per key) | Added `.env` variable `API_LIMIT` and implemented caching |
| Title parsing inconsistencies (e.g., extra spaces, missing year) | Regex-based title cleaning + fallback handling |
| Large data handling during concatenation | Added null-safe concatenation logic |
| Idempotency of data loads | Used `schema.sql` + replace mode in SQLAlchemy |
| Missing or “N/A” values from API | Normalized to `None` for consistency |

---

## 🚀 6. Future Improvements & Scalability

| **Area** | **Improvement** |
|-----------|----------------|
| Scalability | Move from SQLite → PostgreSQL / Snowflake for large datasets |
| Performance | Parallel API requests (e.g., `aiohttp` for async fetching) |
| Scheduling | Automate daily runs via Apache Airflow or Prefect |
| Monitoring | Add logging and error tracking using `logging` + retry logic |
| Visualization | Integrate Power BI / Streamlit dashboard for analytics |
| Data Quality | Add validation rules for API fields (e.g., schema enforcement) |

---

## License

This project is open-source for educational and personal use.
