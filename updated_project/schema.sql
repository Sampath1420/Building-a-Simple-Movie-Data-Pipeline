-- schema.sql

-- 1. Movies Table (Stores core and enriched data)
CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    release_year INTEGER,
    imdb_id TEXT UNIQUE,
    director TEXT,
    plot TEXT,
    box_office TEXT,  -- Storing Box Office as a clean numerical value
    poster_url TEXT,
    runtime_minutes TEXT,
    metascore INTEGER,
    imdb_rating REAL
);

-- 2. Genres Lookup Table
CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT UNIQUE NOT NULL
);

-- 3. Junction Table for Many-to-Many relationship between Movies and Genres
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
);

-- 4. Ratings Table
CREATE TABLE IF NOT EXISTS ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
);