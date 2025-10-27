-- queries.sql

-- 1. Which movie has the highest average rating?
-- We use a HAVING clause to filter out movies with too few ratings, improving relevance.
SELECT
    m.title,
    ROUND(AVG(r.rating), 1) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM movies AS m
JOIN ratings AS r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(r.rating) >= 5 -- Arbitrary threshold for a significant average
ORDER BY average_rating DESC
LIMIT 1;

---

-- 2. What are the top 5 movie genres that have the highest average rating?
SELECT
    g.genre_name,
    ROUND(AVG(r.rating), 1) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM genres AS g
JOIN movie_genres AS mg ON g.genre_id = mg.genre_id
JOIN ratings AS r ON mg.movie_id = r.movie_id
GROUP BY g.genre_name
HAVING COUNT(r.rating) >= 50 -- Higher threshold for genres as they have more total ratings
ORDER BY average_rating DESC
LIMIT 5;

---

-- 3. Who is the director with the most movies in this dataset?
SELECT
    director,
    COUNT(movie_id) AS movie_count
FROM movies
WHERE director IS NOT NULL AND director != 'N/A' -- Exclude missing/N/A directors
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

---

-- 4. What is the average rating of movies released each year?
SELECT
    release_year,
    ROUND(AVG(r.rating), 1) AS average_rating,
    COUNT(DISTINCT m.movie_id) AS movie_count,
    COUNT(r.rating) AS total_ratings
FROM movies AS m
JOIN ratings AS r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY release_year
HAVING movie_count >= 10 -- Only consider years with a decent number of movies
ORDER BY release_year ASC;