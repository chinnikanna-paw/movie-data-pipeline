-- queries.sql
-- Task 5: Analytical Questions
-- Database: movies_pipeline.db

-- Enable pretty printing
.headers on
.mode column

-- ========================================
-- 1. Which movie has the highest average rating?
-- ========================================
.print
.print "1. Movie with highest average rating:"
.print "-------------------------------------"

SELECT 
    m.title,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(r.rating) AS num_ratings
FROM/movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;

-- ========================================
-- 2. Top 5 genres with highest average rating
-- ========================================
.print
.print "2. Top 5 genres by average rating:"
.print "-----------------------------------"

WITH genre_ratings AS (
    SELECT 
        TRIM(value) AS genre,
        r.rating
    FROM movies m
    CROSS JOIN JSON_EACH('["' || REPLACE(m.genres, '|', '","') || '"]') 
    JOIN ratings r ON m.movieId = r.movieId
    WHERE genre != 'IMAX' AND genre != '(no genres listed)'
),
genre_stats AS (
    SELECT 
        genre,
        ROUND(AVG(rating), 2) AS avg_rating,
        COUNT(*) AS num_ratings
    FROM genre_ratings
    GROUP BY genre
    HAVING num_ratings >= 10  -- Filter out rare genres
)
SELECT 
    genre,
    avg_rating,
    num_ratings
FROM genre_stats
ORDER BY avg_rating DESC
LIMIT 5;

-- ========================================
-- 3. Director with the most movies
-- ========================================
.print
.print "3. Director with most movies:"
.print "-----------------------------"

SELECT 
    director,
    COUNT(*) AS movie_count
FROM movie_details
WHERE director IS NOT NULL AND director != 'N/A'
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- ========================================
-- 4. Average rating per year
-- ========================================
.print
.print "4. Average rating by release year:"
.print "----------------------------------"

SELECT 
    CAST(m.year AS INTEGER) AS release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY release_year
HAVING num_ratings >= 5
ORDER BY release_year DESC;