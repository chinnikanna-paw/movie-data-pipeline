-- schema.sql
-- Movie Data Pipeline - Final Database Schema
-- Task 3: Data Modeling

-- Drop tables if exist (for clean rebuild)
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movie_details;
DROP TABLE IF EXISTS temp_links;
DROP TABLE IF EXISTS movies;

-- 1. movies table
CREATE TABLE movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    year TEXT,
    genres TEXT
);

-- 2. ratings table
CREATE TABLE ratings (
    ratingId INTEGER PRIMARY KEY AUTOINCREMENT,
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- 3. movie_details table (for OMDB API)
CREATE TABLE movie_details (
    movieId INTEGER PRIMARY KEY,
    imdbId TEXT,
    director TEXT,
    plot TEXT,
    box_office TEXT,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- 4. temp_links table (for API lookup)
CREATE TABLE temp_links (
    movieId INTEGER PRIMARY KEY,
    imdbId TEXT NOT NULL,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ratings_movieId ON ratings(movieId);
CREATE INDEX IF NOT EXISTS idx_ratings_userId ON ratings(userId);

-- Confirmation
PRAGMA foreign_keys = ON;