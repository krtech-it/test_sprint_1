CREATE SCHEMA IF NOT EXISTS content;

SET search_path TO content,public;

CREATE TABLE IF NOT EXISTS film_work
(
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS person
(
    id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS person_film_work
(
    id UUID PRIMARY KEY,
    person_id UUID NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    film_work_id UUID NOT NULL REFERENCES film_work (id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS genre
(
    id UUID PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS genre_film_work
(
    id UUID PRIMARY KEY,
    genre_id UUID NOT NULL REFERENCES genre (id) ON DELETE CASCADE,
    film_work_id UUID NOT NULL REFERENCES film_work (id) ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX film_work_genre_idx ON genre_film_work(film_work_id, genre_id);

CREATE UNIQUE INDEX film_work_person_idx ON person_film_work(film_work_id, person_id, role);

CREATE INDEX person_idx ON person_film_work(person_id);

CREATE INDEX film_work_creation_date_idx ON film_work(creation_date);
