-- Task 1:
CREATE TYPE film AS (
    film    TEXT,
    votes   INTEGER,
    rating  REAL,
    filmid  TEXT
);

CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad'
);

CREATE TABLE actors (
    actorid         TEXT,
    actor           TEXT,
    films           film[],
    quality_class   quality_class,
    is_active       BOOLEAN,
    current_year    INTEGER,
    PRIMARY KEY(actorid, current_year)
);

-- Task 2:
INSERT INTO actors
WITH last_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 1990
)
, this_year AS (
    SELECT actorid
         , actor
         , year
         , ARRAY_AGG(ROW(film, votes, rating, filmid)::film) AS films
         , AVG(rating)                                       AS avg_rating
    FROM actor_films
    WHERE year = 1991
    GROUP BY actorid, actor, year
)

SELECT COALESCE(last_year.actorid, this_year.actorid) AS actorid
     , COALESCE(last_year.actor, this_year.actor) AS actor
     , CASE
           WHEN last_year.current_year IS NULL THEN
               this_year.films
           WHEN this_year.year IS NOT NULL THEN
               last_year.films || this_year.films
           ELSE last_year.films
       END AS films
     , CASE
           WHEN this_year.year IS NOT NULL THEN
                CASE
                    WHEN this_year.avg_rating > 8 THEN 'star'
                    WHEN this_year.avg_rating > 7 THEN 'good'
                    WHEN this_year.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END::quality_class
           ELSE last_year.quality_class
       END::quality_class AS quality_class
     , CASE
           WHEN this_year.year IS NOT NULL THEN TRUE
           ELSE FALSE
       END AS is_active
     , COALESCE(this_year.year, last_year.current_year + 1) AS current_year
FROM last_year
    FULL OUTER JOIN this_year USING (actorid)
;

-- Task 3:
CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor   TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE,
    snapshot_date DATE,
    PRIMARY KEY (actorid, start_date)
);

-- Task 4:
INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT actorid
         , actor
         , quality_class
         , is_active
         , current_year
         , LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class
         , LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
)
, with_change_indicator AS (
    SELECT *
         , CASE
               WHEN quality_class <> previous_quality_class THEN 1
               WHEN is_active <> previous_is_active THEN 1
               ELSE 0
           END AS change_indicator
    FROM with_previous
), with_streak AS (
    SELECT *
         , SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streaks
    FROM with_change_indicator
)

SELECT actorid
     , actor
     , quality_class
     , is_active
     , TO_DATE(MIN(current_year)::VARCHAR, 'YYYY') AS start_date
     , TO_DATE(MAX(current_year)::VARCHAR, 'YYYY') AS end_date
     , TO_DATE('1990', 'YYYY') AS snapshot_date
FROM with_streak
GROUP BY actorid, actor, streaks, quality_class, is_active
ORDER BY actorid, start_date;

-- Task 5:
CREATE TYPE scd_type AS (
    quality_class   quality_class,
    is_active       BOOLEAN,
    start_date      DATE,
    end_date        DATE
);

WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE snapshot_date = '1990-01-01'
        AND end_date = '1990-01-01'
)
, historical_scd AS (
    SELECT actorid
         , actor
         , quality_class
         , is_active
         , start_date
         , end_date
    FROM actors_history_scd
    WHERE snapshot_date = '1990-01-01'
        AND end_date < '1990-01-01'
)
, this_year_scd AS (
    SELECT *
    FROM actors
    WHERE current_year = 1991
)
, unchanged_records AS (
    SELECT ts.actorid
         , ts.actor
         , ts.quality_class
         , ts.is_active
         , ls.start_date
         , TO_DATE(ts.current_year::VARCHAR, 'YYYY') AS end_date
    FROM this_year_scd ts
        LEFT JOIN last_year_scd ls
            ON ts.actorid = ls.actorid
    WHERE ts.is_active <> ls.is_active
        AND ts.quality_class <> ls.quality_class
)
, changed_records AS (
    SELECT ts.actorid
         , ts.actor
         , UNNEST(ARRAY[
               ROW(ls.quality_class,
                   ls.is_active,
                   ls.start_date,
                   ls.end_date
               )::scd_type,
               ROW(ts.quality_class,
                   ts.is_active,
                   TO_DATE(ts.current_year::VARCHAR, 'YYYY'),
                   TO_DATE(ts.current_year::VARCHAR, 'YYYY')
               )::scd_type
           ]) AS records
    FROM this_year_scd ts
        LEFT JOIN last_year_scd ls
            ON ts.actorid = ls.actorid
    WHERE ts.quality_class <> ls.quality_class
       OR ts.is_active <> ls.is_active
)
, unnested_changed_records AS (
    SELECT actorid
         , actor
         , (records::scd_type).quality_class
         , (records::scd_type).is_active
         , (records::scd_type).start_date
         , (records::scd_type).end_date
    FROM changed_records
)
, new_rercords AS (
    SELECT ts.actorid
         , ts.actor
         , ts.quality_class
         , ts.is_active
         , TO_DATE(ts.current_year::VARCHAR, 'YYYY') AS start_date
         , TO_DATE(ts.current_year::VARCHAR, 'YYYY') AS end_date
    FROM this_year_scd ts
        LEFT JOIN last_year_scd ls
            ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
)

SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_rercords
;

