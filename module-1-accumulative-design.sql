--  Create a new struct named season_stats
CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAL,
	ast REAL
);

-- Create a players table
CREATE TABLE players (
	player_name 	TEXT,
	height 			TEXT,
	college 		TEXT,
	country 		TEXT,
	draft_year 		TEXT,
	draft_round 	TEXT,
	draft_number 	TEXT,
	season_stats	season_stats[],
	current_season	INTEGER,
	PRIMARY KEY(player_name, current_season)
)
;

-- Insert data accumuatively
INSERT INTO players
WITH yesterday AS (
	SELECT *
	FROM players
	WHERE current_season = 2000
)
, today AS (
	SELECT *
	FROM player_seasons
	WHERE season = 2001
)

SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS height,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE
		WHEN y.season_stats IS NULL 
			THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
		WHEN t.season IS NOT NULL
			THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
		ELSE y.season_stats
	END,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
	FULL OUTER JOIN yesterday y
		ON t.player_name = y.player_name
;

-- Unpack struct data into seperated columns
SELECT player_name, (UNNEST(season_stats)::season_stats).*
FROM public.players
WHERE current_season = 2001
;

-- Drop players table :(
DROP TABLE players;

-- Create another new data type (enum of scoring class)
CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create a players table v2
CREATE TABLE players (
	player_name 			TEXT,
	height 					TEXT,
	college 				TEXT,
	country 				TEXT,
	draft_year 				TEXT,
	draft_round 			TEXT,
	draft_number 			TEXT,
	season_stats			season_stats[],
	scoring_class			scoring_class,
	years_since_last_season	INTEGER,
	current_season	INTEGER,
	PRIMARY KEY(player_name, current_season)
)
;

-- Insert data accumuatively
INSERT INTO players
WITH yesterday AS (
	SELECT *
	FROM players
	WHERE current_season = 2000
)
, today AS (
	SELECT *
	FROM player_seasons
	WHERE season = 2001
)

SELECT
	-- Set of non-temporal attributes
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS height,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,

	-- season_stats
	CASE
		WHEN y.season_stats IS NULL 
			THEN ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
		WHEN t.season IS NOT NULL
			THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
		ELSE y.season_stats
	END AS season_stats,

	-- scoring_class
	CASE
		WHEN t.season IS NOT NULL THEN
			CASE
				WHEN t.pts > 20 THEN 'star'
				WHEN t.pts > 15 THEN 'good'
				WHEN t.pts > 10 THEN 'average'
				ELSE 'bad'
			END::scoring_class
		ELSE y.scoring_class
	END AS scoring_class,

	-- years_since_last_season
	CASE
		WHEN t.season IS NOT NULL THEN 0
		ELSE y.years_since_last_season + 1
	END AS years_since_last_season,

	-- current_season
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
	FULL OUTER JOIN yesterday y
		ON t.player_name = y.player_name
;

-- Find out players that have good improvement from the first to current season.
SELECT
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts / 
	CASE
		WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
		ELSE (season_stats[1]::season_stats).pts
	END
FROM public.players
WHERE current_season = 2001
ORDER BY 2 DESC
;

