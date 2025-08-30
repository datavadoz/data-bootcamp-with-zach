from pyspark.sql import SparkSession


query = """
WITH yesterday AS (
    SELECT *
    FROM players
    WHERE current_season = 1995
)
, today AS (
    SELECT *
    FROM player_seasons
    WHERE season = 1996
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
            THEN ARRAY(t.pts)
        WHEN t.season IS NOT NULL
            THEN CONCAT(y.season_stats, ARRAY(t.pts))
        ELSE y.season_stats
    END AS season_stats,
    COALESCE(t.season, y.current_season + 1) AS current_season
FROM today AS t
    FULL OUTER JOIN yesterday AS y
        ON t.player_name = y.player_name
ORDER BY t.player_name
"""


def do_accumulative(spark):
    return spark.sql(query)
