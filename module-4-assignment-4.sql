/* I am using Postgres for following queries */

-- Query 4: Player who scored the most points in a single season
WITH games_augmented AS (
  SELECT g.season          AS season
    , gd.player_id         AS player_id
    , COALESCE(gd.pts, 0)  AS points
  FROM game_details gd
    INNER JOIN games g
      ON gd.game_id = g.game_id
)

SELECT season
  , player_id
  , SUM(points) AS points
FROM games_augmented
GROUP BY season, player_id
ORDER BY 3 DESC
LIMIT 1
;
