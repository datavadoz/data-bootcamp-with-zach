/* I am using Postgres for following queries */

-- Query 3: Player who scored the most points for a single team
WITH games_augmented AS (
  SELECT team_id        AS team_id
    , player_id         AS player_id
    , COALESCE(pts, 0)  AS points
    , ROW_NUMBER() OVER (
      PARTITION BY team_id ORDER BY COALESCE(pts, 0) DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ranking
  FROM game_details
)

SELECT team_id
  , player_id
  , points
FROM games_augmented
WHERE ranking = 1
