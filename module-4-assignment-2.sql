/* I am using Postgres for following queries */

/* Schema of game_details table:
    game_id integer
    team_id integer
    team_abbreviation text
    team_city text
    player_id integer
    player_name text
    nickname text
    start_position text
    comment text
    min text
    fgm real
    fga real
    fg_pct real
    fg3m real
    fg3a real
    fg3_pct real
    ftm real
    fta real
    ft_pct real
    oreb real
    dreb real
    reb real
    ast real
    stl real
    blk real
    "TO" real
    pf real
    pts real
    plus_minus real
*/

/* Schema of games table:
    game_date_est date
    game_id integer
    game_status_text tex
    home_team_id integer
    visitor_team_id integer
    season integer
    team_id_home integer
    pts_home real
    fg_pct_home real
    ft_pct_home real
    fg3_pct_home real
    ast_home real
    reb_home real
    team_id_away integer
    pts_away real
    fg_pct_away real
    ft_pct_away real
    fg3_pct_away real
    ast_away real
    reb_away real
    home_team_wins intege
*/

-- Query 2: GROUPING SETS on game_details
WITH games_augmented AS (
  SELECT gd.game_id        AS game_id
    , gd.player_id         AS player_id
    , gd.team_id           AS team_id
    , g.home_team_id       AS home_team_id
    , g.visitor_team_id    AS visitor_team_id
    , g.home_team_wins     AS home_team_wins
    , g.season             AS season
    , CASE
        WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN TRUE
        WHEN gd.team_id <> g.home_team_id AND home_team_wins = 0 THEN TRUE
        ELSE FALSE
      END AS win
    , COALESCE(gd.pts, 0)  AS points
  FROM game_details gd
    INNER JOIN games g
      ON gd.game_id = g.game_id
)

SELECT season
  , team_id
  , player_id
  , SUM(points) AS points
  , COUNT(DISTINCT(CASE WHEN win = TRUE THEN game_id ELSE NULL END)) AS win_counts
  , GROUPING(season, team_id, player_id) AS grp_id
FROM games_augmented
GROUP BY GROUPING SETS (
  (team_id, player_id),
  (season, player_id),
  (team_id)
)
ORDER BY grp_id, points DESC, win_counts DESC;
;
