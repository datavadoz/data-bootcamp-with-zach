from pyspark.sql import SparkSession


query = """
WITH dedup AS (
  SELECT game_id
    , team_id
    , player_id
    , min
    , ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) rn
  FROM game_details
)

SELECT game_id
  , team_id
  , player_id
  , min
FROM dedup
WHERE rn = 1
ORDER BY game_id
"""


def do_dedup(spark):
    return spark.sql(query)
