from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, when, to_timestamp, avg, count
from pyspark.sql.types import IntegerType


# Create spark session
spark = SparkSession.builder.appName("Homework_1").getOrCreate()

# Disabled automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Explicitly broadcast join medals
medal_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")
medal_matches_players = medals_matches_players.join(broadcast(medals), "medal_id")

# Explicitly broadcast join maps
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")
matches = matches.join(broadcast(maps), "mapid")

# Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
## Bucketing matches dataframe
matches_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
    match_id STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    mapid STRING,
    completion_date TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""

spark.sql("DROP TABLE IF EXISTS bootcamp.matches_bucketed")
spark.sql(matches_bucketed_ddl)
matches_bucketed_df = matches.select(col("match_id"), col("is_team_game"), col("playlist_id"), col("mapid"), col("completion_date"))
matches_bucketed_df = matches_bucketed_df.withColumn(
    "is_team_game",
    when(col("is_team_game").isin("true", "TRUE", "1"), True)
    .when(col("is_team_game").isin("false", "FALSE", "0"), False)
    .otherwise(None)
)
matches_bucketed_df = matches_bucketed_df.withColumn(
    "completion_date",
    to_timestamp(col("completion_date"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
)
matches_bucketed_df.write.mode("append").bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")

## Bucketing match_details dataframe
match_details_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""

spark.sql("DROP TABLE IF EXISTS bootcamp.match_details_bucketed")
spark.sql(match_details_bucketed_ddl)
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")
match_details_bucketed_df = match_details.select(col("match_id"), col("player_gamertag"), col("player_total_kills"), col("player_total_deaths"))
match_details_bucketed_df = match_details_bucketed_df.withColumn("player_total_kills", match_details_bucketed_df["player_total_kills"].cast(IntegerType()))
match_details_bucketed_df = match_details_bucketed_df.withColumn("player_total_deaths", match_details_bucketed_df["player_total_deaths"].cast(IntegerType()))
match_details_bucketed_df.write.mode("append").bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")

## Bucketing medal_matches_players dataframe
medal_matches_players_bucketed_ddl = """
CREATE TABLE IF NOT EXISTS bootcamp.medal_matches_players_bucketed (
    match_id STRING,
    player_gamertag STRING,
    medal_id INTEGER,
    name STRING,
    count INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""

spark.sql("DROP TABLE IF EXISTS bootcamp.medal_matches_players_bucketed")
spark.sql(medal_matches_players_bucketed_ddl)
medal_matches_players_bucketed_df = medal_matches_players.select(col("match_id"), col("player_gamertag"), col("medal_id"), col("name"), col("count"))
medal_matches_players_bucketed_df = medal_matches_players_bucketed_df.withColumn("medal_id", medal_matches_players_bucketed_df["medal_id"].cast(IntegerType()))
medal_matches_players_bucketed_df = medal_matches_players_bucketed_df.withColumn("count", medal_matches_players_bucketed_df["count"].cast(IntegerType()))
medal_matches_players_bucketed_df.write.mode("append").bucketBy(16, "match_id").saveAsTable("bootcamp.medal_matches_players_bucketed")

## Perform bucket join
bucketed_df = spark.sql("""
SELECT mb.match_id             AS match_id
    , mb.is_team_game          AS is_team_game
    , mb.playlist_id           AS playlist_id
    , mb.mapid                 AS mapid
    , mb.completion_date       AS completion_date
    , mdb.player_gamertag      AS player_gamertag
    , mdb.player_total_kills   AS player_total_kills
    , mdb.player_total_deaths  AS player_total_deaths
    , mmpd.medal_id            AS medal_id
    , mmpd.name                AS medal_name
    , mmpd.count               AS count
FROM bootcamp.match_details_bucketed mdb
    JOIN bootcamp.matches_bucketed mb
        ON mdb.match_id = mb.match_id
    JOIN bootcamp.medal_matches_players_bucketed mmpd
        ON mdb.match_id = mmpd.match_id
""")

# Aggregate the joined data frame to figure out questions:
# 1. Which player averages the most kills per game?
# 2. Which playlist gets played the most?
# 3. Which map gets played the most?
# 4. Which map do players get the most Killing Spree medals on?

q1 = bucketed_df.groupBy("player_gamertag")\
    .agg(avg("player_total_kills").alias("avg_kills"))\
    .orderBy(col("avg_kills").desc())\
    .limit(1)
q1.show()

q2 = bucketed_df.groupBy("playlist_id")\
    .agg(count("playlist_id").alias("playing_count"))\
    .orderBy(col("playing_count").desc())\
    .limit(1)
q2.show()

q3 = bucketed_df.groupBy("mapid")\
    .agg(count("mapid").alias("playing_count"))\
    .orderBy(col("playing_count").desc())\
    .limit(1)
q3.show()

q4 = bucketed_df.where(col("medal_name") == "Killing Spree")\
    .groupBy("mapid")\
    .agg(count("mapid").alias("playing_count"))\
    .orderBy(col("playing_count").desc())\
    .limit(1)
q4.show()

# With the aggregated data set, Try different .sortWithinPartitions to see
# which has the smallest data size (hint: playlists and maps are both very low cardinality)
playlist_sorted_first = bucketed_df.sortWithinPartitions(col("playlist_id"), col("mapid"))
playlist_sorted_first.write.mode("overwrite").saveAsTable("bootcamp.playlist_sorted_first")

mapid_sorted_first = bucketed_df.sortWithinPartitions(col("mapid"), col("playlist_id"))
mapid_sorted_first.write.mode("overwrite").saveAsTable("bootcamp.mapid_sorted_first")
