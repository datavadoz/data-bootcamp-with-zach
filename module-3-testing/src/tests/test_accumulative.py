from chispa.dataframe_comparer import *
from ..jobs.accumulative import do_accumulative

from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

PlayerSeason = namedtuple("PlayerSeason", "player_name age height weight college country draft_year draft_round draft_number gp pts reb ast netrtg oreb_pct dreb_pct usg_pct ts_pct ast_pct season")
PlayerAccumulative = namedtuple("PlayerAccumulative", "player_name height college country draft_year draft_round draft_number season_stats current_season")

def test_accumulative(spark):
    player_seasons_data = [
        PlayerSeason("A.C. Green",33,"6-9",225,"Oregon State","USA","1985","1","23","83","7.2","7.9","0.8","-7.4","8.9","8.9","18.4","11.8","4.5",1996),
        PlayerSeason("Aaron McKie",24,"6-5",209,"Temple","USA","1994","1","17","83","5.2","2.7","1.9","3.7","2.6","2.6","11.3","14.2","16.3",1996),
        PlayerSeason("Aaron Williams",25,"6-9",225,"Xavier","USA","Undrafted","Undrafted","Undrafted","33","6.2","4.3","0.5","-9.3","11.3","11.3","14.4","16.1","5.1",1996),
        PlayerSeason("Acie Earl",27,"6-11",240,"Iowa","USA","1993","1","19","47","4","2","0.4","-6.4","6.7","6.7","12.2","22","7.7",1996),
        PlayerSeason("Adam Keefe",27,"6-9",241,"Stanford","USA","1992","1","10","62","3.8","3.5","0.5","7.2","9.6","9.6","15.8","12.4","5.1",1996),
        PlayerSeason("Adrian Caldwell",30,"6-8",265,"Lamar","USA","Undrafted","Undrafted","Undrafted","45","2.2","3.7","0.3","-6.5","9.4","9.4","18","10.2","3",1996),
        PlayerSeason("Alan Henderson",24,"6-9",235,"Indiana","USA","1995","1","16","30","6.6","3.9","0.8","-7.5","9.6","9.6","16","19.7","7.8",1996),
        PlayerSeason("Aleksandar Djordjevic",29,"6-2",198,"None","Serbia","Undrafted","Undrafted","Undrafted","8","3.1","0.6","0.6","5.1","1.7","1.7","6.3","16.8","13.5",1996),
        PlayerSeason("Allan Houston",26,"6-6",200,"Tennessee","USA","1993","1","11","81","14.8","3","2.2","2","1.8","1.8","7.5","21.8","11.7",1996),
        PlayerSeason("Allen Iverson",22,"6-0",165,"Georgetown","USA","1996","1","1","76","23.5","4.1","7.5","-7.1","3.5","3.5","6.4","28.1","32",1996),
    ]
    player_seasons_df = spark.createDataFrame(player_seasons_data)
    player_seasons_df.createOrReplaceTempView("player_seasons")

    players_schema = StructType([
        StructField("player_name", StringType(), True),
        StructField("height", StringType(), True),
        StructField("college", StringType(), True),
        StructField("country", StringType(), True),
        StructField("draft_year", StringType(), True),
        StructField("draft_round", StringType(), True),
        StructField("draft_number", StringType(), True),
        StructField("season_stats", ArrayType(DoubleType()), True),
        StructField("current_season", IntegerType(), True),

    ])
    players_df = spark.createDataFrame([], players_schema)
    players_df.createOrReplaceTempView("players")

    expected_result_data = [
        PlayerAccumulative("A.C. Green","6-9","Oregon State","USA","1985","1","23",[7.2],1996),
        PlayerAccumulative("Aaron McKie","6-5","Temple","USA","1994","1","17",[5.2],1996),
        PlayerAccumulative("Aaron Williams","6-9","Xavier","USA","Undrafted","Undrafted","Undrafted",[6.2],1996),
        PlayerAccumulative("Acie Earl","6-11","Iowa","USA","1993","1","19",[4.0],1996),
        PlayerAccumulative("Adam Keefe","6-9","Stanford","USA","1992","1","10",[3.8],1996),
        PlayerAccumulative("Adrian Caldwell","6-8","Lamar","USA","Undrafted","Undrafted","Undrafted",[2.2],1996),
        PlayerAccumulative("Alan Henderson","6-9","Indiana","USA","1995","1","16",[6.6],1996),
        PlayerAccumulative("Aleksandar Djordjevic","6-2","None","Serbia","Undrafted","Undrafted","Undrafted",[3.1],1996),
        PlayerAccumulative("Allan Houston","6-6","Tennessee","USA","1993","1","11",[14.8],1996),
        PlayerAccumulative("Allen Iverson","6-0","Georgetown","USA","1996","1","1",[23.5],1996),
    ]
    expected_result_df = spark.createDataFrame(expected_result_data)

    actual_result_df = do_accumulative(spark)
    assert_df_equality(actual_result_df, expected_result_df)
