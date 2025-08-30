from chispa.dataframe_comparer import *
from ..jobs.dedup import do_dedup

from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

GameDetails = namedtuple("GameDetails", "game_id team_id player_id min")


def test_dedup(spark):
    game_details_data = [
        GameDetails(11600001, 1610612744, 2561, "12:36"),
        GameDetails(11600002, 1610612744, 2562, "10:31"),
        GameDetails(11600003, 1610612744, 2562, "13:23"),
        GameDetails(11600004, 1610612744, 2561, "09:36"),
        GameDetails(11600001, 1610612744, 2561, "12:36"),
    ]
    game_details_df = spark.createDataFrame(game_details_data)
    game_details_df.createOrReplaceTempView("game_details")

    expected_result_data = [
        GameDetails(11600001, 1610612744, 2561, "12:36"),
        GameDetails(11600002, 1610612744, 2562, "10:31"),
        GameDetails(11600003, 1610612744, 2562, "13:23"),
        GameDetails(11600004, 1610612744, 2561, "09:36"),
    ]
    expected_result_df = spark.createDataFrame(expected_result_data)

    actual_result_df = do_dedup(spark)
    assert_df_equality(actual_result_df, expected_result_df)
