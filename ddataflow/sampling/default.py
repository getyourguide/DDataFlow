from datetime import datetime, timedelta
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

DEFAULT_SAMPLING_SIZE = 1000


def sample_by_yesterday(df: DataFrame) -> DataFrame:
    """
    Sample by yesterday
    """

    yesterday = (datetime.now() - timedelta(days=1)).date()

    if "date" in df.columns:
        print("Found a date column, sampling by yesterday")
        return df.filter(F.col("date") == yesterday)

    return df


def filter_function(df: DataFrame) -> DataFrame:
    """
    Default filter function
    :param df:
    :return:
    """
    df = sample_by_yesterday(df)
    df = df.limit(DEFAULT_SAMPLING_SIZE)

    return df


def build_default_sampling_for_sources(sources: Optional[List[dict]] = None) -> dict:
    """
    Setup standard filters for the entries that we do not specify them
    """
    result = {}
    if not sources:
        return result

    for source in sources:
        print("Build default sampling for source: " + source)
        result[source] = {
            "source": lambda spark: spark.table(source),
            "filter": lambda df: filter_function(df),
        }

    return result
