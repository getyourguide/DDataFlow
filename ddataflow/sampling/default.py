from datetime import datetime, timedelta
from typing import List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from dataclasses import dataclass


@dataclass
class DefaultSamplerOptions:
    """
    Options to customize the default sampler
    """

    limit: Optional[int] = 100000

    _instance = None

    @staticmethod
    def set(data: dict):
        DefaultSamplerOptions._instance = DefaultSamplerOptions(**data)

        return DefaultSamplerOptions._instance

    @staticmethod
    def get_instance():
        if not DefaultSamplerOptions._instance:
            DefaultSamplerOptions._instance = DefaultSamplerOptions()

        return DefaultSamplerOptions._instance


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
    df = df.limit(DefaultSamplerOptions.get_instance().limit)

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
