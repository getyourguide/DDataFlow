import os

import pytest

from ddataflow import DDataflow

from datetime import datetime, timedelta

import pyspark.sql.functions as F

from ddataflow.ddataflow import DDataflow

start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_time = datetime.now().strftime("%Y-%m-%d")

config = {
    "data_sources": {
        # data sources define how to access data
        "events": {
            "source": lambda spark: spark.table("events"),
            "filter": lambda df: df.filter(F.col("date") >= start_time)
            .filter(F.col("date") <= end_time)
            .filter(F.col("event_name").isin(["BookAction", "ActivityCardImpression"]))
            .filter(F.hour("kafka_timestamp").between(8, 9))
            .limit(1000),
        },
        "ActivityCardImpression": {
            "source": lambda spark: spark.read.parquet(
                "dbfs:/events/ActivityCardImpression"
            ),
            "filter": lambda df: df.filter(F.col("date") >= start_time)
            .filter(F.col("date") <= end_time)
            .filter(F.hour("kafka_timestamp").between(8, 9))
            .limit(1000),
        },
        "dim_tour": {
            "source": lambda spark: spark.table("tour"),
        },
    },
    "project_folder_name": "tests",
    "data_source_size_limit_gb": 3,
}


def test_initialize_successfully():
    """
    Tests that a correct config will not fail to be instantiated
    """

    DDataflow(**config)


def test_wrong_config_fails():
    with pytest.raises(BaseException, match="wrong param"):

        DDataflow(**{**config, **{"a wrong param": "a wrong value"}})


def test_current_project_path():
    """
    Test that varying our environment we get different paths
    """
    config = {
        "project_folder_name": "my_tests",
    }
    ddataflow = DDataflow(**config)
    # by default do not override
    assert ddataflow._get_overriden_arctifacts_current_path() is None
    ddataflow.enable()
    assert (
        "dbfs:/ddataflow/my_tests" == ddataflow._get_overriden_arctifacts_current_path()
    )
    ddataflow.enable_offline()
    assert (
        os.getenv("HOME") + "/.ddataflow/my_tests"
        == ddataflow._get_overriden_arctifacts_current_path()
    )


def test_temp_table_name():

    config = {
        "sources_with_default_sampling": ["location"],
        "project_folder_name": "unit_tests",
    }

    ddataflow = DDataflow(**config)
    ddataflow.disable()
    # by default do not override
    assert ddataflow.name("location", disable_view_creation=True) == "location"
    ddataflow.enable()
    assert (
        ddataflow.name("location", disable_view_creation=True) == "unit_tests_location"
    )
