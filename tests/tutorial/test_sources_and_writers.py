import os

import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession

from ddataflow import DDataflow

data = [
    {
        "tour_id": 132,
        "tour_title": "Helicopter Tour",
        "status": "active",
        "location_id": 65,
    },
    {
        "tour_id": 558,
        "tour_title": "Walking Tour",
        "location_id": 59,
    },
    {
        "tour_id": 3009,
        "tour_title": "Transfer Hotel",
        "location_id": 65,
    },
]


def test_source_filters_when_toggled():
    """
    Creates a datasource
    """

    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("tour")

    config = {
        "data_sources": {
            # data sources define how to access data
            "tour": {
                "source": lambda spark: spark.table("tour"),
                "filter": lambda df: df.filter("location_id == 65"),
            },
        },
        "project_folder_name": "unit_tests",
    }

    ddataflow = DDataflow(**config)
    assert df.count() == 3

    ddataflow.enable()
    assert ddataflow.source("tour").count() == 2

    ddataflow.disable()
    assert ddataflow.source("tour").count() == 3


def test_writer():
    """Test that writing works"""
    spark = SparkSession.builder.getOrCreate()
    config = {
        "data_writers": {
            "test_new_dim_tour": {
                "writer": lambda df, name, spark: df.registerTempTable(name)
            },
        },
        "project_folder_name": "unit_tests",
    }
    ddataflow = DDataflow(**config)
    # to not write in dbfs lets do this operation offline
    ddataflow.enable_offline()

    df = spark.createDataFrame(data)
    new_df = df.withColumn("new_column", F.lit("a new value"))

    ddataflow.write(new_df, "test_new_dim_tour")
    loaded_from_disk = ddataflow.read("test_new_dim_tour")

    assert new_df.columns == loaded_from_disk.columns


def test_mlflow():
    """
    Test that varying our environment we get different paths
    """
    original_model_path = "/Shared/RnR/MLflow/MyModel"
    ddataflow = DDataflow(
        **{
            "project_folder_name": "my_tests",
        }
    )

    # when ddataflow is disabled do not change the path
    original_model_path == ddataflow.get_mlflow_path(original_model_path)

    # when it is enabled write do a special place in dbfs
    ddataflow.enable()
    assert "dbfs:/ddataflow/my_tests/MyModel" == ddataflow.get_mlflow_path(
        original_model_path
    )

    # when it is in offline mode write to the local filesystems
    ddataflow.enable_offline()
    assert os.getenv(
        "HOME"
    ) + "/.ddataflow/my_tests/MyModel" == ddataflow.get_mlflow_path(original_model_path)


if __name__ == "__main__":
    import fire

    fire.Fire()
