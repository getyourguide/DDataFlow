import pytest

from ddataflow import DDataflow
from ddataflow.utils import using_databricks_connect
from pyspark.sql.session import SparkSession

@pytest.mark.skipif(
    not using_databricks_connect(), reason="needs databricks connect to work"
)
def test_sql():
    """Run this with db-connect"""
    spark = SparkSession.builder.getOrCreate()

    config = {
        "sources_with_default_sampling": ["location"],
        "project_folder_name": "unit_tests",
    }
    ddataflow = DDataflow(**config)

    query = f""" select count(1) as total
        from {ddataflow.source_name('location')}
    """

    result = spark.sql(query)
    # default amount of tours
    assert result.collect()[0].total > 50000

    ddataflow.enable()
    result = spark.sql(query)
    # defaults to 1000
    assert result.collect()[0].total == 1000


if __name__ == "__main__":
    import fire

    fire.Fire()
