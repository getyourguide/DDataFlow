import os.path

from pyspark.sql.session import SparkSession

from ddataflow import DDataflow


def test_sampling_end2end():
    """
    Tests that a correct _config will not fail to be instantiated
    """
    spark = SparkSession.builder.getOrCreate()
    entries = [
        ["id1", "Sagrada Familila"],
        ["id2", "Eiffel Tower"],
        ["id3", "abc"],
    ]
    df = spark.createDataFrame(entries, ["id", "_name"])
    df.createOrReplaceTempView("location")

    assert spark.table("location").count() == 3

    config = {
        "data_sources": {
            "location_filtered": {
                "source": lambda spark: spark.table("location"),
                "filter": lambda df: df.filter(df._name == "abc"),
            },
            "location": {},
        },
        "project_folder_name": "unit_tests",
        "snapshot_path": "/tmp/ddataflow_test",
    }

    ddataflow = DDataflow(**config)

    ddataflow.disable()
    ddataflow.disable_offline()
    assert ddataflow.source("location").count() == 3
    assert ddataflow.source("location_filtered").count() == 3

    ddataflow.enable()
    assert ddataflow.source("location").count() == 3
    assert ddataflow.source("location_filtered").count() == 1

    ddataflow.save_sampled_data_sources(dry_run=False)

    # after sampling the following destinations have the copy
    assert os.path.exists("/tmp/ddataflow_test/unit_tests/location")
    assert os.path.exists("/tmp/ddataflow_test/unit_tests/location_filtered")


def test_sampling_paths():
    config = {
        "data_sources": {
            "location_filtered": {
                "source": lambda spark: spark.table("location"),
            },
            "/mnt/foo/bar": {},
        },
        "project_folder_name": "unit_tests",
        "snapshot_path": "/tmp/ddataflow_test",
    }

    ddataflow = DDataflow(**config)
    assert (
        ddataflow._data_sources.get_data_source(
            "location_filtered"
        ).get_dbfs_sample_path()
        == "/tmp/ddataflow_test/unit_tests/location_filtered"
    )
    assert (
        ddataflow._data_sources.get_data_source("/mnt/foo/bar").get_dbfs_sample_path()
        == "/tmp/ddataflow_test/unit_tests/_mnt_foo_bar"
    )
