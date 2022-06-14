import os

import pytest

from ddataflow import DDataflow
from ddataflow.utils import using_databricks_connect

config = {
    "sources_with_default_sampling": ["location"],
    "data_sources": {
        # data sources define how to access data
        "tour": {
            "source": lambda spark: spark.table("tour"),
            "filter": lambda df: df.sample(0.1).limit(500),
        },
    },
    "project_folder_name": "unit_tests",
    "data_source_size_limit_gb": 0.5,
}

ddataflow = DDataflow(**config)  # type: ignore
ddataflow.enable_offline()


@pytest.mark.skipif(
    not using_databricks_connect(), reason="needs databricks connect to work"
)
def test_sample_and_download():
    ddataflow.sample_and_download(ask_confirmation=False)
    assert os.path.exists(ddataflow._local_path + "/tour")
    assert os.path.exists(ddataflow._local_path + "/location")

    # Done! From now on you can use your sources entirely offline


@pytest.mark.skipif(
    not using_databricks_connect(), reason="needs databricks connect to work"
)
def test_read_offline_files():
    ddataflow = DDataflow(**config)  # type: ignore
    ddataflow.enable_offline()

    df = ddataflow.source("tour")
    assert df.count() == 500
    df.show(3)

    df = ddataflow.source("location")
    assert df.count() == 1000
    df.show(3)


if __name__ == "__main__":
    import fire

    fire.Fire()
