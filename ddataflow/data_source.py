import os
from typing import Any, Dict, List

from ddataflow.exceptions import BiggerThanMaxSize
from ddataflow.utils import (estimate_spark_dataframe_size,
                                     get_or_create_spark)
import logging as logger

class DataSource:
    """
    Utility functions at data source level
    """

    def __init__(
        self,
        *,
        name: str,
        config: dict,
        local_data_folder: str,
        snapshot_path: str,
        size_limit,
    ):
        self.name = name
        self.config = config
        self.local_data_folder = local_data_folder
        self.snapshot_path = snapshot_path
        self._size_limit = size_limit

    def query(self, spark):
        """
        query with filter unless none is present
        """
        df = self.query_without_filter(spark)

        if self.config.get("filter") is not None:
            print(f"Filter set for {self.name}, applying it")
            df = self.config["filter"](df)
        else:
            print(f"No filter set for {self.name}")

        return df

    def query_without_filter(self, spark):
        logger.info(f"Querying without filter {self.name}")
        return self.config["source"](spark)

    def query_locally(self, spark):
        logger.info(f"Querying locally {self.name}")
        df = spark.read.parquet(self.get_local_path())

        return df

    def get_dbfs_sample_path(self) -> str:
        return os.path.join(self.snapshot_path, self.get_name())

    def get_local_path(self) -> str:
        return os.path.join(self.local_data_folder, self.get_name())

    def get_name(self) -> str:
        return self.name

    def get_parquet_filename(self) -> str:
        return self.name + ".parquet"

    def estimate_size_and_fail_if_too_big(self, *, debug=False, return_dataset=False):
        """
        Estimate the size of the data source use the name used in the config
        It will throw an exception if the estimated size is bigger than the maximum allowed in the configuration
        """

        spark = get_or_create_spark()
        df = self.query(spark)
        size_estimation = self._estimate_size(df)

        print("Estimated size of the Dataset in GB: ", size_estimation)

        if debug:
            breakpoint()

        if size_estimation > self._size_limit:
            raise BiggerThanMaxSize(self.name, size_estimation, self._size_limit)

        if return_dataset:
            return df

    def _estimate_size(self, df):
        return estimate_spark_dataframe_size(spark_dataframe=df)


class DataSources:
    """
    Validates and Abstract the access to data sources
    """

    def __init__(
        self, *, config, local_folder: str, snapshot_path: str, size_limit: int
    ):
        self.config = config
        self.data_source: Dict[str, Any] = {}
        self.download_folder = local_folder
        for data_source_name, data_source_config in self.config.items():
            self.data_source[data_source_name] = DataSource(
                name=data_source_name,
                config=data_source_config,
                local_data_folder=local_folder,
                snapshot_path=snapshot_path,
                size_limit=size_limit,
            )

    def all_data_sources_names(self) -> List[str]:
        return list(self.data_source.keys())

    def get_data_source(self, name) -> DataSource:
        if name not in self.data_source:
            raise Exception(f"Data source does not exist {name}")
        return self.data_source[name]

    def get_filter(self, data_source_name: str):
        return self.config[data_source_name]["query"]

    def get_parquet_name(self, data_source_name: str):
        return self.config[data_source_name]["parquet_name"]
