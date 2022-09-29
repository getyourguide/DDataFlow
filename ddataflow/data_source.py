import logging as logger
import os

from pyspark.sql import DataFrame

from ddataflow.exceptions import BiggerThanMaxSize
from ddataflow.utils import get_or_create_spark


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

    def query(self):
        """
        query with filter unless none is present
        """
        df = self.query_without_filter()

        if self.config.get("filter") is not None:
            print(f"Filter set for {self.name}, applying it")
            df = self.config["filter"](df)
        else:
            print(f"No filter set for {self.name}")

        return df

    def query_without_filter(self):
        """
        Go to the raw data source without any filtering
        """
        spark = get_or_create_spark()
        logger.debug(f"Querying without filter source: '{self.name}'")
        return self.config["source"](spark)

    def query_locally(self):
        logger.info(f"Querying locally {self.name}")

        path = self.get_local_path()
        if not os.path.exists(path):
            raise Exception(
                f"""Data source '{self.get_name()}' does not have data locally.
            Consider downloading using  the following command:
            ddataflow current_project download_data_sources"""
            )
        spark = get_or_create_spark()
        df = spark.read.parquet(path)

        return df

    def get_dbfs_sample_path(self) -> str:
        return os.path.join(self.snapshot_path, self.get_name())

    def get_local_path(self) -> str:
        return os.path.join(self.local_data_folder, self.get_name())

    def get_name(self) -> str:
        return self.name

    def get_parquet_filename(self) -> str:
        return self.name + ".parquet"

    def estimate_size_and_fail_if_too_big(self):
        """
        Estimate the size of the data source use the name used in the config
        It will throw an exception if the estimated size is bigger than the maximum allowed in the configuration
        """

        print("Estimating size of data source: ", self.get_name())
        df = self.query()
        size_estimation = self._estimate_size(df)

        print("Estimated size of the Dataset in GB: ", size_estimation)

        if size_estimation > self._size_limit:
            raise BiggerThanMaxSize(self.name, size_estimation, self._size_limit)

        return df

    def _estimate_size(self, df: DataFrame):
        """
        Estimatest the size of a dataframe in Gigabytes

        Formula:
            number of gigabytes = (N*V*W) / 1024^3
        """

        print(f"Amount of rows in dataframe to estimate size: {df.count()}")
        average_variable_size_bytes = 50
        return (df.count() * len(df.columns) * average_variable_size_bytes) / (
            1024**3
        )
