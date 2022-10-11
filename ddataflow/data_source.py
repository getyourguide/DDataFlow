import logging as logger
import os

from pyspark.sql import DataFrame

from ddataflow.exceptions import BiggerThanMaxSize
from ddataflow.sampling.default import filter_function
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
        self._name = name
        self._local_data_folder = local_data_folder
        self._snapshot_path = snapshot_path
        self._size_limit = size_limit
        self._config = config
        self._filter = None
        self._source = None

        if "source" in self._config:
            self._source = config["source"]
        else:
            if self._config.get("file-type") == "parquet":
                self._source = lambda spark: spark.read.parquet(self._name)
            else:
                self._source = lambda spark: spark.table(self._name)

        if "filter" in self._config:
            self._filter = self._config["filter"]
        else:
            if self._config.get("default_sampling"):
                self._filter = lambda df: filter_function(df)

    def query(self):
        """
        query with filter unless none is present
        """
        df = self.query_without_filter()

        if self._filter is not None:
            print(f"Filter set for {self._name}, applying it")
            df = self._filter(df)
        else:
            print(f"No filter set for {self._name}")

        return df

    def has_filter(self) -> bool:
        return self._filter is not None

    def query_without_filter(self):
        """
        Go to the raw data source without any filtering
        """
        spark = get_or_create_spark()
        logger.debug(f"Querying without filter source: '{self._name}'")
        return self._source(spark)

    def query_locally(self):
        logger.info(f"Querying locally {self._name}")

        path = self.get_local_path()
        if not os.path.exists(path):
            raise Exception(
                f"""Data source '{self.get_name()}' does not have data in {path}.
            Consider downloading using  the following command:
            ddataflow current_project download_data_sources"""
            )
        spark = get_or_create_spark()
        df = spark.read.parquet(path)

        return df

    def get_dbfs_sample_path(self) -> str:
        return os.path.join(self._snapshot_path, self._get_name_as_path())

    def get_local_path(self) -> str:
        return os.path.join(self._local_data_folder, self._get_name_as_path())

    def _get_name_as_path(self):
        """
        converts the name when it has "/mnt/envents" in the name to a single file in a (flat structure) _mnt_events
        """
        return self.get_name().replace("/", "_")

    def get_name(self) -> str:
        return self._name

    def get_parquet_filename(self) -> str:
        return self._name + ".parquet"

    def estimate_size_and_fail_if_too_big(self):
        """
        Estimate the size of the data source use the _name used in the _config
        It will throw an exception if the estimated size is bigger than the maximum allowed in the configuration
        """

        print("Estimating size of data source: ", self.get_name())
        df = self.query()
        size_estimation = self._estimate_size(df)

        print("Estimated size of the Dataset in GB: ", size_estimation)

        if size_estimation > self._size_limit:
            raise BiggerThanMaxSize(self._name, size_estimation, self._size_limit)

        return df

    def _estimate_size(self, df: DataFrame) -> float:
        """
        Estimates the size of a dataframe in Gigabytes

        Formula:
            number of gigabytes = (N*V*W) / 1024^3
        """

        print(f"Amount of rows in dataframe to estimate size: {df.count()}")
        average_variable_size_bytes = 50
        return (df.count() * len(df.columns) * average_variable_size_bytes) / (
            1024**3
        )
