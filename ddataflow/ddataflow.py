import logging
import os
from typing import List, Optional, Union

from ddataflow.data_source import DataSource
from ddataflow.data_sources import DataSources
from ddataflow.downloader import DataSourceDownloader
from ddataflow.exceptions import WriterNotFoundException
from ddataflow.sampling.default import (
    build_default_sampling_for_sources,
    DefaultSamplerOptions,
)
from ddataflow.sampling.sampler import Sampler
from ddataflow.utils import get_or_create_spark, using_databricks_connect
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
logger.addHandler(handler)


class DDataflow:
    """
    DDataflow is an end2end tests solution.
    See our docs manual for more details.
    Additionally, use help(ddataflow) to see the available methods.
    """

    _DEFAULT_SNAPSHOT_BASE_PATH = "dbfs:/ddataflow"
    _LOCAL_BASE_SNAPSHOT_PATH = os.environ["HOME"] + "/.ddataflow"
    _ENABLE_DDATAFLOW_ENVVARIABLE = "ENABLE_DDATAFLOW"
    _ENABLE_OFFLINE_MODE_ENVVARIABLE = "ENABLE_OFFLINE_MODE"
    _DDATAFLOW_CONFIG_FILE = "ddataflow_config.py"

    _local_path: str

    def __init__(
        self,
        project_folder_name: str,
        data_sources: Optional[dict] = None,
        data_writers: Optional[dict] = None,
        data_source_size_limit_gb: int = 1,
        enable_ddataflow=False,
        sources_with_default_sampling: Optional[List[str]] = None,
        snapshot_path: Optional[str] = None,
        default_sampler: Optional[dict] = None,
        default_database: Optional[str] = None,
    ):
        """
        Initialize the dataflow object.
        The input of this object is the config dictionary outlined in our integrator manual.

        Important params:
        project_folder_name:
            the name of the project that will be stored in the disk
        snapshot_path:
            path to the snapshot folder
        data_source_size_limit_gb:
            limit the size of the data sources
        default_sampler:
         options to pass to the default sampler
        sources_with_default_sampling:
         if you have tables you want to have by default and dont want to sample them first
        default_database:
            name of the default database. If ddataflow is enabled, a test db will be created and used.
        sources_with_default_sampling :
         Deprecated: use sources with default_sampling=True instead
         if you have tables you want to have by default and dont want to sample them first
        """
        self._size_limit = data_source_size_limit_gb

        self.project_folder_name = project_folder_name

        base_path = snapshot_path if snapshot_path else self._DEFAULT_SNAPSHOT_BASE_PATH

        self._snapshot_path = base_path + "/" + project_folder_name
        self._local_path = self._LOCAL_BASE_SNAPSHOT_PATH + "/" + project_folder_name

        if default_sampler:
            # set this before creating data sources
            DefaultSamplerOptions.set(default_sampler)

        if not data_sources:
            data_sources = {}

        all_data_sources = {
            **build_default_sampling_for_sources(sources_with_default_sampling),
            **data_sources,
        }

        self._data_sources = DataSources(
            config=all_data_sources,
            local_folder=self._local_path,
            snapshot_path=self._snapshot_path,
            size_limit=self._size_limit,
        )

        self._data_writers: dict = data_writers if data_writers else {}

        self._offline_enabled = os.getenv(self._ENABLE_OFFLINE_MODE_ENVVARIABLE, False)

        self._ddataflow_enabled: Union[str, bool] = os.getenv(
            self._ENABLE_DDATAFLOW_ENVVARIABLE, enable_ddataflow
        )

        # if offline is enabled we should use local data
        if self._offline_enabled:
            self.enable_offline()

        self.save_sampled_data_sources = Sampler(
            self._snapshot_path, self._data_sources
        ).save_sampled_data_sources

        if default_database:
            self.set_up_database(default_database)

        # Print detailed logs when ddataflow is enabled
        if self._ddataflow_enabled:
            self.set_logger_level(logging.DEBUG)
        else:
            logger.info(
                "DDataflow is now DISABLED."
                "PRODUCTION data will be used and it will write to production tables."
            )

    @staticmethod
    def setup_project():
        """
        Sets up a new ddataflow project with empty data sources in the current directory
        """
        from ddataflow.setup.setup_project import setup_project

        setup_project()

    @staticmethod
    def current_project() -> "DDataflow":
        """
        Returns a ddataflow configured with the current directory configuration file
        Requirements for this to work:

        1. MLTools must be called from withing the project root directory
        2. There must be a file called ddataflow_config.py there
        3. the module must have defined DDataflow object with the name of ddataflow

        @todo investigate if we can use import_class_from_string
        """
        import sys

        CONFIGURATION_FILE_NAME = "ddataflow_config.py"

        current_folder = os.getcwd()
        logger.debug("Loading config from folder", current_folder)
        config_location = os.path.join(current_folder, CONFIGURATION_FILE_NAME)

        if not os.path.exists(config_location):
            raise Exception(
                f"""
This command needs to be executed within a project containing a {CONFIGURATION_FILE_NAME} file.
You can start a new one for the current folder by running the following command:
$ ddataflow setup_project"""
            )

        sys.path.append(current_folder)

        import ddataflow_config

        if hasattr(ddataflow_config, "ddataflow_client"):
            return ddataflow_config.ddataflow_client

        if not hasattr(ddataflow_config, "ddataflow"):
            raise Exception("ddataflow object is not defined in your _config file")

        return ddataflow_config.ddataflow

    def source(self, name: str, debugger=False) -> DataFrame:
        """
        Gives access to the data source configured in the dataflow

        You can also use this function in the terminal with --debugger=True to inspect the dataframe.
        """
        self.print_status()

        logger.debug("Loading data source")
        data_source: DataSource = self._data_sources.get_data_source(name)
        logger.debug("Data source loaded")
        df = self._get_data_from_data_source(data_source)

        if debugger:
            logger.debug(f"Debugger enabled: {debugger}")
            logger.debug("In debug mode now, use query to inspect it")
            breakpoint()

        return df

    def _get_spark(self):
        return get_or_create_spark()

    def enable(self):
        """
        When enabled ddataflow will read from the filtered data sources
        instead of production tables. And write to testing tables instead of production ones.
        """

        self._ddataflow_enabled = True

    def is_enabled(self) -> bool:
        return self._ddataflow_enabled

    def enable_offline(self):
        """Programatically enable offline mode"""
        self._offline_enabled = True
        self.enable()

    def is_local(self) -> bool:
        return self._offline_enabled

    def disable_offline(self):
        """Programatically enable offline mode"""
        self._offline_enabled = False

    def source_name(self, name, disable_view_creation=False) -> str:
        """
        Given the name of a production table, returns the name of the corresponding ddataflow table when ddataflow is enabled
        If ddataflow is disabled get the production one.
        """
        logger.debug("Source name used: ", name)
        source_name = name

        # the gist of ddtafalow
        if self._ddataflow_enabled:
            source_name = self._get_new_table_name(name)
            if disable_view_creation:
                return source_name

            logger.debug(f"Creating a temp view with the name: {source_name}")
            data_source: DataSource = self._data_sources.get_data_source(name)

            if self._offline_enabled:
                df = data_source.query_locally()
            else:
                df = data_source.query()

            df.createOrReplaceTempView(source_name)

            return source_name

        return source_name

    def path(self, path):
        """
        returns a deterministic path replacing the real production path with one based on the current environment needs.
        Currently support path starts with 'dbfs:/' and 's3://'.
        """
        if not self._ddataflow_enabled:
            return path

        base_path = self._get_current_environment_data_folder()

        for path_prefix in ["dbfs:/", "s3://"]:
            path = path.replace(path_prefix, "")

        return base_path + "/" + path

    def set_up_database(self, db_name: str):
        """
        Perform USE $DATABASE query to set up a default database.
        If ddataflow is enabled, use a test db to prevent writing data into production.
        """
        # rename database if ddataflow is enabled
        if self._ddataflow_enabled:
            db_name = f"ddataflow_{db_name}"
        # get spark
        spark = self._get_spark()
        # create db if not exist
        sql = "CREATE DATABASE IF NOT EXISTS {0}".format(db_name)
        spark.sql(sql)
        # set default db
        spark.sql("USE {}".format(db_name))
        logger.warning(f"The default database is now set to {db_name}")

    def _get_new_table_name(self, name) -> str:
        overriden_name = name.replace("dwh.", "")
        return self.project_folder_name + "_" + overriden_name

    def name(self, *args, **kwargs):
        """
        A shorthand for source_name
        """
        return self.source_name(*args, **kwargs)

    def disable(self):
        """Disable ddtaflow overriding tables, uses production state in other words"""
        self._ddataflow_enabled = False

    def _get_data_from_data_source(self, data_source: DataSource) -> DataFrame:
        if not self._ddataflow_enabled:
            logger.debug("DDataflow not enabled")
            # goes directly to production without prefilters
            return data_source.query_without_filter()

        if self._offline_enabled:
            # uses snapshot data
            if using_databricks_connect():
                logger.debug(
                    "Looks like you are using databricks-connect in offline mode. You probably want to run it "
                    "without databricks connect in offline mode"
                )

            return data_source.query_locally()

        logger.debug("DDataflow enabled and filtering")
        return data_source.query()

    def download_data_sources(self, overwrite: bool = True, debug=False):
        """
        Download the data sources locally for development offline
        Note: you need databricks-cli for this command to work

        Options:
            overwrite: will first clean the existing files
        """
        DataSourceDownloader().download_all(self._data_sources, overwrite, debug)

    def sample_and_download(
        self, ask_confirmation: bool = True, overwrite: bool = True
    ):
        """
        Create a sample folder in dbfs and then downloads it in the local machine
        """
        self.save_sampled_data_sources(dry_run=False, ask_confirmation=ask_confirmation)
        self.download_data_sources(overwrite)

    def write(self, df, name: str):
        """
        Write a dataframe either to a local folder or the production one
        """
        if name not in self._data_writers:
            raise WriterNotFoundException(name)

        if self._ddataflow_enabled:
            writing_path = self._snapshot_path

            if self._offline_enabled:
                writing_path = self._local_path
            else:
                if not writing_path.startswith(DDataflow._DEFAULT_SNAPSHOT_BASE_PATH):
                    raise Exception(
                        f"Only writing to {DDataflow._DEFAULT_SNAPSHOT_BASE_PATH} is enabled"
                    )

            writing_path = os.path.join(writing_path, name)
            logger.info("Writing data to parquet file: " + writing_path)
            return df.write.parquet(writing_path, mode="overwrite")

        # if none of the above writes to production
        return self._data_writers[name]["writer"](df, name, self._get_spark())  # type: ignore

    def read(self, name: str):
        """
        Read the data writers parquet file which are stored in the ddataflow folder
        """
        path = self._snapshot_path
        if self._offline_enabled:
            path = self._local_path

        parquet_path = os.path.join(path, name)
        return self._get_spark().read.parquet(parquet_path)

    def _print_snapshot_size(self):
        """
        Prints the final size of the dataset in the folder
        Note: Only works in notebooks.
        """
        import subprocess

        location = "/dbfs/ddataflow/"
        output = subprocess.getoutput(f"du -h -d2 {location}")
        print(output)

    def _print_download_folder_size(self):
        """
        Prints the final size of the dataset in the folder
        """
        import subprocess

        output = subprocess.getoutput(f"du -h -d2 {self._local_path}")
        print(output)

    def get_mlflow_path(self, original_path: str):
        """
        overrides the mlflow path if
        """
        overriden_path = self._get_overriden_arctifacts_current_path()
        if overriden_path:
            model_name = original_path.split("/")[-1]
            return overriden_path + "/" + model_name

        return original_path

    def _get_overriden_arctifacts_current_path(self):
        if self._offline_enabled:
            return self._local_path

        if self._ddataflow_enabled:
            return self._snapshot_path

        return None

    def is_enabled(self):
        """
        To be enabled ddataflow has to be either in offline mode or with enable=True
        """
        return self._offline_enabled or self._ddataflow_enabled

    def print_status(self):
        """
        Print the status of the ddataflow
        """
        if self._offline_enabled:
            logger.debug("DDataflow is now ENABLED in OFFLINE mode")
            logger.debug(
                "To disable it remove from your code or unset the enviroment variable 'unset ENABLE_DDATAFLOW ; unset ENABLE_OFFLINE_MODE'"
            )
        elif self._ddataflow_enabled:
            logger.debug(
                """
                DDataflow is now ENABLED in ONLINE mode. Filtered data will be used and it will write to temporary tables.
                """
            )
        else:
            logger.debug(
                f"""
                DDataflow is now DISABLED. So PRODUCTION data will be used and it will write to production tables.
                Use enable() function or export {self._ENABLE_DDATAFLOW_ENVVARIABLE}=True to enable it.
                If you are working offline use export ENABLE_OFFLINE_MODE=True instead.
                """
            )

    def _get_current_environment_data_folder(self) -> Optional[str]:
        if not self._ddataflow_enabled:
            raise Exception("DDataflow is disabled so no data folder is available")

        if self._offline_enabled:
            return self._local_path

        return self._snapshot_path

    def set_logger_level(self, level):
        """
        Set logger level.
        Levels can be found here: https://docs.python.org/3/library/logging.html#logging-levels
        """
        logger.info(f"Set logger level to: {level}")
        logger.setLevel(level)


def main():
    import fire

    fire.Fire(DDataflow)
