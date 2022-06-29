import os
from typing import List, Optional, Union

from ddataflow.data_source import DataSource, DataSources
from ddataflow.downloader import DataSourceDownloader
from ddataflow.exceptions import WriterNotFoundException
from ddataflow.sampler import DataSourceSampler
from ddataflow.utils import (get_or_create_spark,
                                     using_databricks_connect)
import logging as logger

class DDataflow:
    """
    DDataflow is our ML end2end tests solution.
    See our integrator manual for details.
    Additionally, use help(ddataflow) to see the available methods.
    """

    _DBFS_BASE_SNAPSHOT_PATH = "dbfs:/ddataflow"
    _LOCAL_BASE_SNAPSHOT_PATH = os.environ["HOME"] + "/.ddataflow"
    _ENABLE_DDATAFLOW_ENVVARIABLE = "ENABLE_DDATAFLOW"
    _ENABLE_OFFLINE_MODE_ENVVARIABLE = "ENABLE_OFFLINE_MODE"
    _DEFAULT_SAMPLING_SIZE = 1000
    _DDATAFLOW_CONFIG_FILE = 'ddataflow_config.py'

    _local_path: str

    def __init__(
        self,
        project_folder_name: str,
        data_sources: Optional[dict] = None,
        data_writers: Optional[dict] = None,
        data_source_size_limit_gb: int = 1,
        enable_ddataflow=False,
        sources_with_default_sampling: Optional[List[str]] = None,
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
        sources_with_default_sampling:
         if you have tables you want to have by default and dont want to sample them first
        """
        self._size_limit = data_source_size_limit_gb


        self.project_folder_name = project_folder_name
        self._dbfs_path = self._DBFS_BASE_SNAPSHOT_PATH + "/" + project_folder_name
        self._local_path = self._LOCAL_BASE_SNAPSHOT_PATH + "/" + project_folder_name

        if not data_sources:
            data_sources = {}

        all_data_sources = {
            **data_sources,
            **self._build_default_sampling_for_sources(sources_with_default_sampling),
        }

        self._data_sources = DataSources(
            config=all_data_sources,
            local_folder=self._local_path,
            snapshot_path=self._dbfs_path,
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

    @staticmethod
    def setup_project():
        content = '''
from ddataflow import DDataflow

config = {
    # add here tables or paths to data sources with default sampling
    "sources_with_default_sampling": [],
    # add here your tables or paths with customized sampling logic
    "data_sources": {},
    # add here your writing logic
    "data_writers": {},
    # this is the name of the project to identify this project in the filesystem
    "project_folder_name": "myproject",
    # to customize the location of your datasets 
    # "snapshot_path": "dbfs:/another_databricks_path",
    # to customize the size of your samples uncomment the line below
    # "data_source_size_limit_gb": "5"
}

# initialize the application and validate the configuration
ddataflow_client = DDataflow(**config)
'''

        with open(DDataflow._DDATAFLOW_CONFIG_FILE, 'w') as f:
            f.write(content)

        print(f"File {DDataflow._DDATAFLOW_CONFIG_FILE} created in the current directory")


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
        print("Loading config from folder", current_folder)
        config_location = os.path.join(current_folder, CONFIGURATION_FILE_NAME)

        if not os.path.exists(config_location):
            raise Exception(
                f"This command needs to be executed within a project containing a "
                f" {CONFIGURATION_FILE_NAME} file."
                f"\n Additionally, the file needs to configure a ddataflow object within it."
            )

        sys.path.append(current_folder)

        import ddataflow_config

        return ddataflow_config.ddataflow

    def _get_spark(self):
        return get_or_create_spark()


    def enable(self):
        """
        When enabled ddataflow will read from the filtered datasoruces
        instead of production tables. And write to testing tables instead of production ones.
        """

        self._ddataflow_enabled = True
        self.printStatus()

    def enable_offline(self):
        """Programatically enable offline mode"""
        self._offline_enabled = True
        self.enable()

    def source(self, name: str, debugger=False):
        """
        Gives access to the data source configured in the dataflow
        You can also use this function in the terminal with --debugger=True to inspect hte dataframe.
        """
        logger.info("Debugger enabled: ", debugger)
        self.printStatus()

        logger.info("Loading data source")
        data_source: DataSource = self._data_sources.get_data_source(name)
        logger.info("Data source loaded")
        df = self._get_df_from_source(data_source)

        if debugger:
            logger.info("In debug mode now, use query to inspect it")
            breakpoint()

        return df

    def source_name(self, name):
        """
        Return a new table name for the sampled data
        """
        logger.info("Source name used: ", name)
        source_name = self._get_source_name_only(name)

        if self._ddataflow_enabled:
            print(f"Creating a temp view the the name: {source_name}")
            data_source: DataSource = self._data_sources.get_data_source(name)
            df = data_source.query(self._get_spark())
            df.createOrReplaceTempView(source_name)

            return source_name

        return source_name

    def name(self, name_str):
        """
        A shorthand for source_name
        """
        return self.source_name(name_str)


    def _get_source_name_only(self, name) -> str:
        if self._ddataflow_enabled:
            overriden_name = name.replace("dwh.", "")
            return self.project_folder_name + "_" + overriden_name

        return name

    def disable(self):
        """Disable ddtaflow overriding tables, uses production state in other words"""
        self._ddataflow_enabled = False

    def _get_df_from_source(self, data_source):
        if not self._ddataflow_enabled:
            print("DDataflow not enabled")
            # goes directly to production without prefilters
            return data_source.query_without_filter(self._get_spark())

        if self._offline_enabled:
            # uses snapshot data
            if using_databricks_connect():
                print(
                    "Looks like you are using databricks-connect in offline mode. You probably want to run it "
                    "without databricks connect in offline mode"
                )

            return data_source.query_locally(self._get_spark())

        print("DDataflow enabled and filtering")
        return data_source.query(self._get_spark())

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
        self.save_sampled_data_sources(ask_confirmation)
        self.download_data_sources(overwrite)

    def save_sampled_data_sources(self, ask_confirmation=True):
        """
        Make a snapshot of the sampled data for later downloading
        """
        # @todo raise error if not setted up

        return DataSourceSampler(self._DBFS_BASE_SNAPSHOT_PATH).sample_all(
            self._data_sources, ask_confirmation
        )

    def write(self, df, name: str):
        """
        Write a dataframe either to a local folder or the production one
        """
        if name not in self._data_writers:
            raise WriterNotFoundException(name)

        if self._ddataflow_enabled:
            writing_path = self._dbfs_path

            if self._offline_enabled:
                writing_path = self._local_path
            else:
                if not writing_path.startswith(DDataflow._DBFS_BASE_SNAPSHOT_PATH):
                    raise Exception(
                        f"Only writing to {DDataflow._DBFS_BASE_SNAPSHOT_PATH} is enabled"
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
        path = self._dbfs_path
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
        # return self.

        if self._offline_enabled:
            return self._local_path

        if self._ddataflow_enabled:
            return self._dbfs_path

        return None

    def printStatus(self):
        """
        Print the status of the ddataflow
        """
        if self._offline_enabled:
            print("DDataflow is now ENABLED in OFFLINE mode")
        elif self._ddataflow_enabled:
            print(
                """
DDataflow is now ENABLED in ONLINE mode.
So filtered data will be used and it will write to
temporary tables"""
            )
        else:
            print(
                f"""
DDataflow is now DISABLED, so PRODUCTION data will be used and it will write to production tables.
Use enable() function or export {self._ENABLE_DDATAFLOW_ENVVARIABLE}=True to enable"""
            )

    def _build_default_sampling_for_sources(self, sources=None):
        result = {}
        if not sources:
            return result

        for source in sources:
            print("Build default sampling for source: " + source)
            result[source] = {
                "source": lambda spark: spark.table(source),
                "filter": lambda df: df.limit(
                    DDataflow._DEFAULT_SAMPLING_SIZE
                ),
            }
        return result

def main():
    import fire
    fire.Fire(DDataflow)
