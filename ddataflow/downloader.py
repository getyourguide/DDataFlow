import logging as logger
import os

from ddataflow.data_source import DataSource
from ddataflow.data_sources import DataSources


class DataSourceDownloader:

    _download_folder: str

    def download_all(
        self, data_sources: DataSources, overwrite: bool = True, debug=False
    ):
        """
        Download the data sources locally for development offline
        Note: you need databricks-cli for this command to work

        Options:
            overwrite: will first clean the existing files
        """
        self._download_folder = data_sources.download_folder
        if overwrite:
            if ".ddataflow" not in self._download_folder:
                raise Exception("Can only clean folders within .ddataflow")

            cmd_delete = f"rm -rf {self._download_folder}"
            print("Deleting content from", cmd_delete)
            os.system(cmd_delete)

        print("Starting to download the data-sources into your snapshot folder")

        for data_source_name in data_sources.all_data_sources_names():
            print(f"Starting download process for datasource: {data_source_name}")
            data_source = data_sources.get_data_source(data_source_name)
            self._download_data_source(data_source, debug)

        print("Download of all data-sources finished successfully!")

    def _download_data_source(self, data_source: DataSource, debug=False):
        """
        Download the latest data snapshot to the local machine for developing locally
        """
        os.makedirs(self._download_folder, exist_ok=True)

        debug_str = ""
        if debug:
            debug_str = "--debug"

        cmd = f'databricks fs cp {debug_str} -r "{data_source.get_dbfs_sample_path()}" "{data_source.get_local_path()}"'

        logger.info(cmd)
        result = os.system(cmd)

        if result != 0:
            raise Exception(
                f"""
            Databricks cli failed! See error message above.
            Also consider rerunning the download command in your terminal to see the results.
            {cmd}
            """
            )
