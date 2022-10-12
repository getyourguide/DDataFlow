import os
from typing import List, Optional

from ddataflow.data_source import DataSource
from ddataflow.data_sources import DataSources
from ddataflow.exceptions import WritingToLocationDenied


class Sampler:
    """
    Samples and copy datasources
    """

    def __init__(self, snapshot_path: str, data_sources: DataSources):
        self._BASE_SNAPSHOT_PATH = snapshot_path
        self._data_sources: DataSources = data_sources
        self._dry_run = True

    def save_sampled_data_sources(
        self,
        *,
        dry_run=True,
        ask_confirmation=False,
        sample_only: Optional[List[str]] = None,
    ):
        """
        Make a snapshot of the sampled data for later downloading.

        The writing part is overriding! SO watch out where you write
        By default only do a dry-run. Do a dry_run=False to actually write to the sampled folder.
        """
        self._dry_run = dry_run
        if self._dry_run:
            print(
                "Dry run enabled, no data will be written. Use dry_run=False to actually perform the operation."
            )

        if sample_only is not None:
            print(f"Sampling only the following data sources: {sample_only}")

        print(
            "Sampling process starting for all data-sources."
            f" Saving the results to {self._BASE_SNAPSHOT_PATH} so you can download them."
        )

        for data_source_name in self._data_sources.all_data_sources_names():

            if sample_only is not None and data_source_name not in sample_only:
                print("data_source_name not in selected list", data_source_name)
                continue

            print(f"Starting sampling process for datasource: {data_source_name}")
            data_source: DataSource = self._data_sources.get_data_source(
                data_source_name
            )

            print(
                f"""
Writing copy to folder: {data_source.get_dbfs_sample_path()}.
If you are writing to the wrong folder it could lead to data loss.
            """
            )

            df = data_source.estimate_size_and_fail_if_too_big()
            if ask_confirmation:
                proceed = input(
                    f"Do you want to proceed with the sampling creation? (y/n):"
                )
                if proceed != "y":
                    print("Skipping the creation of the data source")
                    continue
            self._write_sample_data(df, data_source)

        print("Success! Copying of sample data sources finished")

    def _write_sample_data(self, df, data_source: DataSource) -> None:
        """
        Write the sampled data source into a temporary location in dbfs so it can be later downloaded
        If the location already exists it will be overwritten
        If you write to a location different than the standard ddataflow location in it will fail.
        """

        # a security check so we dont write by mistake where we should not
        sample_destination = data_source.get_dbfs_sample_path()
        if not sample_destination.startswith(self._BASE_SNAPSHOT_PATH):
            raise WritingToLocationDenied(self._BASE_SNAPSHOT_PATH)

        if self._dry_run:
            print("Not writing, dry run enabled")
            return
        #  add by default repartition so we only download a single file
        df = df.repartition(1)

        df.write.mode("overwrite").parquet(data_source.get_dbfs_sample_path())
