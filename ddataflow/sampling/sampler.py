from ddataflow.data_source import DataSource
from ddataflow.data_sources import DataSources
from ddataflow.exceptions import WritingToLocationDenied


class Sampler:
    """
    Samples and copy datasources
    """

    def __init__(self, snapshot_path):
        self._BASE_SNAPSHOT_PATH = snapshot_path

    def sample_all(self, data_sources: DataSources, ask_confirmation):
        print(
            "Sampling process starting for all data-sources."
            f"Saving the results to {self._BASE_SNAPSHOT_PATH} so you can download them."
        )

        for data_source_name in data_sources.all_data_sources_names():
            print(f"Starting download process for datasource: {data_source_name}")
            data_source: DataSource = data_sources.get_data_source(data_source_name)

            print(f"Writing copy to folder: {data_source.get_dbfs_sample_path()}")
            df = data_source.estimate_size_and_fail_if_too_big()
            if ask_confirmation:
                proceed = input(
                    f"Proceed with the creation of the data source {data_source_name}? (y/n): "
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

        #  add by default repartition so we only download a single file
        df = df.repartition(1)

        df.write.mode("overwrite").parquet(data_source.get_dbfs_sample_path())
