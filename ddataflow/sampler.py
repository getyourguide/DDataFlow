from ddataflow.data_source import DataSource, DataSources
from ddataflow.exceptions import WritingToLocationDenied


class DataSourceSampler:
    """
    Samples and copy datasources
    """

    def __init__(self, snapshot_path):
        self.BASE_SNAPSHOT_PATH = snapshot_path

    def sample_all(self, data_sources: DataSources, ask_confirmation):
        print(
            "Starting the process to copy all datasources to a temporary place in dbfs so you can donwload them"
        )

        for data_source_name in data_sources.all_data_sources_names():
            print(f"Starting download process for datasource: {data_source_name}")
            data_source = data_sources.get_data_source(data_source_name)

            print(f"Writing copy to folder: {data_source.get_dbfs_sample_path()}")
            df = data_source.estimate_size_and_fail_if_too_big(return_dataset=True)
            if ask_confirmation:
                proceed = input(
                    f"Proceed with the creation of the data source {data_source_name}? (y/n): "
                )
                if proceed != "y":
                    print("Skipping the creation of the data source")
                    continue
            self._write_sample_data(df, data_source)

        print("Copying of sample data sources finished")

    def _write_sample_data(self, df, data_source: DataSource) -> None:
        """
        Write the sampled data source into a temporary location in dbfs so it can be later downloaded
        If the location already exists it will be oveewriten
        If you write to a location different than the standard ddataflow location in it will fail.
        """

        # a security check so we dont write by mistake where we should not
        sample_destination = data_source.get_dbfs_sample_path()
        if not sample_destination.startswith(self.BASE_SNAPSHOT_PATH):
            raise WritingToLocationDenied(self.BASE_SNAPSHOT_PATH)

        #  add by default repartition so we only download a single file
        df = df.repartition(1)

        df.write.mode("overwrite").parquet(data_source.get_dbfs_sample_path())
