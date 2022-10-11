from typing import Any, Dict, List

from ddataflow.data_source import DataSource


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
