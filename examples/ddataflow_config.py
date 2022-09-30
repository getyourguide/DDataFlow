from ddataflow import DDataflow

config = {
    # add here tables or paths to data sources with default sampling
    "sources_with_default_sampling": ["demo_locations"],
    # add here your tables or paths with customized sampling logic
    "data_sources": {
        "demo_tours": {
            "source": lambda spark: spark.table("demo_tours"),
            "filter": lambda df: df.limit(500),
        }
    },
    # add here your writing logic
    "data_writers": {},
    # this is the _name of the project to identify this project in the filesystem
    "project_folder_name": "ddataflow_demo",
    # to customize the location of your datasets
    # "_snapshot_path": "dbfs:/another_databricks_path",
    # to customize the size of your samples uncomment the line below
    # "data_source_size_limit_gb": 3
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)
