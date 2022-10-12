from ddataflow import DDataflow

config = {
    # change the name of the project  for a meaningful name (github project name usually a good idea)
    "project_folder_name": "your_project_name",
    # add here your tables or paths with customized sampling logic
    "data_sources": {
        #"demo_tours": {
        #    # use default_sampling=True to rely on automatic sampling otherwise it will use the whole data
        #    "default_sampling": True,
        #},
        #"demo_tours2": {
        #    "source": lambda spark: spark.table("demo_tours"),
        #    # to customize the sampling logic
        #    "filter": lambda df: df.limit(500),
        #},
        #"/mnt/cleaned/EventName": {
        #    "file-type": "parquet",
        #    "default_sampling": True,
        #},
    },
    #"default_sampler": {
    #    # defines the amount of rows retrieved with the default sampler, used as .limit(limit) in the dataframe
    #    # default = 10000
    #    "limit": 100000,
    #},
    # to customize the max size of your examples uncomment the line below
    # "data_source_size_limit_gb": 3
    # to customize the location of your test datasets in your data wharehouse
    # "snapshot_path": "dbfs:/another_databricks_path",
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)
