def setup_project(config_file):
    content = """
from ddataflow import DDataflow

config = {
    # change the name of the project  for a meaningful name (github project name usually a good idea)
    "project_folder_name": "my_project",
    # add here your tables or paths with customized sampling logic
    "data_sources": {
    #    'demo_tours': {
    #        # use default_sampling=True to rely on automatic sampling otherwise it will use the hole data
    #        'default_sampling': True,
    #    },
    #    'demo_tours2': {
    #        'source': lambda spark: spark.table('demo_tours'),
    #        # to customize the sampling logic
    #        'filter': lambda df: df.limit(500)
    #    }
    },
    # to customize the max size of your examples uncomment the line below
    # "data_source_size_limit_gb": 3
    # to customize the location of your test datasets in your data wharehouse
    # "snapshot_path": "dbfs:/another_databricks_path",
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)
"""

    with open(config_file, "w") as f:
        f.write(content)
    print(f"File {config_file} created in the current directory.")
