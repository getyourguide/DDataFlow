def setup_project(config_file):
    content = """
from ddataflow import DDataflow

_config = {
    # add here your tables or paths with customized sampling logic
    "data_sources": {
        'demo_tours': {
            'source': lambda spark: spark.table('demo_tours'),
            'default_sampling': True,
        }
    },
    # add here your writing logic
    "data_writers": {},
    "project_folder_name": "my_project",
    # to customize the location of your datasets 
    # "snapshot_path": "dbfs:/another_databricks_path",
    # to customize the size of your samples uncomment the line below
    # "data_source_size_limit_gb": 3
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)
"""

    with open(config_file, "w") as f:
        f.write(content)
    print(f"File {config_file} created in the current directory.")
