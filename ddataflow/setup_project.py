def setup_project(config_file):
    content = """
from ddataflow import DDataflow

config = {
    # (optional) add here tables with ddataflow predefined sampling default sampling (a good way to start)
    # files with default_sampling are not supported at the moment
    "sources_with_default_sampling": [],
    # add here your tables or paths with customized sampling logic
    "data_sources": {},
    # add here your writing logic
    "data_writers": {},
    # this is the name of the project to identify this project in the filesystem
    # do not use - minus - signs in the name
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
