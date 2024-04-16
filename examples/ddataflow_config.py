from ddataflow import DDataflow

config = {
    # add here your tables or paths with customized sampling logic
    "data_sources": {
        "demo_tours": {
            "source": lambda spark: spark.table('demo_tours'),
            "filter": lambda df: df.limit(500)
        },
        "demo_locations": {
            "source": lambda spark: spark.table('demo_locations'),
            "default_sampling": True,
        }
    },
    "project_folder_name": "ddataflow_demo",
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)