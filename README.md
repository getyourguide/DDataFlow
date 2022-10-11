# DDataFlow

DDataFlow is an end2end tests and local development solution for machine learning and data pipelines using pyspark.
It samples the data as an approach to get slow pipelines run fast in the CI.

## Features

- Read a subset of our data so to speed up the running of the pipelines during tests
- Write to a test location our artifacts  so you don't pollute production
- Download data for enabling local machine development

Enables to run on the pipelines in the CI

## 1. Install Ddataflow

```sh
pip install ddataflow
```

# Getting Started (<5min Tutorial)

This tutorial aims to show you the core features though, for the complete reference see the [integration manual](docs/integrator_manual.md) in the docs.

## 1. Create some synthetic data

```py
# create a script  called create_syntetic_data.py and place the following code in it
from random import randrange
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()

locations = [
    {"location_id": i, "location_name": f"Location {i}"} for i in range(2000)
]

spark.createDataFrame(locations).write.parquet("/tmp/demo_locations.parquet")

tours = [
    {"tour_id": i, "tour_name": f"Tour {i}", "location_id": randrange(1000)} for i in range(50000)
]
spark.createDataFrame(tours).write.parquet("/tmp/demo_tours.parquet")
```

And run it **python create_syntetic_data.py**

## 2. Create a ddataflow_config.py file

```py
from ddataflow import DDataflow

config = {
    # add here tables or paths to data sources with default sampling
    "sources_with_default_sampling": ['demo_locations'],
    # add here your tables or paths with customized sampling logic
    "data_sources": {
        "demo_tours": {
            "source": lambda spark: spark.table('demo_tours'),
            "filter": lambda df: df.limit(500)
        }
    },
    "project_folder_name": "ddataflow_demo",
}

# initialize the application and validate the configuration
ddataflow = DDataflow(**config)
```

Note: the command **ddtaflow setup_project** creates a file like this for you.

## 3. Create an example pipeline

```py
# filename: pipeline.py
from pyspark.sql import SparkSession
from ddataflow_config import ddataflow

spark = SparkSession.builder.getOrCreate()

# register the tables to mimick a real environment 
# when you use ddatflow for real you will have your production tables in place already
spark.read.parquet("/tmp/demo_locations.parquet").registerTempTable("demo_locations")
spark.read.parquet("/tmp/demo_tours.parquet").registerTempTable("demo_tours")

# pyspark code using a different source name
total_locations = spark.table(ddataflow.name('demo_locations')).count()
# sql code also works
total_tours = spark.sql(f""" SELECT COUNT(1) from {ddataflow.name('demo_tours')}""").collect()[0]['count(1)']
print("Totals follow below:")
print({
    "total_locations": total_locations,
    "total_tours": total_tours,
})
```

Now run it twice and observe the difference in the amount of records:
**python pipeline.py**

**ENABLE_DDATAFLOW=True python pipeline.py**

You will see that the dataframes are sampled when ddataflow is enabled and full when the tool is disabled.

You completed the short demo!


## Support

In case of questions feel free to reach out or create an issue.

## Contributing

This project requires manual release at the moment. See the docs and request a pypi access if you want to contribute.