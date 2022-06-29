# DDataFlow

DDataFlow is an end2end tests and local development solution for machine learning and data pipelines using pyspark.

## Features

- Read a subset of our data so to speed up the running of the pipelines
- Write to a test location our artifacts  so you don't pollute production
- Download data for enabling local machine development

Enables to run on the pipelines in the CI

## 1. Install Ddataflow

```sh
pip install ddataflow
```

# 5M Getting Started Tutorial
You can also find it in our examples folder.

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

## 2. Create a ddataflow config file

```py
# file name: ddataflow-config.py
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
ddataflow_client = DDataflow(**config)
```

Note: the command *ddtaflow setup_project* creates a file like this for you.


## 3. Create an example pipeline

```py
# filename: pipeline.py
from pyspark.sql import SparkSession
from ddataflow_config import ddataflow_client as ddataflow
spark = SparkSession.builder.getOrCreate()
spark.read.parquet("/tmp/demo_locations.parquet").registerTempTable("demo_locations")
spark.read.parquet("/tmp/demo_tours.parquet").registerTempTable("demo_tours")

def inspect_dataframes():
    # pyspark code using a different source name
    total_locations = spark.table(ddataflow.name('demo_locations')).count()
    # sql code also works
    total_tours = spark.sql(f""" SELECT COUNT(1) from {ddataflow.name('demo_tours')}""").collect()[0]['count(1)']
    print("Totals follow below:")
    print({
        "total_locations": total_locations,
        "total_tours": total_tours,
    })


print("By default return the real dataset")
inspect_dataframes()

ddataflow.enable()
print("If enabled dataframes should be sampled")
inspect_dataframes()
```
And run it: **python pipeline.py**

You will see that the dataframes are sampled when ddataflow is enabled and full when the tool is disabled.
Thats it for the demo, for further details see the integration manual in the docs.

## Support

In case of questions feel free to reach out or create an issue.
