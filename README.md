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

# Integration Steps


## 2. Mapping your data sources

You can either start in a notebook, using databricks-connect or in the repository with db-rocket.
```py
#later save this script as ddataflow_config.py to follow our convention
from ddataflow import DDataflow
import pyspark.sql.functions as F

start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_time = datetime.now().strftime("%Y-%m-%d")

config = {
    "data_sources": {
        # data sources define how to access data
        "events": {
            "source": lambda spark: spark.table("events"),
            #  here we define the spark query to reduce the size of the data
            #  the filtering strategy will most likely dependend on the domain.
            "filter": lambda df:
                df.filter(F.col("date") >= start_time)
                    .filter(F.col("date") <= end_time)
                    .filter(F.col("event_name").isin(["BookAction", "ActivityCardImpression"])),
        },
        "ActivityCardImpression": {
            # source can also be partquet files
            "source": lambda spark: spark.read.parquet(
                f"dbfs:/events/eventname/date={start_time}/"
            )
        },
    },
    "project_folder_name": "myproject",
}

# initialize the application and validate the configuration
ddataflow_client = DDataflow(**config)
```

## 3. Estimating your data size

Use the estimate_source_size function to narrow down the size of your dataset by changing the filter options until
you are
satisfied with the result.
```sh
ddataflow_client.estimate_source_size('ActivityCardImpression')
#Estimated size of the Dataset in GB:  1.2867986224591732
```

## 4. Replace the sources

Replace in your code the calls to the original data sources for the ones provided by ddataflow.

```py
spark.table('events') #...
spark.read.parquet("dbfs:/mnt/analytics/cleaned/v1/ActivityCardImpression") # ...
```

Replace with the following:
```py
from ddataflow_config import ddataflow_client

ddataflow_client.source('events')
ddataflow_client.source("ActivityCardImpression")
```
Its not a problem if you dont map all data sources if you dont map one it will keep going to production tables and
might be slower.
From this point you can use dddataflow to run your pipelines on the sample data instead of the full data.

**Note: BY DEFUAULT ddataflow is DISABLED, so the calls will attempt to go to production, which if done wrong can
lead to writing trash data**.

To enable DDataFlow you can either export an environment variable without changing the code.

```shell
# in shell or in the CICD pipeline
export ENABLE_DDATAFLOW=true
# run your pipeline as normal
python conduction_time_predictor/train.py
```

Or you can enable it programmatically in python

```shell
ddataflow_client.enable()
```

At any point in time you can check if the tool is enabled or disabled by running:

```py
ddataflow_client.printStatus()
```


## 6. Setup Data writers

Writers are what we use to write data in our pipelines.
DDdataflow also provides an abstraction to write the data to a
different location while it is enabled.

Add  writers entry in your config as well:

```py
# in ddataflow_config.py

     "data_writers": {
        "gdp.latest_sellout_likelihood_predictions_slot_level": {
            "writer": lambda df, name, spark: df.write.mode("overwrite").saveAsTable(
                name
            )
        },
    }
}
```

Now in your code replace:

```py
df.write.mode("overwrite").saveAsTable("gdp.latest_sellout_likelihood_predictions_slot_level")
```

with

```py
ddataflow_client.write('gdp.latest_sellout_likelihood_predictions_slot_level')

```
And you are good to go!

## 7. Add to the CI

Add the following to your drone.yml file, in the section before you create the production Docker image.

## 8. Setup data locally

DDataflow also enables one to develop with local data.
We see this though as a more advanced use case, which might be
the first choice for everybody.
First, make a copy of the files you need to download in dbfs.


```sh
ddataflow current_project sample_and_download
```

Now you can use the pipeline locally by exporting the following env variables:

```shell
export ENABLE_DDATAFLOW=true ; export ENABLE_OFFLINE_MODE=true
# run your pipeline as normal
python yourproject/train.py
```

Your pipeline now should work with local data!
That's it!

## Support

In case of questions feel free to reach out or create an issue.
