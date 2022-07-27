# DDataFlow

This is the DDataFlow integration manual.
If you want to know how to use DDataFlow in the local machine, see [this section](local_development).

## Install Ddataflow

```sh
pip install ddataflow
```

## Mapping your data sources

DDataflow is declarative and is completely configurable a single configuration in DDataflow startup. To create a configuration for you project simply run:

```shell

ddataflow setup_project
```

You can use this config also in in a notebook, or using databricks-connect or in the repository with db-rocket. Example config below:

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

## Estimating your data size

Use the estimate_source_size function to narrow down the size of your dataset by changing the filter options until you are
satisfied with the result.

```sh
ddataflow_client.estimate_source_size('ActivityCardImpression')
#Estimated size of the Dataset in GB:  1.2867986224591732
```

## Replace the sources

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
might be slower. From this point you can use dddataflow to run your pipelines on the sample data instead of the full data.

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

## Writing data

To write data we adivse you use the same code as production just write to a different destination.
DDataflow provides the path function that will return a path in ddataflow when ddataflow is enable.

```py
final_path = ddataflow.path('/mnt/my/production/path')
# final path will be /mnt/my/production/path when ddataflow is disabled
# final path will be $DDATAFLOW_FOLDER/project_name/mnt/my/production/path when ddataflow is enabled
```

And you are good to go!

## 8. Setup data locally

DDataflow also enables one to develop with local data. We see this though as a more advanced use case, which might be
the first choice for everybody. First, make a copy of the files you need to download in dbfs.

```py
ddataflow.save_sampled_data_sources(ask_confirmation=False)
```

Then in your machine:

```sh
ddataflow current_project download_data_sources
```

Now you can use the pipeline locally by exporting the following env variables:

```shell
export ENABLE_OFFLINE_MODE=true
# run your pipeline as normal
python yourproject/train.py
```

Your pipeline now should work with local data!
That's it!
