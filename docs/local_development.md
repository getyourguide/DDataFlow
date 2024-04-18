# Local Development

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

The downloaded data sources will be stored at `$HOME/.ddataflow`.

## Local setup for spark

if you run spark locally you might need to tweak some parameters compared to your cluster. Below is a good example you can use.

```py
def configure_spark():

    if ddataflow.is_local():
        import pyspark

        spark_conf = pyspark.SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", "/tmp")
        spark_conf.set("spark.sql.catalogImplementation", "hive")
        spark_conf.set("spark.driver.memory", "15g")
        spark_conf.setMaster("local[*]")
        sc = pyspark.SparkContext(conf=spark_conf)
        session = pyspark.sql.SparkSession(sc)

        return session

    return SparkSession.builder.getOrCreate()
```

If you run into Snappy compression problem: Please reinstall pyspark! 
