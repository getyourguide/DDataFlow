# This file is aimed to consolidate the learnings of running spark on the local machine


## Example local setup

```py
def configure_spark():

    if ddataflow.is_local():
        import pyspark

        spark_conf = pyspark.SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", "/tmp")
        spark_conf.set("spark.sql.catalogImplementation", "hive")
        spark_conf.setMaster("local[*]")
        sc = pyspark.SparkContext(conf=spark_conf)
        session = pyspark.sql.SparkSession(sc)

        return session

    return SparkSession.builder.getOrCreate()
```

## Issues

1. Snappy compression problem: Please reinstall pyspark
    
    
    
