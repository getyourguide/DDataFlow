from pyspark.sql import SparkSession
from ddataflow_config import ddataflow
import os
spark = SparkSession.builder.getOrCreate()

# if we are dealing with offline data we dont need to  register anything as ddataflow will take care of it for us
if 'ENABLE_OFFLINE_MODE' not in os.environ:
    spark.read.parquet("/tmp/demo_locations.parquet").registerTempTable("demo_locations")
    spark.read.parquet("/tmp/demo_tours.parquet").registerTempTable("demo_tours")

# pyspark code using a different source name
total_locations = spark.table(ddataflow.name('demo_locations')).count()
# sql code also works
total_tours = spark.sql(f""" SELECT COUNT(1) from {ddataflow.name('demo_tours')}""").collect()[0]['count(1)']
print({
    "total_locations": total_locations,
    "total_tours": total_tours,
})

