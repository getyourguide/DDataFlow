from pyspark.sql import SparkSession
from ddataflow_config import ddataflow
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


inspect_dataframes()

