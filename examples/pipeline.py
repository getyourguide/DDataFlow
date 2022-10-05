import os

from ddataflow_config import ddataflow
from random import randrange

from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


def create_data():
    locations = [
        {"location_id": i, "location_name": f"Location {i}"} for i in range(2000)
    ]

    locations_df = spark.createDataFrame(locations)
    locations_df.write.parquet("/tmp/demo_locations.parquet")

    tours = [
        {"tour_id": i, "tour_name": f"Tour {i}", "location_id": randrange(2000)}
        for i in range(50000)
    ]
    tours_df = spark.createDataFrame(tours)
    tours_df.write.parquet("/tmp/demo_tours.parquet")


def pipeline():
    spark = SparkSession.builder.getOrCreate()

    # if we are dealing with offline data we dont need to  register anything as ddataflow will take care of it for us
    if "ENABLE_OFFLINE_MODE" not in os.environ:
        spark.read.parquet("/tmp/demo_locations.parquet").registerTempTable(
            "demo_locations"
        )
        spark.read.parquet("/tmp/demo_tours.parquet").registerTempTable("demo_tours")

    # pyspark code using a different source name
    total_locations = spark.table(ddataflow.name("demo_locations")).count()
    # sql code also works
    total_tours = spark.sql(
        f""" SELECT COUNT(1) from {ddataflow.name('demo_tours')}"""
    ).collect()[0]["count(1)"]
    return {
        "total_locations": total_locations,
        "total_tours": total_tours,
    }

def run_scenarios():
    #create_data()
    ddataflow.disable()
    result = pipeline()
    assert result["total_tours"] == 50000

    ddataflow.enable()
    result = pipeline()
    print(result)
    assert result["total_tours"] == 500




if __name__ == "__main__":
    import fire
    fire.Fire()

