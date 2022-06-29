#!/usr/bin/env python3

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
        {"tour_id": i, "tour_name": f"Tour {i}", "location_id": randrange(2000)} for i in range(50000)
    ]
    tours_df = spark.createDataFrame(tours)
    tours_df.write.parquet("/tmp/demo_tours.parquet")



if __name__ == '__main__':
    create_data()

