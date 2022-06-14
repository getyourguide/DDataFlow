from datetime import datetime, timedelta

import pyspark.sql.functions as F

from ddataflow.ddataflow import DDataflow

start_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_time = datetime.now().strftime("%Y-%m-%d")

config = {
    "data_sources": {
        # data sources define how to access data
        "events": {
            "source": lambda spark: spark.table("events"),
            "filter": lambda df: df.filter(F.col("date") >= start_time)
            .filter(F.col("date") <= end_time)
            .filter(F.col("event_name").isin(["BookAction", "ActivityCardImpression"]))
            .filter(F.hour("kafka_timestamp").between(8, 9))
            .limit(1000),
        },
        "ActivityCardImpression": {
            "source": lambda spark: spark.read.parquet(
                "dbfs:/events/ActivityCardImpression"
            ),
            "filter": lambda df: df.filter(F.col("date") >= start_time)
            .filter(F.col("date") <= end_time)
            .filter(F.hour("kafka_timestamp").between(8, 9))
            .limit(1000),
        },
        "dim_tour": {
            "source": lambda spark: spark.table("tour"),
        },
    },
    "project_folder_name": "tests",
    "data_source_size_limit_gb": 3,
}
ddataflow = DDataflow(**config)  # type: ignore
