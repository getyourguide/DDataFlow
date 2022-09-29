from pyspark.sql import SparkSession


def get_or_create_spark():
    return SparkSingleton.get_instance()


class SparkSingleton:
    spark = None

    @staticmethod
    def get_instance():
        if SparkSingleton.spark is None:
            SparkSingleton.spark = SparkSession.builder.getOrCreate()

        return SparkSingleton.spark


def summarize_spark_dataframe(spark_dataframe):
    spark_dataframe.show(3)
    result = {
        "num_of_rows": spark_dataframe.count(),
        "num_of_columns": len(spark_dataframe.columns),
        "estimated_size": estimate_spark_dataframe_size(
            spark_dataframe=spark_dataframe
        ),
    }
    return result


def is_local(spark):
    setting = spark.conf.get("spark.master")
    return "local" in setting


def using_databricks_connect() -> bool:
    """
    Return true if databricks connect is being used
    """
    import os

    result = os.system("pip show databricks-connect")

    return result == 0
