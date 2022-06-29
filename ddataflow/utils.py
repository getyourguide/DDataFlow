def get_or_create_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def estimate_spark_dataframe_size(spark_dataframe):
    # @todo use spark official estimation function
    # number Of gigabytes = M = (N*V*W) / 1024^3
    average_variable_size_bytes = 50
    return (
        spark_dataframe.count()
        * len(spark_dataframe.columns)
        * average_variable_size_bytes
    ) / (1024 ** 3)


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
