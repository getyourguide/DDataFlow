from pyspark.sql.session import SparkSession

from ddataflow import DDataflow


def test_sql():
    spark = SparkSession.builder.getOrCreate()

    config = {
        "project_folder_name": "unit_tests",
        "data_sources": {
            "location": {
                'default_sampling': True
            }
        },
        'default_sampler': {
            'limit': 2
        }
    }
    ddataflow = DDataflow(**config)

    query = f""" select count(1) as total
        from {ddataflow.source_name('location')}
    """

    ddataflow.disable()
    result = spark.sql(query)
    # default amount of tours
    assert result.collect()[0].total == 3

    ddataflow.enable()
    query = f""" select count(1) as total
        from {ddataflow.source_name('location')}
    """
    result = spark.sql(query)
    assert result.collect()[0].total == 2


if __name__ == "__main__":
    import fire

    fire.Fire()
