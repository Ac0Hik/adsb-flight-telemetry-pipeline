# spark/tests/conftest.py

import pytest
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("adsb_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()