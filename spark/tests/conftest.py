# spark/tests/conftest.py

import pytest
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType,
    IntegerType, ArrayType, DateType
)
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when, concat, lit, count
from pyspark.sql.functions import  sum as spark_sum

@pytest.fixture(scope="session")
def expected_schema():
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", LongType(), True),
        StructField("last_contact", LongType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", ArrayType(IntegerType()), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True),
        StructField("category", IntegerType(), True),
        StructField("api_timestamp", LongType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("ingest_date", DateType(), True),
        StructField("ingest_hour", IntegerType(), True),
    ])


@pytest.fixture(scope="session")
def apply_segmentation(spark, expected_schema):
    def _apply(test_data):
        df = spark.createDataFrame(test_data, schema = expected_schema)

        #compute_lag_features(df, window_spec)
        window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
        df = df.withColumn("prev_onground",lag("on_ground",1).over(window_spec)).\
                withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))
        
        # compute_flight_segments(df, window_spec) 
        df = df.withColumn("is_new_flight", 
                           when(col("prev_api_timestamp").isNull(), True) #prev_api_timestamp first record of the flight 
                           .when((col("on_ground") == False) & (col("prev_onground") == True) , True) #takeoff 
                           .when(col("api_timestamp") - col("prev_api_timestamp") > 600, True).otherwise(False)) 
        
        # compute_flight_id(df)
        cumulative_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)#include the first row
        df = df.withColumn("segment",spark_sum(col("is_new_flight").cast("int")).over(cumulative_window))
        df = df.withColumn('flight_id', concat(col("icao24"), lit("_"), col("segment")))
        return df
    return _apply

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