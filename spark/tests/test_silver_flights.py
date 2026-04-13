from datetime import date
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType,
    IntegerType, ArrayType, DateType
)

expected_schema = StructType([
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

def test_drop_duplicates(spark):

    duplicates_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(duplicates_data, schema = expected_schema)
    df = df.dropDuplicates(['icao24', "api_timestamp"]).na.drop(subset=['longitude', 'latitude','icao24'])
    assert df.count() == 1


def test_flight_id_uniqueness(spark):

    test_data = [
            ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775058980, 1775058980, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058980, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775059010, 1775059010, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775059010, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775059040, 1775059040, -0.4614, 51.4775, 2000.0, False, 250.0, 90.0, 10.0, None, 1900.0,"1234", False, 0, 0, 1775059040, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]

    df = spark.createDataFrame(test_data, schema = expected_schema)

    #compute_lag_features(df, window_spec)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, col

    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    df = df.withColumn("prev_onground",lag("on_ground",1).over(window_spec)).\
            withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))

    # compute_flight_segments(df, window_spec) 
    from pyspark.sql.functions import when

    df = df.withColumn("is_new_flight", 
                       when(col("prev_api_timestamp").isNull(), True) #prev_api_timestamp first record of the flight 
                       .when((col("on_ground") == False) & (col("prev_onground") == True) , True) #takeoff 
                       .when(col("api_timestamp") - col("prev_api_timestamp") > 600, True).otherwise(False)) 
    
    # compute_flight_id(df)
    from pyspark.sql.functions import sum as spark_sum

    cumulative_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)#include the first row
    df = df.withColumn("segment",spark_sum(col("is_new_flight").cast("int")).over(cumulative_window))

    from pyspark.sql.functions import concat, lit, count

    df = df.withColumn('flight_id', concat(col("icao24"), lit("_"), col("segment")))

    df_with_id = df.groupBy("flight_id").agg(count("*").alias('row_count'))

    assert df_with_id.filter(col("row_count") > 1).count() > 0


def test_short_flight_excluded(spark):
    test_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059070, 1775059070, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775059070, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)

    #compute_lag_features(df, window_spec)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, col

    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    df = df.withColumn("prev_onground",lag("on_ground",1).over(window_spec)).\
            withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))

    # compute_flight_segments(df, window_spec) 
    from pyspark.sql.functions import when

    df = df.withColumn("is_new_flight", 
                       when(col("prev_api_timestamp").isNull(), True) #prev_api_timestamp first record of the flight 
                       .when((col("on_ground") == False) & (col("prev_onground") == True) , True) #takeoff 
                       .when(col("api_timestamp") - col("prev_api_timestamp") > 600, True).otherwise(False)) 
    
    # compute_flight_id(df)
    from pyspark.sql.functions import sum as spark_sum

    cumulative_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)#include the first row
    df = df.withColumn("segment",spark_sum(col("is_new_flight").cast("int")).over(cumulative_window))

    from pyspark.sql.functions import concat, lit, to_date, from_unixtime, first, count, min, max

    df = df.withColumn('flight_id', concat(col("icao24"), lit("_"), col("segment")))
    short_flights_excluded = df.groupBy("flight_id") \
        .agg(
          first("icao24").alias("icao24"),
          first("callsign").alias("callsign"),
          min("api_timestamp").alias("departure_ts"),
          max("api_timestamp").alias("arrival_ts"),
        ).withColumn("duration_min", (col("arrival_ts") - col("departure_ts")) / 60 
        ).withColumn("flight_date", to_date(from_unixtime(col("departure_ts")))
        ).filter((col("duration_min") >= 5))
    
    assert short_flights_excluded.count() == 0


def test_few_states_excluded (spark):
    test_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059190, 1775059190, -0.4614, 51.4775, 2000.0, False, 250.0, 90.0, 10.0, None, 1900.0, "1234", False, 0, 0, 1775059190, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059430, 1775059430, -0.4614, 51.4775, 3000.0, False, 250.0, 90.0, 10.0, None, 2900.0, "1234", False, 0, 0, 1775059430, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)

    #compute_lag_features(df, window_spec)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, col

    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    df = df.withColumn("prev_onground",lag("on_ground",1).over(window_spec)).\
            withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))

    # compute_flight_segments(df, window_spec) 
    from pyspark.sql.functions import when

    df = df.withColumn("is_new_flight", 
                       when(col("prev_api_timestamp").isNull(), True) #prev_api_timestamp first record of the flight 
                       .when((col("on_ground") == False) & (col("prev_onground") == True) , True) #takeoff 
                       .when(col("api_timestamp") - col("prev_api_timestamp") > 600, True).otherwise(False)) 
    
    # compute_flight_id(df)
    from pyspark.sql.functions import sum as spark_sum

    cumulative_window = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)#include the first row
    df = df.withColumn("segment",spark_sum(col("is_new_flight").cast("int")).over(cumulative_window))

    from pyspark.sql.functions import concat, lit, to_date, from_unixtime, first, count, min, max

    df = df.withColumn('flight_id', concat(col("icao24"), lit("_"), col("segment")))
    few_states_df = df.groupBy("flight_id") \
        .agg(
          first("icao24").alias("icao24"),
          first("callsign").alias("callsign"),
          min("api_timestamp").alias("departure_ts"),
          max("api_timestamp").alias("arrival_ts"),
          count('segment').alias("state_count")
        ).withColumn("duration_min", (col("arrival_ts") - col("departure_ts")) / 60 
        ).withColumn("flight_date", to_date(from_unixtime(col("departure_ts")))
        ).filter((col("state_count") >= 10))

    assert few_states_df.count() == 0 