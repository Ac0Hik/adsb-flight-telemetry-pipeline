from datetime import date
from pyspark.sql.functions import col, count, first, min, max, from_unixtime, to_date


def test_drop_duplicates(spark,expected_schema):
    duplicates_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(duplicates_data, schema = expected_schema)
    df = df.dropDuplicates(['icao24', "api_timestamp"]).na.drop(subset=['longitude', 'latitude','icao24'])
    assert df.count() == 1


def test_flight_id_uniqueness(apply_segmentation):

    test_data = [
            ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775058980, 1775058980, -0.4614, 51.4775, 0.0,    True,  0.0,   0.0,  0.0,  None, 0.0,  "1234", False, 0, 0, 1775058980, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775059010, 1775059010, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775059010, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
            ("abc123", "BAW123", "United Kingdom", 1775059040, 1775059040, -0.4614, 51.4775, 2000.0, False, 250.0, 90.0, 10.0, None, 1900.0,"1234", False, 0, 0, 1775059040, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = apply_segmentation(test_data)

    df_with_id = df.groupBy("flight_id").agg(count("*").alias('row_count'))

    assert df_with_id.filter(col("row_count") > 1).count() > 0


def test_short_flight_excluded(apply_segmentation):
    test_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059070, 1775059070, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775059070, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = apply_segmentation(test_data)
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


def test_few_states_excluded (apply_segmentation):
    test_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 1000.0, False, 250.0, 90.0, 10.0, None, 950.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059190, 1775059190, -0.4614, 51.4775, 2000.0, False, 250.0, 90.0, 10.0, None, 1900.0, "1234", False, 0, 0, 1775059190, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
        ("abc123", "BAW123", "United Kingdom", 1775059430, 1775059430, -0.4614, 51.4775, 3000.0, False, 250.0, 90.0, 10.0, None, 2900.0, "1234", False, 0, 0, 1775059430, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = apply_segmentation(test_data)
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