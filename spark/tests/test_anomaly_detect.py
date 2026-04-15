from datetime import date
from pyspark.sql.functions import col,concat, lag, to_date, current_timestamp, lit,when, abs as abs_spark
from pyspark.sql.window import Window


def test_emergency_squawk(spark, expected_schema):
    test_data = [("4ca2b1", "BAW291", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 8000.0, False, 250.0, 90.0, 0.0, None, 7900.0, "7700", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))
    df = df.dropna(subset=["squawk"]).filter(col("squawk").isin(["7500", "7600", "7700"]))\
        .withColumn("anomaly_type", lit("EMERGENCY_SQUAWK")) \
        .withColumn("anomaly_severity", lit("CRITICAL")) \
        .withColumn("detail", when(col("squawk") == "7500", lit("Squawk 7500 detected — Hijack in progress"))
                              .when(col("squawk") == "7600", lit("Squawk 7600 detected — Radio communication failure"))
                                .otherwise(lit("Squawk 7700 detected — General emergency declared") )
        )
    
    assert df.count() == 1
    assert df.first()['anomaly_severity'] == 'CRITICAL'


def test_rapid_altitude_drop(spark, expected_schema):
    test_data = [    
        ("4d3a1b", "DLH441", "Germany", 1775058950, 1775058950, 8.6821, 50.0379, 8000.0, False, 250.0, 90.0, 0.0, None, 7900.0, "1234", False, 0, 0,    1775058950, "2025-01-01T10:00:00Z", date(2025, 1, 1), 10),
        ("4d3a1b", "DLH441", "Germany", 1775058980, 1775058980, 8.6821, 50.0379, 1200.0, False, 250.0, 90.0, 0.0, None, 1100.0, "1234", False, 0, 0,    1775058980, "2025-01-01T10:00:00Z", date(2025, 1, 1), 10),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))

    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    rapid_altitude_drop_df = df.dropna(subset=["baro_altitude", "api_timestamp"]
                                ).withColumn("prev_baro_altitude",lag("baro_altitude",1).over(window_spec)
                                ).withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))
    
    BARO_ALTITUDE_DROP_THRESHOLD = -500
    MIN_ARIBORN_BARO_ALTITUDE = 1000
    rapid_altitude_drop_df = rapid_altitude_drop_df.filter( (col("baro_altitude") - col("prev_baro_altitude") < BARO_ALTITUDE_DROP_THRESHOLD) &
                                                           ( ~col("on_ground")) &
                                                           ( col("baro_altitude") > MIN_ARIBORN_BARO_ALTITUDE) )\
            .withColumn("anomaly_type", lit("RAPID_ALTITUDE_DROP")) \
            .withColumn("anomaly_severity", lit("HIGH")) \
            .withColumn("detail", concat(
                                    lit("Rapid altitude drop detected — from "),
                                    col("prev_baro_altitude").cast("string"),
                                    lit("m to "),
                                    col("baro_altitude").cast("string"),
                                    lit("m")
                                )
                        )
    assert rapid_altitude_drop_df.count() == 1
    assert rapid_altitude_drop_df.first()['anomaly_type'] == 'RAPID_ALTITUDE_DROP'


def test_extreme_vertical_rate(spark, expected_schema):

    test_data = [    
        ("4b7f2a", "RYR892", "Ireland", 1775058950, 1775058950, -6.2603, 53.3498, 7000.0, False, 230.0, 90.0, 45.0, None, 6900.0, "1234", False, 0, 0, 1775058950, "2025-01-01T14:00:00Z", date(2025, 1, 1), 14),
        ("3a9c4e", "EZY331", "France", 1775058950, 1775058950, 2.5478, 49.0097, 5000.0, False, 210.0, 180.0, -30.0, None, 4900.0, "1234", False, 0, 0, 1775058950, "2025-01-01T08:00:00Z", date(2025, 1, 1), 8),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))

    EXTREME_VERTICAL_RATE_CRITICAL_THRESHOLD = 40
    EXTREME_VERTICAL_RATE_HIGH_THRESHOLD = 25

    extreme_vertical_rate_df = df.dropna(subset=["vertical_rate"])\
                                .withColumn("abs_vertical_rate", abs_spark(col("vertical_rate")))\
                                .filter( (col("abs_vertical_rate") > EXTREME_VERTICAL_RATE_HIGH_THRESHOLD) & (col("on_ground") ==False) )\
                                .withColumn("anomaly_type", lit("EXTREME_VERTICAL_RATE")
                                ).withColumn("anomaly_severity", when(
                                                                    col("abs_vertical_rate") > EXTREME_VERTICAL_RATE_CRITICAL_THRESHOLD,
                                                                        lit("CRITICAL")
                                                                    )
                                                                .otherwise(lit("HIGH"))
                                ).withColumn("detail", concat(
                                                        lit("Extreme vertical rate detected — "),
                                                        col("abs_vertical_rate").cast("string"),
                                                        lit("m/s")
                                                        )
                                ).drop("abs_vertical_rate")
    
    df_with_high_vr = extreme_vertical_rate_df.filter(col("anomaly_severity") == "HIGH")
    df_with_critical_vr = extreme_vertical_rate_df.filter(col("anomaly_severity") == "CRITICAL")
    assert (df_with_critical_vr.count() == 1) and (df_with_high_vr.count() == 1)
    assert df_with_high_vr.first()['anomaly_severity'] == 'HIGH'
    assert df_with_critical_vr.first()['anomaly_severity'] == 'CRITICAL'


def test_unusual_speed(spark, expected_schema):
    test_data = [    
        ("a3f1c7", "UAL789", "United States", 1775058950, 1775058950, -87.9048, 41.9742, 10000.0, False, 400.0, 90.0,  0.0, None, 9900.0, "1234",   False, 0, 0, 1775058950, "2025-01-01T12:00:00Z", date(2025, 1, 1), 12),
        ("3c6b2e", "AFR991", "France",        1775058950, 1775058950, 2.5478,  49.0097,  7000.0, False,  30.0, 180.0, 0.0, None, 6900.0, "1234",    False, 0, 0, 1775058950, "2025-01-01T16:00:00Z", date(2025, 1, 1), 16),
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 3000.0, False, 200.0, 90.0, 0.0, None, 2900.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))
    

    MAX_SPEED_THRESHOLD = 350
    MIN_SPEED_HIGH_ALTITUDE_THRESHOLD = 50
    MIN_HIGH_ALTITUDE = 6000

    df = df.dropna(subset=["velocity", "baro_altitude"]
                        ).filter(( col("on_ground") == False) & ( ( col("velocity") > MAX_SPEED_THRESHOLD) 
                                                                                            | ( 
                                                                                            (col("baro_altitude") > MIN_HIGH_ALTITUDE) & (col("velocity") < MIN_SPEED_HIGH_ALTITUDE_THRESHOLD))
                                                                                        ) 
                        ).withColumn("anomaly_type", lit("UNUSUAL_SPEED")) \
                        .withColumn("anomaly_severity", lit("MEDIUM"))\
                        .withColumn("detail", 
                                            when(col("velocity") > MAX_SPEED_THRESHOLD, concat( 
                                                                                            lit("Unusual speed detected — velocity "),
                                                                                            col("velocity").cast("string"),
                                                                                            lit(" m/s exceeds maximum threshold")
                                                                                        )
                                            ).otherwise(concat(
                                                        lit("Unusual speed detected — velocity "),
                                                        col("velocity").cast("string"),
                                                        lit(" m/s too low at "),
                                                        col("baro_altitude").cast("string"),
                                                        lit("m altitude")
                                                    )
                                            )
                        )
    assert df.count() == 2


def test_signal_gap(spark, expected_schema):
    test_data = [
        ("4e2b1a", "BAW442", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 5000.0, False, 250.0, 90.0, 0.0, None, 4900.0, "1234", False,  0, 0, 1775058950, "2025-01-01T09:00:00Z", date(2025, 1, 1), 9),
        ("4e2b1a", "BAW442", "United Kingdom", 1775059550, 1775059550, -0.4614, 51.4775, 5000.0, False, 250.0, 90.0, 0.0, None, 4900.0, "1234", False,  0, 0, 1775059550, "2025-01-01T09:10:00Z", date(2025, 1, 1), 9),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))

    MIN_SIGNAL_GAP_SECONDS = 300 #5min
    MAX_SIGNAL_GAP_SECONDS = 3600 #60min

    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    signal_gap_df = df.withColumn("prev_api_timestamp",lag("api_timestamp",1).over(window_spec))

    signal_gap_df = signal_gap_df.dropna(subset=["api_timestamp"]).filter(
                                        ( col("api_timestamp") - col("prev_api_timestamp") > MIN_SIGNAL_GAP_SECONDS ) & ( col("api_timestamp") - col("prev_api_timestamp") < MAX_SIGNAL_GAP_SECONDS) & (col("on_ground") == False))\
                                .withColumn("anomaly_type", lit("SIGNAL_GAP")) \
                                .withColumn("anomaly_severity", lit("MEDIUM"))\
                                .withColumn("detail",concat(lit("Signal gap detected — "),
                                                (col("api_timestamp") - col("prev_api_timestamp")).cast("string"),
                                                lit(" since last contact")
                                                )
                                )

    assert signal_gap_df.count() == 1
    assert signal_gap_df.first()['anomaly_type'] == 'SIGNAL_GAP'


def test_false_positive(spark, expected_schema):
    test_data = [
        ("abc123", "BAW123", "United Kingdom", 1775058950, 1775058950, -0.4614, 51.4775, 3000.0, False, 200.0, 90.0, 0.0, None, 2900.0, "1234", False, 0, 0, 1775058950, "2025-01-01T00:00:00Z", date(2025, 1, 1), 0),
    ]
    df = spark.createDataFrame(test_data, schema = expected_schema)
    df = df.withColumn("detected_at", current_timestamp()) \
       .withColumn("detection_date", to_date(col("detected_at")))
    
    window_spec = Window.partitionBy("icao24").orderBy("api_timestamp")
    
    #squwak
    squawk_df = df.dropna(subset=["squawk"]).filter(col("squawk").isin(["7500", "7600", "7700"]))\
        .withColumn("anomaly_type", lit("EMERGENCY_SQUAWK")) \
        .withColumn("anomaly_severity", lit("CRITICAL")) \
        .withColumn("detail", when(col("squawk") == "7500", lit("Squawk 7500 detected — Hijack in progress"))
                              .when(col("squawk") == "7600", lit("Squawk 7600 detected — Radio communication failure"))
                                .otherwise(lit("Squawk 7700 detected — General emergency declared") )
        )

    #altitude drop
    altitude_drop_df = df.dropna(subset=["baro_altitude", "api_timestamp"]
                                ).withColumn("prev_baro_altitude",lag("baro_altitude",1).over(window_spec)
                                ).withColumn("prev_api_timestamp", lag("api_timestamp",1).over(window_spec))
    
    BARO_ALTITUDE_DROP_THRESHOLD = -500
    MIN_ARIBORN_BARO_ALTITUDE = 1000
    altitude_drop_df = altitude_drop_df.filter( (col("baro_altitude") - col("prev_baro_altitude") < BARO_ALTITUDE_DROP_THRESHOLD) &
                                                           ( col("on_ground") == False) &
                                                           ( col("baro_altitude") > MIN_ARIBORN_BARO_ALTITUDE) )\
            .withColumn("anomaly_type", lit("RAPID_ALTITUDE_DROP")) \
            .withColumn("anomaly_severity", lit("HIGH")) \
            .withColumn("detail", concat(
                                    lit("Rapid altitude drop detected — from "),
                                    col("prev_baro_altitude").cast("string"),
                                    lit("m to "),
                                    col("baro_altitude").cast("string"),
                                    lit("m")
                                )
                        )
    
    #vertical_Rate
    EXTREME_VERTICAL_RATE_CRITICAL_THRESHOLD = 40
    EXTREME_VERTICAL_RATE_HIGH_THRESHOLD = 25

    vertical_rate_df = df.dropna(subset=["vertical_rate"])\
                                .withColumn("abs_vertical_rate", abs_spark(col("vertical_rate")))\
                                .filter( (col("abs_vertical_rate") > EXTREME_VERTICAL_RATE_HIGH_THRESHOLD) & (col("on_ground") ==False) )\
                                .withColumn("anomaly_type", lit("EXTREME_VERTICAL_RATE")
                                ).withColumn("anomaly_severity", when(
                                                                    col("abs_vertical_rate") > EXTREME_VERTICAL_RATE_CRITICAL_THRESHOLD,
                                                                        lit("CRITICAL")
                                                                    )
                                                                .otherwise(lit("HIGH"))
                                ).withColumn("detail", concat(
                                                        lit("Extreme vertical rate detected — "),
                                                        col("abs_vertical_rate").cast("string"),
                                                        lit("m/s")
                                                        )
                                ).drop("abs_vertical_rate")
    
    #unusual speed
    MAX_SPEED_THRESHOLD = 350
    MIN_SPEED_HIGH_ALTITUDE_THRESHOLD = 50
    MIN_HIGH_ALTITUDE = 6000

    unusual_speed_df = df.dropna(subset=["velocity", "baro_altitude"]
                        ).filter(( col("on_ground") == False) & ( ( col("velocity") > MAX_SPEED_THRESHOLD) 
                                                                                            | ( 
                                                                                            (col("baro_altitude") > MIN_HIGH_ALTITUDE) & (col("velocity") < MIN_SPEED_HIGH_ALTITUDE_THRESHOLD))
                                                                                        ) 
                        ).withColumn("anomaly_type", lit("UNUSUAL_SPEED")) \
                        .withColumn("anomaly_severity", lit("MEDIUM"))\
                        .withColumn("detail", 
                                            when(col("velocity") > MAX_SPEED_THRESHOLD, concat( 
                                                                                            lit("Unusual speed detected — velocity "),
                                                                                            col("velocity").cast("string"),
                                                                                            lit(" m/s exceeds maximum threshold")
                                                                                        )
                                            ).otherwise(concat(
                                                        lit("Unusual speed detected — velocity "),
                                                        col("velocity").cast("string"),
                                                        lit(" m/s too low at "),
                                                        col("baro_altitude").cast("string"),
                                                        lit("m altitude")
                                                    )
                                            )
                        )    
    
    #signal gap
    MIN_SIGNAL_GAP_SECONDS = 300 #5min
    MAX_SIGNAL_GAP_SECONDS = 3600 #60min

    signal_gap_df = df.withColumn("prev_api_timestamp",lag("api_timestamp",1).over(window_spec))

    signal_gap_df = signal_gap_df.dropna(subset=["api_timestamp"]).filter(
                                        ( col("api_timestamp") - col("prev_api_timestamp") > MIN_SIGNAL_GAP_SECONDS ) & ( col("api_timestamp") - col("prev_api_timestamp") < MAX_SIGNAL_GAP_SECONDS) & (col("on_ground") == False))\
                                .withColumn("anomaly_type", lit("SIGNAL_GAP")) \
                                .withColumn("anomaly_severity", lit("MEDIUM"))\
                                .withColumn("detail",concat(lit("Signal gap detected — "),
                                                (col("api_timestamp") - col("prev_api_timestamp")).cast("string"),
                                                lit(" since last contact")
                                                )
                                )

    assert squawk_df.count() == 0
    assert altitude_drop_df.count() == 0
    assert vertical_rate_df.count() == 0
    assert unusual_speed_df.count() == 0
    assert signal_gap_df.count() == 0