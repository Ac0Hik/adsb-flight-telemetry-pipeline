from pyspark.sql import SparkSession 
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, BooleanType, 
    LongType, ArrayType
)
from pyspark.sql.functions import col, to_date, hour
from spark.utils.opensky_client import OpenSkyClient
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


spark = (
    SparkSession.builder
    .appName("stream_ingest")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.driver.memory", "2g") #for local runs
    .getOrCreate()
)

opensky_schema = StructType([
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
    StructField("ingested_at", StringType(), True)
])

def process_batch(batch_df,batch_id, ):

    active_spark = batch_df.sparkSession

    raw_states = client.fetch_states()
    parsed_data = client.parse_states(data=raw_states)

    if not parsed_data:
        logging.info(f"Batch {batch_id}: No records found. Skipping.")
        return

    df = active_spark.createDataFrame(parsed_data, schema=opensky_schema) \
        .withColumn("ingest_date", to_date(col("ingested_at"))) \
        .withColumn("ingest_hour", hour(col("ingested_at")))
    
    record_count = df.count()

    df.write.format("delta").mode("append").partitionBy("ingest_date", "ingest_hour") \
        .save(f"{DELTA_BASE_PATH}/bronze/live_states")
    
    logging.info(f"Batch {batch_id} processed: {record_count} records ingested.")
    
# job's main logic 
OPENSKY_USER = os.getenv("OPENSKY_USER")
OPENSKY_PWD = os.getenv("OPENSKY_PASS")
DELTA_BASE_PATH = os.getenv("DELTA_BASE_PATH", "data/delta")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH", "data/checkpoints")

client = OpenSkyClient(username=OPENSKY_USER, password=OPENSKY_PWD)

streaming_tick = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1) 
    .load()
)

query = (
    streaming_tick.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime='60 seconds')
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/bronze_ingest")
    .start()
)

query.awaitTermination()