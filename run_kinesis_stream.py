from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType
import boto3
from src.spark_session import get_spark_session
from src.streaming_pipeline import run_spark_streaming

# Define schema
schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("payload", StringType())

# Connect to Kinesis stream via Spark
spark = get_spark_session()
df_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", "event-stream") \
    .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com") \
    .option("startingposition", "TRIM_HORIZON") \
    .load()

from pyspark.sql.functions import from_json, col
df_stream_parsed = df_stream.select(from_json(col("data").cast("string"), schema).alias("data")).select("data.*")

run_spark_streaming(df_stream_parsed)
