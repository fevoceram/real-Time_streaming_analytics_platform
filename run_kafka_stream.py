import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql import SparkSession
from src.spark_session import get_spark_session

# Define schema of Kafka messages
schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("payload", StringType())

spark = get_spark_session()
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

df_stream_parsed = df_stream.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

from src.streaming_pipeline import run_spark_streaming
run_spark_streaming(df_stream_parsed)
