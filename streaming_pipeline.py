from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from src.spark_session import get_spark_session

def run_spark_streaming(df_stream, checkpoint_path="data/checkpoints/"):
    # Example transformation: select fields and add timestamp
    df_transformed = df_stream.withColumn("event_time", col("timestamp"))

    query = df_transformed.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    query.awaitTermination()
