
## Streaming Ingestion & Initial Processing - Apache Sparck Streaming
## X.com feeds data ingestion example
## Scala/PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("SocialMediaStreamProcessor") \
    .getOrCreate()

# Define schema for incoming Twitter data (simplified)
tweet_schema = StructType([
    StructField("created_at", StringType()),
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("user_name", StringType()),
    StructField("lang", StringType())
])

# Read from Kafka topic
raw_tweets_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker1:9092,kafka_broker2:9092") \
    .option("subscribe", "raw_social_media_tweets") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_data")

# Parse JSON and select relevant fields
parsed_tweets_df = raw_tweets_df \
    .withColumn("data", from_json(col("json_data"), tweet_schema)) \
    .select(
        col("data.created_at").alias("tweet_timestamp_str"),
        col("data.id").alias("tweet_id"),
        col("data.text").alias("tweet_text"),
        col("data.user_name").alias("user_name"),
        col("data.lang").alias("language")
    )

# Convert timestamp string to actual timestamp and add date partitions
processed_tweets_df = parsed_tweets_df \
    .withColumn("tweet_timestamp", date_format(col("tweet_timestamp_str"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("tweet_date", date_format(col("tweet_timestamp"), "yyyy-MM-dd"))

# Write to Parquet in HDFS, partitioned by date
query = processed_tweets_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/user/telco/refined/social_media_tweets/") \
    .option("checkpointLocation", "/user/telco/checkpoints/social_media_tweets/") \
    .partitionBy("tweet_date") \
    .start()

query.awaitTermination()
