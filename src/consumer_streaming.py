import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import time

# Kafka config
TOPIC = "events"
BOOTSTRAP_SERVERS = "localhost:9092"

scala_version = '2.12'
spark_version = '3.5.3'

packages =[
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0'
]

# Spark session
spark = SparkSession.builder.appName("KafkaStreamEvents").config("spark.jars.packages", ",".join(packages)).getOrCreate()

# schema for event data
# Schema for the event data
# Schema for the event data
schema = StructType() \
    .add("customer_id", StringType()) \
    .add("product_id", StringType()) \
    .add("event_name", StringType()) \
    .add("event_time", TimestampType()) \
    .add("quantity", DoubleType())

# Read data from Kafka
raw_stream = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
    .option("subscribe", TOPIC)\
    .option("startingOffsets", "earliest")\
    .load()

# Parse JSON message
events_stream = raw_stream.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), schema).alias("data"))\
    .select("data.*")

# Add implicit feedback (preference)
events_with_feedback = events_stream.withColumn(
    "preference",
    when(col("event_name") == "ADD_TO_CART", 1)
    .when(col("event_name") == "BOOKING", 3)
    .otherwise(0)
)

# Calculate implicit feedback score for each (user, item)
implicit_feedback = events_with_feedback.groupBy("customer_id", "product_id")\
    .agg(sum("preference").alias("score"))

# Show results in console (or write to a file)
query = implicit_feedback.writeStream\
    .outputMode("update")\
    .format("console")\
    .option("truncate", "false")\
    .start()

query.awaitTermination()
