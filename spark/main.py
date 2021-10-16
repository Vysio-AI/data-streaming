import os
from enum import Enum

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, window, collect_list, rand, round


class KafkaTopic(Enum):
    WATCH = "watch"
    CLASSIFICATIONS = "classifications"


schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("a_x", FloatType()),
    StructField("a_y", FloatType()),
    StructField("a_z", FloatType()),
    StructField("w_x", FloatType()),
    StructField("w_y", FloatType()),
    StructField("w_z", FloatType())
])

kafka_host = os.environ.get("KAFKA_HOST")
kafka_port = os.environ.get("KAFKA_PORT")

spark = SparkSession \
    .builder \
    .appName("Vysio") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:{kafka_port}") \
    .option("subscribe", KafkaTopic.WATCH.value) \
    .load() \
    .selectExpr("CAST(key AS STRING) AS user_id", "CAST(value AS STRING)") \
    .withColumn("json", from_json("value", schema)) \
    .select(col("user_id"), col("json.*")) \
    .withWatermark("timestamp", "10 second") \
    .groupBy(
        window("timestamp", "1 second"),
        "user_id") \
    .agg(
        collect_list("a_x").alias("a_x"),
        collect_list("a_y").alias("a_y"),
        collect_list("a_z").alias("a_z"),
        collect_list("w_x").alias("w_x"),
        collect_list("w_y").alias("w_y"),
        collect_list("w_z").alias("w_z")) \
    .withColumn("classification", round(rand()*6)) \
    .select("user_id", "window", "classification") \
    .selectExpr("user_id AS key", "CAST(to_json(struct(*)) AS STRING) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:{kafka_port}") \
    .option("topic", KafkaTopic.CLASSIFICATIONS.value) \
    .option("checkpointLocation", "./checkpoint") \
    .start() \
    .awaitTermination()
