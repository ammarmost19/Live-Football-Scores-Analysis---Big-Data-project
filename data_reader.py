"""Spark and Kafka streaming setup for the football scores project."""

from pyspark.sql import SparkSession


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "matches"


def spark_session() -> SparkSession:
    """Create the Spark session with the Kafka connector enabled."""
    return (
        SparkSession.builder
        .appName("Live Football Scores Analysis")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Read Kafka values as a Spark Structured Streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # A fresh checkpoint starts at the current end of the existing topic.
        # Start main.py before the producer so the corrected replay is captured.
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )
