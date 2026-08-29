from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


spark = SparkSession.builder.appName("Ecommerce-Bronze").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "ecommerce-events"

BRONZE_PATH = "/opt/spark/data/bronze/events"
CHECKPOINT_PATH = "/opt/spark/data/checkpoints/bronze"


raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


bronze = raw_stream.select(
    "key", "value", "topic", "partition", "offset", "timestamp"
).withColumn("ingested_at", current_timestamp())


query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(BRONZE_PATH)
)


print("=" * 60)
print("E-COMMERCE BRONZE STREAM")
print("=" * 60)
print("Kafka: kafka:29092")
print("Topic: ecommerce-events")
print("Bronze: /opt/spark/data/bronze/events")
print("Streaming started...")
print("=" * 60)


query.awaitTermination()
