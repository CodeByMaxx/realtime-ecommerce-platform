from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


spark = SparkSession.builder.appName("Ecommerce-Bronze").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


KAFKA_BOOTSTRAP_SERVERS = "YOUR_KAFKA_BOOTSTRAP_SERVER"
KAFKA_TOPIC = "ecommerce-events"

BRONZE_PATH = "/Volumes/ecommerce/default/bronze/events"


raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


bronze = raw_stream.select(
    "key", "value", "topic", "partition", "offset", "timestamp"
).withColumn("ingested_at", current_timestamp())


query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/ecommerce/default/checkpoints/bronze")
    .start(BRONZE_PATH)
)


query.awaitTermination()
