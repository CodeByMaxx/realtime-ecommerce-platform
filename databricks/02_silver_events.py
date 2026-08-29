from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, trim
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


spark = SparkSession.builder.appName("Ecommerce-Silver").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


BRONZE_PATH = "/Volumes/ecommerce/default/bronze/events"
SILVER_PATH = "/Volumes/ecommerce/default/silver/events"


bronze = spark.readStream.format("delta").load(BRONZE_PATH)


silver = (
    bronze.select(
        from_json(col("value").cast("string"), event_schema).alias("event"),
        col("ingested_at"),
        col("topic"),
        col("partition"),
        col("offset"),
    )
    .select("event.*", "ingested_at", "topic", "partition", "offset")
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("event_type").isNotNull())
    .dropDuplicates(["event_id"])
)


query = (
    silver.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/ecommerce/default/checkpoints/silver")
    .start(SILVER_PATH)
)


query.awaitTermination()
