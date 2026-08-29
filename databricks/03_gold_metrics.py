from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, approx_count_distinct, window


spark = SparkSession.builder.appName("Ecommerce-Gold").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


SILVER_PATH = "/Volumes/ecommerce/default/silver/events"
GOLD_PATH = "/Volumes/ecommerce/default/gold/metrics"


events = spark.readStream.format("delta").load(SILVER_PATH)


events_with_revenue = events.withColumn(
    "revenue",
    when(col("event_type") == "purchase", col("amount"))
    .when(col("event_type") == "refund", -col("amount"))
    .otherwise(0.0),
)


metrics = (
    events_with_revenue.groupBy(window(col("timestamp"), "1 minute"))
    .agg(
        sum("revenue").alias("revenue"),
        count(when(col("event_type") == "purchase", True)).alias("orders"),
        count("*").alias("events"),
        approx_count_distinct("user_id").alias("unique_users"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("revenue"),
        col("orders"),
        col("events"),
        col("unique_users"),
    )
)


query = (
    metrics.writeStream.format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "/Volumes/ecommerce/default/checkpoints/gold")
    .start(GOLD_PATH)
)


query.awaitTermination()
