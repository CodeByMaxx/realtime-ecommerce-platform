from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ============================================================
# SILVER LAYER
# ============================================================

spark = (
    SparkSession.builder.appName("Ecommerce-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

BRONZE_PATH = "/opt/spark/data/bronze/events"
SILVER_PATH = "/opt/spark/data/silver/events"
CHECKPOINT_PATH = "/opt/spark/data/checkpoints/silver"


# ------------------------------------------------------------
# Schema
# ------------------------------------------------------------

event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


# ------------------------------------------------------------
# Read Bronze
# ------------------------------------------------------------

bronze = spark.readStream.format("delta").load(BRONZE_PATH)


# ------------------------------------------------------------
# Parse JSON
# ------------------------------------------------------------

silver = bronze.select(
    from_json(col("value").cast("string"), event_schema).alias("event")
).select("event.*")


# ------------------------------------------------------------
# Clean + transform
# ------------------------------------------------------------

silver = (
    silver.withColumn("event_id", trim(col("event_id")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    .withColumn(
        "revenue", when(col("event_type") == "purchase", col("amount")).otherwise(0.0)
    )
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isNotNull())
    .drop("timestamp")
)


# ------------------------------------------------------------
# Write Silver
# ------------------------------------------------------------

query = (
    silver.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
)


print()
print("=" * 60)
print("E-COMMERCE SILVER STREAM")
print("=" * 60)
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")
print("Streaming started...")
print("=" * 60)


query.awaitTermination()
