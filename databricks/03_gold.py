import json
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window,
    sum,
    count,
    approx_count_distinct,
    col,
    when
)

SILVER_PATH = "/opt/spark/data/silver/events"
GOLD_PATH = "/opt/spark/data/gold/metrics"
CHECKPOINT_PATH = "/opt/spark/data/checkpoints/gold"

REDIS_HOST = "redis"
REDIS_PORT = 6379

REDIS_METRICS_KEY = "ecommerce:metrics"
REDIS_HISTORY_KEY = "ecommerce:history"


spark = (
    SparkSession.builder
    .appName("Ecommerce-Gold")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

    redis_client.ping()
    print("Redis connection: OK")

except Exception as e:
    redis_client = None
    print(f"Redis connection failed: {e}")


silver = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)


gold = (
    silver
    .withWatermark(
        "event_timestamp",
        "2 minutes"
    )
    .groupBy(
        window(
            col("event_timestamp"),
            "1 minute"
        )
    )
    .agg(
        sum("revenue").alias("revenue"),

        count(
            when(
                col("event_type") == "purchase",
                True
            )
        ).alias("orders"),

        count("*").alias("events"),

        approx_count_distinct(
            "user_id"
        ).alias("unique_users")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("revenue"),
        col("orders"),
        col("events"),
        col("unique_users")
    )
)


def write_gold(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Gold Batch {batch_id}: no data")
        return

    print()
    print("=" * 60)
    print(f"Gold Batch: {batch_id}")
    print("=" * 60)

    batch_df.orderBy("window_start").show(
        truncate=False
    )

    (
        batch_df
        .write
        .format("delta")
        .mode("append")
        .save(GOLD_PATH)
    )

    print(f"Gold Delta written: {GOLD_PATH}")


    if redis_client is None:
        return

    try:

        rows = (
            batch_df
            .orderBy(
                col("window_end").desc()
            )
            .limit(1)
            .collect()
        )

        if not rows:
            return

        row = rows[0]

        metrics = {
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "revenue": float(row["revenue"] or 0),
            "orders": int(row["orders"] or 0),
            "events": int(row["events"] or 0),
            "unique_users": int(row["unique_users"] or 0)
        }

        redis_client.hset(
            REDIS_METRICS_KEY,
            mapping={
                key: str(value)
                for key, value in metrics.items()
            }
        )

        redis_client.lpush(
            REDIS_HISTORY_KEY,
            json.dumps(metrics)
        )

        redis_client.ltrim(
            REDIS_HISTORY_KEY,
            0,
            99
        )

        print("Redis metrics updated:")
        print(json.dumps(metrics, indent=2))

    except Exception as e:
        print(f"Redis error: {e}")


query = (
    gold
    .writeStream
    .foreachBatch(write_gold)
    .outputMode("update")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .start()
)


print()
print("=" * 60)
print("E-COMMERCE GOLD STREAM")
print("=" * 60)
print(f"Silver: {SILVER_PATH}")
print(f"Gold:   {GOLD_PATH}")
print(f"Redis:  {REDIS_HOST}:{REDIS_PORT}")
print(f"Metrics: {REDIS_METRICS_KEY}")
print(f"History: {REDIS_HISTORY_KEY}")
print("Streaming started...")
print("=" * 60)


query.awaitTermination()
