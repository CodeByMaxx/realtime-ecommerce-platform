from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    sum,
    count,
    approx_count_distinct,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

import redis


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "ecommerce-events"

REDIS_HOST = "redis"
REDIS_PORT = 6379

CHECKPOINT_LOCATION = "/tmp/checkpoints/ecommerce"


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("EcommerceStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Kafka Event Schema
# ============================================================

event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("event_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)


# ============================================================
# Read from Kafka
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        KAFKA_TOPIC,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .load()
)


# ============================================================
# Kafka Value -> JSON
# ============================================================

json_stream = raw_stream.select(
    col("value").cast("string").alias("json")
)


# ============================================================
# JSON -> Structured Events
# ============================================================

events = (
    json_stream
    .select(
        from_json(
            col("json"),
            event_schema,
        ).alias("event")
    )
    .select("event.*")
    .filter(col("timestamp").isNotNull())
)


# ============================================================
# Calculate Revenue
#
# purchase -> positive revenue
# refund   -> negative revenue
# everything else -> 0
# ============================================================

events_with_revenue = events.withColumn(
    "revenue",
    when(
        col("event_type") == "purchase",
        col("amount"),
    )
    .when(
        col("event_type") == "refund",
        -col("amount"),
    )
    .otherwise(0.0),
)


# ============================================================
# One Minute Window
# ============================================================

metrics = (
    events_with_revenue
    .groupBy(
        window(
            col("timestamp"),
            "1 minute",
        )
    )
    .agg(
        sum("revenue").alias("revenue"),

        count(
            when(
                col("event_type") == "purchase",
                True,
            )
        ).alias("orders"),

        count("*").alias("events"),

        # countDistinct() is not supported by Spark
        # Structured Streaming.
        #
        # approx_count_distinct() is supported.
        approx_count_distinct(
            "user_id"
        ).alias("unique_users"),
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


# ============================================================
# Redis Writer
# ============================================================

def write_to_redis(batch_df, batch_id):

    print()
    print("=" * 60)
    print(f"Spark Batch: {batch_id}")
    print("=" * 60)

    batch_df.orderBy("window_start").show(
        20,
        truncate=False,
    )

    rows = batch_df.collect()

    if not rows:
        print("No rows in batch.")
        return

    client = None

    try:

        # ----------------------------------------------------
        # Connect to Redis
        # ----------------------------------------------------

        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        client.ping()

        print("Redis connection: OK")


        # ----------------------------------------------------
        # Current metrics
        #
        # We use the newest window from the batch.
        # ----------------------------------------------------

        latest_row = max(
            rows,
            key=lambda row: row["window_start"]
            if row["window_start"] is not None
            else "",
        )

        latest_revenue = float(
            latest_row["revenue"] or 0.0
        )

        latest_orders = int(
            latest_row["orders"] or 0
        )

        latest_events = int(
            latest_row["events"] or 0
        )

        latest_users = int(
            latest_row["unique_users"] or 0
        )


        # ----------------------------------------------------
        # Store current metrics
        # ----------------------------------------------------

        metrics_data = {
            "revenue": latest_revenue,
            "orders": latest_orders,
            "events": latest_events,
            "unique_users": latest_users,

            "window_start": (
                latest_row["window_start"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if latest_row["window_start"]
                else ""
            ),

            "window_end": (
                latest_row["window_end"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if latest_row["window_end"]
                else ""
            ),
        }

        client.hset(
            "ecommerce:metrics",
            mapping=metrics_data,
        )


        # ----------------------------------------------------
        # Store every window in history
        # ----------------------------------------------------

        for row in rows:

            if row["window_start"] is None:
                continue

            window_start = row["window_start"]
            window_end = row["window_end"]

            start_key = window_start.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            history_key = (
                f"ecommerce:history:{start_key}"
            )

            history_data = {
                "window_start": start_key,

                "window_end": (
                    window_end.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    if window_end
                    else ""
                ),

                "revenue": float(
                    row["revenue"] or 0.0
                ),

                "orders": int(
                    row["orders"] or 0
                ),

                "events": int(
                    row["events"] or 0
                ),

                "unique_users": int(
                    row["unique_users"] or 0
                ),
            }

            client.hset(
                history_key,
                mapping=history_data,
            )

            # Keep history for two hours.
            client.expire(
                history_key,
                7200,
            )

            print(
                f"Redis history updated: {history_key}"
            )


    except Exception as e:

        print(
            f"Redis error: {e}"
        )


    finally:

        if client is not None:
            try:
                client.close()
            except Exception:
                pass


# ============================================================
# Start Streaming Query
# ============================================================

query = (
    metrics.writeStream
    .outputMode("complete")
    .foreachBatch(write_to_redis)
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .start()
)


# ============================================================
# Keep Application Running
# ============================================================

query.awaitTermination()
