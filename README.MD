# Real-Time E-Commerce Data Platform

A real-time data engineering project that demonstrates how e-commerce events can be generated, streamed through Kafka, processed with Apache Spark, stored in Redis, visualized through a dashboard, and prepared for analytics in Databricks.

The project combines **real-time streaming**, **event processing**, **NoSQL storage**, **REST APIs**, **dashboard visualization**, and **Databricks/Data Lake analytics** into one end-to-end data platform.

![Architecture Overview](docs/dashboard.png)

---

## 1. Project Overview

The platform simulates an e-commerce environment where user activity is continuously generated as events.

Typical events include:

* `view_product`
* `add_to_cart`
* `purchase`
* `refund`

Each event contains information such as:

* Event ID
* User ID
* Product ID
* Event type
* Amount
* Timestamp

The events are sent to **Apache Kafka**, processed by **Apache Spark Structured Streaming**, aggregated into one-minute windows, and stored in **Redis**.

A web application exposes the processed metrics through an API and displays them in a dashboard.

In addition, the project is designed to integrate with **Databricks** for historical analytics and Data Lake processing.

---

# 2. Architecture

The current architecture follows this pipeline:

```text
                    ┌─────────────────────┐
                    │   E-Commerce        │
                    │   Event Producer    │
                    └──────────┬──────────┘
                               │
                               │ JSON Events
                               ▼
                    ┌─────────────────────┐
                    │      Apache Kafka   │
                    │                     │
                    │ ecommerce-events    │
                    └──────────┬──────────┘
                               │
                               │ Streaming
                               ▼
                    ┌─────────────────────┐
                    │ Apache Spark        │
                    │ Structured          │
                    │ Streaming           │
                    └──────────┬──────────┘
                               │
                     Aggregation│
                     1-minute   │
                     windows    │
                               ▼
                    ┌─────────────────────┐
                    │       Redis         │
                    │                     │
                    │ ecommerce:metrics   │
                    └──────────┬──────────┘
                               │
                               │ REST API
                               ▼
                    ┌─────────────────────┐
                    │      Flask API      │
                    │      app.py         │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Dashboard      │
                    │                     │
                    │ Real-Time Metrics   │
                    └─────────────────────┘


                         Historical Data
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Data Lake      │
                    │                     │
                    │ Raw / Bronze        │
                    │ Silver              │
                    │ Gold                │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     Databricks      │
                    │                     │
                    │ Analytics / SQL     │
                    │ Delta Lake          │
                    │ Notebooks           │
                    └─────────────────────┘
```

The real-time and historical parts are intentionally separated:

### Real-Time Layer

```text
Producer
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Redis
   ↓
Flask API
   ↓
Dashboard
```

### Historical / Analytics Layer

```text
Streaming / Raw Events
        ↓
     Data Lake
        ↓
   Delta Lake
        ↓
   Databricks
        ↓
 Analytics / BI
```

---

# 3. Technology Stack

| Component             | Technology                 | Purpose                                  |
| --------------------- | -------------------------- | ---------------------------------------- |
| Event Generator       | Python                     | Generates synthetic e-commerce events    |
| Message Broker        | Apache Kafka               | Real-time event streaming                |
| Stream Processing     | Apache Spark 3.5.7         | Processes and aggregates events          |
| Streaming API         | Spark Structured Streaming | Continuous event processing              |
| Cache / Metrics Store | Redis                      | Stores current metrics                   |
| Backend API           | Flask                      | Provides metrics to the dashboard        |
| Frontend              | HTML / CSS / JavaScript    | Displays real-time metrics               |
| Containerization      | Docker                     | Reproducible local environment           |
| Orchestration         | Docker Compose             | Starts the platform                      |
| Data Lake             | Data Lake / Delta Lake     | Historical event storage                 |
| Analytics             | Databricks                 | SQL, notebooks and large-scale analytics |

---

# 4. Repository Structure

A typical project structure looks like this:

```text
realtime-ecommerce-platform/
│
├── producer/
│   ├── producer.py
│   └── requirements.txt
│
├── spark/
│   ├── streaming.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── dashboard/
│   ├── app.py
│   ├── templates/
│   │   └── index.html
│   └── static/
│       ├── style.css
│       └── dashboard.js
│
├── docs/
│   └── dashboard.png
│
├── databricks/
│   ├── notebooks/
│   └── ...
│
├── docker-compose.yml
├── README.md
└── requirements.txt
```

The exact directory structure can differ depending on the current implementation.

---

# 5. Event Model

Events are represented as JSON messages.

Example:

```json
{
  "event_id": "b5463552-84ed-4532-923d-7c0422ec1c69",
  "user_id": 425,
  "product_id": 3,
  "event_type": "add_to_cart",
  "amount": 0.0,
  "timestamp": "2026-08-29T13:30:18.924514"
}
```

A purchase event can look like:

```json
{
  "event_id": "3614ce48-78d6-4d60-b6c7-13f37be17add",
  "user_id": 243,
  "product_id": 5,
  "event_type": "purchase",
  "amount": 299.99,
  "timestamp": "2026-08-29T13:30:52.779863"
}
```

A refund event:

```json
{
  "event_id": "279e89d5-3d30-4836-9011-8a16844c1ef3",
  "user_id": 382,
  "product_id": 2,
  "event_type": "refund",
  "amount": 149.99,
  "timestamp": "2026-08-29T13:30:50.133488"
}
```

---

# 6. Kafka

Kafka is used as the central event streaming platform.

The main topic is:

```text
ecommerce-events
```

The current Kafka topic uses three partitions.

Example:

```text
Topic: ecommerce-events
PartitionCount: 3
ReplicationFactor: 1
```

Kafka decouples event generation from event processing.

The producer does not need to know how Spark processes the events.

Likewise, Spark does not need to know how the events were generated.

---

# 7. Producer

The Python producer continuously generates synthetic e-commerce activity.

Example event types:

```text
view_product
add_to_cart
purchase
refund
```

The producer sends events to Kafka.

Example:

```bash
python3 producer.py
```

If Kafka's Python dependency is missing:

```bash
pip install kafka-python
```

or install the project's requirements:

```bash
pip install -r requirements.txt
```

The producer connects to the Kafka broker exposed by the local environment.

---

# 8. Spark Structured Streaming

Apache Spark consumes events from Kafka.

The Spark application:

1. Reads the Kafka topic
2. Converts Kafka values into JSON
3. Parses the JSON into a Spark schema
4. Calculates revenue
5. Groups events into one-minute windows
6. Calculates streaming metrics
7. Writes the results to Redis

The event schema is:

```text
event_id       STRING
user_id        INTEGER
product_id     INTEGER
event_type     STRING
amount         DOUBLE
timestamp      TIMESTAMP
```

---

# 9. Revenue Calculation

Revenue is calculated according to the event type.

For purchases:

```text
revenue = amount
```

For refunds:

```text
revenue = -amount
```

For other events:

```text
revenue = 0
```

Conceptually:

```text
purchase  → +amount
refund    → -amount
view      → 0
cart      → 0
```

This allows the streaming pipeline to calculate net revenue.

---

# 10. One-Minute Window Aggregation

Spark groups events into one-minute windows.

The resulting metrics are:

```text
window_start
window_end
revenue
orders
events
unique_users
```

Example:

```text
window_start       2026-08-29 14:05:00
window_end         2026-08-29 14:06:00
revenue            -459.93
orders             4
events             37
unique_users       38
```

The `unique_users` metric uses Spark's streaming-compatible approximate distinct aggregation.

This is important because regular:

```python
countDistinct("user_id")
```

is not supported in this streaming aggregation setup.

The implementation therefore uses:

```python
approx_count_distinct("user_id")
```

---

# 11. Redis

Redis stores the current metrics.

The main Redis key is:

```text
ecommerce:metrics
```

The stored fields are:

```text
revenue
orders
events
unique_users
window_start
window_end
```

Example:

```text
revenue
-459.93000000000006

orders
4

events
37

unique_users
38

window_start
2026-08-29 14:05:00

window_end
2026-08-29 14:06:00
```

Redis acts as the low-latency serving layer for the dashboard.

---

# 12. Redis Debugging

Check the current metrics:

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

Expected output:

```text
1) "revenue"
2) "-459.93"
3) "orders"
4) "4"
5) "events"
6) "37"
7) "unique_users"
8) "38"
```

If Redis is empty, check whether Spark is successfully writing metrics.

---

# 13. Docker Networking

The services communicate using the Docker Compose network.

For example:

```text
Kafka:
kafka:29092

Redis:
redis:6379
```

The Spark container must be connected to the same Docker network as Redis and Kafka.

Example network:

```text
realtime-ecommerce-platform_default
```

To inspect the network:

```bash
sudo docker network inspect realtime-ecommerce-platform_default
```

A healthy setup should show Kafka and Spark on the same network.

---

# 14. Flask API

The Flask application acts as the backend for the dashboard.

Its responsibilities are:

* Read current metrics from Redis
* Provide metrics through HTTP
* Serve the dashboard frontend
* Provide historical data if configured

The API is exposed locally on:

```text
http://localhost:5000
```

The exact API endpoints depend on the current `app.py` implementation.

For example, a metrics endpoint can return:

```json
{
  "revenue": -459.93,
  "orders": 4,
  "events": 37,
  "unique_users": 38,
  "window_start": "2026-08-29 14:05:00",
  "window_end": "2026-08-29 14:06:00"
}
```

---

# 15. Dashboard

The dashboard provides a visual representation of the streaming metrics.

Typical metrics include:

* Current revenue
* Number of orders
* Number of events
* Unique users
* Current processing window

The dashboard communicates with the Flask backend rather than connecting directly to Redis.

Architecture:

```text
Browser
   ↓
Flask API
   ↓
Redis
   ↑
Spark
```

This keeps Redis private inside the Docker environment.

---

# 16. Data Lake

The next layer of the project is the historical Data Lake.

Redis is intentionally not used as the long-term storage layer.

Redis is optimized for fast access to current state and metrics.

Historical event data should instead be stored in a durable Data Lake.

A typical architecture is:

```text
Kafka
  ↓
Raw Event Storage
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Databricks
```

---

# 17. Medallion Architecture

The Data Lake follows the Medallion Architecture.

## Bronze

The Bronze layer contains raw events.

Example:

```text
event_id
user_id
product_id
event_type
amount
timestamp
```

The goal is to preserve the original event data with minimal transformation.

---

## Silver

The Silver layer contains cleaned and normalized data.

Typical transformations:

* Schema validation
* Type conversion
* Timestamp normalization
* Duplicate handling
* Data quality checks
* Invalid event filtering

Example:

```text
event_id
user_id
product_id
event_type
amount
event_timestamp
processing_timestamp
```

---

## Gold

The Gold layer contains analytics-ready datasets.

Examples:

```text
daily_revenue
daily_orders
product_sales
customer_activity
conversion_metrics
refund_metrics
```

These datasets are optimized for analytics and reporting.

---

# 18. Databricks

Databricks is used as the analytics and Data Lake processing environment.

The Databricks layer can be used for:

* Delta Lake
* SQL analytics
* Data transformations
* Data quality
* Notebooks
* Historical analysis
* Aggregations
* Business intelligence
* Machine learning preparation

A typical flow is:

```text
Kafka / Raw Data
       ↓
   Data Lake
       ↓
  Delta Tables
       ↓
   Databricks
       ↓
   Analytics
```

Databricks should be treated as the historical analytics layer rather than the low-latency Redis serving layer.

---

# 19. Delta Lake

Delta Lake provides reliable storage on top of the Data Lake.

Typical tables could include:

```text
bronze_ecommerce_events
silver_ecommerce_events
gold_daily_metrics
gold_product_metrics
gold_customer_metrics
```

Delta Lake provides features such as:

* ACID transactions
* Schema enforcement
* Schema evolution
* Time travel
* Reliable batch processing
* Reliable streaming ingestion

---

# 20. Real-Time vs Historical Architecture

The project intentionally demonstrates two different workloads.

## Real-Time

Used for:

```text
Current dashboard metrics
Low-latency monitoring
Live revenue
Live order counts
Current event volume
```

Technology:

```text
Kafka
Spark
Redis
Flask
Dashboard
```

## Historical

Used for:

```text
Daily analysis
Product performance
Customer behavior
Long-term revenue
Trend analysis
Business intelligence
```

Technology:

```text
Data Lake
Delta Lake
Databricks
SQL
```

---

# 21. Starting the Platform

Build and start the Docker environment:

```bash
sudo docker compose up -d --build
```

Check the running services:

```bash
sudo docker compose ps
```

Expected services include:

```text
ecommerce-kafka
ecommerce-spark
ecommerce-redis
ecommerce-dashboard
```

The exact names depend on the Docker Compose configuration.

---

# 22. Checking Logs

Spark:

```bash
sudo docker compose logs --tail=50 spark
```

Kafka:

```bash
sudo docker compose logs --tail=50 kafka
```

Redis:

```bash
sudo docker compose logs --tail=50 redis
```

Dashboard:

```bash
sudo docker compose logs --tail=50 dashboard
```

Follow Spark logs live:

```bash
sudo docker compose logs -f spark
```

---

# 23. Checking Kafka

List Kafka topics:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Describe the e-commerce topic:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

Expected:

```text
Topic: ecommerce-events
PartitionCount: 3
ReplicationFactor: 1
```

---

# 24. Debugging the Complete Pipeline

A useful debugging order is:

```text
1. Docker
2. Kafka
3. Producer
4. Spark
5. Redis
6. Flask
7. Dashboard
```

---

## Step 1 — Check Docker

```bash
sudo docker compose ps
```

All required services should be running.

---

## Step 2 — Check Kafka

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

The topic must exist.

---

## Step 3 — Start the Producer

```bash
python3 producer.py
```

The producer should continuously generate events.

---

## Step 4 — Check Spark

```bash
sudo docker compose logs -f spark
```

You should see batches similar to:

```text
Spark Batch: 80

window_start       window_end
2026-08-29 13:50   2026-08-29 13:51

revenue
4030.06

orders
23

events
88

unique_users
85
```

---

## Step 5 — Check Redis

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

Redis should contain the latest metrics.

---

## Step 6 — Check Flask

Open:

```text
http://localhost:5000
```

The dashboard should load.

---

# 25. Common Problems

## Spark: `countDistinct` not supported

Error:

```text
Distinct aggregations are not supported on streaming DataFrames/Datasets.
```

Solution:

Use:

```python
approx_count_distinct("user_id")
```

instead of:

```python
countDistinct("user_id")
```

---

## Redis: Temporary failure in name resolution

Error:

```text
Error -3 connecting to redis:6379.
Temporary failure in name resolution.
```

This usually means Spark cannot resolve the Redis Docker service.

Check:

```bash
sudo docker network inspect realtime-ecommerce-platform_default
```

Spark and Redis must be attached to the same Docker network.

Test from Spark:

```bash
sudo docker exec ecommerce-spark \
  python3 -c "import socket; print(socket.gethostbyname('redis')); print(socket.create_connection(('redis',6379,5)))"
```

A successful result should look similar to:

```text
172.19.0.x
<socket.socket ...>
```

---

## Kafka: `UnknownTopicOrPartitionException`

Error:

```text
UnknownTopicOrPartitionException:
This server does not host this topic-partition.
```

Check that the topic exists:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

The topic should have valid leaders and ISR entries.

---

## Producer: `ModuleNotFoundError: kafka`

Error:

```text
ModuleNotFoundError: No module named 'kafka'
```

Install the Python Kafka client:

```bash
pip install kafka-python
```

or:

```bash
pip install -r requirements.txt
```

---

## Docker: `/opt/venv/bin/pip: not found`

Error:

```text
/opt/venv/bin/pip: not found
```

The Docker image does not contain the expected virtual environment.

The Dockerfile must either create `/opt/venv` or use the Python/pip installation actually provided by the base image.

For example, depending on the base image:

```dockerfile
RUN pip install --no-cache-dir --target=/tmp/python-packages -r requirements.txt
```

instead of assuming:

```text
/opt/venv/bin/pip
```

---

# 26. Important Spark Streaming Considerations

The streaming application uses a checkpoint directory:

```text
/tmp/checkpoints/ecommerce
```

Checkpoints allow Spark to maintain streaming state and offsets.

For production deployments, the checkpoint directory should use persistent storage rather than a temporary container filesystem.

For example:

```text
/data/checkpoints/ecommerce
```

or cloud/object storage:

```text
s3://...
abfss://...
gs://...
```

depending on the deployment environment.

---

# 27. Production Improvements

The current project is primarily designed as a learning and portfolio project.

For production, several improvements would be appropriate.

### Kafka

* Increase replication factor
* Configure retention
* Tune partitions
* Configure monitoring
* Use secure authentication
* Use TLS

### Spark

* Persistent checkpoints
* Better watermarking
* Proper state management
* Structured logging
* Resource configuration
* Monitoring

### Redis

* Persistent storage
* Authentication
* High availability
* Redis Sentinel or Cluster
* Memory policies

### Flask

* Production WSGI server
* Authentication
* Rate limiting
* API versioning
* Structured logging

### Dashboard

* Authentication
* Error handling
* WebSocket/SSE updates
* Historical charts
* Alerting

### Data Lake

* Object storage
* Delta Lake
* Data quality checks
* Partitioning
* Schema evolution
* Governance

### Databricks

* Workflows
* Unity Catalog
* Delta Live Tables / Lakeflow where appropriate
* Data quality monitoring
* SQL Warehouses
* Job orchestration

---

# 28. Example End-to-End Flow

A complete event travels through the system like this:

```text
User performs a purchase
        ↓
Python Producer
        ↓
Kafka
        ↓
ecommerce-events
        ↓
Spark Structured Streaming
        ↓
JSON parsing
        ↓
Revenue calculation
        ↓
1-minute aggregation
        ↓
Redis
        ↓
Flask API
        ↓
Dashboard
```

For historical processing:

```text
Kafka / Raw Events
        ↓
Data Lake
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
Delta Lake
        ↓
Databricks
        ↓
SQL / Analytics / BI
```

---

# 29. Example Business Metrics

The platform can support metrics such as:

### Revenue

```text
Purchases - Refunds
```

### Orders

Number of purchase events.

### Events

Total number of events.

### Unique Users

Approximate number of unique users within the aggregation window.

### Conversion Rate

A future Gold-layer metric could be:

```text
purchases / product_views
```

### Refund Rate

```text
refunds / purchases
```

### Average Order Value

```text
revenue / orders
```

These metrics can later be calculated in Databricks from the historical event data.

---

# 30. Future Extensions

Possible extensions include:

* Persistent Data Lake ingestion
* Delta Lake Bronze/Silver/Gold tables
* Databricks notebooks
* Databricks Workflows
* Product-level analytics
* Customer-level analytics
* Conversion funnels
* Revenue forecasting
* Fraud detection
* Real-time alerts
* Prometheus/Grafana monitoring
* Kubernetes deployment
* Cloud deployment
* CI/CD pipeline
* Automated data quality tests

---

# 31. Project Goals

The project demonstrates practical knowledge of modern data engineering concepts:

* Event-driven architecture
* Real-time streaming
* Kafka
* Spark Structured Streaming
* Stream aggregations
* Redis
* REST APIs
* Docker
* Docker Compose
* Data Lake architecture
* Medallion architecture
* Delta Lake
* Databricks
* SQL analytics
* Data quality
* Production-oriented debugging

The goal is not simply to build a dashboard, but to demonstrate an **end-to-end data platform** from event generation to real-time processing and historical analytics.

---

# 32. Current Status

The current platform successfully demonstrates the following pipeline:

```text
Python Producer
      ↓
Apache Kafka
      ↓
Spark Structured Streaming
      ↓
1-Minute Metrics
      ↓
Redis
      ↓
Flask API
      ↓
Dashboard
```

The Databricks/Data Lake layer extends the architecture toward historical and analytical workloads:

```text
Streaming Events
      ↓
Data Lake
      ↓
Delta Lake
      ↓
Databricks
      ↓
Historical Analytics
```

---

# 33. Conclusion

This project provides a complete foundation for a modern real-time e-commerce data platform.

It combines a low-latency streaming architecture with a scalable historical analytics architecture.

The real-time path provides immediate business metrics, while the Data Lake and Databricks layer provides the foundation for long-term analysis, reporting, and advanced analytics.

The resulting architecture is:

```text
                 REAL-TIME
                     │
                     ▼
Producer → Kafka → Spark → Redis → Flask → Dashboard
                     │
                     │
                     ▼
                 DATA LAKE
                     │
                     ▼
                Delta Lake
                     │
                     ▼
                 Databricks
                     │
                     ▼
              HISTORICAL ANALYTICS
```

This makes the project suitable as a practical demonstration of **real-time data engineering, streaming analytics, Data Lake architecture, and Databricks-based analytics**.

