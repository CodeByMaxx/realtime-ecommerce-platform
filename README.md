# Real-Time E-Commerce Data Platform

A real-time data engineering platform that simulates e-commerce activity, processes events with **Apache Kafka** and **Spark Structured Streaming**, stores current metrics in **Redis**, and visualizes them through a web dashboard.

The project also contains a **Data Lake / Databricks** layer for historical analytics and demonstrates how real-time and batch-oriented workloads can be combined in one data platform.

## Architecture

```text
                         E-Commerce Events
                                │
                                ▼
                       Python Event Producer
                                │
                                ▼
                         Apache Kafka
                       ecommerce-events
                                │
                                ▼
                  Spark Structured Streaming
                                │
                     1-minute aggregation
                                │
                                ▼
                              Redis
                                │
                                ▼
                           Flask API
                                │
                                ▼
                           Dashboard
```

Historical analytics follow a separate path:

```text
                         Kafka / Raw Events
                                │
                                ▼
                            Data Lake
                                │
                                ▼
                             Bronze
                                │
                                ▼
                             Silver
                                │
                                ▼
                              Gold
                                │
                                ▼
                           Delta Lake
                                │
                                ▼
                           Databricks
                                │
                                ▼
                        Historical Analytics
```

The real-time layer is designed for low-latency metrics, while the Data Lake and Databricks layer is intended for historical analysis.

## Features

* Synthetic e-commerce event generation
* Apache Kafka event streaming
* Spark Structured Streaming
* One-minute streaming windows
* Real-time revenue calculation
* Purchase and refund handling
* Approximate unique-user calculation
* Redis-based metric storage
* Flask REST API
* Web dashboard
* Docker Compose development environment
* Data Lake architecture
* Bronze / Silver / Gold processing concept
* Delta Lake
* Databricks analytics
* Real-time and historical workload separation

## Technology Stack

| Component          | Technology                        |
| ------------------ | --------------------------------- |
| Event Producer     | Python                            |
| Message Broker     | Apache Kafka                      |
| Stream Processing  | Apache Spark Structured Streaming |
| Metrics Store      | Redis                             |
| Backend API        | Flask                             |
| Frontend           | HTML / CSS / JavaScript           |
| Containerization   | Docker                            |
| Orchestration      | Docker Compose                    |
| Historical Storage | Data Lake / Delta Lake            |
| Analytics          | Databricks                        |

The current README documents Spark 3.5.7 as the Spark version used by the project.

## Repository Structure

```text
realtime-ecommerce-platform/
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
├── databricks/
│   └── ...
│
├── docs/
│   └── dashboard.png
│
├── docker-compose.yml
└── README.MD
```

The repository currently contains the producer, Spark, dashboard, Databricks and documentation components shown above.

## Event Model

The producer generates synthetic e-commerce events.

Supported event types include:

```text
view_product
add_to_cart
purchase
refund
```

Each event contains information such as:

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

Purchase events contain a positive amount, while refund events are used to reduce calculated revenue.

## Kafka

Apache Kafka is used as the central event broker.

The main topic is:

```text
ecommerce-events
```

The current project documentation describes the topic with three partitions and a replication factor of one.

Kafka separates event generation from event processing:

```text
Producer
   │
   ▼
 Kafka
   │
   ▼
 Spark
```

The producer does not need to know how Spark processes the events, and Spark does not need to know how the events were generated.

## Spark Structured Streaming

Spark consumes events from Kafka and performs the real-time processing.

The streaming pipeline:

1. Reads events from Kafka
2. Parses the JSON payload
3. Applies the event schema
4. Calculates revenue
5. Groups events into one-minute windows
6. Calculates streaming metrics
7. Writes the current metrics to Redis

The event schema is:

```text
event_id       STRING
user_id        INTEGER
product_id     INTEGER
event_type     STRING
amount         DOUBLE
timestamp      TIMESTAMP
```

## Revenue Calculation

Revenue is derived from the event type:

```text
purchase  → +amount
refund    → -amount
view      → 0
cart      → 0
```

Conceptually:

```text
net revenue = purchases - refunds
```

This allows the streaming pipeline to calculate current net revenue directly from the event stream.

## Streaming Metrics

Events are aggregated in one-minute windows.

The current metrics include:

* `window_start`
* `window_end`
* `revenue`
* `orders`
* `events`
* `unique_users`

For unique users, the implementation uses Spark's streaming-compatible approximate distinct aggregation:

```python
approx_count_distinct("user_id")
```

rather than `countDistinct()`, which is not supported for this streaming aggregation setup.

## Redis

Redis acts as the low-latency serving layer for the dashboard.

The current metrics are stored under:

```text
ecommerce:metrics
```

Stored values include:

```text
revenue
orders
events
unique_users
window_start
window_end
```

The dashboard accesses these values through the Flask API rather than connecting directly to Redis.

## Flask API

The Flask application provides the backend for the dashboard.

Its responsibilities include:

* Reading current metrics from Redis
* Providing metrics through HTTP
* Serving the dashboard frontend
* Providing historical data where configured

The local application is exposed on:

```text
http://localhost:5000
```

The exact API endpoints are defined by the current `dashboard/app.py` implementation.

## Dashboard

The web dashboard displays the current streaming metrics.

Typical metrics include:

* Current revenue
* Number of orders
* Number of events
* Unique users
* Current processing window

The repository contains a dashboard screenshot at:

```text
docs/dashboard.png
```

![Real-Time E-Commerce Dashboard](docs/dashboard.png)

## Docker

The complete real-time environment can be started with Docker Compose.

```bash
docker compose up -d --build
```

Check the running services:

```bash
docker compose ps
```

The documented environment contains services for:

```text
Kafka
Spark
Redis
Dashboard
```

Docker Compose provides the network that allows the services to communicate using their container/service names.

## Running the Producer

The Python producer can be started with:

```bash
python3 producer.py
```

If the Kafka Python client is not installed:

```bash
pip install kafka-python
```

or install the producer requirements:

```bash
pip install -r requirements.txt
```

The producer continuously generates synthetic e-commerce events and sends them to Kafka.

## Monitoring the Pipeline

### Docker

```bash
docker compose ps
```

### Spark logs

```bash
docker compose logs -f spark
```

### Kafka

List topics:

```bash
docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Inspect the main topic:

```bash
docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

### Redis

Inspect the latest metrics:

```bash
docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

### Dashboard

Open:

```text
http://localhost:5000
```

These checks follow the debugging workflow documented by the project.

## Debugging

A useful troubleshooting order is:

```text
1. Docker
2. Kafka
3. Producer
4. Spark
5. Redis
6. Flask
7. Dashboard
```

### Kafka topic problems

Verify that `ecommerce-events` exists:

```bash
docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

### Redis connection problems

Verify that Spark and Redis are connected to the same Docker network.

```bash
docker network inspect realtime-ecommerce-platform_default
```

### Producer dependency

If Python reports:

```text
ModuleNotFoundError: No module named 'kafka'
```

install:

```bash
pip install kafka-python
```

### Spark distinct aggregation

For streaming unique-user calculations, use:

```python
approx_count_distinct("user_id")
```

instead of:

```python
countDistinct("user_id")
```

These are documented issues and solutions in the current project README.

## Checkpointing

The Spark streaming application currently uses:

```text
/tmp/checkpoints/ecommerce
```

Checkpoints allow Spark to maintain streaming state and offsets.

For production deployments, the checkpoint location should use persistent storage instead of a temporary container filesystem. Object storage such as S3, Azure Blob Storage or Google Cloud Storage can be used depending on the deployment environment.

## Historical Analytics

Redis is used for current metrics and is not intended to be the long-term storage layer.

The historical architecture follows a Medallion-style approach:

```text
Raw Events
    │
    ▼
 Bronze
    │
    ▼
 Silver
    │
    ▼
 Gold
    │
    ▼
Delta Lake
    │
    ▼
Databricks
```

Potential analytical datasets include:

* Daily revenue
* Daily orders
* Product metrics
* Customer activity
* Conversion metrics
* Refund metrics

The repository contains a `databricks/` directory for this analytics layer.

## Real-Time vs. Historical Processing

The project intentionally separates two workloads.

### Real-Time

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Redis
  ↓
Flask
  ↓
Dashboard
```

Used for:

* Live revenue
* Current order counts
* Current event volume
* Low-latency monitoring

### Historical

```text
Raw Events
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
```

Used for:

* Long-term analysis
* Product performance
* Customer behavior
* Revenue trends
* Business intelligence

This separation keeps the low-latency serving path independent from the historical analytics layer.

## Production Considerations

The current repository is primarily a portfolio and learning project. For production deployment, additional work would be required.

Potential areas include:

### Kafka

* Higher replication factor
* Authentication
* TLS
* Monitoring
* Partition and retention configuration

### Spark

* Persistent checkpoints
* Watermarking
* State management
* Resource configuration
* Monitoring

### Redis

* Persistent storage
* Authentication
* High availability
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
* Historical charts
* Server-sent or WebSocket updates

### Data Lake / Databricks

* Object storage
* Data-quality checks
* Partitioning
* Schema evolution
* Workflow orchestration
* Monitoring

These items represent production extensions rather than requirements for the current local project.

## Example End-to-End Flow

A purchase event travels through the platform as follows:

```text
User performs a purchase
        │
        ▼
Python Producer
        │
        ▼
Apache Kafka
        │
        ▼
ecommerce-events
        │
        ▼
Spark Structured Streaming
        │
        ├── JSON parsing
        ├── Revenue calculation
        └── 1-minute aggregation
        │
        ▼
Redis
        │
        ▼
Flask API
        │
        ▼
Dashboard
```

For historical processing:

```text
Streaming / Raw Events
        │
        ▼
Data Lake
        │
        ▼
Bronze
        │
        ▼
Silver
        │
        ▼
Gold
        │
        ▼
Delta Lake
        │
        ▼
Databricks
        │
        ▼
Analytics / BI
```

## Project Goals

This project demonstrates practical data-engineering concepts including:

* Event-driven architecture
* Real-time streaming
* Apache Kafka
* Spark Structured Streaming
* Stream aggregation
* Redis
* REST APIs
* Docker and Docker Compose
* Data Lake architecture
* Medallion architecture
* Delta Lake
* Databricks
* SQL analytics
* Production-oriented debugging

The focus is an end-to-end data platform rather than only a dashboard.

## Current Status

The currently documented real-time pipeline is:

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

The repository also contains the Databricks/Data Lake components for extending the platform toward historical analytics.

