# 🛒 Real-Time E-Commerce Analytics Platform

A containerized real-time data engineering platform for processing, aggregating and visualizing e-commerce events in real time.

The project simulates an e-commerce environment where user interactions such as product views, cart additions, purchases and refunds are continuously generated, streamed through Apache Kafka, processed with Spark Structured Streaming, stored in Redis and visualized through a Flask-based dashboard.

The architecture is designed as a modular event-driven pipeline and can later be extended with Databricks and Delta Lake for persistent historical analytics.

---

## 📊 Dashboard

The platform includes a web-based dashboard for monitoring the current state of the e-commerce event stream.

![Real-Time E-Commerce Dashboard](docs/dashboard.png)

The dashboard provides an overview of:

* 💰 Revenue
* 🛒 Orders
* 📦 Total events
* 👥 Unique users
* 📈 Historical revenue
* 📊 Streaming activity

The frontend communicates with the Flask REST API, which retrieves the latest metrics from Redis.

---

# 🏗️ Architecture

```text
                         E-COMMERCE EVENTS
                                │
                                ▼
                       ┌─────────────────┐
                       │ Python Producer │
                       └────────┬────────┘
                                │
                                │ JSON
                                ▼
                       ┌─────────────────┐
                       │ Apache Kafka    │
                       │                 │
                       │ ecommerce-events│
                       └────────┬────────┘
                                │
                                ▼
                  ┌──────────────────────────┐
                  │ Spark Structured         │
                  │ Streaming                │
                  │                          │
                  │ • JSON parsing           │
                  │ • transformations        │
                  │ • revenue calculation    │
                  │ • window aggregations    │
                  └────────────┬─────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │     Redis       │
                       │                 │
                       │ Live Metrics    │
                       │ History         │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ Flask REST API  │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ Web Dashboard   │
                       └─────────────────┘
```

---

# 🎯 Project Goals

The main goal of this project is to demonstrate a complete end-to-end real-time data engineering pipeline.

The project covers:

* Event-driven architecture
* Message streaming
* Kafka topic management
* JSON event processing
* Spark Structured Streaming
* Time-window aggregations
* Real-time metric calculation
* Redis-based serving
* REST APIs
* Web visualization
* Docker containerization
* Service-to-service networking
* Pipeline debugging
* Scalable architecture concepts

The individual components are loosely coupled so that the system can be extended without redesigning the complete pipeline.

---

# ⚡ Data Pipeline

The data flows through the system in several stages.

```text
Event Generation
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Window Aggregation
      ↓
Redis
      ↓
Flask REST API
      ↓
Dashboard
```

---

# 1️⃣ Event Generation

A Python producer continuously generates simulated e-commerce events.

Supported event types include:

```text
view_product
add_to_cart
purchase
refund
```

Example event:

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

Each event contains:

| Field        | Description                    |
| ------------ | ------------------------------ |
| `event_id`   | Unique event identifier        |
| `user_id`    | Simulated customer ID          |
| `product_id` | Product identifier             |
| `event_type` | Type of e-commerce interaction |
| `amount`     | Transaction amount             |
| `timestamp`  | Event timestamp                |

The producer publishes every event to the Kafka topic:

```text
ecommerce-events
```

---

# 2️⃣ Apache Kafka

Apache Kafka acts as the event streaming backbone of the platform.

The producer sends events to:

```text
ecommerce-events
```

The current topic configuration is:

```text
Partitions:        3
Replication factor: 1
```

The partitioned architecture demonstrates how Kafka can distribute events across multiple partitions.

```text
                  ecommerce-events
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
        Partition 0  Partition 1  Partition 2
```

Kafka decouples event generation from downstream processing.

The producer does not need to know how Spark processes the events.

---

# 3️⃣ Spark Structured Streaming

Spark Structured Streaming consumes events directly from Kafka.

The streaming application performs the following operations:

1. Read Kafka messages
2. Convert Kafka values to JSON
3. Parse JSON into a structured schema
4. Calculate revenue
5. Group events into one-minute windows
6. Calculate streaming metrics
7. Write results to Redis

The Spark application consumes Kafka internally using:

```text
kafka:29092
```

---

# 🧮 Revenue Calculation

Revenue is calculated based on the event type.

### Purchase

```text
purchase → +amount
```

### Refund

```text
refund → -amount
```

### Other events

```text
view_product → 0
add_to_cart  → 0
```

Therefore:

```text
Revenue =
Purchase Revenue
-
Refund Amount
```

Example:

```text
Purchase:  999.99
Purchase:  299.99
Refund:    149.99

Revenue = 1149.99
```

---

# ⏱️ Window Aggregation

Events are aggregated in one-minute windows.

For every window Spark calculates:

```text
revenue
orders
events
unique_users
```

Example:

```text
+-------------------+-------------------+--------+------+-------+------------+
|window_start       |window_end         |revenue |orders|events |unique_users|
+-------------------+-------------------+--------+------+-------+------------+
|2026-08-29 13:50:00|2026-08-29 13:51:00|4030.06 |23    |88     |85          |
|2026-08-29 13:51:00|2026-08-29 13:52:00|2369.99 |26    |100    |99          |
|2026-08-29 13:52:00|2026-08-29 13:53:00|500.00  |24    |99     |100         |
+-------------------+-------------------+--------+------+-------+------------+
```

---

# 👥 Unique Users

Spark Structured Streaming does not support regular exact `countDistinct()` aggregations in this streaming configuration.

Therefore the pipeline uses a streaming-compatible approximate distinct-count function.

This provides an efficient estimate of unique users per time window without breaking the streaming query.

---

# 4️⃣ Redis

Redis is used as the low-latency serving layer.

The dashboard needs fast access to the most recent metrics, which makes Redis a suitable choice.

Redis is not intended to be the long-term analytical storage layer.

## Current metrics

The latest metrics are stored under:

```text
ecommerce:metrics
```

Example:

```text
revenue
-459.93

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

## Historical metrics

Historical windows are stored using:

```text
ecommerce:history:*
```

This allows the dashboard to retrieve previous aggregation windows.

---

# 5️⃣ Flask REST API

The Flask application provides a lightweight REST API between Redis and the frontend.

## Current Metrics

```http
GET /api/metrics
```

Returns the latest calculated metrics.

Example:

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

## Historical Metrics

```http
GET /api/history
```

Returns the historical streaming windows stored in Redis.

---

## Health Check

```http
GET /health
```

Used to verify that the Flask application can communicate with Redis.

Example:

```json
{
  "status": "ok",
  "redis": "ok"
}
```

---

# 6️⃣ Web Dashboard

The dashboard consumes the REST API and presents the streaming metrics in a human-readable interface.

The frontend is intentionally lightweight.

The dashboard can display:

* Current revenue
* Order count
* Event count
* Unique users
* Historical revenue
* Streaming activity

The architecture separates the presentation layer from the processing layer.

```text
Spark
  ↓
Redis
  ↓
Flask API
  ↓
Frontend
```

This means the browser never needs direct access to Kafka or Spark.

---

# 🔌 Ports & Services

| Service   | Container             | Host Port | Internal Address | Purpose           |
| --------- | --------------------- | --------: | ---------------- | ----------------- |
| Kafka     | `ecommerce-kafka`     |    `9092` | `kafka:29092`    | Event streaming   |
| Redis     | `ecommerce-redis`     |    `6379` | `redis:6379`     | Real-time metrics |
| Spark     | `ecommerce-spark`     |         — | —                | Stream processing |
| Dashboard | `ecommerce-dashboard` |    `5000` | —                | Web UI + REST API |

## Kafka

From the host:

```text
localhost:9092
```

Inside Docker:

```text
kafka:29092
```

Kafka topic:

```text
ecommerce-events
```

---

## Redis

From the host:

```text
localhost:6379
```

Inside Docker:

```text
redis:6379
```

---

## Dashboard

The dashboard is available at:

```text
http://localhost:5000
```

REST API:

```text
http://localhost:5000/api/metrics
http://localhost:5000/api/history
http://localhost:5000/health
```

---

# 🐳 Docker

The complete infrastructure is containerized using Docker Compose.

Current services:

```text
ecommerce-kafka
ecommerce-redis
ecommerce-spark
ecommerce-dashboard
```

Docker Compose creates a shared network:

```text
realtime-ecommerce-platform_default
```

Services communicate through Docker DNS using their service names.

For example:

```text
Spark → kafka:29092
Spark → redis:6379
Dashboard → redis:6379
```

This avoids relying on dynamically assigned container IP addresses.

---

# 🧰 Technology Stack

| Technology                 | Purpose                    |
| -------------------------- | -------------------------- |
| Python                     | Event generation           |
| Apache Kafka               | Event streaming            |
| Apache Spark               | Stream processing          |
| Spark Structured Streaming | Real-time transformations  |
| Redis                      | Low-latency metric storage |
| Flask                      | REST API                   |
| HTML                       | Dashboard structure        |
| CSS                        | Dashboard styling          |
| JavaScript                 | Dashboard interaction      |
| Docker                     | Containerization           |
| Docker Compose             | Local orchestration        |

---

# 📁 Project Structure

```text
realtime-ecommerce-platform/
│
├── producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark/
│   ├── streaming.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── dashboard/
│   ├── app.py
│   ├── requirements.txt
│   ├── Dockerfile
│   │
│   ├── templates/
│   │   └── index.html
│   │
│   └── static/
│       ├── css/
│       └── js/
│
├── docs/
│   └── dashboard.png
│
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

# 🚀 Getting Started

## Prerequisites

Install:

* Docker
* Docker Compose
* Python 3
* Git

---

## Clone the Repository

```bash
git clone <repository-url>

cd realtime-ecommerce-platform
```

---

# ▶️ Start the Platform

Start all services:

```bash
sudo docker compose up -d
```

Check the containers:

```bash
sudo docker compose ps
```

Expected services:

```text
ecommerce-kafka
ecommerce-redis
ecommerce-spark
ecommerce-dashboard
```

---

# 📨 Start the Producer

If running the producer locally:

```bash
cd producer
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Start the producer:

```bash
python3 producer.py
```

The producer continuously generates events and publishes them to Kafka.

---

# 🌐 Open the Dashboard

Open:

```text
http://localhost:5000
```

Once the producer is generating events and Spark is processing them, the dashboard will start showing live metrics.

---

# 🔍 Debugging

The pipeline can be debugged layer by layer.

---

## 1. Check Docker

```bash
sudo docker compose ps
```

All required services should be running.

---

## 2. Check Kafka

List topics:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected:

```text
ecommerce-events
```

Describe the topic:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

Expected configuration:

```text
PartitionCount: 3
ReplicationFactor: 1
```

---

## 3. Check Redis

Test the connection:

```bash
sudo docker exec ecommerce-redis redis-cli ping
```

Expected:

```text
PONG
```

Check current metrics:

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

Check history:

```bash
sudo docker exec ecommerce-redis \
  redis-cli KEYS 'ecommerce:history:*'
```

---

## 4. Check Spark

View the Spark logs:

```bash
sudo docker compose logs --tail=100 spark
```

Spark should show streaming batches similar to:

```text
============================================================
Spark Batch: 80
============================================================

window_start        window_end          revenue    orders
2026-08-29 13:50    2026-08-29 13:51     4030.06      23
```

---

## 5. Check the Flask API

Health check:

```bash
curl http://localhost:5000/health
```

Metrics:

```bash
curl http://localhost:5000/api/metrics
```

History:

```bash
curl http://localhost:5000/api/history
```

---

# 🔄 End-to-End Test

The complete pipeline should be verified in this order:

```text
1. Docker
      ↓
2. Kafka
      ↓
3. Producer
      ↓
4. Spark
      ↓
5. Redis
      ↓
6. Flask API
      ↓
7. Dashboard
```

A successful end-to-end test means:

```text
Producer
   ↓
Kafka event
   ↓
Spark batch
   ↓
Redis metric
   ↓
Flask API response
   ↓
Dashboard visualization
```

---

# 📈 Example

A simulated purchase:

```json
{
  "event_id": "922351eb-d227-4af5-b57c-cf466193ee0b",
  "user_id": 42,
  "product_id": 2,
  "event_type": "purchase",
  "amount": 149.99,
  "timestamp": "2026-08-29T13:30:50.736587"
}
```

is published to:

```text
Kafka
└── ecommerce-events
```

Spark receives the event and calculates the corresponding revenue contribution:

```text
+149.99
```

The event is then included in the corresponding one-minute aggregation window.

The resulting metrics are stored in Redis and exposed through Flask.

The dashboard then displays the updated values.

---

# 🧠 Design Decisions

## Why Kafka?

Kafka provides a durable and scalable event streaming layer.

It decouples producers from consumers and allows multiple downstream systems to consume the same events.

---

## Why Spark Structured Streaming?

Spark provides a powerful framework for processing streaming data and performing time-based aggregations.

It also provides a natural path toward larger distributed data processing workloads.

---

## Why Redis?

The dashboard requires fast access to the latest metrics.

Redis provides low-latency reads and is well suited for serving real-time application state.

---

## Why Flask?

Flask provides a lightweight interface between the Redis data layer and the frontend.

It keeps the frontend independent from the underlying streaming infrastructure.

---

## Why Docker?

Docker makes the complete development environment reproducible.

Kafka, Redis, Spark and Flask can be started together without requiring each service to be manually installed and configured on the host system.

---

# 🧱 Architectural Separation

One of the main design principles of the project is separating the responsibilities of each component.

```text
Producer
→ Generates events

Kafka
→ Transports events

Spark
→ Processes events

Redis
→ Serves real-time metrics

Flask
→ Exposes the data

Dashboard
→ Visualizes the data
```

This makes individual components replaceable and simplifies future development.

---

# ☁️ Future Databricks Integration

The current platform is fully functional as a local real-time streaming system.

A future version can extend the architecture with Databricks and Delta Lake.

The main purpose would be persistent historical storage and analytical processing.

```text
                         Kafka
                           │
             ┌─────────────┴─────────────┐
             │                           │
             ▼                           ▼
      Spark Streaming              Databricks
             │                           │
             ▼                           ▼
          Redis                    Delta Lake
             │                           │
             ▼                           ▼
        Live Dashboard             SQL Analytics
```

Redis would continue to serve the live dashboard.

Databricks would provide the historical analytical layer.

Potential use cases include:

* Daily revenue reporting
* Product performance analysis
* Customer behavior analysis
* Conversion funnels
* Refund analysis
* Cohort analysis
* Historical trends
* Data warehouse integration
* Large-scale analytical queries

---

# 🛣️ Roadmap

## Completed

* [x] Python event producer
* [x] E-commerce event simulation
* [x] Kafka integration
* [x] Kafka topic with multiple partitions
* [x] Spark Structured Streaming
* [x] JSON event parsing
* [x] Revenue calculation
* [x] Purchase tracking
* [x] Refund handling
* [x] One-minute window aggregation
* [x] Unique-user calculation
* [x] Redis integration
* [x] Historical metrics
* [x] Flask REST API
* [x] Health endpoint
* [x] Real-time dashboard
* [x] Docker containers
* [x] Docker Compose networking
* [x] End-to-end pipeline debugging

## Planned

* [ ] Databricks integration
* [ ] Delta Lake
* [ ] Persistent event storage
* [ ] Historical analytics
* [ ] Product-level analytics
* [ ] Conversion funnel
* [ ] Customer segmentation
* [ ] Advanced KPIs
* [ ] Monitoring and alerting
* [ ] Automated tests
* [ ] CI/CD pipeline
* [ ] Cloud deployment

---

# 📊 Potential Future KPIs

The current system can be extended with additional e-commerce metrics.

Examples:

### Conversion Rate

```text
Purchases / Product Views
```

### Cart Conversion

```text
Purchases / Add-to-Cart Events
```

### Average Order Value

```text
Revenue / Orders
```

### Refund Rate

```text
Refunds / Purchases
```

### Revenue per User

```text
Revenue / Unique Users
```

These metrics could eventually be calculated by Spark and persisted in Delta Lake for historical analysis.

---

# 🔐 Configuration

Environment-specific configuration can be moved into environment variables.

Potential configuration:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
REDIS_HOST
REDIS_PORT
```

This would make the application easier to deploy across:

```text
Development
    ↓
Staging
    ↓
Production
```

without changing the application code.

---

# 🧪 Development Notes

This project is primarily designed as a learning and portfolio project demonstrating real-time data engineering concepts.

The architecture intentionally uses lightweight and understandable components while maintaining a realistic event-driven structure.

The local Docker environment provides a reproducible development setup.

The architecture can later be extended toward cloud infrastructure and a lakehouse-based analytical platform.

---

# 📸 Screenshots

## Real-Time Dashboard

![Real-Time E-Commerce Dashboard](docs/dashboard.png)

---

# 👨‍💻 Project Summary

This project demonstrates a complete real-time e-commerce analytics pipeline.

From event generation:

```text
Python Producer
```

through event streaming:

```text
Apache Kafka
```

through stream processing:

```text
Spark Structured Streaming
```

through real-time serving:

```text
Redis
```

to data access and visualization:

```text
Flask REST API
        ↓
Web Dashboard
```

The modular architecture provides a foundation for extending the project into a more advanced streaming and lakehouse platform using technologies such as Databricks and Delta Lake.

---

# ⭐ Key Takeaways

This project demonstrates practical experience with:

* Real-time data pipelines
* Event-driven architecture
* Apache Kafka
* Kafka partitions
* Spark Structured Streaming
* Streaming aggregations
* Time-window processing
* Redis
* REST APIs
* Flask
* Docker
* Docker Compose
* Service networking
* Real-time dashboards
* Data pipeline debugging
* Scalable data architecture

The next major architectural step is adding **Databricks + Delta Lake** to provide persistent historical data and advanced analytical capabilities.

