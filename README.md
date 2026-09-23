# Real-Time E-Commerce Analytics Platform

A real-time e-commerce analytics pipeline built around **Kafka**, **Spark Structured Streaming**, **Redis**, and a **Flask dashboard**.

The project demonstrates how continuously generated e-commerce events can be processed in real time and transformed into live metrics for a web dashboard.

## ✨ Features

* Real-time e-commerce event generation
* Apache Kafka event streaming
* Spark Structured Streaming
* Redis for live metrics
* Flask dashboard
* Structured event processing
* Kafka topic with multiple partitions
* Spark checkpointing for streaming recovery
* Separate real-time and historical analytics concepts
* Docker-based development environment

## 🏗️ Architecture

The main real-time pipeline is:

```text
                    E-Commerce Events
                           │
                           ▼
                    ┌─────────────┐
                    │   Producer  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │    Kafka    │
                    │ ecommerce-  │
                    │   events    │
                    └──────┬──────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Spark Structured        │
              │ Streaming              │
              └───────────┬────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │    Redis    │
                    │ ecommerce:  │
                    │   metrics   │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │    Flask    │
                    │  Dashboard  │
                    └─────────────┘
```

The real-time path is intentionally kept separate from the historical analytics layer.

## 🛠️ Technology Stack

* **Python**
* **Apache Kafka**
* **Apache Spark**
* **Spark Structured Streaming**
* **Redis**
* **Flask**
* **Docker**
* **Databricks** for extended/historical analytics

## 📡 Event Streaming

The event producer generates e-commerce events which are published to the Kafka topic:

```text
ecommerce-events
```

The current Kafka configuration uses:

```text
Partitions: 3
Replication Factor: 1
```

The three partitions allow the stream to be processed in parallel.

A simplified event looks like:

```json id="e3t1ku"
{
  "event_id": "12345",
  "event_type": "purchase",
  "product_id": "product-001",
  "user_id": "user-123",
  "amount": 49.99,
  "timestamp": "2026-01-01T12:00:00Z"
}
```

The event model can be extended with additional product, customer, and transaction attributes.

## ⚡ Spark Structured Streaming

Spark Structured Streaming consumes the Kafka events and processes them continuously.

The streaming layer is responsible for:

* reading events from Kafka
* parsing the event data
* applying transformations
* calculating real-time metrics
* writing the resulting metrics to Redis

The streaming application uses checkpoints so that the processing state can be recovered after a restart.

## 📊 Real-Time Metrics

Processed metrics are stored in Redis under:

```text
ecommerce:metrics
```

Redis provides a lightweight and fast data store for the dashboard to retrieve current metrics without querying the streaming engine directly.

The resulting flow is:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Aggregated Metrics
  ↓
Redis
  ↓
Flask Dashboard
```

## 🖥️ Dashboard

The Flask application provides a simple web interface for displaying the processed real-time metrics.

### Dashboard Result

![Real-Time E-Commerce Dashboard](docs/dashboard.png)

The dashboard is the visual result of the complete streaming pipeline.

## 🔄 End-to-End Workflow

The complete workflow can be summarized as:

```text
1. Generate e-commerce event
2. Publish event to Kafka
3. Kafka distributes the event
4. Spark Structured Streaming consumes the event
5. Spark transforms and aggregates the data
6. Metrics are written to Redis
7. Flask retrieves the metrics
8. Dashboard displays the current results
```

## 📂 Project Structure

```text
realtime-ecommerce-platform/
├── ...
├── docs/
│   └── dashboard.png
├── ...
└── README.md
```

The project is organized around the individual components of the streaming pipeline.

The exact implementation can evolve independently for the producer, Kafka integration, Spark streaming job, Redis storage, and dashboard.

## 🐳 Docker

Docker is used to simplify the local infrastructure required by the project.

The development environment can include the services required for:

```text
Kafka
Spark
Redis
Dashboard
```

This makes it possible to reproduce the streaming architecture locally without installing every infrastructure component directly on the host system.

## 🧪 Debugging & Checkpointing

Streaming systems require special attention to state and recovery.

Spark Structured Streaming uses checkpoints to keep track of streaming progress and state.

When debugging the pipeline, the following components are particularly important:

```text
Producer
   ↓
Kafka Topic
   ↓
Spark Consumer
   ↓
Spark Processing
   ↓
Checkpoint
   ↓
Redis
   ↓
Dashboard
```

When a metric appears incorrect, each stage can therefore be inspected independently.

## 🔁 Restart & Reliability Considerations

A real-time pipeline must account for failures and retries.

Important considerations include:

* Kafka consumer offsets
* Spark checkpoints
* duplicate events
* Redis updates
* service restarts
* malformed events
* temporary service outages

For a production deployment, metric updates should be designed so that retries do not unintentionally count the same business event multiple times.

The current project is primarily a portfolio/demo implementation rather than a fully hardened production streaming platform.

## 📈 Historical Analytics

Real-time analytics and historical analytics serve different purposes.

The real-time pipeline focuses on:

```text
Kafka
→ Spark Streaming
→ Redis
→ Dashboard
```

Historical analytics can be handled separately using a data-lake or Databricks-oriented workflow.

This separation avoids forcing the live dashboard pipeline to also act as the long-term analytical storage layer.

## ☁️ Production Considerations

A production version could extend the architecture with:

* Kafka replication across multiple brokers
* schema management
* stronger event validation
* exactly-once/idempotent processing strategies
* persistent analytical storage
* monitoring and alerting
* centralized logging
* secrets management
* authentication for the dashboard
* container orchestration
* cloud deployment

The current implementation intentionally focuses on demonstrating the core real-time data flow.

## 🚀 Getting Started

Start the required infrastructure using the project's Docker configuration.

After the services are running, start the event producer and streaming application.

The Flask dashboard is available locally at:

```text id="q4tq1e"
http://localhost:5000
```

The exact startup commands depend on the individual services included in the repository.

## 🎯 Project Purpose

This project demonstrates a complete real-time data engineering workflow:

```text
Event Generation
       ↓
     Kafka
       ↓
Spark Structured Streaming
       ↓
     Redis
       ↓
Flask Dashboard
```

The project is intended to demonstrate practical experience with **event streaming, distributed processing, real-time aggregation, caching, and dashboard presentation**.

## 🔮 Possible Improvements

Potential future improvements include:

* schema registry integration
* stronger event validation
* Kafka replication
* dead-letter handling
* automated integration tests
* metrics and monitoring
* persistent historical storage
* Databricks integration
* cloud deployment
* authentication and authorization
* more advanced dashboard visualizations

---

**Project:** Real-Time E-Commerce Analytics Platform
**Author:** Markus

