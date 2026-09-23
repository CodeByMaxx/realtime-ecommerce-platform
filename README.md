# Real-Time E-Commerce Analytics Platform

A real-time e-commerce analytics pipeline built around Apache Kafka, Spark Structured Streaming, Redis, and a Flask dashboard.

The project demonstrates how e-commerce events can be streamed, processed in real time, aggregated into metrics, and displayed through a web dashboard.

## Features

* Real-time e-commerce event streaming
* Apache Kafka event ingestion
* Spark Structured Streaming
* Redis-based real-time metrics
* Flask dashboard
* Docker-based development environment
* Configurable streaming pipeline
* Historical analytics as an extension of the real-time pipeline

## Architecture

```text
                    E-Commerce Events
                           │
                           ▼
                    Kafka Producer
                           │
                           ▼
                    Apache Kafka
                  ecommerce-events
                           │
                           ▼
             Spark Structured Streaming
                           │
                           ▼
                        Redis
                  ecommerce:metrics
                           │
                           ▼
                    Flask Dashboard
                           │
                           ▼
                    Web Browser
```

## Technology Stack

| Component         | Technology                        |
| ----------------- | --------------------------------- |
| Event Streaming   | Apache Kafka                      |
| Stream Processing | Apache Spark Structured Streaming |
| Metrics Store     | Redis                             |
| Dashboard         | Flask                             |
| Containers        | Docker                            |
| Data Format       | JSON                              |
| Analytics         | Spark / SQL                       |

## Event Streaming

E-commerce events are published to the Kafka topic:

```text
ecommerce-events
```

The current Kafka configuration uses:

* **3 partitions**
* **Replication factor: 1**

A simplified event can look like:

```json
{
  "event_type": "purchase",
  "product_id": "product-123",
  "customer_id": "customer-456",
  "amount": 49.99,
  "timestamp": "2025-01-01T12:00:00"
}
```

The producer sends these events to Kafka, where they become available for stream processing.

## Spark Structured Streaming

Spark Structured Streaming consumes events from Kafka and processes them continuously.

The streaming layer is responsible for:

* reading Kafka events
* parsing incoming JSON data
* transforming event data
* calculating real-time metrics
* writing aggregated results to Redis

The pipeline uses Spark Structured Streaming for continuous event processing rather than processing the complete dataset in batch mode.

## Redis Metrics

Processed metrics are stored in Redis under the key:

```text
ecommerce:metrics
```

Redis provides a lightweight, fast-access store for the dashboard.

This separates the stream-processing layer from the presentation layer.

## Dashboard

The Flask dashboard displays the processed e-commerce metrics.

### Result

![Real-Time E-Commerce Dashboard](docs/dashboard.png)

The screenshot shows the visual result of the real-time analytics pipeline.

## End-to-End Workflow

```text
1. Producer generates an e-commerce event
                │
                ▼
2. Event is published to Kafka
                │
                ▼
3. Spark Structured Streaming reads the event
                │
                ▼
4. Spark transforms and aggregates the data
                │
                ▼
5. Metrics are written to Redis
                │
                ▼
6. Flask dashboard reads the metrics
                │
                ▼
7. Metrics are displayed in the browser
```

## Project Structure

```text
realtime-ecommerce-platform/
├── docs/
│   └── dashboard.png
├── README.md
└── ...
```

The remaining project files contain the producer, streaming, Redis, dashboard, and supporting components of the application.

## Docker

The project uses Docker to simplify the local development environment and run the infrastructure components consistently.

The main infrastructure consists of:

* Kafka
* Spark
* Redis
* Flask application

Start the environment according to the project's Docker configuration.

## Debugging & Checkpointing

Spark Structured Streaming uses checkpointing to keep track of streaming progress.

Checkpointing is important when restarting a streaming application because it allows Spark to recover the state and continue processing.

During development, checkpoint locations should be kept separate from application source code.

## Restart & Reliability Considerations

A real-time pipeline needs to consider what happens when individual components restart.

Relevant areas include:

* Kafka consumer offsets
* Spark checkpoints
* Redis state
* duplicate events
* application restarts
* temporary infrastructure failures

For a production system, event processing should be designed with appropriate idempotency and recovery behaviour.

## Historical Analytics

The real-time pipeline can be extended with a separate historical analytics layer.

A possible architecture is:

```text
Kafka
  │
  ├──► Real-Time Processing ──► Redis ──► Dashboard
  │
  └──► Historical Storage ────► Analytics / Data Lake
```

This allows real-time operational metrics and longer-term business analytics to coexist.

## Production Considerations

The current project is primarily a demonstration of a real-time analytics architecture.

A production deployment could additionally require:

* Kafka replication across multiple brokers
* persistent infrastructure
* authentication and authorization
* monitoring
* structured logging
* schema management
* stronger delivery guarantees
* scalable Redis configuration
* deployment automation

These concerns are intentionally separate from the core demonstration pipeline.

## Getting Started

After starting the required services, the Flask dashboard can be accessed locally at:

```text
http://localhost:5000/
```

The complete workflow is:

```text
Start Infrastructure
        │
        ▼
Start Producer
        │
        ▼
Start Spark Streaming
        │
        ▼
Write Metrics to Redis
        │
        ▼
Open Flask Dashboard
```

## Possible Improvements

Future extensions could include:

* More e-commerce event types
* Additional real-time KPIs
* Product and customer analytics
* Better event schemas
* Persistent historical storage
* Advanced dashboards
* Automated deployment
* Monitoring and alerting
* More comprehensive integration tests

## Project Purpose

The project demonstrates a complete real-time data pipeline for e-commerce analytics.

It combines event-driven architecture with stream processing and a lightweight dashboard:

**Kafka → Spark → Redis → Flask**

This makes the project a practical example of how streaming technologies can be combined to process and visualize continuously arriving business events.

## Author

**Markus**

