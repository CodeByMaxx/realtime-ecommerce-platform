# Real-Time E-Commerce Platform

Eine end-to-end **Real-Time E-Commerce Data Platform** zur Erfassung, Verarbeitung, Speicherung und Visualisierung von E-Commerce-Events.

Das Projekt kombiniert eine lokale Streaming-Pipeline mit einer Databricks-Lakehouse-Architektur.

---

## 1. Projektübersicht

Die Plattform simuliert einen realen E-Commerce-Datenfluss:

```text
                    ┌────────────────────┐
                    │   E-Commerce       │
                    │   Event Producer   │
                    └─────────┬──────────┘
                              │
                              │ JSON Events
                              ▼
                    ┌────────────────────┐
                    │      Apache        │
                    │       Kafka        │
                    │                    │
                    │ ecommerce-events   │
                    └─────────┬──────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                 ▼                         ▼
       ┌──────────────────┐       ┌──────────────────┐
       │ Spark Structured  │       │    Databricks    │
       │ Streaming         │       │    Lakehouse     │
       │                  │       │                  │
       │ Transformations │       │ Bronze            │
       │ Aggregations     │       │ Silver            │
       └────────┬─────────┘       │ Gold              │
                │                 └──────────────────┘
                │
                ▼
       ┌──────────────────┐
       │      Redis       │
       │                  │
       │ Live Metrics     │
       └────────┬─────────┘
                │
                ▼
       ┌──────────────────┐
       │ Flask API        │
       │ + Dashboard      │
       └──────────────────┘
```

Kafka dient dabei als Event-Streaming-Schicht. Kafka organisiert Events in Topics und partitionierte Logs.

Databricks übernimmt die langfristige Speicherung und analytische Verarbeitung im Lakehouse.

---

# 2. Technologie-Stack

| Komponente           | Technologie                       | Aufgabe                         |
| -------------------- | --------------------------------- | ------------------------------- |
| Event Generator      | Python                            | Generiert E-Commerce-Events     |
| Message Broker       | Apache Kafka                      | Streaming und Buffering         |
| Stream Processing    | Apache Spark Structured Streaming | Echtzeitverarbeitung            |
| Cache / Live Metrics | Redis                             | Aktuelle Kennzahlen             |
| Backend              | Flask / Python                    | REST API                        |
| Frontend             | Dashboard / HTML / JavaScript     | Visualisierung                  |
| Data Lakehouse       | Databricks                        | Historische Daten und Analytics |
| Storage Format       | Delta Lake                        | Persistente Lakehouse-Tabellen  |
| Containerisierung    | Docker Compose                    | Lokale Infrastruktur            |

Databricks basiert auf Apache Spark und verwendet Delta Lake als Storage-Layer für Lakehouse-Tabellen.

---

# 3. Aktuelle Architektur

## 3.1 Event Producer

Der Producer befindet sich im Projekt beispielsweise unter:

```text
producer/
└── producer.py
```

Der Producer erzeugt kontinuierlich E-Commerce-Events.

Beispiel:

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

Mögliche Event-Typen sind unter anderem:

```text
view_product
add_to_cart
purchase
refund
```

---

# 4. Kafka

Kafka läuft lokal als Docker-Container.

Container:

```text
ecommerce-kafka
```

Externer Port:

```text
9092
```

Interne Kommunikation innerhalb des Docker-Netzwerks:

```text
kafka:29092
```

---

## Kafka Topic

Das verwendete Topic ist:

```text
ecommerce-events
```

Aktueller Aufbau:

```text
Partitions:         3
Replication Factor: 1
```

Topic überprüfen:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

Erwartete Struktur:

```text
Partition 0
Partition 1
Partition 2
```

Topic auflisten:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Kafka Topics bilden den zentralen Event-Stream zwischen Producer und den verschiedenen Konsumenten.

---

# 5. Redis

Redis wird für die **aktuellen Live-Metriken** verwendet.

Container:

```text
ecommerce-redis
```

Port:

```text
6379
```

Redis wird innerhalb des Docker-Netzwerks über:

```text
redis:6379
```

erreicht.

Verbindung testen:

```bash
sudo docker exec ecommerce-redis redis-cli ping
```

Erwartete Ausgabe:

```text
PONG
```

---

## Aktuelle Redis-Metriken

Die Metriken werden unter folgendem Redis-Key gespeichert:

```text
ecommerce:metrics
```

Auslesen:

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

Beispiel:

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

Redis dient dabei nicht als historischer Data Lake.

Es ist der schnelle **Serving Layer für das Live-Dashboard**.

---

# 6. Spark Structured Streaming

Spark verarbeitet die Events aus Kafka.

Container:

```text
ecommerce-spark
```

Die Anwendung befindet sich im Container unter:

```text
/opt/spark/streaming.py
```

Kafka-Verbindung:

```text
kafka:29092
```

Topic:

```text
ecommerce-events
```

---

## Spark Pipeline

Der Stream wird grundsätzlich wie folgt verarbeitet:

```text
Kafka
  │
  ▼
Raw Kafka Value
  │
  ▼
JSON
  │
  ▼
Structured Event
  │
  ▼
Revenue Calculation
  │
  ▼
1 Minute Window
  │
  ▼
Aggregations
  │
  ▼
Redis
```

---

# 7. Event Schema

Spark verwendet folgendes Schema:

| Feld       | Typ       | Beschreibung         |
| ---------- | --------- | -------------------- |
| event_id   | String    | Eindeutige Event-ID  |
| user_id    | Integer   | Benutzer-ID          |
| product_id | Integer   | Produkt-ID           |
| event_type | String    | Art des Events       |
| amount     | Double    | Betrag               |
| timestamp  | Timestamp | Zeitpunkt des Events |

---

# 8. Revenue Calculation

Der Umsatz wird abhängig vom Event-Typ berechnet.

Bei einem Kauf:

```text
purchase → +amount
```

Bei einer Rückerstattung:

```text
refund → -amount
```

Alle anderen Events:

```text
view_product → 0
add_to_cart → 0
```

Beispiel:

```text
purchase  999.99
refund    -149.99
view      0
cart      0
```

Dadurch entsteht ein Net-Revenue-Wert.

---

# 9. Window Aggregation

Spark aggregiert die Events in:

```text
1-Minute Windows
```

Dabei werden aktuell folgende Kennzahlen berechnet:

```text
revenue
orders
events
unique_users
```

Beispiel:

```text
window_start        2026-08-29 13:50:00
window_end          2026-08-29 13:51:00

revenue             4030.06
orders              23
events              88
unique_users        85
```

---

## Unique Users

Eine exakte `countDistinct()`-Aggregation wird in dieser Streaming-Konfiguration nicht verwendet, da Spark für diese Streaming-Aggregation eine Distinct-Aggregation ablehnt.

Stattdessen wird:

```python
approx_count_distinct()
```

verwendet.

Dadurch erhalten wir eine effiziente Approximation der Anzahl eindeutiger User.

---

# 10. Spark → Redis

Die aggregierten Ergebnisse werden über `foreachBatch()` an Redis übertragen.

Vereinfacht:

```text
Spark Batch
     │
     ▼
Aggregation
     │
     ▼
foreachBatch()
     │
     ▼
Redis
```

Redis speichert die aktuell relevanten Kennzahlen für das Dashboard.

---

# 11. Docker

Die lokale Infrastruktur wird über Docker Compose gestartet.

Typische Services:

```text
ecommerce-kafka
ecommerce-redis
ecommerce-spark
ecommerce-dashboard
```

Status überprüfen:

```bash
sudo docker compose ps
```

Logs anzeigen:

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

---

# 12. Docker Network

Die Services kommunizieren über das Compose-Netzwerk:

```text
realtime-ecommerce-platform_default
```

Netzwerk überprüfen:

```bash
sudo docker network inspect realtime-ecommerce-platform_default
```

Beispielhafte Container:

```text
ecommerce-kafka
ecommerce-spark
ecommerce-redis
```

Innerhalb des Docker-Netzwerks werden die Service-Namen als DNS-Namen verwendet:

```text
kafka
redis
```

Beispiel:

```text
kafka:29092
redis:6379
```

---

# 13. Dashboard

Das Dashboard stellt die aktuellen Metriken grafisch dar.

Architektur:

```text
Redis
  │
  ▼
Flask API
  │
  ▼
Dashboard
```

Das Dashboard verwendet also nicht direkt Kafka.

Kafka und Spark sind für die Verarbeitung zuständig.

Redis stellt die bereits aggregierten Werte schnell bereit.

---

# 14. Flask API

Das Dashboard wird durch eine Flask-Anwendung versorgt.

Grundsätzlich:

```text
Browser
   │
   ▼
Flask
   │
   ▼
Redis
```

Die API liest beispielsweise:

```text
ecommerce:metrics
```

aus Redis.

Damit bleibt die GUI unabhängig von der laufenden Spark-Verarbeitung.

---

# 15. Databricks / Data Lakehouse

Neben der lokalen Real-Time-Pipeline ist **Databricks** Bestandteil des Projekts.

Der Zweck ist ein anderer als bei Redis.

### Redis

```text
Live
Fast
Current metrics
Dashboard
```

### Databricks

```text
Historical
Persistent
Analytics
Data Lakehouse
```

Databricks kombiniert Data-Lake- und Data-Warehouse-Konzepte in einer Lakehouse-Architektur. Delta Lake stellt dabei die Storage-Schicht bereit.

---

# 16. Medallion Architecture

Die Databricks-Seite folgt der klassischen:

```text
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

Databricks beschreibt Bronze als Raw-Layer, Silver als bereinigten/validierten Layer und Gold als für Analytics und Business-Anwendungen optimierten Layer.

---

## 16.1 Bronze

Bronze enthält die Rohdaten.

Beispielsweise:

```text
event_id
user_id
product_id
event_type
amount
timestamp
```

Ziel:

```text
Originaldaten erhalten
```

Die Bronze-Schicht dient damit auch als Ausgangspunkt für spätere Reprocessing- und Audit-Prozesse.

Beispiel:

```text
bronze.ecommerce_events
```

---

# 17. Silver

Silver enthält bereinigte und validierte Events.

Mögliche Transformationen:

```text
Schema Validation
      │
      ▼
Null Checks
      │
      ▼
Type Casting
      │
      ▼
Deduplication
      │
      ▼
Valid Events
```

Beispielsweise werden ungültige Events entfernt oder separat behandelt.

Beispiel:

```text
silver.ecommerce_events
```

Silver ist die Schicht für Datenbereinigung, Normalisierung, Deduplizierung und weitere fachliche Anreicherung.

---

# 18. Gold

Gold enthält Business-Metriken.

Beispielsweise:

```text
gold.daily_revenue
gold.product_performance
gold.user_activity
gold.ecommerce_metrics
```

Mögliche Kennzahlen:

```text
Revenue
Orders
Events
Unique Users
Average Order Value
Refund Amount
Conversion Rate
```

Gold ist für BI, Dashboards, Reporting und Business Analytics vorgesehen.

---

# 19. Gesamtarchitektur mit Databricks

Die vollständige Zielarchitektur:

```text
                    ┌──────────────────┐
                    │ Python Producer  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    │ ecommerce-events │
                    └───────┬──────────┘
                            │
                 ┌──────────┴───────────┐
                 │                      │
                 ▼                      ▼
        ┌─────────────────┐    ┌─────────────────────┐
        │ Spark Streaming │    │     Databricks      │
        │                 │    │                     │
        │ Real-Time       │    │ Bronze              │
        │ Processing      │    │   ↓                 │
        └────────┬────────┘    │ Silver              │
                 │             │   ↓                 │
                 ▼             │ Gold                │
        ┌─────────────────┐    └─────────────────────┘
        │      Redis      │
        │   Live Metrics  │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │ Flask API        │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │    Dashboard    │
        └─────────────────┘
```

---

# 20. Warum zwei Datenpfade?

Das Projekt verwendet bewusst zwei unterschiedliche Pfade.

## Real-Time Path

```text
Kafka
 ↓
Spark
 ↓
Redis
 ↓
Dashboard
```

Ziel:

```text
Niedrige Latenz
Live-Metriken
Monitoring
```

## Data Lake Path

```text
Kafka / Events
 ↓
Databricks
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

Ziel:

```text
Historische Daten
Reprocessing
Analytics
Reporting
Data Science
```

Diese Trennung entspricht dem Grundgedanken einer Lakehouse-Architektur: Streaming und Batch können auf einer gemeinsamen persistenten Datenbasis aufbauen. Delta Lake ist speziell auf die Zusammenarbeit mit Spark und Structured Streaming ausgelegt.

---

# 21. Projektstruktur

Die aktuelle Struktur kann beispielsweise so aussehen:

```text
realtime-ecommerce-platform/
│
├── producer/
│   └── producer.py
│
├── spark/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streaming.py
│
├── dashboard/
│   ├── app.py
│   ├── templates/
│   │   └── index.html
│   └── static/
│       ├── css/
│       └── js/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── docker-compose.yml
│
└── README.md
```

Die tatsächlichen Dateinamen können abhängig vom aktuellen Repository leicht abweichen.

---

# 22. Starten der lokalen Pipeline

Zuerst die Docker-Infrastruktur starten:

```bash
sudo docker compose up -d kafka redis
```

Danach prüfen:

```bash
sudo docker compose ps
```

Kafka sollte laufen:

```text
ecommerce-kafka    Up
```

Redis:

```text
ecommerce-redis   Up
```

---

# 23. Kafka Topic überprüfen

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Erwartet:

```text
ecommerce-events
```

Details:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

---

# 24. Redis überprüfen

```bash
sudo docker exec ecommerce-redis redis-cli ping
```

Erwartet:

```text
PONG
```

Danach:

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

---

# 25. Spark starten

Spark starten:

```bash
sudo docker compose up -d spark
```

Logs:

```bash
sudo docker compose logs -f spark
```

Wenn Spark korrekt läuft, erscheinen Batches wie:

```text
Spark Batch: 80
```

und anschließend:

```text
window_start
window_end
revenue
orders
events
unique_users
```

---

# 26. Producer starten

Falls der Producer lokal läuft:

```bash
python3 producer.py
```

Falls das Modul Kafka benötigt:

```bash
pip install kafka-python
```

Anschließend sollte der Producer Events an Kafka senden.

---

# 27. End-to-End-Test

Die komplette Pipeline kann folgendermaßen getestet werden:

```text
1. Kafka starten
2. Redis starten
3. Spark starten
4. Dashboard starten
5. Producer starten
6. Kafka Events prüfen
7. Spark Logs prüfen
8. Redis prüfen
9. Dashboard öffnen
```

---

# 28. Debugging

## Docker läuft nicht

```bash
sudo docker compose ps
```

---

## Kafka läuft nicht

```bash
sudo docker compose logs --tail=100 kafka
```

---

## Kafka Topic fehlt

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Topic erstellen:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecommerce-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## Spark startet nicht

```bash
sudo docker compose logs --tail=100 spark
```

Bei Änderungen an `streaming.py` das Image neu bauen:

```bash
sudo docker compose build spark
```

Danach:

```bash
sudo docker compose up -d spark
```

---

## Prüfen, welche streaming.py wirklich im Image liegt

```bash
sudo docker run --rm \
  realtime-ecommerce-platform-spark \
  grep -n "approx_count_distinct\|countDistinct" \
  /opt/spark/streaming.py
```

Damit kann ausgeschlossen werden, dass ein altes Docker-Image verwendet wird.

---

# 29. Redis-Verbindung aus Spark testen

Wenn Redis aus Spark nicht erreichbar ist:

```bash
sudo docker exec ecommerce-spark \
  python3 -c \
  "import socket; print(socket.gethostbyname('redis')); print(socket.create_connection(('redis',6379,5)))"
```

Eine erfolgreiche Verbindung sieht beispielsweise so aus:

```text
172.19.0.x
<socket.socket ...>
```

Wichtig ist:

```text
redis
```

als Hostname zu verwenden.

Nicht:

```text
localhost
```

Denn `localhost` innerhalb des Spark-Containers zeigt auf den Spark-Container selbst.

---

# 30. Kafka-Verbindungsproblem

Spark verwendet innerhalb von Docker:

```text
kafka:29092
```

Nicht:

```text
localhost:9092
```

`localhost:9092` ist für Zugriffe vom Host gedacht.

Innerhalb des Compose-Netzwerks gilt:

```text
Spark → kafka:29092
```

Vom Host:

```text
Host → localhost:9092
```

---

# 31. UnknownTopicOrPartitionException

Wenn Spark meldet:

```text
UnknownTopicOrPartitionException
```

zuerst prüfen:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Danach:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

Es sollten alle drei Partitionen vorhanden sein:

```text
Partition 0
Partition 1
Partition 2
```

---

# 32. Spark Streaming Checkpoint

Spark verwendet einen Checkpoint:

```text
/tmp/checkpoints/ecommerce
```

Der Checkpoint ermöglicht Spark Structured Streaming, den Streaming-Zustand und Fortschritt zu verwalten.

Wenn bei einer Entwicklungsänderung ein komplett neuer Streaming-Zustand benötigt wird, kann der alte Checkpoint entfernt werden.

Beispielsweise:

```bash
sudo docker compose down
```

und anschließend den verwendeten Checkpoint je nach Volume-/Container-Konfiguration löschen.

**Achtung:** Checkpoints nicht während einer laufenden produktiven Pipeline löschen.

---

# 33. Dashboard Debugging

Dashboard-Container prüfen:

```bash
sudo docker compose ps
```

Logs:

```bash
sudo docker compose logs --tail=100 dashboard
```

API testen:

```bash
curl http://localhost:5000/
```

Eine konkrete API-Route muss exakt mit der in `app.py` definierten Route übereinstimmen.

Beispielsweise ist:

```bash
curl http://localhost:5000/api/history
```

nur dann gültig, wenn `/api/history` tatsächlich in der aktuellen Flask-Anwendung definiert ist.

---

# 34. Databricks Pipeline

Die Databricks-Pipeline soll langfristig folgende Verarbeitung abbilden:

```text
Kafka / Raw Events
       │
       ▼
   BRONZE
       │
       ▼
   SILVER
       │
       ▼
    GOLD
       │
       ├────────► BI
       ├────────► SQL
       ├────────► Dashboard
       └────────► Data Science
```

Databricks empfiehlt die Trennung von Ingestion und Transformation, sodass Fehler in nachgelagerten Transformationen nicht verhindern, dass Rohdaten weiterhin in Bronze landen.

---

# 35. Bronze-Ziel

Bronze soll möglichst nah an den ursprünglichen Events bleiben.

Beispiel:

```text
bronze.ecommerce_events
```

Spalten:

```text
event_id
user_id
product_id
event_type
amount
timestamp
_ingest_timestamp
_source
```

Zusätzliche Metadaten sind sinnvoll, um Herkunft und Verarbeitung nachvollziehbar zu machen.

---

# 36. Silver-Ziel

Silver verarbeitet die Rohdaten:

```text
bronze.ecommerce_events
          │
          ▼
     Validation
          │
          ▼
     Deduplication
          │
          ▼
     Type Cleaning
          │
          ▼
silver.ecommerce_events
```

Mögliche zusätzliche Spalten:

```text
revenue
event_date
event_hour
is_purchase
is_refund
```

---

# 37. Gold-Ziel

Gold enthält die Business-Metriken.

Beispiel:

```text
gold.ecommerce_daily
```

mit:

```text
date
revenue
orders
events
unique_users
refunds
refund_amount
average_order_value
```

Weitere mögliche Tabellen:

```text
gold.product_performance
gold.user_activity
gold.hourly_metrics
gold.event_type_metrics
```

---

# 38. Delta Lake

Die Databricks-Tabellen werden als Delta Tables aufgebaut.

Delta Lake bietet unter anderem ACID-Transaktionen und ist eng mit Apache Spark und Structured Streaming integriert.

Dadurch kann die Plattform sowohl Streaming- als auch Batch-Verarbeitung auf derselben persistenten Datenbasis verwenden.

---

# 39. Aktueller Status

## Fertig / funktionierend

* [x] Python Event Producer
* [x] Apache Kafka
* [x] Kafka Topic `ecommerce-events`
* [x] 3 Kafka Partitionen
* [x] Redis
* [x] Spark Structured Streaming
* [x] JSON Event Parsing
* [x] Revenue Calculation
* [x] Purchase Counting
* [x] Event Counting
* [x] Approximate Unique Users
* [x] 1-Minute Window Aggregation
* [x] Spark → Redis
* [x] Docker Compose
* [x] Flask Backend
* [x] Dashboard
* [x] Live Metrics
* [x] Databricks als Lakehouse-Komponente
* [x] Bronze/Silver/Gold Architektur

---

# 40. Noch auszubauen

Folgende Punkte sind sinnvolle nächste Schritte:

* [ ] Kafka → Databricks produktiv anbinden
* [ ] Bronze Delta Table
* [ ] Silver Cleaning Pipeline
* [ ] Gold Business Metrics
* [ ] Databricks Jobs / Workflows
* [ ] Unity Catalog
* [ ] Data Quality Checks
* [ ] Dead-Letter-/Quarantine-Layer
* [ ] Historische Dashboard-Daten
* [ ] Authentication für API
* [ ] Monitoring
* [ ] Tests
* [ ] CI/CD
* [ ] Cloud Deployment

Databricks empfiehlt für produktive Umgebungen unter anderem eine klare Storage-, Compute-, Governance- und Observability-Strategie.

---

# 41. Zielbild

Das langfristige Ziel des Projekts ist eine vollständige Data-Engineering-Plattform:

```text
                    E-COMMERCE EVENTS
                           │
                           ▼
                    ┌──────────────┐
                    │    KAFKA     │
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
       REAL-TIME PATH             DATA LAKE PATH
              │                         │
              ▼                         ▼
           SPARK                  DATABRICKS
              │                         │
              ▼                         ▼
           REDIS                    BRONZE
              │                         │
              ▼                         ▼
         FLASK API                  SILVER
              │                         │
              ▼                         ▼
         DASHBOARD                    GOLD
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼              ▼
                        BI          Analytics       ML/DS
```

Damit deckt das Projekt die wesentlichen Bereiche einer modernen Data-Engineering-Plattform ab:

```text
Event Generation
        ↓
Event Streaming
        ↓
Real-Time Processing
        ↓
Caching / Serving
        ↓
API
        ↓
Visualization
        ↓
Data Lakehouse
        ↓
Data Transformation
        ↓
Analytics
```

---

# 42. Wichtige Architekturentscheidung

Redis und Databricks haben unterschiedliche Aufgaben und sollten nicht miteinander verwechselt werden.

```text
Redis
→ kurzfristig
→ schnell
→ Live Dashboard
→ aktuelle Metriken
```

```text
Databricks / Delta Lake
→ langfristig
→ historisch
→ reproduzierbar
→ analytisch
→ Data Science / BI
```

Die eigentliche historische Quelle der Wahrheit soll perspektivisch im Lakehouse liegen, während Redis als schneller Serving Layer für die Live-Anwendung dient.

---

# 43. Zusammenfassung

Das Projekt ist eine lokale, containerisierte Real-Time-E-Commerce-Plattform mit einer zusätzlichen Databricks-Lakehouse-Schicht.

Die lokale Pipeline:

```text
Python
  ↓
Kafka
  ↓
Spark
  ↓
Redis
  ↓
Flask
  ↓
Dashboard
```

Die analytische Pipeline:

```text
Events
  ↓
Databricks
  ↓
Bronze
  ↓
Silver
  ↓
Gold
```

Damit entsteht eine Architektur, die sowohl **Real-Time Analytics** als auch **historische Data-Lake-/Lakehouse-Verarbeitung** unterstützt.

---

## Technische Referenzen

* Apache Kafka: https://kafka.apache.org/
* Apache Spark: https://spark.apache.org/
* Databricks Lakehouse: https://docs.databricks.com/
* Delta Lake: https://docs.databricks.com/aws/en/delta
* Databricks Medallion Architecture: https://docs.databricks.com/gcp/en/lakehouse/medallion

