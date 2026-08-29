# Realtime E-Commerce Platform

Eine containerisierte **Realtime-E-Commerce-Datenplattform**, die Events über Kafka empfängt, mit Apache Spark Structured Streaming verarbeitet, aggregierte Echtzeit-Metriken in Redis speichert und über ein Dashboard visualisiert.

Zusätzlich ist **Databricks** als Bestandteil der geplanten bzw. erweiterten Data-Lake-/Analytics-Schicht vorgesehen.

---

## Architektur

![Realtime E-Commerce Platform](docs/dashboard.png)

### Datenfluss

```text
                    ┌─────────────────────┐
                    │   E-Commerce        │
                    │   Producer          │
                    │   producer.py       │
                    └──────────┬──────────┘
                               │
                               │ JSON Events
                               ▼
                    ┌─────────────────────┐
                    │       Kafka         │
                    │                     │
                    │ ecommerce-events    │
                    │ 3 Partitions        │
                    └──────────┬──────────┘
                               │
                               │ Streaming
                               ▼
                    ┌─────────────────────┐
                    │   Spark Structured  │
                    │      Streaming      │
                    │                     │
                    │ - JSON Parsing      │
                    │ - Revenue          │
                    │ - Orders            │
                    │ - Events            │
                    │ - Unique Users      │
                    │ - 1-Minute Windows  │
                    └──────────┬──────────┘
                               │
                               │ Aggregated Metrics
                               ▼
                    ┌─────────────────────┐
                    │       Redis         │
                    │                     │
                    │ ecommerce:metrics   │
                    └──────────┬──────────┘
                               │
                               │ HTTP/API
                               ▼
                    ┌─────────────────────┐
                    │      Dashboard      │
                    │                     │
                    │ Flask + Frontend    │
                    │ Realtime Metrics    │
                    └─────────────────────┘


             ┌────────────────────────────────┐
             │          Databricks            │
             │                                │
             │ Data Lake / Analytics Layer    │
             │ Historical Data / BI / ML      │
             └────────────────────────────────┘
```

---

# 1. Projektziel

Das Projekt bildet eine typische moderne Realtime-Data-Engineering-Pipeline für einen E-Commerce-Anwendungsfall ab.

Dabei werden kontinuierlich Events erzeugt, über Kafka transportiert und anschließend mit Spark verarbeitet.

Beispielsweise:

* `view_product`
* `add_to_cart`
* `purchase`
* `refund`

Aus diesen Events werden Echtzeitmetriken berechnet.

Aktuell werden unter anderem folgende Kennzahlen verarbeitet:

| Metrik         | Beschreibung                                  |
| -------------- | --------------------------------------------- |
| `revenue`      | Umsatz inklusive Berücksichtigung von Refunds |
| `orders`       | Anzahl der Käufe                              |
| `events`       | Anzahl aller Events                           |
| `unique_users` | Anzahl unterschiedlicher Benutzer             |
| `window_start` | Beginn des Zeitfensters                       |
| `window_end`   | Ende des Zeitfensters                         |

Die Aggregation erfolgt aktuell in **1-Minuten-Zeitfenstern**.

---

# 2. Technologie-Stack

## Core

* Python
* Docker
* Docker Compose

## Streaming

* Apache Kafka
* Apache Spark Structured Streaming

## Storage / Cache

* Redis

## Backend

* Flask
* REST API

## Frontend

* Web Dashboard
* Realtime-Metriken

## Data Lake / Analytics

* Databricks
* perspektivisch Delta Lake
* historische Daten
* Analytics / BI
* Machine Learning

---

# 3. Projektstruktur

Die aktuelle Projektstruktur ist ungefähr:

```text
realtime-ecommerce-platform/
│
├── docker-compose.yml
│
├── README.md
│
├── producer/
│   ├── producer.py
│   └── requirements.txt
│
├── spark/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streaming.py
│
├── dashboard/
│   ├── Dockerfile
│   ├── app.py
│   ├── requirements.txt
│   └── templates/
│       └── ...
│
├── docs/
│   └── dashboard.png
│
└── ...
```

Die tatsächlichen Dateien können je nach aktuellem lokalen Stand noch leicht abweichen.

---

# 4. Docker Services

Die Anwendung wird über Docker Compose betrieben.

Die wichtigsten Services sind:

```text
ecommerce-kafka
ecommerce-redis
ecommerce-spark
ecommerce-dashboard
```

Kafka stellt den Event-Stream bereit.

Redis speichert die aktuellen aggregierten Kennzahlen.

Spark liest die Events aus Kafka und verarbeitet sie.

Das Dashboard greift auf die in Redis gespeicherten Metriken zu.

---

# 5. Kafka

Kafka bildet die zentrale Streaming-Schicht.

Der verwendete Topic ist:

```text
ecommerce-events
```

Aktuell besitzt der Topic:

```text
3 Partitions
Replication Factor: 1
```

## Topic anzeigen

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Erwartete Ausgabe:

```text
ecommerce-events
```

## Topic detailliert anzeigen

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic ecommerce-events
```

Beispiel:

```text
PartitionCount: 3
ReplicationFactor: 1
```

---

# 6. E-Commerce Events

Der Producer erzeugt JSON-Events.

Ein Event besitzt aktuell folgende Struktur:

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

Das Schema wird in Spark definiert als:

```text
event_id     String
user_id      Integer
product_id   Integer
event_type   String
amount       Double
timestamp    Timestamp
```

---

# 7. Producer

Der Producer erzeugt Testdaten und schreibt diese nach Kafka.

Start beispielsweise:

```bash
python3 producer/producer.py
```

Falls das Kafka-Python-Modul fehlt:

```bash
pip install kafka-python
```

oder über die vorhandenen Requirements:

```bash
pip install -r producer/requirements.txt
```

Der Producer muss Kafka erreichen können.

---

# 8. Spark Structured Streaming

Spark liest den Kafka-Topic:

```text
ecommerce-events
```

Innerhalb des Docker-Netzwerks wird Kafka über:

```text
kafka:29092
```

angesprochen.

Spark verarbeitet den JSON-Stream und wandelt die Events in strukturierte Daten um.

---

# 9. Revenue-Berechnung

Der Umsatz wird anhand des Event-Typs berechnet.

Für einen Kauf:

```text
purchase -> +amount
```

Für eine Rückerstattung:

```text
refund -> -amount
```

Alle anderen Events:

```text
view_product -> 0
add_to_cart  -> 0
```

Beispiel:

```text
purchase  999.99
refund    149.99
```

ergibt:

```text
revenue = 849.99
```

---

# 10. Zeitfenster

Spark aggregiert die Daten aktuell in:

```text
1-Minute Windows
```

Beispiel:

```text
13:50:00 - 13:51:00
13:51:00 - 13:52:00
13:52:00 - 13:53:00
```

Dadurch entstehen fortlaufende Echtzeitmetriken.

Beispiel:

```text
window_start        window_end          revenue   orders  events  unique_users
2026-08-29 13:50:00 2026-08-29 13:51:00 4030.06   23      88      85
2026-08-29 13:51:00 2026-08-29 13:52:00 2369.99   26      100     99
```

---

# 11. Unique Users

Spark Structured Streaming unterstützt in dieser Konfiguration keine normale:

```python
countDistinct()
```

Aggregation.

Daher wird:

```python
approx_count_distinct()
```

verwendet.

Das liefert eine effiziente Näherung der Anzahl unterschiedlicher Benutzer und funktioniert für den Streaming-Anwendungsfall.

---

# 12. Redis

Redis speichert die aktuell berechneten Metriken.

Der verwendete Redis-Key ist:

```text
ecommerce:metrics
```

Die Werte enthalten beispielsweise:

```text
revenue
orders
events
unique_users
window_start
window_end
```

---

# 13. Redis testen

Verbindung testen:

```bash
sudo docker exec ecommerce-redis redis-cli ping
```

Erwartete Ausgabe:

```text
PONG
```

Metriken anzeigen:

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

Damit lässt sich überprüfen, ob Spark erfolgreich nach Redis schreibt.

---

# 14. Dashboard

Das Dashboard stellt die Redis-Metriken über eine Weboberfläche dar.

Die typische Architektur ist:

```text
Browser
   │
   ▼
Flask
   │
   ▼
Redis
   │
   ▼
ecommerce:metrics
```

Das Dashboard zeigt die aktuellen Streaming-Daten beispielsweise als:

* Revenue
* Orders
* Events
* Unique Users
* aktuelles Zeitfenster

---

# 15. API

Das Flask-Backend stellt die Daten über HTTP bereit.

Ein möglicher Endpoint ist beispielsweise:

```text
/api/metrics
```

Der Endpoint liest die aktuellen Daten aus:

```text
ecommerce:metrics
```

und liefert sie als JSON zurück.

Beispiel:

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

# 16. Docker starten

Alle Services starten:

```bash
sudo docker compose up -d
```

Status prüfen:

```bash
sudo docker compose ps
```

Beispiel:

```text
NAME                 SERVICE     STATUS
ecommerce-kafka      kafka       Up
ecommerce-redis      redis       Up
ecommerce-spark      spark       Up
ecommerce-dashboard  dashboard   Up
```

---

# 17. Logs prüfen

### Kafka

```bash
sudo docker compose logs --tail=100 kafka
```

### Redis

```bash
sudo docker compose logs --tail=100 redis
```

### Spark

```bash
sudo docker compose logs --tail=100 spark
```

### Dashboard

```bash
sudo docker compose logs --tail=100 dashboard
```

Live-Logs:

```bash
sudo docker compose logs -f spark
```

---

# 18. Debugging der Pipeline

Die Pipeline sollte beim Debugging von links nach rechts überprüft werden.

## Schritt 1 – Container

```bash
sudo docker compose ps
```

Alle benötigten Container müssen laufen.

---

## Schritt 2 – Kafka

Topic prüfen:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Es muss erscheinen:

```text
ecommerce-events
```

---

## Schritt 3 – Producer

Producer starten:

```bash
python3 producer/producer.py
```

Es müssen Events erzeugt werden.

---

## Schritt 4 – Spark

Logs beobachten:

```bash
sudo docker compose logs -f spark
```

Erwartet werden Batches wie:

```text
Spark Batch: 80
```

mit Daten:

```text
window_start
window_end
revenue
orders
events
unique_users
```

---

## Schritt 5 – Redis

Redis testen:

```bash
sudo docker exec ecommerce-redis redis-cli ping
```

Danach:

```bash
sudo docker exec ecommerce-redis \
  redis-cli HGETALL ecommerce:metrics
```

Wenn dort Werte vorhanden sind, funktioniert die Verarbeitungskette:

```text
Producer
   ↓
Kafka
   ↓
Spark
   ↓
Redis
```

---

## Schritt 6 – Dashboard

Danach das Dashboard im Browser öffnen und überprüfen, ob die Redis-Werte angezeigt werden.

---

# 19. Docker Network

Die Services müssen sich innerhalb desselben Docker-Netzwerks befinden.

Aktuell verwendet Docker Compose beispielsweise:

```text
realtime-ecommerce-platform_default
```

Netzwerk anzeigen:

```bash
sudo docker network ls
```

Details:

```bash
sudo docker network inspect realtime-ecommerce-platform_default
```

Kafka und Spark sollten dort gemeinsam vorhanden sein.

Redis muss ebenfalls mit diesem Netzwerk verbunden sein, damit Spark:

```text
redis:6379
```

erreichen kann.

---

# 20. Typische Fehler

## `ModuleNotFoundError: No module named 'kafka'`

Der Producer benötigt:

```text
kafka-python
```

Installation:

```bash
pip install kafka-python
```

---

## `countDistinct` wird von Streaming nicht unterstützt

Fehler:

```text
Distinct aggregations are not supported on streaming DataFrames/Datasets
```

Lösung:

```python
approx_count_distinct("user_id")
```

anstatt:

```python
countDistinct("user_id")
```

---

## Redis kann nicht aufgelöst werden

Fehler:

```text
Temporary failure in name resolution
```

Dann prüfen:

```bash
sudo docker network inspect realtime-ecommerce-platform_default
```

und sicherstellen, dass Spark und Redis im selben Docker-Netzwerk sind.

Verbindung aus Spark testen:

```bash
sudo docker exec ecommerce-spark \
  python3 -c "import socket; print(socket.gethostbyname('redis')); print(socket.create_connection(('redis',6379,5)))"
```

Wenn eine IP-Adresse und ein Socket ausgegeben werden, funktioniert die Netzwerkverbindung.

---

## Kafka `UnknownTopicOrPartitionException`

Fehler:

```text
UnknownTopicOrPartitionException
```

Topic prüfen:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Falls der Topic fehlt:

```bash
sudo docker exec ecommerce-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic ecommerce-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

Danach:

```bash
sudo docker compose restart spark
```

---

# 21. Data Lake / Databricks

Neben der Realtime-Pipeline ist **Databricks** für die historische Speicherung und Analyse vorgesehen.

Die Architektur kann dadurch um eine Data-Lake-Schicht erweitert werden:

```text
                         Realtime Layer
                              │
                              ▼
Producer → Kafka → Spark Streaming → Redis → Dashboard
                  │
                  │
                  ▼
             Data Lake
                  │
                  ▼
              Databricks
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
      SQL       BI        ML
```

---

# 22. Zielarchitektur mit Data Lake

Eine langfristige Architektur sieht beispielsweise so aus:

```text
                     ┌───────────────┐
                     │    Producer   │
                     └───────┬───────┘
                             │
                             ▼
                     ┌───────────────┐
                     │     Kafka     │
                     └───────┬───────┘
                             │
                 ┌───────────┴───────────┐
                 │                       │
                 ▼                       ▼
        ┌────────────────┐       ┌────────────────┐
        │ Spark Streaming│       │   Data Lake    │
        │                │       │                │
        │ Realtime       │       │ Raw Events     │
        │ Aggregation    │       │ Historical     │
        └───────┬────────┘       └───────┬────────┘
                │                        │
                ▼                        ▼
        ┌────────────────┐       ┌────────────────┐
        │     Redis      │       │   Databricks   │
        │                │       │                │
        │ Realtime       │       │ Delta Lake     │
        │ Metrics        │       │ Analytics      │
        └───────┬────────┘       │ ML / BI        │
                │                └────────────────┘
                ▼
        ┌────────────────┐
        │   Dashboard    │
        └────────────────┘
```

---

# 23. Bronze / Silver / Gold

Für die Data-Lake-Schicht bietet sich eine klassische Medallion Architecture an.

## Bronze

Rohdaten aus Kafka:

```text
event_id
user_id
product_id
event_type
amount
timestamp
```

Die Daten werden möglichst unverändert gespeichert.

---

## Silver

Bereinigte und normalisierte Events.

Beispielsweise:

* Validierung
* Datentypen
* Bereinigung fehlerhafter Events
* Zeitstempel-Normalisierung
* Deduplication

---

## Gold

Business-Aggregationen.

Beispielsweise:

```text
daily_revenue
daily_orders
product_revenue
customer_activity
conversion_rate
refund_rate
```

Diese Schicht kann anschließend für BI, Reporting und Machine Learning verwendet werden.

---

# 24. Realtime vs. Historical Analytics

Das Projekt trennt zwei unterschiedliche Anforderungen.

### Realtime

```text
Kafka
  ↓
Spark Streaming
  ↓
Redis
  ↓
Dashboard
```

Ziel:

```text
Sekunden-/Minutenbereich
```

Beispielsweise:

* aktueller Umsatz
* aktuelle Orders
* aktuelle Events
* aktive User

### Historical

```text
Kafka / Batch
    ↓
Data Lake
    ↓
Databricks
    ↓
Delta Lake
    ↓
Analytics / BI / ML
```

Ziel:

```text
Tage / Monate / Jahre
```

Beispielsweise:

* Umsatzentwicklung
* Kundenverhalten
* Produktanalyse
* Churn
* Forecasting

---

# 25. Aktueller Projektstand

Die Realtime-Pipeline ist aktuell funktionsfähig:

```text
Producer
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Redis
   ↓
Dashboard
```

Kafka empfängt Events.

Spark verarbeitet die Events in 1-Minuten-Fenstern.

Redis speichert die aktuellen Metriken.

Das Dashboard visualisiert diese Daten.

Die Data-Lake-/Databricks-Schicht bildet die nächste Erweiterungsstufe für historische Daten und Analytics.

---

# 26. Empfohlene nächste Schritte

## 1. Dashboard vervollständigen

Das Dashboard kann erweitert werden um:

* Revenue Chart
* Orders Chart
* Events pro Minute
* User Aktivität
* Event-Type-Verteilung
* automatische Aktualisierung

---

## 2. Historische Metriken speichern

Aktuell wird primär der aktuelle Redis-Zustand verwendet.

Als nächster Schritt können historische Aggregationen gespeichert werden.

Beispielsweise:

```text
realtime metrics
        +
historical metrics
```

---

## 3. Data Lake integrieren

Kafka-/Spark-Daten können zusätzlich in einen Data Lake geschrieben werden.

Beispielsweise:

```text
Kafka
  ↓
Spark
  ↓
Delta Lake
```

---

## 4. Databricks Analytics

Danach können Databricks Jobs und Notebooks verwendet werden für:

* SQL Analytics
* historische Reports
* Dashboards
* Produktanalyse
* Kundenanalyse
* Forecasting
* Machine Learning

---

# 27. Ziel des Projekts

Das Projekt soll eine realitätsnahe Data-Engineering-Plattform demonstrieren, die mehrere wichtige Bereiche kombiniert:

```text
Event Generation
       ↓
Streaming
       ↓
Distributed Processing
       ↓
Realtime Storage
       ↓
API
       ↓
Visualization
       ↓
Data Lake
       ↓
Analytics
       ↓
Machine Learning
```

Damit entsteht aus einem einfachen E-Commerce-Event-Generator schrittweise eine vollständige moderne **Realtime- und Data-Lake-Plattform**.

---

# 28. Kurzfassung

```text
Python Producer
      │
      ▼
    Kafka
      │
      ▼
Spark Structured Streaming
      │
      ├──────────────► Data Lake / Databricks
      │
      ▼
    Redis
      │
      ▼
 Flask Dashboard
```

**Realtime:** Kafka → Spark → Redis → Dashboard

**Historical Analytics:** Kafka/Spark → Data Lake → Databricks → BI/ML

