# Food Order Analytics Pipeline

An end-to-end data engineering pipeline that simulates a food delivery platform. Orders are generated in real time, streamed through a message broker, stored in both a relational data warehouse and a document-based data lake, transformed into analytics-ready models, and visualized on a live dashboard.

## Architecture

```
[Python Producer] ──→ [Apache Kafka] ──→ [Python Consumer] ──→ PostgreSQL (star schema)
                                                            └─→ MongoDB (raw events)
                                                                      │
                                                              [dbt transforms]
                                                                      │
                                                            ┌─────────┴──────────┐
                                                     [Metabase dashboards]   [FastAPI REST API]
                                                                      │
                                                            [Apache Airflow]
                                                          (pipeline orchestration)
```

## Tech Stack

| Tool | Purpose |
|---|---|
| **Docker Compose** | Orchestrates all 9 services in isolated containers |
| **Python** | Order simulator (Kafka producer), event consumer, REST API |
| **Apache Kafka** | Distributed message broker — decouples data producers from consumers |
| **PostgreSQL** | Relational data warehouse with a star schema (fact + dimension tables) |
| **MongoDB** | Document store for raw event data (data lake / audit log) |
| **dbt** | SQL transformation framework — staging models, analytics marts, data quality tests |
| **FastAPI** | REST API for querying and updating order data |
| **Apache Airflow** | Workflow orchestration — schedules and monitors pipeline tasks |
| **Metabase** | BI dashboard (open-source alternative to Tableau / Power BI) |
| **DBeaver** | Desktop database GUI for exploring PostgreSQL and MongoDB |

## Data Model

The PostgreSQL warehouse uses a **star schema**:

**Fact table:**
- `fact_orders` — one row per order event (quantity, price, status, timestamps)

**Dimension tables:**
- `dim_customers` — customer profiles (name, email, city)
- `dim_items` — menu items (name, category, unit price)
- `dim_restaurants` — restaurants (name, cuisine type, city)

**Raw staging:**
- `raw_orders` — raw JSON payloads preserved as-is from Kafka

## dbt Models

### Staging
| Model | Type | Description |
|---|---|---|
| `stg_orders` | View | Cleans and enriches raw orders — joins dimensions, adds calculated fields (order hour, day of week, delivery duration) |

### Marts
| Model | Type | Description |
|---|---|---|
| `daily_revenue` | Table | Daily revenue, order count, running totals, day-over-day change (uses `LAG`, `SUM OVER`) |
| `top_items` | Table | Item popularity ranked by quantity and revenue, with category ranking (`ROW_NUMBER`, `RANK`) |
| `customer_segments` | Table | Customer segmentation by spend and frequency quartiles (`NTILE`) — VIP, Loyal, Regular, New |
| `delivery_performance` | Table | Delivery time analysis per restaurant — median, P95, speed tier (`PERCENTILE_CONT`) |

### Data Quality Tests (22 total)
- **Schema tests:** `not_null`, `unique`, `accepted_values` on key columns
- **Custom tests:** no negative prices, delivered orders must have a `delivered_at` timestamp

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/orders` | List orders with optional status filter and pagination |
| `GET` | `/orders/{id}` | Get a specific order with full details |
| `PATCH` | `/orders/{id}` | Update order status (pending → preparing → delivered) |
| `GET` | `/stats` | Aggregated analytics: total revenue, order counts, top items |

Interactive API documentation available at `http://localhost:8000/docs` (Swagger UI).

## Quick Start

**Prerequisites:** Docker Desktop, Git, DBeaver (optional)

```bash
# Clone the repository
git clone https://github.com/decarlimichelle/food-order-pipeline.git
cd food-order-pipeline

# Start infrastructure (databases + Kafka)
docker compose up -d postgres mongodb zookeeper kafka

# Start the data pipeline (producer + consumer)
docker compose up -d producer consumer

# Start the API, dbt, Airflow, and Metabase
docker compose up -d api dbt airflow metabase

# Run dbt transformations
docker exec pipeline-dbt dbt run
docker exec pipeline-dbt dbt test

# Verify everything is running
docker compose ps
```

**Access points:**

| Service | URL |
|---|---|
| FastAPI docs | http://localhost:8000/docs |
| Airflow UI | http://localhost:8080 (admin / admin) |
| Metabase | http://localhost:3000 |
| PostgreSQL | localhost:5432 (pipeline / pipeline123) |
| MongoDB | localhost:27017 (pipeline / pipeline123) |

## Stopping the Pipeline

```bash
# Stop all containers
docker compose down

# Stop and remove all data
docker compose down -v
```

## Project Structure

```
food-order-pipeline/
├── docker-compose.yml
├── init-scripts/
│   └── postgres-init.sql           # Star schema DDL + seed data
├── producer/
│   └── producer.py                 # Kafka order simulator
├── consumer/
│   └── consumer.py                 # Kafka → PostgreSQL + MongoDB
├── api/
│   └── app.py                      # FastAPI REST API
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_orders.sql      # Cleaned + enriched orders
│   │   └── marts/
│   │       ├── daily_revenue.sql   # Revenue analytics
│   │       ├── top_items.sql       # Item popularity
│   │       ├── customer_segments.sql # Customer segmentation
│   │       └── delivery_performance.sql
│   └── tests/
│       ├── assert_no_negative_prices.sql
│       └── assert_delivered_have_timestamp.sql
└── airflow/
    └── dags/
        └── food_pipeline_dag.py    # Pipeline orchestration DAG
```
