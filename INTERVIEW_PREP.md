# 📋 Data Engineering Interview Prep

Questions this project prepares you to answer, organized by topic.

---

## 1. General Pipeline & Architecture

**Q: Walk me through an ETL pipeline you've built.**
> Talk through the project: Python producer generates order events → Kafka streams them → consumer loads to MongoDB (raw) and PostgreSQL (structured) → dbt transforms staging into analytics marts → Airflow orchestrates everything → Metabase visualizes the results. Mention the star schema, data quality tests, and REST API.

**Q: What's the difference between ETL and ELT?**
> ETL = Extract, Transform, Load — transform before writing to the target.
> ELT = Extract, Load, Transform — load raw data first, then transform in the target system.
> This project uses ELT: the consumer loads raw data into Postgres, then dbt transforms it in-place. Modern data engineering prefers ELT because storage is cheap, and you preserve the raw data.

**Q: What's the difference between batch and stream processing?**
> Batch: process data in scheduled chunks (e.g., hourly dbt runs via Airflow).
> Stream: process data as it arrives in real time (e.g., Kafka consumer).
> This project uses both: Kafka + consumer is streaming, dbt + Airflow is batch.

**Q: What would you change if this handled 1 million orders/day?**
> - Kafka: add partitions, multiple consumer instances in a consumer group
> - Postgres: partitioning on order_timestamp, read replicas
> - dbt: incremental models instead of full table rebuilds
> - Consider a data lake (S3/GCS) with Parquet files instead of loading everything into Postgres
> - Kubernetes for auto-scaling the services

---

## 2. Kafka / Message Brokers

**Q: What is Kafka and why would you use it?**
> Kafka is a distributed message broker. It decouples producers from consumers — the order service publishes events, and any number of consumers process them independently. Benefits: decoupling, scalability, fault tolerance, message persistence.

**Q: What's the difference between a Kafka topic and a partition?**
> A topic is a named category (e.g., "food_orders"). Partitions split a topic for parallelism. Messages with the same key always go to the same partition, preserving order for that key.

**Q: What's a consumer group?**
> A set of consumers that coordinate to read from a topic. Each partition is assigned to exactly one consumer in the group, so messages are processed once. Adding consumers to the group lets you scale horizontally.

**Q: What happens if a Kafka consumer crashes?**
> Kafka retains messages for a configurable period. When the consumer restarts, it picks up from its last committed offset — no data loss.

**Q: What's the difference between "at-least-once" and "exactly-once" delivery?**
> At-least-once: messages are guaranteed to be delivered but may be duplicated (our project uses this). Exactly-once: harder to achieve, requires idempotent producers and transactional consumers.

---

## 3. Databases

**Q: Why use both MongoDB and PostgreSQL?**
> Different purposes. MongoDB stores raw, semi-structured event data (data lake / audit log) — flexible schema, stores the full JSON. PostgreSQL stores clean, structured data with a star schema for analytics queries. In production, the raw store might be S3/ADLS; the structured store is the data warehouse.

**Q: What is a star schema?**
> A data modeling pattern with a central fact table (fact_orders — the events/transactions) surrounded by dimension tables (dim_customers, dim_items, dim_restaurants — the descriptive attributes). Optimized for analytical queries because you join the fact table to only the dimensions you need.

**Q: Star schema vs. snowflake schema?**
> Star: dimensions are denormalized (flat). Fewer joins, faster queries, more storage.
> Snowflake: dimensions are normalized (split into sub-tables). Less storage, more complex queries.
> Star is more common in data warehousing because query speed matters more than storage cost.

**Q: Star schema vs. normalized (3NF)?**
> Normalized: eliminates redundancy, used for OLTP (transactional apps). Many small tables, many joins.
> Star schema: allows redundancy, used for OLAP (analytics). Fewer tables, faster aggregations.
> Use normalized for the application database, star schema for the data warehouse.

**Q: What indexes did you create and why?**
> Indexes on status (frequent filter), order_timestamp (range queries, sorting), customer_id and restaurant_id (join performance), and ingested_at on raw_orders (time-based queries). Indexes speed up reads but slow down writes — balance is key.

---

## 4. SQL

**Q: What's a CTE and when would you use one?**
> Common Table Expression (WITH clause). Defines a temporary named result set within a query. Use it to break complex queries into readable steps, avoid repeating subqueries, or create recursive queries.

**Q: Explain window functions. Give an example.**
> Window functions perform calculations across related rows without collapsing them (unlike GROUP BY). Example from the project: `LAG(revenue) OVER (ORDER BY order_date)` gives the previous day's revenue for comparison. Others: ROW_NUMBER, RANK, NTILE, running SUM.

**Q: What's the difference between RANK, DENSE_RANK, and ROW_NUMBER?**
> For values [100, 100, 90]:
> - ROW_NUMBER: 1, 2, 3 (always unique)
> - RANK: 1, 1, 3 (ties get same rank, next rank skips)
> - DENSE_RANK: 1, 1, 2 (ties get same rank, no skip)

**Q: What's the difference between WHERE and HAVING?**
> WHERE filters rows before aggregation. HAVING filters groups after aggregation. Example: WHERE total_price > 5 (per row) vs. HAVING SUM(total_price) > 100 (per group).

**Q: Write a query to find customers who have ordered more than 3 times.**
```sql
SELECT customer_id, customer_name, COUNT(*) AS order_count
FROM staging.stg_orders
GROUP BY customer_id, customer_name
HAVING COUNT(*) > 3
ORDER BY order_count DESC;
```

**Q: Write a query for a moving average.**
```sql
SELECT
    customer_name,
    order_timestamp,
    total_price,
    AVG(total_price) OVER (
        PARTITION BY customer_id
        ORDER BY order_timestamp
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3
FROM staging.stg_orders;
```

---

## 5. dbt

**Q: What is dbt and why not just write raw SQL?**
> dbt is a framework around SQL. The SQL is the same — dbt adds: dependency management (ref()), testing, documentation, version control (models are .sql files in git), and idempotent runs. Like using Laravel instead of raw PHP.

**Q: What's the difference between a dbt view and a table materialization?**
> View: a saved SQL query, no data stored. Fast to build, always up to date, but slow to query on large data.
> Table: physically stores the result. Slower to build, takes storage, but fast to query.
> Use views for staging (lightweight, always current), tables for marts (pre-computed for dashboards).

**Q: What are dbt tests?**
> Schema tests (in YAML): not_null, unique, accepted_values, relationships — applied to columns.
> Custom tests (SQL files): any query that returns rows = test failure. Example: checking that delivered orders have a delivered_at timestamp.

**Q: What's the staging → mart pattern?**
> Staging: clean and standardize raw source data (rename columns, cast types, filter junk). One staging model per source.
> Marts: business-logic transformations and aggregations. Consumer-facing analytics tables. Built on top of staging models.

---

## 6. Docker

**Q: What is Docker and why use it?**
> Docker packages an application with all its dependencies into a container. Consistent across environments — "works on my machine" is solved. In this project, 9 services all run in isolated containers with one command.

**Q: What's the difference between a Docker image and a container?**
> Image: the blueprint (your Dockerfile builds it). Read-only.
> Container: a running instance of an image. You can have many containers from one image.

**Q: What is Docker Compose?**
> A tool for defining and running multi-container applications. One YAML file describes all services, their networks, volumes, and dependencies. `docker compose up` starts everything.

**Q: What are volumes in Docker?**
> Persistent storage that survives container restarts. Without volumes, all data inside a container is lost when it stops. In this project, postgres_data volume keeps the database.

---

## 7. Airflow

**Q: What is Airflow?**
> A workflow orchestration tool. You define Directed Acyclic Graphs (DAGs) that describe the order tasks should run in. It handles scheduling, retries, monitoring, and dependencies.

**Q: What's a DAG?**
> Directed Acyclic Graph — a set of tasks with defined dependencies and no circular references. Task A → Task B means B runs only after A succeeds.

**Q: What operators have you used?**
> BashOperator (runs shell commands, like dbt run), PythonOperator (runs Python functions). Others exist: PostgresOperator, S3 operators, HTTP operators, etc.

**Q: How do you handle task failures in Airflow?**
> Configure retries and retry_delay in default_args. Set email_on_failure for alerts. Use depends_on_past to prevent running if the previous schedule failed. For critical pipelines, set up Slack/PagerDuty integrations.

---

## 8. Data Modeling & Quality

**Q: How do you ensure data quality in a pipeline?**
> - dbt tests: not_null, unique, accepted_values, referential integrity
> - Custom tests: business rule validation (e.g., no negative prices)
> - Schema validation at ingestion (Pydantic in FastAPI)
> - Monitoring: track row counts, null rates, data freshness
> - Alerting: Airflow task failures trigger notifications

**Q: What is data lineage?**
> The ability to trace data from its source through every transformation to its final form. dbt provides this out of the box with its docs and the `ref()` function — you can see exactly which models depend on which.

**Q: What file formats are used in data engineering?**
> - CSV: simple, human-readable, slow, no schema
> - JSON: semi-structured, flexible, used in APIs and event logs
> - Parquet: columnar binary format, compressed, fast for analytics
> - Avro: row-based binary format, schema-embedded, good for streaming
> For analytics, Parquet is the standard. For streaming events, JSON or Avro.

---

## 9. APIs & FastAPI

**Q: What is a REST API?**
> A set of HTTP endpoints that follow REST principles. Uses standard methods: GET (read), POST (create), PATCH/PUT (update), DELETE. Stateless — each request contains all needed information.

**Q: What HTTP status codes should you know?**
> - 200: OK
> - 201: Created
> - 400: Bad Request (client error)
> - 404: Not Found
> - 422: Unprocessable Entity (validation error)
> - 500: Internal Server Error

---

## 10. Metabase / BI Tools

**Q: Have you used Tableau or Power BI?**
> I've used Metabase, which is the open-source equivalent. Same concepts: connect to a data source, build charts by selecting dimensions and measures, combine charts into dashboards, set up filters. The skills transfer — the core concepts of BI (measures vs. dimensions, aggregations, filters, chart types) are the same across all tools.

**Q: What's the difference between a measure and a dimension?**
> Dimension: categorical/descriptive (customer name, city, item category) — used to slice data.
> Measure: numeric/quantitative (revenue, order count, avg price) — what you aggregate.

---

## Tips for the Interview

1. **Always relate to this project** — when they ask about a tool, describe how you used it here.
2. **Talk about trade-offs** — "I chose X because of Y, but the trade-off is Z."
3. **Mention scale** — "This works for our dataset, but at 10M rows I would change to..."
4. **Know your SQL** — window functions, CTEs, and GROUP BY are the most common technical questions.
5. **Understand the WHY** — not just what each tool does, but why it exists and when you'd use alternatives.
