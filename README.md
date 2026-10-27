# Restaurant Data Engineering Analytics Platform

End-to-end pipeline: Python/Pandas -> Kafka -> Spark -> PostgreSQL -> Airflow -> Power BI,
with a FastAPI layer for programmatic access to KPIs.

## Project Structure
```
restaurant_data_platform/
├── data_generator/generate_data.py   # synthetic data (customers, menu, orders, ...)
├── sql/schema.sql                    # PostgreSQL schema (raw + analytics tables)
├── etl/
│   ├── clean_transform.py            # Pandas cleaning/preprocessing
│   └── load_to_postgres.py           # loads cleaned CSVs into Postgres
├── kafka/
│   ├── producer.py                   # simulates real-time order events
│   └── consumer.py                   # streams events into Postgres
├── spark/spark_transform.py          # Spark aggregations -> analytics_* tables
├── airflow/dags/restaurant_etl_dag.py# orchestrates the batch pipeline
├── api/main.py                       # FastAPI KPI endpoints
├── docker-compose.yml                # Postgres, Kafka, Zookeeper, API
├── Dockerfile.api
└── requirements.txt
```

## Quick Start (batch pipeline, no Docker)

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start PostgreSQL** (local install or `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16`)
   and create the schema:
   ```bash
   createdb restaurant_analytics
   psql -d restaurant_analytics -f sql/schema.sql
   ```

3. **Generate sample data**
   ```bash
   python data_generator/generate_data.py
   ```

4. **Clean & load**
   ```bash
   python etl/clean_transform.py
   python etl/load_to_postgres.py
   ```

5. **Run Spark aggregations** (needs the Postgres JDBC driver jar)
   ```bash
   spark-submit --jars postgresql-42.7.3.jar spark/spark_transform.py
   ```

6. **Serve the API**
   ```bash
   uvicorn api.main:app --reload --port 8000
   # try http://localhost:8000/revenue/daily
   ```

7. **Connect Power BI** to the `restaurant_analytics` Postgres database and build
   dashboards on `analytics_daily_revenue`, `analytics_top_dishes`,
   `analytics_peak_hours`, `analytics_prep_performance`.

## Real-Time Streaming Demo (optional)

```bash
docker compose up -d postgres zookeeper kafka   # start Postgres + Kafka
python kafka/consumer.py                        # in one terminal
python kafka/producer.py                        # in another terminal
```

## Orchestration with Airflow

Copy `airflow/dags/restaurant_etl_dag.py` into your Airflow `dags/` folder,
update `PROJECT_DIR` to this repo's absolute path, and the DAG
`restaurant_etl_pipeline` will run the clean -> load -> Spark-transform
sequence on the configured schedule.

## Full Stack with Docker Compose

```bash
docker compose up -d
```
Brings up PostgreSQL (schema auto-applied), Kafka + Zookeeper, and the FastAPI
service on port 8000. Run the data generator, ETL scripts, and Spark job
against `PG_HOST=localhost` as in the Quick Start above.
