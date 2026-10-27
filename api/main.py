"""
FastAPI service exposing processed restaurant KPIs from PostgreSQL.
Run with: uvicorn api.main:app --reload --port 8000
"""

import os
from typing import List

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Restaurant Analytics API", version="1.0")

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "restaurant_analytics"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}


def query(sql: str, params: tuple = ()) -> List[dict]:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


@app.get("/")
def root():
    return {"status": "ok", "service": "restaurant-analytics-api"}


@app.get("/revenue/daily")
def daily_revenue(limit: int = 30):
    rows = query(
        "SELECT order_date, total_revenue, total_orders, avg_order_value "
        "FROM analytics_daily_revenue ORDER BY order_date DESC LIMIT %s",
        (limit,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No revenue data found")
    return rows


@app.get("/dishes/top")
def top_dishes(limit: int = 10):
    return query(
        "SELECT item_name, total_quantity, total_revenue, rank_position "
        "FROM analytics_top_dishes ORDER BY rank_position ASC LIMIT %s",
        (limit,),
    )


@app.get("/hours/peak")
def peak_hours():
    return query(
        "SELECT hour_of_day, total_orders, total_revenue "
        "FROM analytics_peak_hours ORDER BY total_orders DESC"
    )


@app.get("/prep/performance")
def prep_performance(limit: int = 30):
    return query(
        "SELECT order_date, avg_prep_minutes, max_prep_minutes, cancelled_orders "
        "FROM analytics_prep_performance ORDER BY order_date DESC LIMIT %s",
        (limit,),
    )


@app.get("/customers/{customer_id}/orders")
def customer_orders(customer_id: int):
    rows = query(
        "SELECT order_id, order_time, order_type, status, total_amount "
        "FROM orders WHERE customer_id = %s ORDER BY order_time DESC",
        (customer_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No orders found for this customer")
    return rows
