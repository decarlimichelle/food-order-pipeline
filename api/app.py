"""
FastAPI — Order Management API

REST endpoints to read and update orders in PostgreSQL.
Access the auto-generated docs at http://localhost:8000/docs
"""

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# ─── App Setup ───────────────────────────────────────────────
app = FastAPI(
    title="Food Order Pipeline API",
    description="REST API for reading and updating food delivery orders",
    version="1.0.0",
)

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
    "dbname": os.getenv("POSTGRES_DB", "food_orders"),
}

VALID_STATUSES = ["pending", "preparing", "out_for_delivery", "delivered", "cancelled"]


# ─── Database Helper ─────────────────────────────────────────
@contextmanager
def get_db_cursor():
    """Context manager for database connections."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


# ─── Pydantic Models ─────────────────────────────────────────
class OrderUpdate(BaseModel):
    """Schema for updating an order's status."""
    status: str

    class Config:
        json_schema_extra = {
            "example": {"status": "preparing"}
        }


class StatsResponse(BaseModel):
    """Schema for the /stats endpoint response."""
    total_orders: int
    total_revenue: float
    avg_order_value: float
    orders_by_status: dict
    top_items: list


# ─── Endpoints ───────────────────────────────────────────────

@app.get("/")
def root():
    """Health check."""
    return {"status": "ok", "service": "Food Order Pipeline API"}


@app.get("/orders")
def list_orders(
    status: Optional[str] = Query(None, description="Filter by order status"),
    limit: int = Query(50, ge=1, le=500, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """
    List all orders with optional status filter and pagination.

    Examples:
      GET /orders
      GET /orders?status=pending
      GET /orders?status=delivered&limit=10
    """
    with get_db_cursor() as cursor:
        if status:
            if status not in VALID_STATUSES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status. Must be one of: {VALID_STATUSES}",
                )
            cursor.execute(
                """
                SELECT fo.*, dc.customer_name, di.item_name, dr.restaurant_name
                FROM fact_orders fo
                JOIN dim_customers dc ON fo.customer_id = dc.customer_id
                JOIN dim_items di ON fo.item_id = di.item_id
                JOIN dim_restaurants dr ON fo.restaurant_id = dr.restaurant_id
                WHERE fo.status = %s
                ORDER BY fo.order_timestamp DESC
                LIMIT %s OFFSET %s
                """,
                (status, limit, offset),
            )
        else:
            cursor.execute(
                """
                SELECT fo.*, dc.customer_name, di.item_name, dr.restaurant_name
                FROM fact_orders fo
                JOIN dim_customers dc ON fo.customer_id = dc.customer_id
                JOIN dim_items di ON fo.item_id = di.item_id
                JOIN dim_restaurants dr ON fo.restaurant_id = dr.restaurant_id
                ORDER BY fo.order_timestamp DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
        orders = cursor.fetchall()

    return {"count": len(orders), "orders": orders}


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    """Get details of a specific order by ID."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT fo.*, dc.customer_name, dc.email, di.item_name,
                   di.category, dr.restaurant_name
            FROM fact_orders fo
            JOIN dim_customers dc ON fo.customer_id = dc.customer_id
            JOIN dim_items di ON fo.item_id = di.item_id
            JOIN dim_restaurants dr ON fo.restaurant_id = dr.restaurant_id
            WHERE fo.order_id = %s
            """,
            (order_id,),
        )
        order = cursor.fetchone()

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    return order


@app.patch("/orders/{order_id}")
def update_order_status(order_id: int, update: OrderUpdate):
    """
    Update an order's status.

    Valid transitions: pending → preparing → out_for_delivery → delivered
    Can also be set to: cancelled
    """
    if update.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {VALID_STATUSES}",
        )

    with get_db_cursor() as cursor:
        # Set delivered_at timestamp when status becomes 'delivered'
        if update.status == "delivered":
            cursor.execute(
                """
                UPDATE fact_orders
                SET status = %s, delivered_at = NOW()
                WHERE order_id = %s
                RETURNING order_id, status, delivered_at
                """,
                (update.status, order_id),
            )
        else:
            cursor.execute(
                """
                UPDATE fact_orders
                SET status = %s
                WHERE order_id = %s
                RETURNING order_id, status
                """,
                (update.status, order_id),
            )

        result = cursor.fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Order not found")

    return {"message": "Order updated", "order": result}


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    """
    Aggregated statistics from the dbt-transformed data
    (or raw fact_orders if dbt hasn't run yet).
    """
    with get_db_cursor() as cursor:
        # Total orders and revenue
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(total_price), 0) AS total_revenue,
                COALESCE(AVG(total_price), 0) AS avg_order_value
            FROM fact_orders
            """
        )
        summary = cursor.fetchone()

        # Orders by status
        cursor.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM fact_orders
            GROUP BY status
            ORDER BY count DESC
            """
        )
        status_rows = cursor.fetchall()
        orders_by_status = {row["status"]: row["count"] for row in status_rows}

        # Top 5 items
        cursor.execute(
            """
            SELECT di.item_name, SUM(fo.quantity) AS total_qty,
                   SUM(fo.total_price) AS total_revenue
            FROM fact_orders fo
            JOIN dim_items di ON fo.item_id = di.item_id
            GROUP BY di.item_name
            ORDER BY total_qty DESC
            LIMIT 5
            """
        )
        top_items = cursor.fetchall()

    return StatsResponse(
        total_orders=summary["total_orders"],
        total_revenue=float(summary["total_revenue"]),
        avg_order_value=round(float(summary["avg_order_value"]), 2),
        orders_by_status=orders_by_status,
        top_items=top_items,
    )
