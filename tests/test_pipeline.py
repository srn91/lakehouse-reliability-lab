from __future__ import annotations

from decimal import Decimal
from datetime import date

import duckdb

from app.pipeline import build_all


def test_build_outputs_expected_gold_metrics() -> None:
    artifacts = build_all()
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            "create or replace table gold_daily_region_sales as select * from read_parquet(?)",
            [str(artifacts.gold_daily_region_sales)],
        )
        rows = connection.execute(
            """
            select event_date, region, delivered_orders, delivered_revenue
            from gold_daily_region_sales
            order by event_date, region
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (date(2026, 4, 20), "east", 1, Decimal("120.50")),
        (date(2026, 4, 20), "west", 1, Decimal("89.00")),
        (date(2026, 4, 21), "central", 1, Decimal("140.25")),
        (date(2026, 4, 22), "east", 1, Decimal("210.00")),
        (date(2026, 4, 22), "west", 1, Decimal("45.00")),
    ]


def test_duplicate_event_is_removed_in_silver() -> None:
    artifacts = build_all()
    connection = duckdb.connect(database=":memory:")
    try:
        silver_rows = connection.execute(
            "select count(*) from read_parquet(?)",
            [str(artifacts.silver_orders)],
        ).fetchone()[0]
    finally:
        connection.close()

    assert silver_rows == 10


def test_customer_gold_metrics_reconcile_with_latest_state() -> None:
    artifacts = build_all()
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            "create or replace table gold_customer_order_metrics as select * from read_parquet(?)",
            [str(artifacts.gold_customer_order_metrics)],
        )
        rows = connection.execute(
            """
            select customer_id, active_orders, delivered_orders, delivered_revenue
            from gold_customer_order_metrics
            order by customer_id
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        ("cust-001", 2, 2, Decimal("330.50")),
        ("cust-002", 1, 1, Decimal("89.00")),
        ("cust-003", 1, 1, Decimal("140.25")),
        ("cust-004", 1, 0, Decimal("0.00")),
        ("cust-005", 1, 1, Decimal("45.00")),
    ]
