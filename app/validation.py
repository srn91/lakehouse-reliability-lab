from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import duckdb

from app.pipeline import BuildArtifacts


@dataclass(frozen=True)
class ValidationSummary:
    bronze_rows: int
    silver_rows: int
    duplicate_event_ids: int
    latest_state_rows: int
    gold_customer_metric_rows: int
    delivered_revenue_from_silver: Decimal
    delivered_revenue_from_gold_daily: Decimal
    delivered_revenue_from_gold_customer: Decimal
    delivered_orders_from_latest_state: int
    delivered_orders_from_gold_customer: int


def validate_artifacts(artifacts: BuildArtifacts) -> ValidationSummary:
    missing_artifacts = [
        str(path)
        for path in (
            artifacts.bronze_orders,
            artifacts.silver_orders,
            artifacts.silver_latest_order_state,
            artifacts.gold_daily_region_sales,
            artifacts.gold_customer_order_metrics,
        )
        if not path.exists()
    ]
    if missing_artifacts:
        missing = ", ".join(missing_artifacts)
        raise FileNotFoundError(f"missing built artifacts: {missing}")

    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            "create or replace table bronze_orders as select * from read_parquet(?)",
            [str(artifacts.bronze_orders)],
        )
        connection.execute(
            "create or replace table silver_orders as select * from read_parquet(?)",
            [str(artifacts.silver_orders)],
        )
        connection.execute(
            "create or replace table silver_latest_order_state as select * from read_parquet(?)",
            [str(artifacts.silver_latest_order_state)],
        )
        connection.execute(
            "create or replace table gold_daily_region_sales as select * from read_parquet(?)",
            [str(artifacts.gold_daily_region_sales)],
        )
        connection.execute(
            "create or replace table gold_customer_order_metrics as select * from read_parquet(?)",
            [str(artifacts.gold_customer_order_metrics)],
        )

        bronze_rows = connection.execute("select count(*) from bronze_orders").fetchone()[0]
        silver_rows = connection.execute("select count(*) from silver_orders").fetchone()[0]
        duplicate_event_ids = connection.execute(
            """
            select count(*)
            from (
                select event_id
                from silver_orders
                group by 1
                having count(*) > 1
            )
            """
        ).fetchone()[0]
        latest_state_rows = connection.execute(
            "select count(*) from silver_latest_order_state"
        ).fetchone()[0]
        gold_customer_metric_rows = connection.execute(
            "select count(*) from gold_customer_order_metrics"
        ).fetchone()[0]
        delivered_revenue_from_silver = connection.execute(
            """
            select coalesce(cast(sum(order_amount) as decimal(12, 2)), cast(0 as decimal(12, 2)))
            from silver_latest_order_state
            where status = 'delivered'
            """
        ).fetchone()[0]
        delivered_revenue_from_gold_daily = connection.execute(
            """
            select coalesce(cast(sum(delivered_revenue) as decimal(12, 2)), cast(0 as decimal(12, 2)))
            from gold_daily_region_sales
            """
        ).fetchone()[0]
        delivered_revenue_from_gold_customer = connection.execute(
            """
            select coalesce(cast(sum(delivered_revenue) as decimal(12, 2)), cast(0 as decimal(12, 2)))
            from gold_customer_order_metrics
            """
        ).fetchone()[0]
        delivered_orders_from_latest_state = connection.execute(
            """
            select count(*)
            from silver_latest_order_state
            where status = 'delivered'
            """
        ).fetchone()[0]
        delivered_orders_from_gold_customer = connection.execute(
            """
            select coalesce(sum(delivered_orders), 0)
            from gold_customer_order_metrics
            """
        ).fetchone()[0]
        distinct_customers_from_latest_state = connection.execute(
            "select count(distinct customer_id) from silver_latest_order_state"
        ).fetchone()[0]
    finally:
        connection.close()

    if bronze_rows <= 0:
        raise ValueError("bronze layer is empty")
    if silver_rows <= 0:
        raise ValueError("silver layer is empty")
    if duplicate_event_ids != 0:
        raise ValueError("silver layer still contains duplicate event_id values")
    if latest_state_rows <= 0:
        raise ValueError("latest state table is empty")
    if gold_customer_metric_rows != distinct_customers_from_latest_state:
        raise ValueError("gold customer metrics does not cover every latest-state customer")
    if delivered_revenue_from_silver != delivered_revenue_from_gold_daily:
        raise ValueError("daily gold revenue does not reconcile with silver latest state")
    if delivered_revenue_from_silver != delivered_revenue_from_gold_customer:
        raise ValueError("customer gold revenue does not reconcile with silver latest state")
    if delivered_orders_from_latest_state != delivered_orders_from_gold_customer:
        raise ValueError("customer gold delivered order counts do not reconcile")

    return ValidationSummary(
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        duplicate_event_ids=duplicate_event_ids,
        latest_state_rows=latest_state_rows,
        gold_customer_metric_rows=gold_customer_metric_rows,
        delivered_revenue_from_silver=delivered_revenue_from_silver,
        delivered_revenue_from_gold_daily=delivered_revenue_from_gold_daily,
        delivered_revenue_from_gold_customer=delivered_revenue_from_gold_customer,
        delivered_orders_from_latest_state=delivered_orders_from_latest_state,
        delivered_orders_from_gold_customer=delivered_orders_from_gold_customer,
    )
