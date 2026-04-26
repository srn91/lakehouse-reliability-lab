from __future__ import annotations

from dataclasses import dataclass

import duckdb

from app.pipeline import BuildArtifacts


@dataclass(frozen=True)
class ValidationSummary:
    bronze_rows: int
    silver_rows: int
    duplicate_event_ids: int
    latest_state_rows: int
    delivered_revenue_from_silver: float
    delivered_revenue_from_gold: float


def validate_artifacts(artifacts: BuildArtifacts) -> ValidationSummary:
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
        delivered_revenue_from_silver = connection.execute(
            """
            select coalesce(round(sum(order_amount), 2), 0)
            from silver_latest_order_state
            where status = 'delivered'
            """
        ).fetchone()[0]
        delivered_revenue_from_gold = connection.execute(
            """
            select coalesce(round(sum(delivered_revenue), 2), 0)
            from gold_daily_region_sales
            """
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
    if delivered_revenue_from_silver != delivered_revenue_from_gold:
        raise ValueError("gold revenue does not reconcile with silver latest state")

    return ValidationSummary(
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        duplicate_event_ids=duplicate_event_ids,
        latest_state_rows=latest_state_rows,
        delivered_revenue_from_silver=delivered_revenue_from_silver,
        delivered_revenue_from_gold=delivered_revenue_from_gold,
    )

