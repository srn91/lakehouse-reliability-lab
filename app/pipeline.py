from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from app.config import BRONZE_DIR, GOLD_DIR, RAW_DIR, SILVER_DIR


@dataclass(frozen=True)
class BuildArtifacts:
    bronze_orders: Path
    silver_orders: Path
    silver_latest_order_state: Path
    gold_daily_region_sales: Path
    gold_customer_order_metrics: Path


def _ensure_directories() -> None:
    for directory in (BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def build_all() -> BuildArtifacts:
    _ensure_directories()
    connection = duckdb.connect(database=":memory:")
    try:
        raw_glob = str(RAW_DIR / "*.csv")

        connection.execute(
            """
            create or replace table raw_orders as
            select
                filename as source_file,
                event_id,
                order_id,
                customer_id,
                region,
                status,
                cast(order_amount as double) as order_amount,
                cast(event_ts as timestamp) as event_ts,
                cast(ingestion_ts as timestamp) as ingestion_ts
            from read_csv_auto(?, filename=true)
            """,
            [raw_glob],
        )
        bronze_orders = BRONZE_DIR / "bronze_orders.parquet"
        connection.execute("copy raw_orders to ? (format parquet)", [str(bronze_orders)])

        connection.execute(
            """
            create or replace table silver_orders as
            with deduped as (
                select
                    *,
                    row_number() over (
                        partition by event_id
                        order by ingestion_ts desc, event_ts desc
                    ) as dedupe_rank
                from raw_orders
                where event_id is not null
                  and order_id is not null
                  and customer_id is not null
                  and region is not null
                  and status is not null
            )
            select
                event_id,
                order_id,
                customer_id,
                lower(region) as region,
                lower(status) as status,
                order_amount,
                event_ts,
                ingestion_ts,
                cast(event_ts as date) as event_date,
                cast(ingestion_ts as date) as ingestion_date,
                source_file
            from deduped
            where dedupe_rank = 1
            """
        )
        silver_orders = SILVER_DIR / "silver_orders.parquet"
        connection.execute("copy silver_orders to ? (format parquet)", [str(silver_orders)])

        connection.execute(
            """
            create or replace table silver_latest_order_state as
            with ranked as (
                select
                    *,
                    row_number() over (
                        partition by order_id
                        order by event_ts desc, ingestion_ts desc
                    ) as latest_rank
                from silver_orders
            )
            select
                order_id,
                customer_id,
                region,
                status,
                order_amount,
                event_ts,
                ingestion_ts,
                event_date,
                ingestion_date
            from ranked
            where latest_rank = 1
            """
        )
        silver_latest_order_state = SILVER_DIR / "silver_latest_order_state.parquet"
        connection.execute(
            "copy silver_latest_order_state to ? (format parquet)",
            [str(silver_latest_order_state)],
        )

        connection.execute(
            """
            create or replace table gold_daily_region_sales as
            select
                event_date,
                region,
                count(*) as delivered_orders,
                round(sum(order_amount), 2) as delivered_revenue
            from silver_latest_order_state
            where status = 'delivered'
            group by 1, 2
            order by 1, 2
            """
        )
        gold_daily_region_sales = GOLD_DIR / "gold_daily_region_sales.parquet"
        connection.execute(
            "copy gold_daily_region_sales to ? (format parquet)",
            [str(gold_daily_region_sales)],
        )

        connection.execute(
            """
            create or replace table gold_customer_order_metrics as
            select
                customer_id,
                count(*) as active_orders,
                sum(case when status = 'delivered' then 1 else 0 end) as delivered_orders,
                round(sum(case when status = 'delivered' then order_amount else 0 end), 2) as delivered_revenue,
                max(event_ts) as latest_order_event_ts
            from silver_latest_order_state
            group by 1
            order by 1
            """
        )
        gold_customer_order_metrics = GOLD_DIR / "gold_customer_order_metrics.parquet"
        connection.execute(
            "copy gold_customer_order_metrics to ? (format parquet)",
            [str(gold_customer_order_metrics)],
        )
    finally:
        connection.close()

    return BuildArtifacts(
        bronze_orders=bronze_orders,
        silver_orders=silver_orders,
        silver_latest_order_state=silver_latest_order_state,
        gold_daily_region_sales=gold_daily_region_sales,
        gold_customer_order_metrics=gold_customer_order_metrics,
    )


def summarize_artifacts(artifacts: BuildArtifacts) -> dict[str, str]:
    return {
        "bronze_orders": str(artifacts.bronze_orders),
        "silver_orders": str(artifacts.silver_orders),
        "silver_latest_order_state": str(artifacts.silver_latest_order_state),
        "gold_daily_region_sales": str(artifacts.gold_daily_region_sales),
        "gold_customer_order_metrics": str(artifacts.gold_customer_order_metrics),
    }
