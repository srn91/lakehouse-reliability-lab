from __future__ import annotations

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
        (date(2026, 4, 20), "east", 1, 120.5),
        (date(2026, 4, 20), "west", 1, 89.0),
        (date(2026, 4, 21), "central", 1, 140.25),
        (date(2026, 4, 22), "east", 1, 210.0),
        (date(2026, 4, 22), "west", 1, 45.0),
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
