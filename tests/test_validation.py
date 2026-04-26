from __future__ import annotations

import subprocess
from decimal import Decimal
from pathlib import Path

from app.pipeline import build_all
from app.schema import inspect_raw_schema
from app.validation import validate_artifacts


def test_validation_summary_reconciles_gold_and_silver() -> None:
    artifacts = build_all()
    summary = validate_artifacts(artifacts)

    assert summary.schema_files_checked == 2
    assert summary.schema_additive_columns == {
        "orders_batch_1.csv": [],
        "orders_batch_2.csv": [],
    }
    assert summary.bronze_rows == 11
    assert summary.silver_rows == 10
    assert summary.duplicate_event_ids == 0
    assert summary.latest_state_rows == 6
    assert summary.gold_customer_metric_rows == 5
    assert summary.delivered_revenue_from_silver == Decimal("604.75")
    assert summary.delivered_revenue_from_gold_daily == Decimal("604.75")
    assert summary.delivered_revenue_from_gold_customer == Decimal("604.75")
    assert summary.delivered_orders_from_latest_state == 5
    assert summary.delivered_orders_from_gold_customer == 5
    assert [check.layer for check in summary.layer_freshness] == [
        "bronze_orders",
        "silver_orders",
        "silver_latest_order_state",
        "gold_customer_order_metrics",
        "gold_daily_region_sales",
    ]
    assert all(check.status == "healthy" for check in summary.layer_freshness)
    assert summary.layer_freshness[0].lag_minutes == 0
    assert summary.layer_freshness[-1].lag_days == 0


def test_cli_validate_does_not_rebuild_existing_artifacts() -> None:
    artifacts = build_all()
    original_mtime = artifacts.gold_daily_region_sales.stat().st_mtime_ns

    completed = subprocess.run(
        ["python3", "-m", "app.cli", "validate"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert artifacts.gold_daily_region_sales.stat().st_mtime_ns == original_mtime
    assert '"layer_freshness"' in completed.stdout


def test_schema_compatibility_allows_additive_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders_with_additive_column.csv"
    csv_path.write_text(
        "\n".join(
            [
                "event_id,order_id,customer_id,region,status,order_amount,event_ts,ingestion_ts,source_system",
                "evt-2001,ord-200,cust-200,east,created,10.00,2026-04-20 09:00:00,2026-04-20 09:05:00,erp-a",
            ]
        ),
        encoding="utf-8",
    )

    summary = inspect_raw_schema([csv_path])

    assert summary.issues == []
    assert summary.additive_columns == {"orders_with_additive_column.csv": ["source_system"]}


def test_schema_compatibility_rejects_breaking_changes(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders_with_breaking_change.csv"
    csv_path.write_text(
        "\n".join(
            [
                "event_id,order_id,customer_id,region,state,order_amount,event_ts,ingestion_ts",
                "evt-2002,ord-201,cust-201,east,created,not-a-decimal,2026-04-20 09:00:00,2026-04-20 09:05:00",
            ]
        ),
        encoding="utf-8",
    )

    summary = inspect_raw_schema([csv_path])

    assert any(issue.issue == "missing_required_column" for issue in summary.issues)
    assert any(issue.column == "status" for issue in summary.issues)
    assert any(issue.column == "order_amount" and issue.issue == "incompatible_column_type" for issue in summary.issues)
