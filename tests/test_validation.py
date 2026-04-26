from __future__ import annotations

import subprocess
from decimal import Decimal

from app.pipeline import build_all
from app.validation import validate_artifacts


def test_validation_summary_reconciles_gold_and_silver() -> None:
    artifacts = build_all()
    summary = validate_artifacts(artifacts)

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
