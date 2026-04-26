from __future__ import annotations

from app.pipeline import build_all
from app.validation import validate_artifacts


def test_validation_summary_reconciles_gold_and_silver() -> None:
    artifacts = build_all()
    summary = validate_artifacts(artifacts)

    assert summary.bronze_rows == 11
    assert summary.silver_rows == 10
    assert summary.duplicate_event_ids == 0
    assert summary.latest_state_rows == 6
    assert summary.delivered_revenue_from_silver == 604.75
    assert summary.delivered_revenue_from_gold == 604.75

