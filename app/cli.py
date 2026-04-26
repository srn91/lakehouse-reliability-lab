from __future__ import annotations

import argparse
import json

from app.pipeline import build_all, expected_artifacts, summarize_artifacts
from app.validation import validate_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse reliability lab CLI")
    parser.add_argument("command", choices=["build", "validate"])
    args = parser.parse_args()

    if args.command == "build":
        artifacts = build_all()
        print(json.dumps({"artifacts": summarize_artifacts(artifacts)}, indent=2))
        return

    artifacts = expected_artifacts()
    validation = validate_artifacts(artifacts)
    print(
        json.dumps(
            {
                "validation": {
                    "bronze_rows": validation.bronze_rows,
                    "silver_rows": validation.silver_rows,
                    "duplicate_event_ids": validation.duplicate_event_ids,
                    "latest_state_rows": validation.latest_state_rows,
                    "gold_customer_metric_rows": validation.gold_customer_metric_rows,
                    "delivered_revenue_from_silver": validation.delivered_revenue_from_silver,
                    "delivered_revenue_from_gold_daily": validation.delivered_revenue_from_gold_daily,
                    "delivered_revenue_from_gold_customer": validation.delivered_revenue_from_gold_customer,
                    "delivered_orders_from_latest_state": validation.delivered_orders_from_latest_state,
                    "delivered_orders_from_gold_customer": validation.delivered_orders_from_gold_customer,
                }
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
