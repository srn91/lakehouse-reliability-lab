from __future__ import annotations

import argparse
import json

from app.pipeline import build_all, summarize_artifacts
from app.validation import validate_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse reliability lab CLI")
    parser.add_argument("command", choices=["build", "validate"])
    args = parser.parse_args()

    artifacts = build_all()

    if args.command == "build":
        print(json.dumps({"artifacts": summarize_artifacts(artifacts)}, indent=2))
        return

    validation = validate_artifacts(artifacts)
    print(
        json.dumps(
            {
                "validation": {
                    "bronze_rows": validation.bronze_rows,
                    "silver_rows": validation.silver_rows,
                    "duplicate_event_ids": validation.duplicate_event_ids,
                    "latest_state_rows": validation.latest_state_rows,
                    "delivered_revenue_from_silver": validation.delivered_revenue_from_silver,
                    "delivered_revenue_from_gold": validation.delivered_revenue_from_gold,
                }
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

