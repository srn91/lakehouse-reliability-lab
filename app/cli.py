from __future__ import annotations

import argparse
import json

from app.pipeline import build_all, expected_artifacts, summarize_artifacts
from app.scaleout import validate_scaleout_assets
from app.validation import validate_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Lakehouse reliability lab CLI")
    parser.add_argument("command", choices=["build", "validate", "validate-scaleout"])
    args = parser.parse_args()

    if args.command == "build":
        artifacts = build_all()
        print(json.dumps({"artifacts": summarize_artifacts(artifacts)}, indent=2))
        return

    if args.command == "validate-scaleout":
        summary = validate_scaleout_assets()
        print(json.dumps({"scaleout": summary.to_dict()}, indent=2))
        return

    artifacts = expected_artifacts()
    validation = validate_artifacts(artifacts)
    print(json.dumps({"validation": validation.to_dict()}, indent=2, default=str))


if __name__ == "__main__":
    main()
