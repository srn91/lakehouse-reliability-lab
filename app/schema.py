from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from app.config import RAW_DIR


RAW_SCHEMA_CONTRACT: dict[str, set[str]] = {
    "event_id": {"VARCHAR"},
    "order_id": {"VARCHAR"},
    "customer_id": {"VARCHAR"},
    "region": {"VARCHAR"},
    "status": {"VARCHAR"},
    "order_amount": {"DECIMAL", "DOUBLE", "FLOAT", "INTEGER", "BIGINT", "HUGEINT"},
    "event_ts": {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DATE"},
    "ingestion_ts": {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DATE"},
}


@dataclass(frozen=True)
class SchemaIssue:
    file_name: str
    column: str
    issue: str
    expected: str
    observed: str


@dataclass(frozen=True)
class SchemaCompatibilitySummary:
    files_checked: int
    additive_columns: dict[str, list[str]]
    issues: list[SchemaIssue]


def _inspect_columns(csv_path: Path) -> dict[str, str]:
    connection = duckdb.connect(database=":memory:")
    try:
        rows = connection.execute(
            "describe select * from read_csv_auto(?)",
            [str(csv_path)],
        ).fetchall()
    finally:
        connection.close()

    return {str(name): str(type_name).upper() for name, type_name, *_ in rows}


def inspect_raw_schema(csv_paths: list[Path] | None = None) -> SchemaCompatibilitySummary:
    paths = sorted(csv_paths or RAW_DIR.glob("*.csv"))
    additive_columns: dict[str, list[str]] = {}
    issues: list[SchemaIssue] = []

    for csv_path in paths:
        observed_columns = _inspect_columns(csv_path)
        missing_columns = sorted(set(RAW_SCHEMA_CONTRACT) - set(observed_columns))
        extra_columns = sorted(set(observed_columns) - set(RAW_SCHEMA_CONTRACT))
        additive_columns[csv_path.name] = extra_columns

        for column in missing_columns:
            issues.append(
                SchemaIssue(
                    file_name=csv_path.name,
                    column=column,
                    issue="missing_required_column",
                    expected=", ".join(sorted(RAW_SCHEMA_CONTRACT[column])),
                    observed="missing",
                )
            )

        for column, allowed_types in RAW_SCHEMA_CONTRACT.items():
            if column not in observed_columns:
                continue
            observed_type = observed_columns[column]
            if observed_type not in allowed_types:
                issues.append(
                    SchemaIssue(
                        file_name=csv_path.name,
                        column=column,
                        issue="incompatible_column_type",
                        expected=", ".join(sorted(allowed_types)),
                        observed=observed_type,
                    )
                )

    return SchemaCompatibilitySummary(
        files_checked=len(paths),
        additive_columns=additive_columns,
        issues=issues,
    )


def assert_raw_schema_compatibility(csv_paths: list[Path] | None = None) -> SchemaCompatibilitySummary:
    summary = inspect_raw_schema(csv_paths)
    if summary.issues:
        details = "; ".join(
            f"{issue.file_name}:{issue.column}:{issue.issue} expected {issue.expected} observed {issue.observed}"
            for issue in summary.issues
        )
        raise ValueError(f"raw schema compatibility failed: {details}")
    return summary
