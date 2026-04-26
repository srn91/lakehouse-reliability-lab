from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from app.config import DBT_PROJECT_DIR, DEPLOYMENT_DIR, ROOT_DIR, SPARK_JOB_DIR


@dataclass(frozen=True)
class ScaleoutSummary:
    spark_entrypoint: str
    spark_requirements: str
    dbt_project_name: str
    dbt_model_files: list[str]
    dbt_macro_files: list[str]
    deployment_manifest: str
    deployment_targets: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _relative(path: Path) -> str:
    return str(path.relative_to(ROOT_DIR))


def _read_yaml(path: Path) -> object:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def validate_scaleout_assets() -> ScaleoutSummary:
    spark_entrypoint = SPARK_JOB_DIR / "lakehouse_job.py"
    spark_requirements = SPARK_JOB_DIR / "requirements-spark.txt"
    dbt_project = DBT_PROJECT_DIR / "dbt_project.yml"
    deployment_manifest = DEPLOYMENT_DIR / "databricks_job.yml"

    required_paths = [
        spark_entrypoint,
        spark_requirements,
        dbt_project,
        DBT_PROJECT_DIR / "profiles.example.yml",
        DBT_PROJECT_DIR / "models" / "sources.yml",
        DBT_PROJECT_DIR / "models" / "schema.yml",
        DBT_PROJECT_DIR / "models" / "bronze" / "bronze_orders.sql",
        DBT_PROJECT_DIR / "models" / "silver" / "silver_orders.sql",
        DBT_PROJECT_DIR / "models" / "silver" / "silver_latest_order_state.sql",
        DBT_PROJECT_DIR / "models" / "gold" / "gold_daily_region_sales.sql",
        DBT_PROJECT_DIR / "models" / "gold" / "gold_customer_order_metrics.sql",
        DBT_PROJECT_DIR / "macros" / "as_decimal_money.sql",
        deployment_manifest,
    ]
    missing_paths = [str(path) for path in required_paths if not path.exists()]
    if missing_paths:
        raise FileNotFoundError(f"missing scale-out assets: {', '.join(missing_paths)}")

    spark_source = spark_entrypoint.read_text(encoding="utf-8")
    expected_symbols = [
        "SparkSession",
        "bronze_orders.parquet",
        "silver_orders.parquet",
        "silver_latest_order_state.parquet",
        "gold_daily_region_sales.parquet",
        "gold_customer_order_metrics.parquet",
    ]
    missing_symbols = [symbol for symbol in expected_symbols if symbol not in spark_source]
    if missing_symbols:
        raise ValueError(f"spark job is missing expected symbols: {', '.join(missing_symbols)}")

    dbt_project_payload = _read_yaml(dbt_project)
    if not isinstance(dbt_project_payload, dict) or dbt_project_payload.get("name") != "lakehouse_reliability_lab":
        raise ValueError("dbt_project.yml does not define the expected lakehouse_reliability_lab project")

    dbt_model_files = sorted(
        _relative(path)
        for path in (DBT_PROJECT_DIR / "models").rglob("*.sql")
    )
    dbt_macro_files = sorted(
        _relative(path)
        for path in (DBT_PROJECT_DIR / "macros").rglob("*.sql")
    )

    schema_payload = _read_yaml(DBT_PROJECT_DIR / "models" / "schema.yml")
    if not isinstance(schema_payload, dict) or "models" not in schema_payload:
        raise ValueError("dbt schema.yml is missing model test definitions")

    deployment_payload = _read_yaml(deployment_manifest)
    resources = deployment_payload.get("resources", {}) if isinstance(deployment_payload, dict) else {}
    jobs = resources.get("jobs", {}) if isinstance(resources, dict) else {}
    if "lakehouse_reliability_lab" not in jobs:
        raise ValueError("deployment manifest is missing the lakehouse_reliability_lab job definition")

    job_definition = jobs["lakehouse_reliability_lab"]
    tasks = job_definition.get("tasks", [])
    if not isinstance(tasks, list) or len(tasks) < 2:
        raise ValueError("deployment manifest must include both Spark and dbt tasks")

    deployment_text = deployment_manifest.read_text(encoding="utf-8")
    if "spark_job/lakehouse_job.py" not in deployment_text or "project_directory: ../dbt" not in deployment_text:
        raise ValueError("deployment manifest is not wired to the repo Spark/dbt assets")

    deployment_targets = [task.get("task_key", "unknown") for task in tasks if isinstance(task, dict)]
    return ScaleoutSummary(
        spark_entrypoint=_relative(spark_entrypoint),
        spark_requirements=_relative(spark_requirements),
        dbt_project_name=str(dbt_project_payload["name"]),
        dbt_model_files=dbt_model_files,
        dbt_macro_files=dbt_macro_files,
        deployment_manifest=_relative(deployment_manifest),
        deployment_targets=deployment_targets,
    )
