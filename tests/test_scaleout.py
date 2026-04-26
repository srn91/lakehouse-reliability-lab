from __future__ import annotations

from app.scaleout import validate_scaleout_assets


def test_scaleout_assets_cover_spark_dbt_and_deployment() -> None:
    summary = validate_scaleout_assets()

    assert summary.spark_entrypoint == "spark_job/lakehouse_job.py"
    assert summary.spark_requirements == "spark_job/requirements-spark.txt"
    assert summary.dbt_project_name == "lakehouse_reliability_lab"
    assert len(summary.dbt_model_files) == 5
    assert "dbt/models/gold/gold_daily_region_sales.sql" in summary.dbt_model_files
    assert summary.dbt_macro_files == ["dbt/macros/as_decimal_money.sql"]
    assert summary.deployment_manifest == "deployment/databricks_job.yml"
    assert summary.deployment_targets == ["spark_medallion_build", "dbt_transform_validation"]
