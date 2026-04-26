from __future__ import annotations

from fastapi.testclient import TestClient

from app.web import app


def test_web_surface_exposes_read_only_status_and_summary() -> None:
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json() == {
            "status": "ready",
            "artifacts_ready": True,
            "validation_ready": True,
        }

        summary = client.get("/summary")
        assert summary.status_code == 200
        payload = summary.json()
        assert payload["service"] == "lakehouse-reliability-lab"
        assert payload["status"] == "ready"
        assert payload["artifacts"]["bronze_orders"].endswith(
            "warehouse/bronze/bronze_orders.parquet"
        )
        assert payload["validation"]["bronze_rows"] == 11
        assert payload["validation"]["schema_files_checked"] == 2
        assert payload["validation"]["schema_additive_columns"] == {
            "orders_batch_1.csv": [],
            "orders_batch_2.csv": [],
        }
        assert payload["validation"]["delivered_revenue_from_silver"] == "604.75"
        assert payload["validation"]["layer_freshness"][0]["layer"] == "bronze_orders"
        assert payload["validation"]["layer_freshness"][0]["lag_minutes"] == 0
        assert payload["validation"]["layer_freshness"][-1]["layer"] == "gold_daily_region_sales"
        assert payload["validation"]["layer_freshness"][-1]["lag_days"] == 0
        assert payload["scaleout"]["spark_entrypoint"] == "spark_job/lakehouse_job.py"
        assert payload["scaleout"]["dbt_project_name"] == "lakehouse_reliability_lab"
        assert payload["scaleout"]["deployment_targets"] == [
            "spark_medallion_build",
            "dbt_transform_validation",
        ]

        root = client.get("/")
        assert root.status_code == 200
        assert root.json() == {
            "service": "lakehouse-reliability-lab",
            "health": "/health",
            "summary": "/summary",
            "docs": "/docs",
        }

        docs = client.get("/docs")
        assert docs.status_code == 200
