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
        assert payload["validation"]["delivered_revenue_from_silver"] == "604.75"

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
