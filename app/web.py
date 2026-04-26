from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status

from app.pipeline import build_all, expected_artifacts, summarize_artifacts
from app.validation import validate_artifacts


def _validation_payload(summary) -> dict[str, Any]:
    return {
        "schema_files_checked": summary.schema_files_checked,
        "schema_additive_columns": summary.schema_additive_columns,
        "bronze_rows": summary.bronze_rows,
        "silver_rows": summary.silver_rows,
        "duplicate_event_ids": summary.duplicate_event_ids,
        "latest_state_rows": summary.latest_state_rows,
        "gold_customer_metric_rows": summary.gold_customer_metric_rows,
        "delivered_revenue_from_silver": str(summary.delivered_revenue_from_silver),
        "delivered_revenue_from_gold_daily": str(summary.delivered_revenue_from_gold_daily),
        "delivered_revenue_from_gold_customer": str(summary.delivered_revenue_from_gold_customer),
        "delivered_orders_from_latest_state": summary.delivered_orders_from_latest_state,
        "delivered_orders_from_gold_customer": summary.delivered_orders_from_gold_customer,
    }


def _build_runtime_snapshot() -> dict[str, Any]:
    expected = expected_artifacts()

    try:
        built = build_all()
        validation = validate_artifacts(built)
    except Exception as exc:  # pragma: no cover - surfaced through HTTP responses
        return {
            "service": "lakehouse-reliability-lab",
            "status": "degraded",
            "error": str(exc),
            "artifacts": summarize_artifacts(expected),
            "validation": None,
            "commands": {
                "build": "make build",
                "validate": "make validate",
                "verify": "make verify",
                "serve": "make serve",
            },
            "deployment": {
                "platform": "Render",
                "health_check_path": "/health",
                "docs_path": "/docs",
            },
        }

    return {
        "service": "lakehouse-reliability-lab",
        "status": "ready",
        "artifacts": summarize_artifacts(built),
        "validation": _validation_payload(validation),
        "commands": {
            "build": "make build",
            "validate": "make validate",
            "verify": "make verify",
            "serve": "make serve",
        },
        "deployment": {
            "platform": "Render",
            "health_check_path": "/health",
            "docs_path": "/docs",
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.runtime_snapshot = _build_runtime_snapshot()
    yield


app = FastAPI(
    title="lakehouse-reliability-lab",
    summary="Read-only lakehouse reliability surface for the medallion pipeline demo.",
    version="0.1.0",
    lifespan=lifespan,
)


def _runtime_snapshot(request: Request) -> dict[str, Any]:
    snapshot = getattr(request.app.state, "runtime_snapshot", None)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="runtime snapshot is not available yet",
        )
    return snapshot


@app.get("/")
def root(request: Request) -> dict[str, str]:
    _runtime_snapshot(request)
    return {
        "service": "lakehouse-reliability-lab",
        "health": "/health",
        "summary": "/summary",
        "docs": "/docs",
    }


@app.get("/health")
def health(request: Request) -> dict[str, Any]:
    snapshot = _runtime_snapshot(request)
    if snapshot["status"] != "ready":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=snapshot["error"],
        )

    return {
        "status": snapshot["status"],
        "artifacts_ready": True,
        "validation_ready": True,
    }


@app.get("/summary")
def summary(request: Request) -> dict[str, Any]:
    return _runtime_snapshot(request)
