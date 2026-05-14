from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse

from app.pipeline import build_all, expected_artifacts, summarize_artifacts
from app.scaleout import validate_scaleout_assets
from app.validation import validate_artifacts


def _validation_payload(summary) -> dict[str, Any]:
    return summary.to_dict()


def _build_runtime_snapshot() -> dict[str, Any]:
    expected = expected_artifacts()

    try:
        built = build_all()
        validation = validate_artifacts(built)
        scaleout = validate_scaleout_assets()
    except Exception as exc:  # pragma: no cover - surfaced through HTTP responses
        return {
            "service": "lakehouse-reliability-lab",
            "status": "degraded",
            "error": str(exc),
            "artifacts": summarize_artifacts(expected),
            "validation": None,
            "scaleout": None,
            "commands": {
                "build": "make build",
                "validate": "make validate",
                "scaleout": "make scaleout",
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
        "scaleout": scaleout.to_dict(),
        "commands": {
            "build": "make build",
            "validate": "make validate",
            "scaleout": "make scaleout",
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


@app.get("/", response_class=HTMLResponse)
def root(request: Request) -> str:
    snapshot = _runtime_snapshot(request)
    status_text = snapshot["status"]
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lakehouse Reliability Lab</title>
<style>
body{{margin:0;background:#f8fafc;color:#0f172a;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;line-height:1.5}}
main{{max-width:1080px;margin:0 auto;padding:56px 24px}}.hero{{background:linear-gradient(135deg,#111827,#0369a1);color:white;border-radius:22px;padding:38px;box-shadow:0 24px 60px rgba(15,23,42,.18)}}
.eyebrow{{font-size:13px;letter-spacing:.12em;text-transform:uppercase;color:#bae6fd;font-weight:700}}h1{{font-size:42px;line-height:1.05;margin:10px 0 14px}}.hero p{{font-size:17px;color:#e0f2fe;max-width:780px}}
.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:22px 0}}.card{{background:white;border:1px solid #e2e8f0;border-radius:16px;padding:18px;box-shadow:0 10px 30px rgba(15,23,42,.06)}}
.metric{{font-size:25px;font-weight:800;color:#0f172a}}.label{{font-size:13px;color:#64748b;margin-top:3px}}.links{{display:flex;flex-wrap:wrap;gap:12px;margin-top:22px}}
a.button{{background:#0f172a;color:white;text-decoration:none;padding:11px 14px;border-radius:10px;font-weight:700}}a.secondary{{background:white;color:#0f172a;border:1px solid #cbd5e1}}
@media(max-width:800px){{.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}h1{{font-size:34px}}}}
</style></head>
<body><main>
<section class="hero"><div class="eyebrow">Lakehouse reliability</div><h1>Lakehouse Reliability Lab</h1>
<p>Read-only reliability surface for a medallion pipeline with deduplication, late-arrival handling, reconciliation, and validation checks.</p>
<div class="links"><a class="button" href="/summary">Reliability summary</a><a class="button secondary" href="/docs">API docs</a></div></section>
<section class="grid">
<div class="card"><div class="metric">{status_text}</div><div class="label">pipeline status</div></div>
<div class="card"><div class="metric">bronze</div><div class="label">raw layer</div></div>
<div class="card"><div class="metric">silver</div><div class="label">clean layer</div></div>
<div class="card"><div class="metric">gold</div><div class="label">serving layer</div></div>
</section>
<section class="card"><p>The summary shows freshness, reconciliation, schema checks, generated parquet paths, and deployment commands for the reliability workflow.</p></section>
</main></body></html>"""


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
