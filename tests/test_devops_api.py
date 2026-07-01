"""Tests de integración para la DevOps Fleet API."""

import json
import time
import pytest
from fastapi.testclient import TestClient

# Mockear env vars antes de importar la app
import os
os.environ.setdefault("MINIMAX_API_KEY", "test-key")
os.environ.setdefault("OPENROUTER_API_KEY", "test-key")
os.environ.setdefault("FLEET_DB", "/tmp/devops_test.db")

from devops_api import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_dashboard_loads():
    r = client.get("/")
    assert r.status_code == 200
    assert "DevOps Fleet" in r.text


def test_trigger_deploy_returns_job_id():
    r = client.post("/pipeline/deploy", json={
        "environment": "staging",
        "triggered_by": "test",
    })
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    assert data["pipeline"] == "deploy"
    assert data["status"] == "queued"


def test_trigger_remediate_returns_job_id():
    r = client.post("/pipeline/remediate", json={
        "alert_message": "Test incident for unit test",
        "severity": "P3",
        "alert_source": "manual",
    })
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    assert data["pipeline"] == "remediate"


def test_trigger_metrics_returns_job_id():
    r = client.post("/pipeline/metrics", json={
        "window_days": 7,
        "report_type": "dora",
    })
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    assert data["pipeline"] == "metrics"


def test_job_status_after_trigger():
    r = client.post("/pipeline/deploy", json={"environment": "staging"})
    job_id = r.json()["job_id"]

    # Esperar un poco (pipeline async)
    time.sleep(1)

    r2 = client.get(f"/status/{job_id}")
    assert r2.status_code == 200
    data = r2.json()
    assert data["job_id"] == job_id
    assert data["pipeline"] == "deploy"
    assert data["status"] in ("queued", "running", "success", "failed", "completed_with_issues")


def test_job_not_found():
    r = client.get("/status/nonexistent-job-id")
    assert r.status_code == 404


def test_list_jobs():
    r = client.get("/jobs")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_dora_metrics_endpoint():
    r = client.get("/metrics/dora")
    assert r.status_code == 200
    assert "snapshots" in r.json()


def test_slo_budget_endpoint():
    r = client.get("/slo/budget")
    assert r.status_code == 200
    assert "slos" in r.json()


def test_define_slo():
    r = client.post("/slo", json={
        "name": "test-availability",
        "target_pct": 99.9,
        "window_days": 30,
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True

    # Verificar que aparece en /slo/budget
    r2 = client.get("/slo/budget")
    slo_names = [s["name"] for s in r2.json()["slos"]]
    assert "test-availability" in slo_names
