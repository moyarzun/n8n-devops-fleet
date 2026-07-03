"""
DevOps Fleet API — FastAPI
==========================
Endpoints:
  POST /pipeline/deploy      → Pipeline 1
  POST /pipeline/remediate   → Pipeline 2
  POST /pipeline/metrics     → Pipeline 3
  POST /slo                  → Definir/actualizar SLO
  GET  /status/<job_id>      → Estado de un job
  GET  /events               → SSE stream de todos los jobs activos
  GET  /metrics/dora         → Últimas métricas DORA calculadas
  GET  /slo/budget           → Estado de error budgets
  GET  /health               → Health check
  GET  /                     → Dashboard web
"""

import os
import uuid
import asyncio
import json
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, HTMLResponse
from pydantic import BaseModel

from langgraph_devops import (
    build_deploy_pipeline, build_remediate_pipeline, build_metrics_pipeline,
    DeployState, RemediateState, MetricsState,
    set_log_callback, _db_connect, _db_lock, FLEET_DB,
    _project_get, _project_upsert,
)

app = FastAPI(title="DevOps Fleet API", version="1.0.0")

_executor = ThreadPoolExecutor(max_workers=int(os.getenv("FLEET_WORKERS", "6")))

# ─── Modelos de request ───────────────────────────────────────────────────────

class DeployRequest(BaseModel):
    project_id: str = "default"
    github_owner: str = ""
    github_repo: str = ""
    environment: str = "staging"      # production | staging | preview
    commit_sha: str = ""
    triggered_by: str = "manual"
    workspace: str = "/workspace"
    deploy_url: str = ""
    deploy_strategy: str = ""         # "vercel" | "github_actions" — si vacío, usa la config del proyecto
    github_workflow: str = ""         # nombre del workflow file (solo para github_actions)
    extra_smoke_urls: list = []       # URLs extra para smoke test (multi-servicio)


class ProjectConfigRequest(BaseModel):
    deploy_strategy: Optional[str] = None
    github_owner: Optional[str] = None
    github_repo: Optional[str] = None
    github_repo_id: Optional[str] = None
    github_workflow: Optional[str] = None
    github_token: Optional[str] = None
    vercel_token: Optional[str] = None
    vercel_project_id: Optional[str] = None
    vercel_org_id: Optional[str] = None
    production_url: Optional[str] = None
    staging_url: Optional[str] = None
    smoke_urls: Optional[list] = None
    clerk_publishable_key: Optional[str] = None


class RemediateRequest(BaseModel):
    project_id: str = "default"
    github_owner: str = ""
    github_repo: str = ""
    alert_source: str = "manual"
    alert_message: str
    severity: str = "P2"              # P1 | P2 | P3
    environment: str = "production"
    workspace: str = "/workspace"


class MetricsRequest(BaseModel):
    project_id: str = "default"
    window_days: int = 7
    report_type: str = "full"         # dora | slo | full


class SLODefinition(BaseModel):
    project_id: str = "default"
    name: str
    target_pct: float                 # ej. 99.9
    window_days: int = 30
    sli_query: Optional[str] = None


# ─── Estado de jobs (in-memory + DB) ─────────────────────────────────────────

@dataclass
class JobState:
    job_id: str
    pipeline: str
    status: str = "queued"
    phase: str = ""
    logs: List[str] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: Optional[str] = None
    result: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "pipeline": self.pipeline,
            "status": self.status,
            "phase": self.phase,
            "logs": self.logs[-150:],
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "result": self.result,
        }


_jobs: Dict[str, JobState] = {}
_jobs_lock = threading.Lock()
_sse_queues: List[asyncio.Queue] = []
_sse_lock = threading.Lock()


def _broadcast(event: dict) -> None:
    data = "data: " + json.dumps(event) + "\n\n"
    with _sse_lock:
        for q in list(_sse_queues):
            try:
                q.put_nowait(data)
            except Exception:
                pass


def _make_log_cb(job_id: str) -> Any:
    def cb(msg: str):
        with _jobs_lock:
            job = _jobs.get(job_id)
            if job:
                job.logs.append(msg)
                job.phase = msg[:80]
        _broadcast({"type": "log", "job_id": job_id, "msg": msg})
    return cb


# ─── Runners de pipelines ────────────────────────────────────────────────────

def _run_deploy(job_id: str, req: DeployRequest) -> None:
    with _jobs_lock:
        _jobs[job_id].status = "running"

    set_log_callback(_make_log_cb(job_id))
    pipeline = build_deploy_pipeline()

    initial: DeployState = {
        "project_id":       req.project_id,
        "github_owner":     req.github_owner,
        "github_repo":      req.github_repo,
        "environment":      req.environment,
        "commit_sha":       req.commit_sha,
        "triggered_by":     req.triggered_by,
        "workspace":        req.workspace,
        "deploy_url":       req.deploy_url,
        "deploy_strategy":  req.deploy_strategy,
        "github_workflow":  req.github_workflow,
        "extra_smoke_urls": req.extra_smoke_urls,
        "preflight_ok":   False,
        "preflight_notes": "",
        "deploy_ok":      False,
        "deploy_output":  "",
        "smoke_ok":       False,
        "smoke_notes":    "",
        "slo_ok":         False,
        "slo_notes":      "",
        "rollback_triggered": False,
        "dora_recorded":  False,
        "notify_sent":    False,
        "error":          "",
        "logs":           [],
    }

    try:
        final = pipeline.invoke(initial)
        result = {
            "deploy_ok":         final.get("deploy_ok"),
            "rollback_triggered":final.get("rollback_triggered"),
            "smoke_ok":          final.get("smoke_ok"),
            "slo_ok":            final.get("slo_ok"),
            "preflight_notes":   final.get("preflight_notes"),
            "smoke_notes":       final.get("smoke_notes"),
            "slo_notes":         final.get("slo_notes"),
        }
        with _jobs_lock:
            _jobs[job_id].status = "success" if final.get("deploy_ok") and not final.get("rollback_triggered") else "completed_with_issues"
            _jobs[job_id].result = result
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id].status = "failed"
            _jobs[job_id].result = {"error": str(exc)}
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()

    _broadcast({"type": "job_done", "job_id": job_id, "status": _jobs[job_id].status})


def _run_remediate(job_id: str, req: RemediateRequest) -> None:
    with _jobs_lock:
        _jobs[job_id].status = "running"

    set_log_callback(_make_log_cb(job_id))
    pipeline = build_remediate_pipeline()

    initial: RemediateState = {
        "project_id":     req.project_id,
        "github_owner":   req.github_owner,
        "github_repo":    req.github_repo,
        "alert_source":   req.alert_source,
        "alert_message":  req.alert_message,
        "severity":       req.severity,
        "environment":    req.environment,
        "workspace":      req.workspace,
        "incident_context": "",
        "root_cause":     "",
        "remediation_plan": "",
        "auto_remediated": False,
        "remediation_notes": "",
        "escalated":      False,
        "ts_start":       datetime.now(timezone.utc).isoformat(),
        "mttr_recorded":  False,
        "logs":           [],
    }

    try:
        final = pipeline.invoke(initial)
        result = {
            "root_cause":        final.get("root_cause"),
            "auto_remediated":   final.get("auto_remediated"),
            "remediation_notes": final.get("remediation_notes"),
            "escalated":         final.get("escalated"),
        }
        with _jobs_lock:
            _jobs[job_id].status = "success"
            _jobs[job_id].result = result
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id].status = "failed"
            _jobs[job_id].result = {"error": str(exc)}
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()

    _broadcast({"type": "job_done", "job_id": job_id, "status": _jobs[job_id].status})


def _run_metrics(job_id: str, req: MetricsRequest) -> None:
    with _jobs_lock:
        _jobs[job_id].status = "running"

    set_log_callback(_make_log_cb(job_id))
    pipeline = build_metrics_pipeline()

    initial: MetricsState = {
        "project_id":      req.project_id,
        "window_days":     req.window_days,
        "report_type":     req.report_type,
        "raw_events":      "",
        "dora_metrics":    "",
        "slo_status":      "",
        "toil_analysis":   "",
        "report_markdown": "",
        "published":       False,
        "logs":            [],
    }

    try:
        final = pipeline.invoke(initial)
        with _jobs_lock:
            _jobs[job_id].status = "success"
            _jobs[job_id].result = {
                "dora_metrics": json.loads(final.get("dora_metrics", "{}")),
                "report_markdown": final.get("report_markdown") or "",
            }
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()
    except Exception as exc:
        with _jobs_lock:
            _jobs[job_id].status = "failed"
            _jobs[job_id].result = {"error": str(exc)}
            _jobs[job_id].finished_at = datetime.now(timezone.utc).isoformat()

    _broadcast({"type": "job_done", "job_id": job_id, "status": _jobs[job_id].status})


# ─── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "devops-fleet-api"}


@app.post("/pipeline/deploy")
def trigger_deploy(req: DeployRequest):
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = JobState(job_id=job_id, pipeline="deploy")
    _executor.submit(_run_deploy, job_id, req)
    return {"job_id": job_id, "status": "queued", "pipeline": "deploy"}


@app.post("/pipeline/remediate")
def trigger_remediate(req: RemediateRequest):
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = JobState(job_id=job_id, pipeline="remediate")
    _executor.submit(_run_remediate, job_id, req)
    return {"job_id": job_id, "status": "queued", "pipeline": "remediate"}


@app.post("/pipeline/metrics")
def trigger_metrics(req: MetricsRequest):
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = JobState(job_id=job_id, pipeline="metrics")
    _executor.submit(_run_metrics, job_id, req)
    return {"job_id": job_id, "status": "queued", "pipeline": "metrics"}


@app.post("/slo")
def upsert_slo(slo: SLODefinition):
    """Crea o actualiza una definición de SLO."""
    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO slo_definitions (project_id, name, target_pct, window_days, sli_query, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, name) DO UPDATE SET
                target_pct=excluded.target_pct,
                window_days=excluded.window_days,
                sli_query=excluded.sli_query
        """, (slo.project_id, slo.name, slo.target_pct, slo.window_days, slo.sli_query,
              datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    return {"ok": True, "slo": slo.model_dump()}


class DeploymentEvent(BaseModel):
    project_id: str = "default"
    environment: str = "production"
    commit_sha: str = ""
    status: str = "success"           # success | failure | rollback
    triggered_by: str = "github-actions"
    deploy_url: str = ""


@app.post("/events/deployment")
def record_deployment(ev: DeploymentEvent):
    """Registra un deployment ya completado para métricas DORA.
    Llamado por GitHub Actions después de un deploy exitoso."""
    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO deployment_events
            (project_id, ts, environment, commit_sha, status, triggered_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ev.project_id,
            datetime.now(timezone.utc).isoformat(),
            ev.environment,
            ev.commit_sha,
            ev.status,
            ev.triggered_by,
            ev.deploy_url,
        ))
        conn.commit()
        conn.close()
    return {"ok": True, "project_id": ev.project_id, "status": ev.status}


@app.get("/status/{job_id}")
def get_status(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job no encontrado")
    return job.to_dict()


@app.get("/jobs")
def list_jobs(limit: int = 20):
    with _jobs_lock:
        jobs = list(_jobs.values())
    jobs.sort(key=lambda j: j.started_at, reverse=True)
    return [j.to_dict() for j in jobs[:limit]]


@app.get("/metrics/dora")
def get_dora_metrics(project_id: str = "default"):
    with _db_lock:
        conn = _db_connect()
        rows = [dict(r) for r in conn.execute("""
            SELECT * FROM dora_snapshots WHERE project_id = ? ORDER BY ts DESC LIMIT 10
        """, (project_id,)).fetchall()]
        conn.close()
    return {"project_id": project_id, "snapshots": rows}


@app.get("/slo/budget")
def get_slo_budget(project_id: str = "default"):
    with _db_lock:
        conn = _db_connect()
        slos = [dict(r) for r in conn.execute(
            "SELECT * FROM slo_definitions WHERE project_id = ?", (project_id,)
        ).fetchall()]
        latest_budgets = [dict(r) for r in conn.execute("""
            SELECT DISTINCT slo_id, remaining_pct, ts
            FROM error_budget_log
            GROUP BY slo_id
            HAVING ts = MAX(ts)
        """).fetchall()]
        conn.close()
    return {"project_id": project_id, "slos": slos, "budgets": latest_budgets}


@app.get("/events")
async def sse_events(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    with _sse_lock:
        _sse_queues.append(queue)

    async def generator():
        try:
            yield "data: {\"type\":\"connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=20)
                    yield data
                except asyncio.TimeoutError:
                    yield "data: {\"type\":\"ping\"}\n\n"
        finally:
            with _sse_lock:
                if queue in _sse_queues:
                    _sse_queues.remove(queue)

    return StreamingResponse(generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/projects/{project_id}")
def get_project(project_id: str):
    cfg = _project_get(project_id)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Proyecto '{project_id}' no registrado")
    # Ocultar tokens en la respuesta
    for secret_key in ("github_token", "vercel_token"):
        if cfg.get(secret_key):
            cfg[secret_key] = "***"
    return cfg


@app.put("/projects/{project_id}")
def upsert_project(project_id: str, body: ProjectConfigRequest):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    _project_upsert(project_id, **fields)
    return {"project_id": project_id, "updated": list(fields.keys())}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=_DASHBOARD_HTML)


# ─── Dashboard HTML ───────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DevOps Fleet Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0d1117; color: #e6edf3; min-height: 100vh; }
  header { background: #161b22; border-bottom: 1px solid #30363d;
           padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; }
  header h1 { font-size: 1.2rem; font-weight: 600; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #3fb950;
                box-shadow: 0 0 6px #3fb950; }
  .tabs { display: flex; border-bottom: 1px solid #30363d; padding: 0 2rem; background: #161b22; }
  .tab { padding: .75rem 1.25rem; cursor: pointer; color: #8b949e; border-bottom: 2px solid transparent;
         transition: all .2s; font-size: .875rem; }
  .tab.active { color: #58a6ff; border-color: #58a6ff; }
  .panel { display: none; padding: 1.5rem 2rem; }
  .panel.active { display: block; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 1.25rem; }
  .card h3 { color: #8b949e; font-size: .75rem; font-weight: 500; text-transform: uppercase;
             letter-spacing: .05em; margin-bottom: .5rem; }
  .card .value { font-size: 1.75rem; font-weight: 700; color: #e6edf3; }
  .card .sub { font-size: .75rem; color: #8b949e; margin-top: .25rem; }
  .tier-elite { color: #3fb950; }
  .tier-high { color: #58a6ff; }
  .tier-medium { color: #d29922; }
  .tier-low { color: #f85149; }
  .job { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
         padding: 1rem; margin-bottom: .75rem; }
  .job-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: .5rem; }
  .job-id { font-family: monospace; font-size: .8rem; color: #8b949e; }
  .badge { padding: .2rem .6rem; border-radius: 12px; font-size: .7rem; font-weight: 600; }
  .badge-success { background: #1a3a2a; color: #3fb950; border: 1px solid #3fb9504d; }
  .badge-running { background: #1a2a3a; color: #58a6ff; border: 1px solid #58a6ff4d; }
  .badge-failed  { background: #3a1a1a; color: #f85149; border: 1px solid #f851494d; }
  .badge-queued  { background: #2a2a1a; color: #d29922; border: 1px solid #d299224d; }
  .badge-issues  { background: #3a2a1a; color: #e3b341; border: 1px solid #e3b3414d; }
  .log-box { font-family: monospace; font-size: .75rem; color: #8b949e; background: #0d1117;
             border: 1px solid #30363d; border-radius: 4px; padding: .75rem;
             max-height: 120px; overflow-y: auto; margin-top: .5rem; white-space: pre-wrap; }
  .pipeline-tag { font-size: .7rem; color: #8b949e; background: #21262d;
                  padding: .15rem .5rem; border-radius: 4px; }
  .btn { padding: .4rem .9rem; border-radius: 6px; border: 1px solid #30363d; background: #21262d;
         color: #e6edf3; cursor: pointer; font-size: .8rem; transition: all .2s; }
  .btn:hover { background: #30363d; }
  .btn-primary { background: #238636; border-color: #238636; }
  .btn-primary:hover { background: #2ea043; }
  .trigger-form { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                  padding: 1.25rem; margin-bottom: 1rem; }
  .trigger-form h3 { color: #8b949e; font-size: .8rem; text-transform: uppercase; margin-bottom: .75rem; }
  .form-row { display: flex; gap: .75rem; flex-wrap: wrap; align-items: flex-end; }
  select, input[type=text], input[type=number] {
    background: #0d1117; border: 1px solid #30363d; border-radius: 6px;
    color: #e6edf3; padding: .4rem .75rem; font-size: .8rem; }
  label { font-size: .75rem; color: #8b949e; display: block; margin-bottom: .25rem; }
  #log-stream { font-family: monospace; font-size: .75rem; color: #8b949e;
                background: #0d1117; border: 1px solid #30363d; border-radius: 8px;
                padding: 1rem; height: 300px; overflow-y: auto; white-space: pre-wrap; }
</style>
</head>
<body>
<header>
  <div class="status-dot" id="conn-dot"></div>
  <h1>⚙️ DevOps Fleet Dashboard</h1>
  <span style="color:#8b949e;font-size:.8rem" id="conn-status">Conectando...</span>
</header>

<div class="tabs">
  <div class="tab active" onclick="showTab('jobs')">Jobs</div>
  <div class="tab" onclick="showTab('dora')">DORA Metrics</div>
  <div class="tab" onclick="showTab('trigger')">Triggers</div>
  <div class="tab" onclick="showTab('stream')">Live Stream</div>
</div>

<!-- JOBS -->
<div class="panel active" id="tab-jobs">
  <div id="jobs-list"><p style="color:#8b949e">Cargando jobs...</p></div>
</div>

<!-- DORA -->
<div class="panel" id="tab-dora">
  <div class="grid" id="dora-cards">
    <div class="card"><h3>Deployment Frequency</h3><div class="value" id="d-df">—</div><div class="sub">deploys/día</div></div>
    <div class="card"><h3>Lead Time p50</h3><div class="value" id="d-lt">—</div><div class="sub">horas</div></div>
    <div class="card"><h3>MTTR</h3><div class="value" id="d-mttr">—</div><div class="sub">horas</div></div>
    <div class="card"><h3>Change Failure Rate</h3><div class="value" id="d-cfr">—</div><div class="sub">%</div></div>
    <div class="card"><h3>DORA Tier</h3><div class="value" id="d-tier">—</div><div class="sub">rendimiento</div></div>
  </div>
</div>

<!-- TRIGGER -->
<div class="panel" id="tab-trigger">
  <div class="trigger-form">
    <h3>🚀 Trigger Deploy</h3>
    <div class="form-row">
      <div><label>Entorno</label><select id="deploy-env"><option>staging</option><option>production</option><option>preview</option></select></div>
      <div><label>Commit SHA</label><input type="text" id="deploy-sha" placeholder="abc1234" style="width:120px"></div>
      <div><label>Deploy URL</label><input type="text" id="deploy-url" placeholder="https://..." style="width:220px"></div>
      <div style="margin-top:1.2rem"><button class="btn btn-primary" onclick="triggerDeploy()">▶ Deploy</button></div>
    </div>
  </div>

  <div class="trigger-form">
    <h3>🚨 Trigger Remediation</h3>
    <div class="form-row">
      <div><label>Severidad</label><select id="rem-severity"><option>P2</option><option>P1</option><option>P3</option></select></div>
      <div><label>Fuente</label><select id="rem-source"><option>manual</option><option>slo_breach</option><option>smoke_fail</option><option>ci_fail</option></select></div>
      <div><label>Mensaje</label><input type="text" id="rem-msg" placeholder="Descripción del incident" style="width:280px"></div>
      <div style="margin-top:1.2rem"><button class="btn" style="border-color:#f85149;color:#f85149" onclick="triggerRemediate()">⚡ Remediate</button></div>
    </div>
  </div>

  <div class="trigger-form">
    <h3>📊 Trigger Metrics Report</h3>
    <div class="form-row">
      <div><label>Ventana (días)</label><input type="number" id="metrics-days" value="7" style="width:80px"></div>
      <div><label>Tipo</label><select id="metrics-type"><option>full</option><option>dora</option><option>slo</option></select></div>
      <div style="margin-top:1.2rem"><button class="btn" style="border-color:#58a6ff;color:#58a6ff" onclick="triggerMetrics()">📈 Generar</button></div>
    </div>
  </div>
</div>

<!-- STREAM -->
<div class="panel" id="tab-stream">
  <div id="log-stream">Esperando eventos SSE...</div>
</div>

<script>
const API = '';
let es = null;

function showTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', ['jobs','dora','trigger','stream'][i]===name));
  document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id==='tab-'+name));
  if (name==='dora') loadDora();
  if (name==='jobs') loadJobs();
}

function badgeClass(s) {
  return s==='success'?'badge-success':s==='running'?'badge-running':s==='failed'?'badge-failed':
    s==='queued'?'badge-queued':'badge-issues';
}

async function loadJobs() {
  const r = await fetch('/jobs').then(r=>r.json());
  const el = document.getElementById('jobs-list');
  if (!r.length) { el.innerHTML='<p style="color:#8b949e">Sin jobs aún. Usa la pestaña Triggers.</p>'; return; }
  el.innerHTML = r.map(j=>`
    <div class="job">
      <div class="job-header">
        <span><span class="pipeline-tag">${j.pipeline}</span> &nbsp;<span class="job-id">#${j.job_id}</span></span>
        <span class="badge ${badgeClass(j.status)}">${j.status}</span>
      </div>
      <div style="font-size:.75rem;color:#8b949e">${j.phase||'—'}</div>
      ${j.logs.length?`<div class="log-box">${j.logs.slice(-8).join('\\n')}</div>`:''}
    </div>`).join('');
}

async function loadDora() {
  const r = await fetch('/metrics/dora').then(r=>r.json());
  const s = r.snapshots?.[0];
  if (!s) return;
  document.getElementById('d-df').textContent = s.deployment_frequency?.toFixed(2)||'—';
  document.getElementById('d-lt').textContent = s.lead_time_p50_h!=null?s.lead_time_p50_h.toFixed(1)+'h':'—';
  document.getElementById('d-mttr').textContent = s.mttr_h!=null?s.mttr_h.toFixed(1)+'h':'—';
  document.getElementById('d-cfr').textContent = s.change_failure_rate!=null?s.change_failure_rate.toFixed(1)+'%':'—';
  const tierEl = document.getElementById('d-tier');
  tierEl.textContent = s.dora_tier||'—';
  tierEl.className = 'value tier-'+(s.dora_tier||'').toLowerCase();
}

async function post(path, body) {
  const r = await fetch(path, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  return r.json();
}

function triggerDeploy() {
  post('/pipeline/deploy', {
    environment: document.getElementById('deploy-env').value,
    commit_sha: document.getElementById('deploy-sha').value,
    deploy_url: document.getElementById('deploy-url').value,
  }).then(r => { alert('Job iniciado: '+r.job_id); showTab('jobs'); });
}

function triggerRemediate() {
  const msg = document.getElementById('rem-msg').value;
  if (!msg) { alert('Ingresa un mensaje del incident'); return; }
  post('/pipeline/remediate', {
    alert_source: document.getElementById('rem-source').value,
    alert_message: msg,
    severity: document.getElementById('rem-severity').value,
  }).then(r => { alert('Remediación iniciada: '+r.job_id); showTab('jobs'); });
}

function triggerMetrics() {
  post('/pipeline/metrics', {
    window_days: parseInt(document.getElementById('metrics-days').value)||7,
    report_type: document.getElementById('metrics-type').value,
  }).then(r => { alert('Reporte iniciado: '+r.job_id); showTab('jobs'); });
}

// SSE
function connectSSE() {
  es = new EventSource('/events');
  es.onopen = () => {
    document.getElementById('conn-dot').style.background='#3fb950';
    document.getElementById('conn-status').textContent='Conectado';
  };
  es.onmessage = (e) => {
    const data = JSON.parse(e.data);
    if (data.type==='ping'||data.type==='connected') return;
    const box = document.getElementById('log-stream');
    const ts = new Date().toLocaleTimeString();
    box.textContent += `[${ts}] ${data.msg||JSON.stringify(data)}\\n`;
    box.scrollTop = box.scrollHeight;
    if (data.type==='job_done') loadJobs();
  };
  es.onerror = () => {
    document.getElementById('conn-dot').style.background='#f85149';
    document.getElementById('conn-status').textContent='Reconectando...';
    setTimeout(connectSSE, 3000);
  };
}

connectSSE();
loadJobs();
setInterval(loadJobs, 10000);
</script>
</body>
</html>"""
