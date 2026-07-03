"""
n8n-devops-fleet — Motor LangGraph
===================================
Tres pipelines DevOps orquestados:
  1. deploy    — preflight → deploy → smoke → slo_guard → dora_record → notify
  2. remediate — incident_ctx → root_cause → remediation_plan → auto_remediate → escalate → mttr_record
  3. metrics   — collect_events → dora_calc → slo_tracker → toil_analyzer → report_gen → publish
"""

import os
import json
import subprocess
import logging
import threading
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import TypedDict, Annotated, List, Dict, Optional, Callable, Any
from pydantic import BaseModel
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
from openai import RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

# ─── Thread-local log callback (igual que agile-fleet) ───────────────────────
_thread_local = threading.local()

def set_log_callback(fn: Callable[[str], None]) -> None:
    _thread_local.log_callback = fn

def _log(msg: str) -> None:
    fn = getattr(_thread_local, "log_callback", None)
    if fn:
        try:
            fn(msg)
        except Exception:
            pass
    logger.info(msg)

# ─── Credenciales ─────────────────────────────────────────────────────────────
MINIMAX_API_KEY    = os.getenv("MINIMAX_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
GITHUB_TOKEN       = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER       = os.getenv("GITHUB_OWNER")
GITHUB_REPO        = os.getenv("GITHUB_REPO")
VERCEL_TOKEN       = os.getenv("VERCEL_TOKEN")
VERCEL_PROJECT_ID  = os.getenv("VERCEL_PROJECT_ID")
VERCEL_ORG_ID      = os.getenv("VERCEL_ORG_ID")
GITHUB_REPO_ID     = os.getenv("GITHUB_REPO_ID")
PRODUCTION_URL     = os.getenv("PRODUCTION_URL", "")
STAGING_URL        = os.getenv("STAGING_URL", "")
FLEET_DB           = os.getenv("FLEET_DB", "/data/devops_store/devops.db")

# ─── LLM Setup (mismo patrón que agile-fleet) ─────────────────────────────────
def _is_quota_error(exc: Exception) -> bool:
    if isinstance(exc, (RateLimitError,)):
        return True
    if isinstance(exc, APIStatusError) and exc.status_code in (401, 402, 403, 429, 500, 502, 503, 529):
        return True
    msg = str(exc).lower()
    return any(kw in msg for kw in (
        "401", "429", "rate limit", "quota", "overload", "529",
        "capacity", "unavailable", "login fail", "authorized_error",
    ))

def _make_minimax(temperature: float) -> ChatOpenAI:
    # Use placeholder when key is absent so the module can import without
    # credentials; actual API calls will fail with a clear auth error.
    return ChatOpenAI(
        api_key=MINIMAX_API_KEY or "no-key-set",
        base_url="https://api.minimax.io/v1",
        model="MiniMax-M2.7",
        temperature=temperature,
        max_tokens=8192,
    )

def _make_or(model: str, temperature: float) -> ChatOpenAI:
    return ChatOpenAI(
        api_key=OPENROUTER_API_KEY or "no-key-set",
        base_url="https://openrouter.ai/api/v1",
        model=model,
        temperature=temperature,
        max_tokens=8192,
        request_timeout=600,
        default_headers={"HTTP-Referer": "https://devops-fleet", "X-Title": "DevOps Fleet"},
    )

_ANALYST_PRIMARY  = _make_minimax(0.1)
_REVIEWER_PRIMARY = _make_minimax(0.0)

_OR_ANALYST_CHAIN = [
    _make_or("meta-llama/llama-3.3-70b-instruct", 0.1),       # paid, barato ~$0.05/1M
    _make_or("google/gemma-3-27b-it", 0.1),                    # paid, barato
    _make_or("nvidia/nemotron-3-ultra-550b-a55b:free", 0.1),   # free fallback
    _make_or("nvidia/nemotron-3-super-120b-a12b:free", 0.1),
    _make_or("meta-llama/llama-3.3-70b-instruct:free", 0.1),
]

def _call_llm(llm_primary: ChatOpenAI, fallback_chain: list, messages: list) -> str:
    """Llama al LLM primario; ante quota errors, prueba la cadena de fallback."""
    for llm in [llm_primary] + fallback_chain:
        try:
            resp = llm.invoke(messages)
            return resp.content
        except Exception as exc:
            if _is_quota_error(exc):
                _log(f"[LLM] quota/rate error en {llm.model_name}, probando fallback...")
                continue
            raise
    raise RuntimeError("Todos los modelos LLM fallaron (quota/rate limit agotado)")

# ─── SQLite helpers ───────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def _db_connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(FLEET_DB), exist_ok=True)
    conn = sqlite3.connect(FLEET_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _db_init() -> None:
    with _db_lock:
        conn = _db_connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS deployment_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id  TEXT NOT NULL DEFAULT 'default',
                ts          TEXT NOT NULL,
                environment TEXT NOT NULL,
                commit_sha  TEXT,
                status      TEXT NOT NULL,
                duration_s  INTEGER,
                lead_time_s INTEGER,
                triggered_by TEXT,
                notes       TEXT
            );
            CREATE TABLE IF NOT EXISTS incident_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id  TEXT NOT NULL DEFAULT 'default',
                ts_start    TEXT NOT NULL,
                ts_end      TEXT,
                severity    TEXT,
                root_cause  TEXT,
                resolution  TEXT,
                mttr_s      INTEGER,
                deployment_id INTEGER REFERENCES deployment_events(id)
            );
            CREATE TABLE IF NOT EXISTS slo_definitions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id  TEXT NOT NULL DEFAULT 'default',
                name        TEXT NOT NULL,
                target_pct  REAL NOT NULL,
                window_days INTEGER NOT NULL DEFAULT 30,
                sli_query   TEXT,
                created_at  TEXT NOT NULL,
                UNIQUE(project_id, name)
            );
            CREATE TABLE IF NOT EXISTS error_budget_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT NOT NULL,
                slo_id      INTEGER REFERENCES slo_definitions(id),
                budget_used_s INTEGER,
                budget_total_s INTEGER,
                remaining_pct REAL
            );
            CREATE TABLE IF NOT EXISTS dora_snapshots (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id           TEXT NOT NULL DEFAULT 'default',
                ts                   TEXT NOT NULL,
                window_days          INTEGER NOT NULL,
                deployment_frequency REAL,
                lead_time_p50_h      REAL,
                lead_time_p90_h      REAL,
                mttr_h               REAL,
                change_failure_rate  REAL,
                dora_tier            TEXT
            );
            CREATE TABLE IF NOT EXISTS toil_catalog (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id  TEXT NOT NULL DEFAULT 'default',
                name        TEXT NOT NULL,
                frequency   TEXT,
                time_min    INTEGER,
                automatable INTEGER DEFAULT 0,
                notes       TEXT,
                detected_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS project_configs (
                project_id            TEXT PRIMARY KEY,
                deploy_strategy       TEXT NOT NULL DEFAULT 'vercel',
                github_owner          TEXT,
                github_repo           TEXT,
                github_repo_id        TEXT,
                github_workflow       TEXT DEFAULT 'deploy.yml',
                vercel_token          TEXT,
                vercel_project_id     TEXT,
                vercel_org_id         TEXT,
                github_token          TEXT,
                production_url        TEXT,
                staging_url           TEXT,
                smoke_urls            TEXT,
                clerk_publishable_key TEXT,
                updated_at            TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)
        # Migración: agregar columnas faltantes en tablas existentes
        for table in ("deployment_events", "incident_events", "dora_snapshots", "toil_catalog"):
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "project_id" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default'")
        for table in ("slo_definitions",):
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "project_id" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN project_id TEXT NOT NULL DEFAULT 'default'")
        # Migración: agregar clerk_publishable_key a project_configs si falta
        cols = [r[1] for r in conn.execute("PRAGMA table_info(project_configs)").fetchall()]
        if "clerk_publishable_key" not in cols:
            conn.execute("ALTER TABLE project_configs ADD COLUMN clerk_publishable_key TEXT")
        conn.commit()
        conn.close()

# ─── Project config store ────────────────────────────────────────────────────
def _project_get(project_id: str) -> dict:
    """Retorna la config del proyecto desde la DB. Fallback a env vars si no existe."""
    if not project_id:
        return {}
    with _db_lock:
        conn = _db_connect()
        row = conn.execute(
            "SELECT * FROM project_configs WHERE project_id = ?", (project_id,)
        ).fetchone()
        conn.close()
    if row:
        cfg = dict(row)
        if cfg.get("smoke_urls"):
            try:
                cfg["smoke_urls"] = json.loads(cfg["smoke_urls"])
            except Exception:
                cfg["smoke_urls"] = []
        return cfg
    return {}

def _project_upsert(project_id: str, **fields) -> None:
    """Crea o actualiza la config de un proyecto."""
    if "smoke_urls" in fields and isinstance(fields["smoke_urls"], list):
        fields["smoke_urls"] = json.dumps(fields["smoke_urls"])
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    cols = list(fields.keys())
    placeholders = ", ".join(["?"] * len(cols))
    updates = ", ".join([f"{c} = excluded.{c}" for c in cols])
    sql = (
        f"INSERT INTO project_configs (project_id, {', '.join(cols)}) "
        f"VALUES (?, {placeholders}) "
        f"ON CONFLICT(project_id) DO UPDATE SET {updates}, updated_at = excluded.updated_at"
    )
    with _db_lock:
        conn = _db_connect()
        conn.execute(sql, [project_id] + list(fields.values()))
        conn.commit()
        conn.close()

def _resolve_project_creds(state: dict) -> dict:
    """Retorna credenciales efectivas: DB project_config > payload > env vars."""
    project_id = state.get("project_id", "")
    cfg = _project_get(project_id) if project_id else {}
    return {
        "github_token":      cfg.get("github_token")      or os.getenv("GITHUB_TOKEN", ""),
        "github_owner":      cfg.get("github_owner")      or state.get("github_owner") or GITHUB_OWNER or "",
        "github_repo":       cfg.get("github_repo")       or state.get("github_repo")  or GITHUB_REPO or "",
        "github_repo_id":    cfg.get("github_repo_id")    or os.getenv("GITHUB_REPO_ID", ""),
        "github_workflow":   cfg.get("github_workflow")   or state.get("github_workflow") or "deploy.yml",
        "vercel_token":      cfg.get("vercel_token")      or VERCEL_TOKEN or "",
        "vercel_project_id": cfg.get("vercel_project_id") or VERCEL_PROJECT_ID or "",
        "vercel_org_id":     cfg.get("vercel_org_id")     or VERCEL_ORG_ID or "",
        "production_url":    cfg.get("production_url")    or state.get("deploy_url") or PRODUCTION_URL or "",
        "staging_url":       cfg.get("staging_url")       or STAGING_URL or "",
        "deploy_strategy":        cfg.get("deploy_strategy")        or state.get("deploy_strategy") or "vercel",
        "smoke_urls":             cfg.get("smoke_urls")             or state.get("extra_smoke_urls") or [],
        "clerk_publishable_key":  cfg.get("clerk_publishable_key")  or "",
    }


# ─── Helpers de infraestructura ───────────────────────────────────────────────
def _run(cmd: str, timeout: int = 120, cwd: str | None = None) -> tuple[int, str, str]:
    """Ejecuta un comando shell y retorna (returncode, stdout, stderr)."""
    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=cwd
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()

def _github_api(method: str, path: str, data: dict | None = None) -> dict:
    """Llama a la GitHub API REST."""
    import urllib.request
    url = f"https://api.github.com{path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        return {"error": str(exc)}

def _smoke_check_clerk(url: str, expected_key: str) -> tuple[bool, str]:
    """Descarga el HTML de url y valida que data-clerk-publishable-key coincide con expected_key.
    Retorna (ok, nota). Si expected_key está vacío, omite la validación."""
    import urllib.request, re
    if not expected_key:
        return True, "clerk: omitido (sin clave esperada configurada)"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "devops-fleet/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read(32768).decode("utf-8", errors="replace")
        match = re.search(r'data-clerk-publishable-key="([^"]+)"', html)
        if not match:
            return False, "clerk: ✗ data-clerk-publishable-key ausente en el HTML"
        actual_key = match.group(1)
        if actual_key == expected_key:
            return True, f"clerk: ✓ clave correcta ({expected_key[:20]}...)"
        return False, (
            f"clerk: ✗ CLAVE INCORRECTA — "
            f"esperada={expected_key[:30]}... | "
            f"encontrada={actual_key[:30]}..."
        )
    except Exception as exc:
        return False, f"clerk: ✗ error al verificar HTML ({exc})"

def _smoke_check(url: str, expected_status: int = 200, timeout: int = 15) -> tuple[bool, int]:
    """HTTP GET simple; retorna (ok, status_code)."""
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "devops-fleet/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return True, r.status
    except Exception as exc:
        code = getattr(getattr(exc, "code", None), "__class__", type(exc)).__name__
        try:
            return False, exc.code  # type: ignore[attr-defined]
        except Exception:
            return False, 0

# ===========================================================================
# PIPELINE 1 — DEPLOY
# ===========================================================================

class DeployState(TypedDict):
    # Input
    project_id: str           # slug del proyecto, ej. "tennis-app"
    github_owner: str         # override del env global
    github_repo: str          # override del env global
    environment: str          # "production" | "staging" | "preview"
    commit_sha: str
    triggered_by: str
    workspace: str
    deploy_url: str           # URL principal a validar post-deploy
    deploy_strategy: str      # "vercel" | "github_actions" (default: "vercel")
    github_workflow: str      # nombre del workflow file, ej. "deploy.yml"
    extra_smoke_urls: list    # URLs adicionales para smoke test (multi-servicio)

    # Interno
    preflight_ok: bool
    preflight_notes: str
    deploy_ok: bool
    deploy_output: str
    smoke_ok: bool
    smoke_notes: str
    slo_ok: bool
    slo_notes: str
    rollback_triggered: bool
    dora_recorded: bool
    notify_sent: bool
    error: str
    logs: Annotated[List[str], lambda a, b: a + b]


def _deploy_preflight_node(state: DeployState) -> dict:
    _log("[PREFLIGHT] Verificando condiciones pre-deploy...")
    notes = []
    ok = True

    # 1. CI status via GitHub API
    _owner = state.get("github_owner") or GITHUB_OWNER
    _repo  = state.get("github_repo")  or GITHUB_REPO
    if GITHUB_TOKEN and _owner and _repo and state.get("commit_sha"):
        result = _github_api("GET", f"/repos/{_owner}/{_repo}/commits/{state['commit_sha']}/check-runs")
        runs = result.get("check_runs", [])
        failed = [r["name"] for r in runs if r.get("conclusion") == "failure"]
        if failed:
            ok = False
            notes.append(f"CI fallando: {', '.join(failed)}")
        else:
            notes.append(f"CI: {len(runs)} checks OK")

    # 2. Verificar error budget en DB (si está cerca del límite, bloquear)
    with _db_lock:
        conn = _db_connect()
        row = conn.execute("""
            SELECT remaining_pct FROM error_budget_log
            ORDER BY ts DESC LIMIT 1
        """).fetchone()
        conn.close()

    if row and row["remaining_pct"] is not None:
        remaining = row["remaining_pct"]
        if remaining < 5.0 and state.get("environment") == "production":
            ok = False
            notes.append(f"Error budget agotado ({remaining:.1f}% restante) — deploy bloqueado")
        else:
            notes.append(f"Error budget: {remaining:.1f}% restante — OK")

    # 3. Verificar secrets en último commit via git log
    rc, out, err = _run(
        f"cd {state.get('workspace', '/workspace')} && "
        f"git log -1 --format='%H %s' 2>/dev/null || echo 'no-git'"
    )
    if "no-git" not in out:
        notes.append("Workspace git: accesible")
    else:
        notes.append("Workspace git: no accesible (omitiendo checks de repo)")

    _log(f"[PREFLIGHT] {'✓ OK' if ok else '✗ BLOQUEADO'} — {'; '.join(notes)}")
    return {
        "preflight_ok": ok,
        "preflight_notes": "; ".join(notes),
        "logs": [f"PREFLIGHT: {'OK' if ok else 'BLOQUEADO'} — {'; '.join(notes)}"],
    }


def _vercel_api_deploy(commit_sha: str, environment: str, token: str = "", project_id: str = "", org_id: str = "", production_url: str = "") -> tuple[bool, str, str]:
    """Dispara un deploy en Vercel vía API REST y espera a que quede READY.
    Retorna (ok, mensaje, deployment_id).
    """
    import urllib.request, urllib.error, time
    _token      = token      or VERCEL_TOKEN or ""
    _project_id = project_id or VERCEL_PROJECT_ID or ""
    _org_id     = org_id     or VERCEL_ORG_ID or ""
    _repo_id    = GITHUB_REPO_ID or ""
    if not _token or not _project_id:
        return False, "VERCEL_TOKEN o VERCEL_PROJECT_ID no configurados", ""
    if not _repo_id:
        return False, "GITHUB_REPO_ID no configurado (requerido por Vercel API)", ""

    target = "production" if environment == "production" else "preview"
    payload = {
        "name": GITHUB_REPO or "app-tennis",
        "target": target,
        "gitSource": {
            "type": "github",
            "repoId": _repo_id,
            "ref": "main",
            "sha": commit_sha,
        },
    }
    body = json.dumps(payload).encode()
    headers = {
        "Authorization": f"Bearer {_token}",
        "Content-Type": "application/json",
    }
    params = f"?projectId={_project_id}"
    if _org_id:
        params += f"&teamId={_org_id}"
    url = f"https://api.vercel.com/v13/deployments{params}"

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            deploy_id = data.get("id", "")
            deploy_url = data.get("url", "")
            ready_state = data.get("readyState", "INITIALIZING")
            _log(f"[DEPLOY] Deployment iniciado: {deploy_id} ({ready_state})")
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:500]}", ""
    except Exception as exc:
        return False, str(exc), ""

    # Esperar hasta READY o ERROR (máx 10 min, poll cada 15s)
    poll_url = f"https://api.vercel.com/v13/deployments/{deploy_id}"
    if _org_id:
        poll_url += f"?teamId={_org_id}"
    poll_headers = {"Authorization": f"Bearer {_token}"}

    for attempt in range(40):
        time.sleep(15)
        try:
            poll_req = urllib.request.Request(poll_url, headers=poll_headers)
            with urllib.request.urlopen(poll_req, timeout=15) as r:
                poll_data = json.loads(r.read())
                ready_state = poll_data.get("readyState", "?")
                _log(f"[DEPLOY] [{attempt+1}/40] {deploy_id} → {ready_state}")
                if ready_state == "READY":
                    if target == "production":
                        _vercel_promote(deploy_id, token=_token, org_id=_org_id, production_url=production_url)
                    return True, f"READY: id={deploy_id} url=https://{deploy_url}", deploy_id
                if ready_state in ("ERROR", "CANCELED"):
                    return False, f"{ready_state}: id={deploy_id}", deploy_id
        except Exception as exc:
            _log(f"[DEPLOY] Poll error: {exc}")

    return False, f"Timeout esperando READY: {deploy_id}", deploy_id


def _vercel_promote(deploy_id: str, token: str = "", org_id: str = "", production_url: str = "") -> None:
    """Asigna el alias de producción al deployment via POST /v2/deployments/{id}/aliases."""
    import urllib.request, urllib.error
    _token = token or VERCEL_TOKEN or ""
    _prod_url = production_url or os.getenv("PRODUCTION_URL", "")
    if not _token or not _prod_url:
        _log("[DEPLOY] Promote omitido: VERCEL_TOKEN o PRODUCTION_URL no configurados")
        return
    alias = _prod_url.replace("https://", "").replace("http://", "").rstrip("/")
    _org_id = org_id or VERCEL_ORG_ID or ""
    url = f"https://api.vercel.com/v2/deployments/{deploy_id}/aliases"
    if _org_id:
        url += f"?teamId={_org_id}"
    headers = {
        "Authorization": f"Bearer {_token}",
        "Content-Type": "application/json",
    }
    body = json.dumps({"alias": alias}).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            old = data.get("oldDeploymentId", "")
            _log(f"[DEPLOY] Alias {alias} → {deploy_id} (era: {old})")
    except urllib.error.HTTPError as e:
        _log(f"[DEPLOY] Alias error HTTP {e.code}: {e.read().decode()[:200]}")
    except Exception as exc:
        _log(f"[DEPLOY] Alias error: {exc}")


def _github_actions_deploy(owner: str, repo: str, workflow: str, environment: str, commit_sha: str, token: str = "") -> tuple[bool, str]:
    """Dispara un workflow_dispatch en GitHub Actions y espera que complete.
    Timeout: 120×15s = 30 min (apto para builds con Docker + App Runner).
    """
    import urllib.request, urllib.error, time
    _token = token or GITHUB_TOKEN
    if not _token:
        return False, "GITHUB_TOKEN no configurado"

    branch = "main" if environment == "production" else environment
    headers = {
        "Authorization": f"Bearer {_token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    # Disparar workflow_dispatch
    trigger_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
    body = json.dumps({"ref": branch}).encode()
    req = urllib.request.Request(trigger_url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            _log(f"[DEPLOY] GitHub Actions dispatch: HTTP {resp.status} — {owner}/{repo}/{workflow}@{branch}")
    except urllib.error.HTTPError as e:
        return False, f"Dispatch error HTTP {e.code}: {e.read().decode()[:300]}"
    except Exception as exc:
        return False, f"Dispatch error: {exc}"

    # Esperar unos segundos para que el run aparezca en la API
    time.sleep(10)

    # Obtener el run más reciente del workflow
    runs_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/runs?branch={branch}&per_page=1"
    run_id = None
    for _ in range(5):
        try:
            req = urllib.request.Request(runs_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
                runs = data.get("workflow_runs", [])
                if runs:
                    run_id = runs[0]["id"]
                    run_status = runs[0].get("status", "?")
                    _log(f"[DEPLOY] Run encontrado: {run_id} ({run_status})")
                    break
        except Exception:
            pass
        time.sleep(5)

    if not run_id:
        return False, "No se encontró el workflow run tras el dispatch"

    run_url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}"
    actions_url = f"https://github.com/{owner}/{repo}/actions/runs/{run_id}"

    # Polling hasta completion (máx 30 min)
    for attempt in range(120):
        time.sleep(15)
        try:
            req = urllib.request.Request(run_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                run_data = json.loads(r.read())
                status = run_data.get("status", "?")
                conclusion = run_data.get("conclusion")
                _log(f"[DEPLOY] [{attempt+1}/120] Run {run_id}: {status}/{conclusion or '…'}")
                if status == "completed":
                    ok = conclusion == "success"
                    return ok, f"GitHub Actions {conclusion}: {actions_url}"
        except Exception as exc:
            _log(f"[DEPLOY] Poll error: {exc}")

    return False, f"Timeout esperando GitHub Actions: {actions_url}"


def _deploy_execute_node(state: DeployState) -> dict:
    if not state.get("preflight_ok"):
        return {"deploy_ok": False, "deploy_output": "Omitido: preflight falló", "logs": ["DEPLOY: omitido"]}

    _log(f"[DEPLOY] Iniciando deploy a {state['environment']}...")
    env = state.get("environment", "staging")
    commit_sha = state.get("commit_sha", "")
    creds = _resolve_project_creds(state)
    strategy = creds["deploy_strategy"]

    if strategy == "github_actions":
        _log(f"[DEPLOY] Usando GitHub Actions: {creds['github_owner']}/{creds['github_repo']}/{creds['github_workflow']}")
        ok, output = _github_actions_deploy(
            creds["github_owner"], creds["github_repo"],
            creds["github_workflow"], env, commit_sha,
            token=creds["github_token"],
        )
    elif creds["vercel_project_id"] and creds["vercel_token"] and commit_sha:
        _log("[DEPLOY] Usando Vercel API (sin build local)...")
        ok, output, _ = _vercel_api_deploy(
            commit_sha, env,
            token=creds["vercel_token"],
            project_id=creds["vercel_project_id"],
            org_id=creds["vercel_org_id"],
            production_url=creds["production_url"],
        )
    else:
        # Fallback: CLI (requiere Node.js con suficiente memoria)
        if env == "production":
            rc, out, err = _run("vercel deploy --prod --force 2>&1", timeout=600,
                                cwd=state.get("workspace", "/workspace"))
        elif env == "staging":
            rc, out, err = _run("vercel deploy --target staging --force 2>&1", timeout=600,
                                cwd=state.get("workspace", "/workspace"))
        else:
            rc, out, err = _run("vercel deploy --force 2>&1", timeout=600,
                                cwd=state.get("workspace", "/workspace"))
        ok = rc == 0
        output = (out + "\n" + err).strip()

    _log(f"[DEPLOY] {'✓ OK' if ok else '✗ FALLÓ'}")
    return {
        "deploy_ok": ok,
        "deploy_output": output[:2000],
        "logs": [f"DEPLOY: {'OK' if ok else 'FALLÓ'} — {output[:200]}"],
    }


def _deploy_smoke_node(state: DeployState) -> dict:
    if not state.get("deploy_ok"):
        return {"smoke_ok": False, "smoke_notes": "Omitido: deploy falló", "logs": ["SMOKE: omitido"]}

    creds = _resolve_project_creds(state)
    url = state.get("deploy_url") or creds.get("production_url") or PRODUCTION_URL or STAGING_URL
    if not url:
        return {"smoke_ok": True, "smoke_notes": "Sin URL configurada — omitido", "logs": ["SMOKE: sin URL"]}

    _log(f"[SMOKE] Verificando {url}...")
    notes = []
    all_ok = True

    # Smoke del servicio principal
    ok_home, code_home = _smoke_check(url + "/", expected_status=200)
    home_ok = code_home in (200, 307, 302)
    notes.append(f"home: {code_home} {'✓' if home_ok else '✗'}")
    if not home_ok:
        all_ok = False

    ok_api, code_api = _smoke_check(url + "/api/health")
    health_ok = code_api == 200
    notes.append(f"health: {code_api} {'✓' if health_ok else '✗'}")
    if not health_ok:
        all_ok = False

    # Validación de Clerk: compara la clave embebida en el HTML vs la clave esperada
    clerk_ok, clerk_note = _smoke_check_clerk(url + "/", creds.get("clerk_publishable_key", ""))
    notes.append(clerk_note)
    if not clerk_ok:
        all_ok = False

    # E2E gate: solo en preview
    if "vercel.app" in url:
        ok_e2e, code_e2e = _smoke_check(url + "/api/e2e/login?email=x@x.com")
        notes.append(f"e2e-gate: {code_e2e} {'✓' if code_e2e in (200, 307, 302) else '✗'}")

    # Smoke de URLs extra (multi-servicio): config de la DB del proyecto
    extra_urls = creds.get("smoke_urls") or []
    for entry in extra_urls:
        # Formato: {"url": "...", "path": "/...", "expect": 200, "label": "..."}
        if isinstance(entry, str):
            entry = {"url": entry, "path": "/", "expect": 200, "label": entry}
        check_url = entry["url"].rstrip("/") + entry.get("path", "/")
        expect    = entry.get("expect", 200)
        label     = entry.get("label", entry["url"])
        _, code   = _smoke_check(check_url)
        entry_ok  = code == expect or (expect == 200 and code in (200, 307, 302))
        notes.append(f"{label}: {code} {'✓' if entry_ok else '✗'}")
        if not entry_ok:
            all_ok = False

    _log(f"[SMOKE] {'✓ OK' if all_ok else '✗ FALLÓ'} — {'; '.join(notes)}")
    return {
        "smoke_ok": all_ok,
        "smoke_notes": "; ".join(notes),
        "logs": [f"SMOKE: {'OK' if all_ok else 'FALLÓ'} — {'; '.join(notes)}"],
    }


def _deploy_slo_guard_node(state: DeployState) -> dict:
    """Verifica SLIs post-deploy. Si hay degradación, activa rollback."""
    if not state.get("smoke_ok"):
        return {"slo_ok": False, "slo_notes": "Omitido: smoke falló", "rollback_triggered": False,
                "logs": ["SLO_GUARD: omitido"]}

    # Sin Prometheus configurado, usamos latencia del smoke como proxy
    _log("[SLO_GUARD] Verificando SLIs post-deploy...")
    url = state.get("deploy_url") or PRODUCTION_URL or ""
    notes = []
    rollback = False

    if url:
        start = time.time()
        ok, code = _smoke_check(url + "/", timeout=10)
        latency_ms = (time.time() - start) * 1000
        notes.append(f"latencia home: {latency_ms:.0f}ms")

        if latency_ms > 5000:
            notes.append("⚠ latencia > 5s — posible degradación")
            if state.get("environment") == "production":
                rollback = True
                notes.append("→ auto-rollback activado")

    if rollback:
        _log("[SLO_GUARD] Iniciando rollback automático...")
        rc, out, err = _run("vercel rollback --yes 2>&1", timeout=300,
                            cwd=state.get("workspace", "/workspace"))
        if rc == 0:
            notes.append("rollback: ✓ completado")
        else:
            notes.append(f"rollback: ✗ falló (rc={rc})")

    _log(f"[SLO_GUARD] {'✓' if not rollback else '⚠ rollback'} — {'; '.join(notes)}")
    return {
        "slo_ok": not rollback,
        "slo_notes": "; ".join(notes),
        "rollback_triggered": rollback,
        "logs": [f"SLO_GUARD: {'; '.join(notes)}"],
    }


def _deploy_dora_record_node(state: DeployState) -> dict:
    """Persiste el evento de deployment para métricas DORA."""
    _log("[DORA] Registrando evento de deployment...")

    # Lead time: tiempo desde commit hasta ahora (requiere timestamp del commit via GitHub)
    _owner = state.get("github_owner") or GITHUB_OWNER
    _repo  = state.get("github_repo")  or GITHUB_REPO
    lead_time_s = None
    if GITHUB_TOKEN and _owner and _repo and state.get("commit_sha"):
        result = _github_api("GET", f"/repos/{_owner}/{_repo}/commits/{state['commit_sha']}")
        commit_date = result.get("commit", {}).get("author", {}).get("date")
        if commit_date:
            from dateutil.parser import parse as parse_date
            commit_ts = parse_date(commit_date)
            lead_time_s = int((datetime.now(timezone.utc) - commit_ts).total_seconds())

    status = "success"
    if state.get("rollback_triggered"):
        status = "rollback"
    elif not state.get("deploy_ok"):
        status = "failure"

    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO deployment_events (project_id, ts, environment, commit_sha, status, lead_time_s, triggered_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            state.get("project_id") or "default",
            datetime.now(timezone.utc).isoformat(),
            state.get("environment", "unknown"),
            state.get("commit_sha", ""),
            status,
            lead_time_s,
            state.get("triggered_by", "manual"),
        ))
        conn.commit()
        conn.close()

    _log(f"[DORA] Registrado: env={state.get('environment')} status={status} lead_time={lead_time_s}s")
    return {
        "dora_recorded": True,
        "logs": [f"DORA: registrado deployment status={status} lead_time={lead_time_s}s"],
    }


def _deploy_notify_node(state: DeployState) -> dict:
    """Notifica resultado via GitHub PR comment y Slack (si está configurado)."""
    _log("[NOTIFY] Preparando notificación...")

    rollback = state.get("rollback_triggered", False)
    deploy_ok = state.get("deploy_ok", False)
    smoke_ok = state.get("smoke_ok", False)

    if rollback:
        icon = "🔄"
        summary = "Deploy completado con **rollback automático** (SLI degradado)"
    elif deploy_ok and smoke_ok:
        icon = "✅"
        summary = "Deploy completado exitosamente"
    elif not deploy_ok:
        icon = "❌"
        summary = "Deploy **falló** en la ejecución"
    else:
        icon = "⚠️"
        summary = "Deploy completado pero smoke tests fallaron"

    body = (
        f"{icon} **DevOps Fleet — Deploy Report**\n\n"
        f"**Entorno:** {state.get('environment', '?')}\n"
        f"**Commit:** `{state.get('commit_sha', '?')[:8]}`\n"
        f"**Resultado:** {summary}\n\n"
        f"**Preflight:** {state.get('preflight_notes', '-')}\n"
        f"**Smoke:** {state.get('smoke_notes', '-')}\n"
        f"**SLO:** {state.get('slo_notes', '-')}\n"
    )

    # GitHub comment en el PR más reciente abierto
    _owner = state.get("github_owner") or GITHUB_OWNER
    _repo  = state.get("github_repo")  or GITHUB_REPO
    if GITHUB_TOKEN and _owner and _repo:
        prs = _github_api("GET", f"/repos/{_owner}/{_repo}/pulls?state=open&per_page=1")
        if isinstance(prs, list) and prs:
            pr_number = prs[0]["number"]
            _github_api("POST", f"/repos/{_owner}/{_repo}/issues/{pr_number}/comments",
                        data={"body": body})
            _log(f"[NOTIFY] Comentado en PR #{pr_number}")

    # Slack webhook
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_url:
        import urllib.request
        slack_body = json.dumps({"text": body.replace("**", "*")}).encode()
        try:
            req = urllib.request.Request(slack_url, data=slack_body,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
            _log("[NOTIFY] Slack notificado")
        except Exception as exc:
            _log(f"[NOTIFY] Slack falló: {exc}")

    return {"notify_sent": True, "logs": [f"NOTIFY: {summary}"]}


def build_deploy_pipeline() -> Any:
    g = StateGraph(DeployState)
    g.add_node("preflight",    _deploy_preflight_node)
    g.add_node("deploy_exec",  _deploy_execute_node)
    g.add_node("smoke_test",   _deploy_smoke_node)
    g.add_node("slo_guard",    _deploy_slo_guard_node)
    g.add_node("dora_record",  _deploy_dora_record_node)
    g.add_node("notify",       _deploy_notify_node)

    g.add_edge(START,        "preflight")
    g.add_edge("preflight",  "deploy_exec")
    g.add_edge("deploy_exec","smoke_test")
    g.add_edge("smoke_test", "slo_guard")
    g.add_edge("slo_guard",  "dora_record")
    g.add_edge("dora_record","notify")
    g.add_edge("notify",     END)
    return g.compile()


# ===========================================================================
# PIPELINE 2 — REMEDIATION
# ===========================================================================

class RemediateState(TypedDict):
    # Input
    project_id: str
    github_owner: str
    github_repo: str
    alert_source: str        # "slo_breach" | "smoke_fail" | "ci_fail" | "manual"
    alert_message: str
    severity: str            # "P1" | "P2" | "P3"
    environment: str
    workspace: str

    # Interno
    incident_context: str
    root_cause: str
    remediation_plan: str
    auto_remediated: bool
    remediation_notes: str
    escalated: bool
    ts_start: str
    mttr_recorded: bool
    logs: Annotated[List[str], lambda a, b: a + b]


def _remediate_context_node(state: RemediateState) -> dict:
    """Recopila contexto del incident: último deployment, logs recientes."""
    _log("[INCIDENT] Recopilando contexto...")
    ctx_parts = []

    # Último deployment
    with _db_lock:
        conn = _db_connect()
        last_deploy = conn.execute("""
            SELECT * FROM deployment_events ORDER BY ts DESC LIMIT 1
        """).fetchone()
        conn.close()

    if last_deploy:
        ctx_parts.append(
            f"Último deployment: {dict(last_deploy)['environment']} "
            f"status={dict(last_deploy)['status']} ts={dict(last_deploy)['ts']}"
        )

    ctx_parts.append(f"Alerta: {state['alert_message']}")
    ctx_parts.append(f"Fuente: {state['alert_source']} | Severidad: {state['severity']}")

    _log(f"[INCIDENT] Contexto recopilado: {len(ctx_parts)} items")
    return {
        "incident_context": "\n".join(ctx_parts),
        "ts_start": datetime.now(timezone.utc).isoformat(),
        "logs": ["INCIDENT_CTX: " + ctx_parts[0] if ctx_parts else "INCIDENT_CTX: sin contexto"],
    }


def _remediate_root_cause_node(state: RemediateState) -> dict:
    """LLM analiza el contexto y determina la causa raíz probable."""
    _log("[ROOT_CAUSE] Analizando causa raíz con LLM...")

    prompt = f"""Eres un SRE senior analizando un incident en producción.

CONTEXTO DEL INCIDENT:
{state['incident_context']}

ALERTA RECIBIDA:
{state['alert_message']}

Analiza y responde en JSON con este formato exacto:
{{
  "root_cause": "descripción concisa de la causa raíz más probable",
  "confidence": "high|medium|low",
  "evidence": ["evidencia 1", "evidencia 2"],
  "deployment_related": true|false
}}

Sé conciso. Si el último deployment ocurrió en los últimos 30 minutos y hay degradación, es deployment_related=true."""

    response = _call_llm(
        _ANALYST_PRIMARY, _OR_ANALYST_CHAIN,
        [SystemMessage(content="Eres un SRE senior. Responde siempre en JSON válido."),
         HumanMessage(content=prompt)]
    )

    try:
        data = json.loads(response.strip().strip("```json").strip("```").strip())
        root_cause = data.get("root_cause", "Causa raíz no determinada")
        confidence = data.get("confidence", "low")
        deployment_related = data.get("deployment_related", False)
    except json.JSONDecodeError:
        root_cause = response[:500]
        confidence = "low"
        deployment_related = False

    _log(f"[ROOT_CAUSE] {root_cause} (confianza: {confidence})")
    return {
        "root_cause": root_cause,
        "logs": [f"ROOT_CAUSE: {root_cause} ({confidence} confidence, deployment_related={deployment_related})"],
    }


def _remediate_plan_node(state: RemediateState) -> dict:
    """LLM genera un plan de remediación basado en la causa raíz."""
    _log("[PLAN] Generando plan de remediación...")

    prompt = f"""Eres un SRE senior generando un plan de remediación.

CAUSA RAÍZ IDENTIFICADA:
{state['root_cause']}

SEVERIDAD: {state['severity']}
ENTORNO: {state.get('environment', 'production')}

Genera un plan en JSON con este formato:
{{
  "immediate_actions": [
    {{
      "action": "descripción de la acción",
      "command": "comando shell a ejecutar (o null si requiere intervención humana)",
      "autonomous": true|false,
      "risk": "low|medium|high"
    }}
  ],
  "requires_human": true|false,
  "escalation_reason": "razón si requires_human=true, null si no"
}}

Acciones autónomas PERMITIDAS (autonomous=true):
- vercel rollback --yes
- Restart de servicios (docker compose restart)
- Smoke checks adicionales

Acciones que REQUIEREN humano (autonomous=false):
- Cambios de infraestructura crítica
- Modificaciones de DB en producción
- Cambios de configuración de seguridad"""

    response = _call_llm(
        _ANALYST_PRIMARY, _OR_ANALYST_CHAIN,
        [SystemMessage(content="Eres un SRE senior. Responde siempre en JSON válido."),
         HumanMessage(content=prompt)]
    )

    try:
        plan = json.loads(response.strip().strip("```json").strip("```").strip())
        plan_str = json.dumps(plan, indent=2, ensure_ascii=False)
    except json.JSONDecodeError:
        plan = {"immediate_actions": [], "requires_human": True, "escalation_reason": "Error al parsear plan"}
        plan_str = response[:1000]

    _log(f"[PLAN] Plan generado: requires_human={plan.get('requires_human')}")
    return {
        "remediation_plan": plan_str,
        "logs": [f"PLAN: {len(plan.get('immediate_actions', []))} acciones — human_required={plan.get('requires_human')}"],
    }


def _auto_remediate_node(state: RemediateState) -> dict:
    """Ejecuta acciones autónomas del plan (low/medium risk, autonomous=true)."""
    _log("[AUTO_REMEDIATE] Ejecutando acciones autónomas...")
    notes = []

    try:
        plan = json.loads(state.get("remediation_plan", "{}"))
    except json.JSONDecodeError:
        return {"auto_remediated": False, "remediation_notes": "Plan inválido",
                "logs": ["AUTO_REMEDIATE: plan JSON inválido"]}

    actions = plan.get("immediate_actions", [])
    executed = 0

    for action in actions:
        if not action.get("autonomous") or action.get("risk") == "high":
            notes.append(f"[skipped] {action['action']}")
            continue

        cmd = action.get("command")
        if not cmd:
            notes.append(f"[manual] {action['action']}")
            continue

        _log(f"[AUTO_REMEDIATE] Ejecutando: {cmd}")
        rc, out, err = _run(cmd, timeout=300, cwd=state.get("workspace", "/workspace"))
        if rc == 0:
            notes.append(f"✓ {action['action']}")
            executed += 1
        else:
            notes.append(f"✗ {action['action']} (rc={rc}): {err[:100]}")

    _log(f"[AUTO_REMEDIATE] {executed}/{len(actions)} acciones ejecutadas")
    return {
        "auto_remediated": executed > 0,
        "remediation_notes": "; ".join(notes),
        "logs": [f"AUTO_REMEDIATE: {executed} acciones ejecutadas"],
    }


def _escalate_gate_node(state: RemediateState) -> dict:
    """Decide si escalar a oncall según severidad y resultado de remediación."""
    _log("[ESCALATE] Evaluando necesidad de escalación...")

    try:
        plan = json.loads(state.get("remediation_plan", "{}"))
        requires_human = plan.get("requires_human", False)
        escalation_reason = plan.get("escalation_reason", "")
    except json.JSONDecodeError:
        requires_human = True
        escalation_reason = "Plan inválido"

    severity = state.get("severity", "P3")
    auto_remediated = state.get("auto_remediated", False)
    escalate = requires_human or (severity == "P1" and not auto_remediated)

    if escalate:
        _log(f"[ESCALATE] ⚠ Escalando — razón: {escalation_reason or severity}")
        slack_url = os.getenv("SLACK_WEBHOOK_URL")
        if slack_url:
            import urllib.request
            msg = (
                f"🚨 *DevOps Fleet — Escalación {severity}*\n"
                f"*Causa raíz:* {state.get('root_cause', '?')}\n"
                f"*Razón:* {escalation_reason or 'Requiere intervención manual'}\n"
                f"*Remediación automática:* {'✓' if auto_remediated else '✗'}\n"
                f"*Notas:* {state.get('remediation_notes', '-')}"
            )
            try:
                req = urllib.request.Request(
                    slack_url,
                    data=json.dumps({"text": msg}).encode(),
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception:
                pass
    else:
        _log("[ESCALATE] Sin escalación necesaria")

    return {"escalated": escalate, "logs": [f"ESCALATE: {'escalado' if escalate else 'no necesario'}"]}


def _mttr_record_node(state: RemediateState) -> dict:
    """Registra el MTTR del incident."""
    _log("[MTTR] Registrando incident...")
    ts_start = state.get("ts_start") or datetime.now(timezone.utc).isoformat()
    ts_end = datetime.now(timezone.utc).isoformat()

    from dateutil.parser import parse as parse_date
    try:
        mttr_s = int((parse_date(ts_end) - parse_date(ts_start)).total_seconds())
    except Exception:
        mttr_s = 0

    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO incident_events (project_id, ts_start, ts_end, severity, root_cause, resolution, mttr_s)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            state.get("project_id") or "default",
            ts_start, ts_end,
            state.get("severity", "P3"),
            state.get("root_cause", ""),
            state.get("remediation_notes", ""),
            mttr_s,
        ))
        conn.commit()
        conn.close()

    _log(f"[MTTR] Registrado: MTTR={mttr_s}s ({mttr_s//60}min)")
    return {"mttr_recorded": True, "logs": [f"MTTR: {mttr_s}s registrado"]}


def build_remediate_pipeline() -> Any:
    g = StateGraph(RemediateState)
    g.add_node("incident_ctx",    _remediate_context_node)
    g.add_node("root_cause",      _remediate_root_cause_node)
    g.add_node("remediation_plan",_remediate_plan_node)
    g.add_node("auto_remediate",  _auto_remediate_node)
    g.add_node("escalate_gate",   _escalate_gate_node)
    g.add_node("mttr_record",     _mttr_record_node)

    g.add_edge(START,               "incident_ctx")
    g.add_edge("incident_ctx",      "root_cause")
    g.add_edge("root_cause",        "remediation_plan")
    g.add_edge("remediation_plan",  "auto_remediate")
    g.add_edge("auto_remediate",    "escalate_gate")
    g.add_edge("escalate_gate",     "mttr_record")
    g.add_edge("mttr_record",       END)
    return g.compile()


# ===========================================================================
# PIPELINE 3 — METRICS / REPORT
# ===========================================================================

class MetricsState(TypedDict):
    project_id: str
    window_days: int
    report_type: str        # "dora" | "slo" | "full"

    # Interno
    raw_events: str
    dora_metrics: str
    slo_status: str
    toil_analysis: str
    report_markdown: str
    published: bool
    logs: Annotated[List[str], lambda a, b: a + b]


def _collect_events_node(state: MetricsState) -> dict:
    """Recolecta eventos de deployment e incident de la DB."""
    _log(f"[COLLECT] Recolectando eventos últimos {state.get('window_days', 7)} días...")
    window = state.get("window_days", 7)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window)).isoformat()

    project_id = state.get("project_id") or "default"
    with _db_lock:
        conn = _db_connect()
        deployments = [dict(r) for r in conn.execute("""
            SELECT * FROM deployment_events WHERE ts >= ? AND project_id = ? ORDER BY ts DESC
        """, (cutoff, project_id)).fetchall()]
        incidents = [dict(r) for r in conn.execute("""
            SELECT * FROM incident_events WHERE ts_start >= ? AND project_id = ? ORDER BY ts_start DESC
        """, (cutoff, project_id)).fetchall()]
        conn.close()

    raw = json.dumps({"deployments": deployments, "incidents": incidents}, default=str)
    _log(f"[COLLECT] {len(deployments)} deployments, {len(incidents)} incidents en {window} días")
    return {
        "raw_events": raw,
        "logs": [f"COLLECT: {len(deployments)} deploys, {len(incidents)} incidents"],
    }


def _dora_calc_node(state: MetricsState) -> dict:
    """Calcula las 4 métricas DORA y determina el tier de rendimiento."""
    _log("[DORA] Calculando métricas DORA...")
    window = state.get("window_days", 7)

    try:
        events = json.loads(state.get("raw_events", "{}"))
    except json.JSONDecodeError:
        return {"dora_metrics": "{}", "logs": ["DORA: error parseando eventos"]}

    deployments = events.get("deployments", [])
    incidents = events.get("incidents", [])

    # 1. Deployment Frequency (deploys/día)
    success_deploys = [d for d in deployments if d.get("status") in ("success", "rollback")]
    df = len(success_deploys) / max(window, 1)

    # 2. Lead Time (promedio y p90)
    lead_times = [d["lead_time_s"] for d in deployments if d.get("lead_time_s")]
    lt_p50 = sorted(lead_times)[len(lead_times)//2] / 3600 if lead_times else None
    lt_p90_idx = int(len(lead_times) * 0.9)
    lt_p90 = sorted(lead_times)[lt_p90_idx] / 3600 if lead_times else None

    # 3. MTTR (promedio en horas)
    mttr_values = [i["mttr_s"] for i in incidents if i.get("mttr_s")]
    mttr_h = (sum(mttr_values) / len(mttr_values) / 3600) if mttr_values else 0.0

    # 4. Change Failure Rate
    total_deploys = len(deployments)
    failed_deploys = len([d for d in deployments if d.get("status") in ("failure", "rollback")])
    cfr = (failed_deploys / total_deploys * 100) if total_deploys > 0 else 0.0

    # DORA tier
    if df >= 1 and (lt_p50 or 999) <= 24 and mttr_h <= 1 and cfr <= 5:
        tier = "Elite"
    elif df >= (1/7) and (lt_p50 or 999) <= 168 and mttr_h <= 24 and cfr <= 10:
        tier = "High"
    elif df >= (1/30) and (lt_p50 or 999) <= 720 and mttr_h <= 168 and cfr <= 15:
        tier = "Medium"
    else:
        tier = "Low"

    dora = {
        "window_days": window,
        "deployment_frequency_per_day": round(df, 3),
        "lead_time_p50_h": round(lt_p50, 2) if lt_p50 else None,
        "lead_time_p90_h": round(lt_p90, 2) if lt_p90 else None,
        "mttr_h": round(mttr_h, 2),
        "change_failure_rate_pct": round(cfr, 1),
        "dora_tier": tier,
        "total_deployments": total_deploys,
        "total_incidents": len(incidents),
    }

    # Persistir snapshot
    project_id = state.get("project_id") or "default"
    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO dora_snapshots
            (project_id, ts, window_days, deployment_frequency, lead_time_p50_h, lead_time_p90_h, mttr_h, change_failure_rate, dora_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            project_id,
            datetime.now(timezone.utc).isoformat(), window,
            dora["deployment_frequency_per_day"], dora["lead_time_p50_h"],
            dora["lead_time_p90_h"], dora["mttr_h"],
            dora["change_failure_rate_pct"], tier,
        ))
        conn.commit()
        conn.close()

    _log(f"[DORA] Tier: {tier} | DF: {df:.2f}/día | MTTR: {mttr_h:.1f}h | CFR: {cfr:.1f}%")
    return {"dora_metrics": json.dumps(dora), "logs": [f"DORA: tier={tier} DF={df:.2f} CFR={cfr:.1f}%"]}


def _slo_tracker_node(state: MetricsState) -> dict:
    """Calcula el error budget restante para cada SLO definido."""
    _log("[SLO] Calculando error budgets...")
    with _db_lock:
        conn = _db_connect()
        slos = [dict(r) for r in conn.execute("SELECT * FROM slo_definitions").fetchall()]
        conn.close()

    if not slos:
        return {"slo_status": "[]", "logs": ["SLO: sin definiciones configuradas"]}

    statuses = []
    for slo in slos:
        window_s = slo["window_days"] * 86400
        budget_total_s = window_s * (1 - slo["target_pct"] / 100)

        with _db_lock:
            conn = _db_connect()
            used = conn.execute("""
                SELECT COALESCE(SUM(budget_used_s), 0) FROM error_budget_log
                WHERE slo_id = ?
                AND ts >= datetime('now', ? || ' days')
            """, (slo["id"], f"-{slo['window_days']}")).fetchone()[0]
            conn.close()

        remaining_s = max(0, budget_total_s - used)
        remaining_pct = (remaining_s / budget_total_s * 100) if budget_total_s > 0 else 100.0

        status_label = "🟢 healthy" if remaining_pct > 20 else ("🟡 burning" if remaining_pct > 5 else "🔴 exhausted")
        statuses.append({
            "name": slo["name"],
            "target": f"{slo['target_pct']}%",
            "budget_total_min": round(budget_total_s / 60, 1),
            "budget_used_min": round(used / 60, 1),
            "remaining_pct": round(remaining_pct, 1),
            "status": status_label,
        })

    _log(f"[SLO] {len(statuses)} SLOs evaluados")
    return {"slo_status": json.dumps(statuses), "logs": [f"SLO: {len(statuses)} evaluados"]}


def _toil_analyzer_node(state: MetricsState) -> dict:
    """Usa LLM para identificar patrones de toil en los eventos recientes."""
    _log("[TOIL] Analizando patrones de toil...")

    try:
        events = json.loads(state.get("raw_events", "{}"))
        dora = json.loads(state.get("dora_metrics", "{}"))
    except json.JSONDecodeError:
        return {"toil_analysis": "[]", "logs": ["TOIL: error parseando datos"]}

    if not events.get("deployments") and not events.get("incidents"):
        return {"toil_analysis": "[]", "logs": ["TOIL: sin datos suficientes"]}

    prompt = f"""Eres un SRE senior analizando toil (trabajo manual repetitivo) en un equipo de DevOps.

MÉTRICAS DORA ACTUALES:
{json.dumps(dora, indent=2)}

DEPLOYMENTS RECIENTES (últimos {state.get('window_days', 7)} días):
{json.dumps(events.get('deployments', [])[:10], indent=2, default=str)}

INCIDENTS RECIENTES:
{json.dumps(events.get('incidents', [])[:5], indent=2, default=str)}

Identifica los 3 principales tipos de toil y responde en JSON:
[
  {{
    "name": "nombre del toil",
    "frequency": "daily|weekly|per_deploy|per_incident",
    "estimated_time_min": 15,
    "automatable": true|false,
    "recommendation": "cómo automatizarlo o reducirlo"
  }}
]

Enfócate en: rollbacks manuales, re-deploys por fallos de CI, resolución manual de incidents repetitivos, tareas de mantenimiento."""

    response = _call_llm(
        _ANALYST_PRIMARY, _OR_ANALYST_CHAIN,
        [SystemMessage(content="Eres un SRE senior. Responde siempre en JSON válido."),
         HumanMessage(content=prompt)]
    )

    try:
        toil_items = json.loads(response.strip().strip("```json").strip("```").strip())
        toil_str = json.dumps(toil_items)
    except json.JSONDecodeError:
        toil_str = "[]"

    _log(f"[TOIL] Análisis completado")
    return {"toil_analysis": toil_str, "logs": ["TOIL: análisis LLM completado"]}


def _report_gen_node(state: MetricsState) -> dict:
    """Genera el reporte markdown con LLM."""
    _log("[REPORT] Generando reporte...")

    try:
        dora = json.loads(state.get("dora_metrics", "{}"))
        slos = json.loads(state.get("slo_status", "[]"))
        toil = json.loads(state.get("toil_analysis", "[]"))
    except json.JSONDecodeError:
        dora, slos, toil = {}, [], []

    prompt = f"""Genera un reporte DevOps semanal profesional en Markdown.

MÉTRICAS DORA:
{json.dumps(dora, indent=2)}

SLO STATUS:
{json.dumps(slos, indent=2)}

ANÁLISIS DE TOIL:
{json.dumps(toil, indent=2)}

El reporte debe tener:
1. Resumen ejecutivo (2-3 oraciones)
2. Tabla de métricas DORA con comparación vs. targets Elite
3. Estado de SLOs (si hay datos)
4. Top 3 items de toil detectados con recomendaciones
5. Próximos pasos priorizados (max 3)

Formato: Markdown profesional, conciso, orientado a acción. Incluye la fecha de generación."""

    report = _call_llm(
        _ANALYST_PRIMARY, _OR_ANALYST_CHAIN,
        [SystemMessage(content="Eres un DevOps lead senior generando reportes ejecutivos."),
         HumanMessage(content=prompt)]
    )

    _log("[REPORT] Reporte generado")
    return {"report_markdown": report, "logs": ["REPORT: generado"]}


def _publish_node(state: MetricsState) -> dict:
    """Publica el reporte via Slack y/o GitHub Discussion."""
    _log("[PUBLISH] Publicando reporte...")

    report = state.get("report_markdown", "")
    slack_url = os.getenv("SLACK_WEBHOOK_URL")

    if slack_url and report:
        import urllib.request
        # Slack tiene límite de 3000 chars por bloque
        snippet = report[:2800] + ("\n...[ver reporte completo]" if len(report) > 2800 else "")
        try:
            req = urllib.request.Request(
                slack_url,
                data=json.dumps({"text": snippet}).encode(),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=15)
            _log("[PUBLISH] Reporte enviado a Slack")
        except Exception as exc:
            _log(f"[PUBLISH] Slack error: {exc}")

    return {"published": True, "logs": ["PUBLISH: reporte publicado"]}


def build_metrics_pipeline() -> Any:
    g = StateGraph(MetricsState)
    g.add_node("collect_events",  _collect_events_node)
    g.add_node("dora_calc",       _dora_calc_node)
    g.add_node("slo_tracker",     _slo_tracker_node)
    g.add_node("toil_analyzer",   _toil_analyzer_node)
    g.add_node("report_gen",      _report_gen_node)
    g.add_node("publish",         _publish_node)

    g.add_edge(START,            "collect_events")
    g.add_edge("collect_events", "dora_calc")
    g.add_edge("dora_calc",      "slo_tracker")
    g.add_edge("slo_tracker",    "toil_analyzer")
    g.add_edge("toil_analyzer",  "report_gen")
    g.add_edge("report_gen",     "publish")
    g.add_edge("publish",        END)
    return g.compile()


# ─── Init DB al importar ──────────────────────────────────────────────────────
_db_init()
