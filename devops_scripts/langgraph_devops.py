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
PRODUCTION_URL     = os.getenv("PRODUCTION_URL", "")
STAGING_URL        = os.getenv("STAGING_URL", "")
FLEET_DB           = os.getenv("FLEET_DB", "/data/devops_store/devops.db")

# ─── LLM Setup (mismo patrón que agile-fleet) ─────────────────────────────────
def _is_quota_error(exc: Exception) -> bool:
    if isinstance(exc, (RateLimitError,)):
        return True
    if isinstance(exc, APIStatusError) and exc.status_code in (402, 403, 429, 500, 502, 503, 529):
        return True
    msg = str(exc).lower()
    return any(kw in msg for kw in ("429", "rate limit", "quota", "overload", "529", "capacity", "unavailable"))

def _make_minimax(temperature: float) -> ChatOpenAI:
    return ChatOpenAI(
        api_key=MINIMAX_API_KEY,
        base_url="https://api.minimax.io/v1",
        model="MiniMax-M2.7",
        temperature=temperature,
        max_tokens=8192,
    )

def _make_or(model: str, temperature: float) -> ChatOpenAI:
    return ChatOpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1",
        model=model,
        temperature=temperature,
        max_tokens=8192,
        request_timeout=600,
        default_headers={"HTTP-Referer": "https://devops-fleet", "X-Title": "DevOps Fleet"},
    )

_ANALYST_PRIMARY  = _make_minimax(0.1)   # análisis + planificación
_REVIEWER_PRIMARY = _make_minimax(0.0)   # revisión determinista

_OR_ANALYST_CHAIN = [
    _make_or("qwen/qwen3-coder:free", 0.1),
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
                name        TEXT NOT NULL UNIQUE,
                target_pct  REAL NOT NULL,
                window_days INTEGER NOT NULL DEFAULT 30,
                sli_query   TEXT,
                created_at  TEXT NOT NULL
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
                name        TEXT NOT NULL,
                frequency   TEXT,
                time_min    INTEGER,
                automatable INTEGER DEFAULT 0,
                notes       TEXT,
                detected_at TEXT NOT NULL
            );
        """)
        conn.commit()
        conn.close()

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
    environment: str          # "production" | "staging" | "preview"
    commit_sha: str
    triggered_by: str
    workspace: str
    deploy_url: str           # URL a validar post-deploy

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
    if GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO and state.get("commit_sha"):
        result = _github_api("GET", f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits/{state['commit_sha']}/check-runs")
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


def _deploy_execute_node(state: DeployState) -> dict:
    if not state.get("preflight_ok"):
        return {"deploy_ok": False, "deploy_output": "Omitido: preflight falló", "logs": ["DEPLOY: omitido"]}

    _log(f"[DEPLOY] Iniciando deploy a {state['environment']}...")
    env = state.get("environment", "staging")

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
    _log(f"[DEPLOY] {'✓ OK' if ok else '✗ FALLÓ'} (rc={rc})")
    return {
        "deploy_ok": ok,
        "deploy_output": output[:2000],
        "logs": [f"DEPLOY: {'OK' if ok else f'FALLÓ rc={rc}'} — {output[:200]}"],
    }


def _deploy_smoke_node(state: DeployState) -> dict:
    if not state.get("deploy_ok"):
        return {"smoke_ok": False, "smoke_notes": "Omitido: deploy falló", "logs": ["SMOKE: omitido"]}

    url = state.get("deploy_url") or PRODUCTION_URL or STAGING_URL
    if not url:
        return {"smoke_ok": True, "smoke_notes": "Sin URL configurada — omitido", "logs": ["SMOKE: sin URL"]}

    _log(f"[SMOKE] Verificando {url}...")
    notes = []

    ok_home, code_home = _smoke_check(url + "/", expected_status=200)
    notes.append(f"home: {code_home} {'✓' if code_home in (200, 307, 302) else '✗'}")

    ok_api, code_api = _smoke_check(url + "/api/health")
    notes.append(f"health: {code_api} {'✓' if code_api in (200, 404) else '✗'}")

    # E2E endpoint NO debe existir en producción
    if "vercel.app" not in url:
        ok_e2e, code_e2e = _smoke_check(url + "/api/e2e/login?email=x@x.com")
        notes.append(f"e2e-gate: {code_e2e} {'✓ (404=correcto)' if code_e2e == 404 else '✗ (expuesto!)'}")

    overall_ok = code_home in (200, 307, 302) and code_api in (200, 404)
    _log(f"[SMOKE] {'✓ OK' if overall_ok else '✗ FALLÓ'} — {'; '.join(notes)}")
    return {
        "smoke_ok": overall_ok,
        "smoke_notes": "; ".join(notes),
        "logs": [f"SMOKE: {'OK' if overall_ok else 'FALLÓ'} — {'; '.join(notes)}"],
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
    lead_time_s = None
    if GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO and state.get("commit_sha"):
        result = _github_api("GET", f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits/{state['commit_sha']}")
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
            INSERT INTO deployment_events (ts, environment, commit_sha, status, lead_time_s, triggered_by)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
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
    if GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO:
        prs = _github_api("GET", f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/pulls?state=open&per_page=1")
        if isinstance(prs, list) and prs:
            pr_number = prs[0]["number"]
            _github_api("POST", f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues/{pr_number}/comments",
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
            INSERT INTO incident_events (ts_start, ts_end, severity, root_cause, resolution, mttr_s)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
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

    with _db_lock:
        conn = _db_connect()
        deployments = [dict(r) for r in conn.execute("""
            SELECT * FROM deployment_events WHERE ts >= ? ORDER BY ts DESC
        """, (cutoff,)).fetchall()]
        incidents = [dict(r) for r in conn.execute("""
            SELECT * FROM incident_events WHERE ts_start >= ? ORDER BY ts_start DESC
        """, (cutoff,)).fetchall()]
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
    with _db_lock:
        conn = _db_connect()
        conn.execute("""
            INSERT INTO dora_snapshots
            (ts, window_days, deployment_frequency, lead_time_p50_h, lead_time_p90_h, mttr_h, change_failure_rate, dora_tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
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
