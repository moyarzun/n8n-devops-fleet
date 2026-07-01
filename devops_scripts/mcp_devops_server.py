"""
MCP Server — DevOps Fleet
=========================
Expone las 3 operaciones del fleet como herramientas MCP para Claude Code.
"""

import os
import json
import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("devops-fleet")

FLEET_API = os.getenv("DEVOPS_FLEET_API_URL", "http://devops-fleet-api:8010")


def _post(path: str, body: dict) -> dict:
    try:
        r = httpx.post(f"{FLEET_API}{path}", json=body, timeout=30)
        return r.json()
    except Exception as exc:
        return {"error": str(exc)}


def _get(path: str) -> dict:
    try:
        r = httpx.get(f"{FLEET_API}{path}", timeout=20)
        return r.json()
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool()
def deploy(
    environment: str = "staging",
    commit_sha: str = "",
    deploy_url: str = "",
    triggered_by: str = "claude-code",
) -> str:
    """
    Dispara el pipeline de deploy del DevOps Fleet.
    Ejecuta: preflight → deploy (Vercel) → smoke tests → SLO guard → DORA record → notify.

    Args:
        environment: 'production', 'staging' o 'preview'
        commit_sha: SHA del commit a deployar (para DORA lead time)
        deploy_url: URL a validar en smoke tests (ej. https://app.com)
        triggered_by: quién disparó el deploy
    """
    result = _post("/pipeline/deploy", {
        "environment": environment,
        "commit_sha": commit_sha,
        "deploy_url": deploy_url,
        "triggered_by": triggered_by,
    })
    job_id = result.get("job_id", "?")
    return f"Deploy iniciado — job_id: {job_id}. Consultá el estado en GET /status/{job_id}"


@mcp.tool()
def remediate(
    alert_message: str,
    severity: str = "P2",
    alert_source: str = "manual",
    environment: str = "production",
) -> str:
    """
    Dispara el pipeline de remediación del DevOps Fleet.
    Ejecuta: recopilar contexto → causa raíz (LLM) → plan → auto-remediate → escalar si necesario → registrar MTTR.

    Args:
        alert_message: descripción del incident o alerta
        severity: 'P1' (crítico), 'P2' (alto), 'P3' (medio)
        alert_source: 'slo_breach', 'smoke_fail', 'ci_fail', 'manual'
        environment: entorno afectado
    """
    result = _post("/pipeline/remediate", {
        "alert_message": alert_message,
        "severity": severity,
        "alert_source": alert_source,
        "environment": environment,
    })
    job_id = result.get("job_id", "?")
    return f"Remediación iniciada — job_id: {job_id}. Consultá el estado en GET /status/{job_id}"


@mcp.tool()
def generate_report(
    window_days: int = 7,
    report_type: str = "full",
) -> str:
    """
    Genera un reporte de métricas DevOps (DORA + SLO + toil analysis).
    Ejecuta: recolectar eventos → calcular DORA → SLO tracker → análisis de toil (LLM) → generar reporte → publicar.

    Args:
        window_days: ventana de análisis en días (7, 14, 30)
        report_type: 'full', 'dora', 'slo'
    """
    result = _post("/pipeline/metrics", {
        "window_days": window_days,
        "report_type": report_type,
    })
    job_id = result.get("job_id", "?")
    return f"Reporte iniciado — job_id: {job_id}. Consultá el estado en GET /status/{job_id}"


@mcp.tool()
def get_job_status(job_id: str) -> str:
    """
    Consulta el estado y logs de un job del DevOps Fleet.

    Args:
        job_id: ID del job retornado por deploy(), remediate() o generate_report()
    """
    result = _get(f"/status/{job_id}")
    return json.dumps(result, indent=2, ensure_ascii=False)


@mcp.tool()
def get_dora_metrics() -> str:
    """
    Retorna las últimas métricas DORA calculadas:
    Deployment Frequency, Lead Time, MTTR, Change Failure Rate, y DORA tier.
    """
    result = _get("/metrics/dora")
    snapshots = result.get("snapshots", [])
    if not snapshots:
        return "Sin snapshots DORA disponibles. Ejecutá generate_report() primero."
    latest = snapshots[0]
    return (
        f"DORA Metrics (ventana: {latest.get('window_days')} días)\n"
        f"  Tier: {latest.get('dora_tier', '?')}\n"
        f"  Deployment Frequency: {latest.get('deployment_frequency', '?')}/día\n"
        f"  Lead Time p50: {latest.get('lead_time_p50_h', '?')}h\n"
        f"  MTTR: {latest.get('mttr_h', '?')}h\n"
        f"  Change Failure Rate: {latest.get('change_failure_rate', '?')}%\n"
    )


@mcp.tool()
def define_slo(
    name: str,
    target_pct: float,
    window_days: int = 30,
) -> str:
    """
    Define o actualiza un SLO (Service Level Objective).

    Args:
        name: nombre del SLO (ej. 'api-availability', 'p99-latency')
        target_pct: porcentaje objetivo (ej. 99.9 para 99.9%)
        window_days: ventana de evaluación en días
    """
    result = _post("/slo", {
        "name": name,
        "target_pct": target_pct,
        "window_days": window_days,
    })
    return f"SLO '{name}' configurado: {target_pct}% en {window_days} días"


if __name__ == "__main__":
    mcp.run()
