---
name: devops-fleet
description: "Orquesta el DevOps Fleet para deploy, remediación de incidents y reportes DORA. Usar cuando el usuario pide: deploy a producción, remediar un incident, generar reporte DORA, o cualquier operación de DevOps."
argument-hint: "[entorno] [commit-sha]"
allowed-tools:
  - Bash
  - Read
  - mcp__devops-fleet__deploy
  - mcp__devops-fleet__remediate
  - mcp__devops-fleet__generate_report
  - mcp__devops-fleet__get_job_status
  - mcp__devops-fleet__get_dora_metrics
  - mcp__devops-fleet__define_slo
---

# Skill: devops-fleet

Orquesta pipelines DevOps: deploy → smoke → SLO guard | incident → root cause → auto-remediate | DORA metrics.

## Cómo usar

### Deploy
```
mcp__devops-fleet__deploy(environment="production", commit_sha="abc1234", deploy_url="https://app.com")
```
Luego monitorear con:
```
mcp__devops-fleet__get_job_status(job_id="<job_id_retornado>")
```

### Remediar un incident
```
mcp__devops-fleet__remediate(alert_message="API latencia > 5s en producción", severity="P1")
```

### Generar reporte DORA
```
mcp__devops-fleet__generate_report(window_days=7, report_type="full")
```

### Ver métricas DORA actuales
```
mcp__devops-fleet__get_dora_metrics()
```

### Definir un SLO
```
mcp__devops-fleet__define_slo(name="api-availability", target_pct=99.9, window_days=30)
```

## Dashboard
- API + Dashboard: http://localhost:8010/
- N8N: http://localhost:5679/
- MCP: http://localhost:8011/
