---
name: devops-fleet
description: "Invoca el n8n-devops-fleet para aplicar prácticas DevOps a un proyecto: deploy automatizado con smoke tests y rollback, remediación de incidents con root cause LLM, métricas DORA y reporte semanal, o definición de SLOs. Usar cuando el usuario exprese: 'quiero DevOps', 'necesito CI/CD', 'deployar a producción', 'hay un incident', 'métricas DORA', 'cómo está la salud del sistema', 'monitorear', 'SLO', 'error budget', 'necesito aplicar DevOps', o cualquier variante."
argument-hint: "[deploy|remediate|metrics|slo] [entorno|severidad|días]"
allowed-tools:
  - Bash
  - Read
  - AskUserQuestion
---

# Skill: devops-fleet

Conecta la sesión actual con el n8n-devops-fleet para ejecutar pipelines DevOps.
El fleet corre en Docker en `http://localhost:8010`.

Anuncia al inicio: "Usando devops-fleet para orquestar la operación DevOps solicitada."

---

## Fase 0 — Detectar intención y verificar fleet

### 0.1 Verificar que el fleet está corriendo

```bash
curl -s http://localhost:8010/health 2>/dev/null
```

Si el resultado **no** es `{"status":"ok",...}`:
- Reportar: "El DevOps Fleet no está corriendo. Para iniciarlo:"
  ```
  cd /Users/moyarzun/Documents/Claude/Projects/n8n-devops-fleet
  make up
  ```
- Detener la skill y pedir al usuario que lo levante.

### 0.2 Recopilar contexto del proyecto actual

```bash
git rev-parse --show-toplevel 2>/dev/null || echo "no-git"
git branch --show-current 2>/dev/null
git rev-parse HEAD 2>/dev/null
git log -1 --format="%H %s" 2>/dev/null
```

Guarda:
- `WORKSPACE` = ruta del proyecto (resultado de `git rev-parse --show-toplevel`)
- `BRANCH` = rama actual
- `COMMIT_SHA` = SHA del último commit
- `PROJECT_NAME` = nombre de la carpeta raíz

### 0.3 Identificar la operación solicitada

Analiza el mensaje del usuario para determinar el **modo**:

| Si el usuario menciona... | Modo |
|---|---|
| deploy, deployar, publicar, subir a producción/staging, CI/CD, release | `DEPLOY` |
| incident, alerta, caído, error en prod, degradado, fallo, crash, P1, P2, P3 | `REMEDIATE` |
| métricas, DORA, reporte, estadísticas, deployment frequency, lead time, MTTR, change failure | `METRICS` |
| SLO, SLI, error budget, disponibilidad, uptime, objetivo de servicio | `SLO` |
| DevOps en general, necesito DevOps, aplicar DevOps, cómo está | `DIAGNOSE` |

Si el modo es `DIAGNOSE` o no está claro → ir a **Fase 0.4**.
Si el modo está claro → saltar directamente a la fase correspondiente.

### 0.4 Preguntar (solo si modo ambiguo)

Usar AskUserQuestion:

**"¿Qué operación DevOps necesitas?"**

Opciones:
1. **Deploy** — Deployar el proyecto a producción o staging con smoke tests y SLO guard automático
2. **Incident** — Hay un incident o alerta: el fleet analiza la causa raíz y ejecuta remediación automática
3. **Reporte DORA** — Calcular métricas DORA (Deployment Frequency, Lead Time, MTTR, CFR) y generar reporte
4. **Diagnóstico general** — El fleet hace un análisis completo: últimos deployments, SLOs, toil detectado

---

## Fase 1 — Pipeline DEPLOY

### 1.1 Determinar parámetros

Si `$ARGUMENTS` contiene "production" o "prod" → `ENVIRONMENT=production`
Si `$ARGUMENTS` contiene "staging" → `ENVIRONMENT=staging`
Si no está especificado → preguntar:

```
¿A qué entorno quieres deployar?
- production (tenniscoach.sancirilo.cl)
- staging (vercel preview)
```

Determinar la `DEPLOY_URL`:
- Si `ENVIRONMENT=production`: leer `PRODUCTION_URL` de `.env` o usar el dominio conocido del proyecto.
- Si `ENVIRONMENT=staging`: puede quedar vacío (el fleet hará smoke a la URL que Vercel genere).

### 1.2 Trigger deploy

```bash
curl -s -X POST http://localhost:8010/pipeline/deploy \
  -H "Content-Type: application/json" \
  -d "{
    \"environment\": \"$ENVIRONMENT\",
    \"commit_sha\": \"$COMMIT_SHA\",
    \"deploy_url\": \"$DEPLOY_URL\",
    \"triggered_by\": \"claude-code-devops-fleet-skill\",
    \"workspace\": \"$WORKSPACE\"
  }"
```

Extrae `job_id` del JSON de respuesta.

### 1.3 Monitorear el job

Polling cada 10 segundos (máximo 15 intentos = ~2.5 minutos):

```bash
JOB_ID="<job_id>"
for i in $(seq 1 15); do
  sleep 10
  RESULT=$(curl -s "http://localhost:8010/status/$JOB_ID")
  STATUS=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','?'))")
  PHASE=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('phase','')[:80])")
  echo "[$i/15] status=$STATUS | $PHASE"
  if [[ "$STATUS" != "queued" && "$STATUS" != "running" ]]; then
    break
  fi
done
echo "$RESULT"
```

### 1.4 Reportar resultado

Parsear el resultado final y mostrar:

```
═══════════════════════════════════════
  DevOps Fleet — Deploy Report
  Entorno: {ENVIRONMENT}
  Commit: {COMMIT_SHA[:8]}
═══════════════════════════════════════

  ✓/✗ Preflight:    {preflight_notes}
  ✓/✗ Deploy:       {deploy_ok}
  ✓/✗ Smoke tests:  {smoke_notes}
  ✓/✗ SLO guard:    {slo_notes}
  {⚠ Rollback automático activado — si aplica}

  Estado final: {status}
═══════════════════════════════════════
```

Si `rollback_triggered=true`:
- Informar que el deploy fue revertido automáticamente por degradación de SLI.
- Sugerir revisar los logs con: `make logs` en la carpeta del fleet.

Si `status=failed`:
- Mostrar los últimos logs del job.
- Sugerir revisar el error y commitear un fix antes de reintentar.

---

## Fase 2 — Pipeline REMEDIATE

### 2.1 Recopilar detalles del incident

Si el usuario no los dio, preguntar con AskUserQuestion:

**"Cuéntame del incident"**

Campos:
- Mensaje/descripción del problema (obligatorio)
- Severidad: P1 (crítico/caído), P2 (degradado), P3 (menor)
- Entorno afectado: production / staging

Defaults si no se especifican: severity=P2, environment=production.

### 2.2 Trigger remediación

```bash
curl -s -X POST http://localhost:8010/pipeline/remediate \
  -H "Content-Type: application/json" \
  -d "{
    \"alert_message\": \"$ALERT_MESSAGE\",
    \"severity\": \"$SEVERITY\",
    \"alert_source\": \"claude-code\",
    \"environment\": \"$ENVIRONMENT\",
    \"workspace\": \"$WORKSPACE\"
  }"
```

Extrae `job_id`.

### 2.3 Monitorear (igual que Fase 1.3)

Polling 10s × 20 intentos máximo (incidents P1 pueden tomar más).

### 2.4 Reportar resultado

```
═══════════════════════════════════════
  DevOps Fleet — Incident Report
  Severidad: {SEVERITY}
═══════════════════════════════════════

  Causa raíz identificada:
  {root_cause}

  Remediación automática: {✓ aplicada / ✗ no fue posible}
  Notas: {remediation_notes}

  Escalación: {✓ escalado a Slack / ✗ no necesario}
  MTTR registrado: {mttr_s}s

  Estado: {status}
═══════════════════════════════════════
```

Si `escalated=true`:
- Indicar que se envió notificación a Slack con el detalle del incident.
- Recordar revisar el canal configurado.

Si `auto_remediated=false` y `escalated=true`:
- La causa raíz requiere intervención manual. Mostrar el plan de remediación generado por el LLM.

---

## Fase 3 — Pipeline METRICS

### 3.1 Determinar ventana

Si `$ARGUMENTS` contiene un número → usar ese número de días.
Default: 7 días.

Si el usuario dice "reporte mensual" → 30 días.
Si dice "semanal" → 7 días.
Si dice "quincenal" → 14 días.

### 3.2 Trigger metrics

```bash
WINDOW=${WINDOW:-7}
curl -s -X POST http://localhost:8010/pipeline/metrics \
  -H "Content-Type: application/json" \
  -d "{
    \"window_days\": $WINDOW,
    \"report_type\": \"full\"
  }"
```

Extrae `job_id`.

### 3.3 Monitorear (polling 15s × 20 intentos — LLM tarda más)

### 3.4 Fetch y mostrar métricas DORA

```bash
curl -s http://localhost:8010/metrics/dora | python3 -c "
import sys, json
d = json.load(sys.stdin)
s = d.get('snapshots', [{}])[0]
tier = s.get('dora_tier', '?')
df = s.get('deployment_frequency', 0)
lt = s.get('lead_time_p50_h')
mttr = s.get('mttr_h', 0)
cfr = s.get('change_failure_rate', 0)

elite = {'DF': '≥1/día', 'LT': '≤24h', 'MTTR': '≤1h', 'CFR': '≤5%'}

print(f'''
DORA Metrics ({s.get(\"window_days\", \"?\")}-day window)
──────────────────────────────────────────
  Tier:               {tier}
  
  Deployment Freq:    {df:.2f}/día     (Elite: {elite[\"DF\"]})
  Lead Time p50:      {(str(round(lt,1))+\"h\") if lt else \"sin datos\"}     (Elite: {elite[\"LT\"]})
  MTTR:               {mttr:.1f}h         (Elite: {elite[\"MTTR\"]})
  Change Fail Rate:   {cfr:.1f}%          (Elite: {elite[\"CFR\"]})
──────────────────────────────────────────
''')
"
```

Interpretar el tier y dar recomendación:
- **Elite** → "El proyecto opera en el tier más alto de DORA. Seguir monitoreando para mantenerlo."
- **High** → "Buen rendimiento. Oportunidades de mejora: {señalar la métrica más lejana de Elite}."
- **Medium** → "Hay margen de mejora. El mayor impacto estaría en: {métrica peor}."
- **Low** → "Se recomienda priorizar DevOps. Empezar por: automatizar el proceso de deploy y reducir el tamaño de los PRs."

---

## Fase 4 — Definir SLO

### 4.1 Recopilar definición

Si el usuario no especificó, preguntar:

**"¿Qué SLO querés definir?"**

Ejemplos:
- Disponibilidad API: 99.9% en 30 días
- Latencia p99 < 500ms: 99% en 7 días
- Tasa de error < 1%: 99.5% en 14 días

### 4.2 Crear SLO

```bash
curl -s -X POST http://localhost:8010/slo \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$SLO_NAME\",
    \"target_pct\": $TARGET_PCT,
    \"window_days\": $WINDOW_DAYS
  }"
```

### 4.3 Mostrar error budget

```
SLO '{SLO_NAME}' configurado:
  Target:  {TARGET_PCT}%
  Ventana: {WINDOW_DAYS} días
  
  Error budget total: {(100-TARGET_PCT)/100 * WINDOW_DAYS * 24 * 60:.0f} minutos
  
  Con este SLO, el sistema puede estar caído hasta {budget_min:.0f} minutos
  en {WINDOW_DAYS} días antes de quemar el budget.
  
  El fleet monitoreará el budget en cada deploy. Si queda < 5%,
  los deploys a producción serán bloqueados automáticamente.
```

---

## Fase 5 — Diagnóstico general (modo DIAGNOSE)

Ejecutar en paralelo:

```bash
# Estado del fleet
curl -s http://localhost:8010/jobs?limit=5

# Últimas métricas DORA
curl -s http://localhost:8010/metrics/dora

# SLO budget
curl -s http://localhost:8010/slo/budget
```

Presentar un resumen:

```
═══════════════════════════════════════
  DevOps Fleet — Estado del Proyecto
  Proyecto: {PROJECT_NAME} / {BRANCH}
═══════════════════════════════════════

  Últimos jobs:
  {lista de últimos 5 jobs con status}

  DORA (últimos datos):
  {tier} — DF:{df}/día | MTTR:{mttr}h | CFR:{cfr}%

  SLOs configurados: {N}
  {lista con nombre y estado del budget}

  Recomendaciones:
  {0-3 acciones concretas basadas en los datos}
═══════════════════════════════════════
```

Luego ofrecer al usuario las opciones de Fase 0.4 para profundizar en alguna operación.

---

## Dashboard

El DevOps Fleet tiene un dashboard web disponible en:
- **Fleet Dashboard**: http://localhost:8010/
- **N8N Workflows**: http://localhost:5679/
- **Documentación de la API**: http://localhost:8010/docs

Para abrir el dashboard, sugerir al usuario: `! open http://localhost:8010`
