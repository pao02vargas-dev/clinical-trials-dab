# Documento de Decisiones Técnicas
## Clinical Trials Data Pipeline & Lakeflow Orchestration

**Autora:** Paola Vargas (pao02.vargas@gmail.com)
**Institución:** Universidad Latina de Costa Rica
**Curso:** Técnico en Ingeniería de Datos con Databricks
**Fecha:** Octubre 2026
**Target:** Development / Serverless Compute

---

## 1. Contexto del Proyecto

El proyecto implementa un pipeline de datos gobernado para ensayos clínicos FDA (PMR/PMC) provenientes del Databricks Marketplace (John Snow Labs — *Biotech Pharma and Research Information Database*). La arquitectura sigue el patrón Medallion (Bronze → Silver → Gold) con Change Data Capture (CDC) automatizado y Slowly Changing Dimension Type 2 (SCD2), orquestado mediante Lakeflow Jobs con control de flujo condicional, y desplegado con Declarative Automation Bundles (DABs) + GitHub Actions.

**Pregunta de negocio:** ¿Cuál es el conteo total de ensayos segmentado por fase (NDA/Supplement/BLA/Other) y nivel de certificación del patrocinador?

---

## 2. Decisiones de Diseño

### 2.1 SDP en Python, no en SQL

**Decisión:** El pipeline SDP se implementó en Python (`src/transformations.py`) en lugar de SQL.

**Justificación:** En SQL, la función `IDENTIFIER()` no funciona dentro de `STREAM()` ni en `AUTO CDC INTO`. Esta limitación se descubrió en el Lab03 del curso. Con Python, los parámetros se obtienen vía `spark.conf.get()` del `configuration` map del pipeline, y los nombres de tabla se construyen con f-strings, logrando 100% parametrización sin limitaciones de sintaxis.

### 2.2 Auto CDC con SCD Type 2

**Decisión:** Se utilizó `dlt.create_auto_cdc_flow()` con `stored_as_scd_type=2` para la dimensión de patrocinadores.

**Parámetros clave:**
- `keys=["sponsor_id"]` — clave natural de negocio para el merge
- `sequence_by=F.col("updated_at")` — columna de secuencia temporal
- `apply_as_deletes=F.expr("operation_type = 'DELETE'")` — filas con DELETE se interpretan como borrados lógicos
- `except_column_list=["operation_type", "_source_file"]` — columnas técnicas excluidas del SCD2
- `stored_as_scd_type=2` — preserva historial completo con `__START_AT` / `__END_AT`

### 2.3 Expectations de Calidad de Datos

**Decisión:** Se implementaron 4 expectations con comportamientos distintos (drop vs warn), todas justificadas en comentarios del código.

| Expectation | Regla | Comportamiento | Justificación |
| --- | --- | --- | --- |
| `valid_trial_id` | `trial_id RLIKE '^[0-9]+$'` | **DROP** | El dataset John Snow Labs tiene filas desalineadas con texto en ID (ej. "discussions."). Se descartan IDs no numéricos. |
| `positive_duration` | `duration_days > 0` | **WARN** | Duraciones ≤ 0 son anomalías que se reportan pero NO se descartan (aportan valor analítico para investigar). |
| `valid_duration` | `duration_days IS NOT NULL` | **DROP** | Filas sin fecha válida producen `duration_days` NULL; se descartan (no aportan al análisis temporal). |
| `valid_sponsor` | `sponsor_name IS NOT NULL` | **DROP** | Sin Applicant no se puede unir a la dimensión de certificación; se descarta. |

### 2.4 Versión Vigente del SCD2 en Gold

**Decisión:** El JOIN en Gold filtra `__END_AT IS NULL` para obtener solo la versión vigente del patrocinador.

**Justificación:** SCD Type 2 preserva todo el historial, pero el análisis de negocio requiere la certificación actual del patrocinador, no versiones históricas.

### 2.5 Bronze Batch en lugar de Streaming para el Hecho

**Decisión:** `trials_bronze` usa `spark.read.table()` (batch) en lugar de `spark.readStream.table()`.

**Justificación:** El share de Delta Sharing (John Snow Labs) no soporta time travel (`DS_TIME_TRAVEL_NOT_PERMITTED`), que es requerido por los checkpoints de streaming. La dimensión `research_sponsors_bronze` sí usa Auto Loader streaming porque los archivos JSON están en un UC Volume local.

### 2.6 Permisos a Nivel de Schema (Unity Catalog)

**Decisión:** Los grants se aplican a nivel de SCHEMA, no de tabla individual.

**Justificación:** `GRANT SELECT ON SCHEMA` hereda a todas las tablas, views y metric views del esquema, demostrando dominio de la jerarquía de Unity Catalog. Se incluye también `GRANT USE SCHEMA` que es necesario para que el SELECT funcione. Solo se otorgan permisos sobre el esquema de producción (`dab__prod`), no sobre development, siguiendo el principio de mínimo privilegio.

### 2.7 Schedule del Job con Cron Schedule

**Decisión:** El Job incluye un trigger por cron schedule además del disparo por CI/CD.

**Justificación:** `quartz_cron_expression: "0 0 8 * * ?"` ejecuta el pipeline diariamente a las 8:00 AM hora de Costa Rica, garantizando que los datos estén actualizados al inicio del día laboral, sin depender únicamente de pushes manuales al repositorio.

---

## 3. Bugs Encontrados y Resueltos

Durante el desarrollo se identificaron y corrigieron 7 bugs, documentados a continuación:

### Bug 1: Decorador `@dlt.expect_all_or_drop` en función sin `@dlt.table`

- **Síntoma:** `TABLE_OR_VIEW_NOT_FOUND` en `trials_gold` al referenciar `research_sponsors_scd2`.
- **Causa:** El decorador `@dlt.expect_all_or_drop` decoraba `build_research_sponsors_scd2()`, una función que internamente llama `dlt.create_streaming_table()`. Al no tener `@dlt.table`, DLT no registraba la tabla en el grafo del pipeline.
- **Fix:** Se removió el decorador y se movieron las expectations como parámetro `expect_all_or_drop` dentro de `dlt.create_streaming_table()`.

### Bug 2: Expectation `valid_operation` referenciaba columna excluida

- **Síntoma:** `UNRESOLVED_COLUMN` en el SCD2.
- **Causa:** La expectation `valid_operation` referenciaba `operation_type`, pero esa columna se excluye del SCD2 vía `except_column_list=["operation_type", "_source_file"]`.
- **Fix:** Se removió `valid_operation` de `expect_all_or_drop`. Quedan solo `valid_sponsor_key` y `valid_event_ts`.

### Bug 3: `readStream` no soportado en Delta Sharing sin history

- **Síntoma:** `DS_TIME_TRAVEL_NOT_PERMITTED` al ejecutar `trials_bronze`.
- **Causa:** `spark.readStream.table(FACT_SOURCE)` requiere time travel para checkpoints, pero el share de Delta Sharing de John Snow Labs no tiene history sharing.
- **Fix:** Se cambió `trials_bronze` a `spark.read.table()` (batch) y `trials_silver` a `dlt.read()` (batch). `research_sponsors_bronze` (Auto Loader local) sigue streaming.

### Bug 4: Canal Client-1 no soportado en Free Edition

- **Síntoma:** `INVALID_PARAMETER_VALUE: Workspace doesn't support Client-1 channel for REPL`.
- **Causa:** El job definía `environments: default` con `spec.client: "1"` en `resources/job.yml`, y este workspace Free Edition no soporta el canal Client-1 para `spark_python_task`.
- **Fix:** Se eliminó `spec: client: "1"` de `resources/job.yml`.

### Bug 5: Función `build_research_sponsors_scd2()` nunca invocada

- **Síntoma:** `TABLE_OR_VIEW_NOT_FOUND` — `research_sponsors_scd2` no existía en el grafo del pipeline.
- **Causa:** La función que llama `dlt.create_streaming_table` y `dlt.create_auto_cdc_flow` estaba definida (líneas 93-110) pero NUNCA se llamaba a nivel de módulo.
- **Fix:** Se agregó `build_research_sponsors_scd2()` después de la definición de la función.
- **Commit:** `821cce5` en GitHub.

### Bug 6: `spark_python_task` con `python_file` pero los archivos son notebooks

- **Síntoma:** `databricks bundle deploy` fallaba porque `job_gate.py` y `check_gold_rows.py` son notebooks en el workspace, no archivos sueltos.
- **Causa:** Un merge conflict de `resources/job.yml` cambió `notebook_task` → `spark_python_task`.
- **Fix:** Se cambiaron las 3 tareas de `spark_python_task`/`python_file` a `notebook_task`/`notebook_path`/`base_parameters`.
- **Commit:** `bc8a433` en GitHub.

### Bug 7: Calidad de datos — filas desalineadas del dataset John Snow Labs

- **Síntoma:** Silver con 994 `trial_id` duplicados, 119 filas corruptas en `phase`, `duration_days` con máximo de 77,137,636 días.
- **Causa:** El dataset FDA tiene filas desalineadas (texto en columnas numéricas, fechas corruptas como "D" o "Sandoz Inc.").
- **Fix (3 cambios):**
  1. `trial_id = F.col("ID")` (no `coalesce(Number, ID)`). `expect_or_drop` descarta IDs no numéricos.
  2. `phase = F.when(F.length(F.trim(F.col("Document_Type"))) == 1, ...)` — NULL para valores corruptos.
  3. `to_date_safe` usa `F.expr("try_to_date(...)")` en vez de `F.to_date()`. `expect_or_drop` descarta filas sin fecha válida.
- **Commit:** `07b0543` en GitHub.
- **Resultado:** Silver 2,504 filas, trial_id único, phase solo N/S/B/O, duration max 26,583 días (~73 años), 0 nulos. Gold 12 filas.

---

## 4. Evidencia de Pruebas Funcionales

### 4.1 Prueba CDC / SCD Type 2

**Escenario:** Se ejecutaron dos lotes de cambios sobre la dimensión `research_sponsors_scd2`.

**Batch 1 (carga inicial):**
- 5 patrocinadores con `INSERT` (S001-S005), derivados de los 6 `Applicant` con más ensayos en el hecho real del Marketplace.
- Cada uno con certificación: GMP, FDA Approved, EMA Certified, ISO 9001, GMP.

**Batch 2 (lote de cambios):**
- `UPDATE` S002: cambia certificación de "FDA Approved" → "FDA + EMA Dual Certified"
- `INSERT` S006: nuevo patrocinador
- `DELETE` S004: patrocinador eliminado

**Resultado esperado en SCD2:**
- S002 tiene 2 versiones: la original con `__END_AT` = timestamp del update, y la nueva con `__END_AT IS NULL` (vigente).
- S004 aparece con `__END_AT` marcado (borrado lógico).
- S006 aparece como nueva versión vigente.
- Total: 7 registros en la dimensión SCD2 (5 originales + 1 update + 1 insert, menos el delete que marca la versión existente).

**Validación:** El pipeline SDP ejecutó full refresh y el SCD2 se construyó correctamente. Gold filtra `__END_AT IS NULL` para usar solo la versión vigente.

**Query de evidencia:**
```sql
SELECT sponsor_id, sponsor_name, country, certification_level, __START_AT, __END_AT
FROM dab__dev.biotech_clinical_trials.research_sponsors_scd2
ORDER BY sponsor_id, __START_AT;
```

**Resultado (7 filas):**

| sponsor_id | sponsor_name | country | certification_level | __START_AT | __END_AT | Estado |
| --- | --- | --- | --- | --- | --- | --- |
| S001 | NOVARTIS PHARMACEUTICALS CORP | United States | GMP | 2026-11-01 09:00 | NULL | ✅ Vigente |
| S002 | Merck Sharp & Dohme Corp. | United States | FDA Approved | 2026-11-01 09:05 | 2026-11-02 09:00 | 🔒 Cerrada (UPDATE) |
| S002 | Merck Sharp & Dohme Corp. | United States | FDA + EMA Dual Certified | 2026-11-02 09:00 | NULL | ✅ Vigente (UPDATE) |
| S003 | GENENTECH INC | United States | EMA Certified | 2026-11-01 09:10 | NULL | ✅ Vigente |
| S004 | Janssen Biotech, Inc. | United States | ISO 9001 | 2026-11-01 09:15 | 2026-11-02 09:10 | 🔒 Cerrada (DELETE) |
| S005 | UCB INC | United States | GMP | 2026-11-01 09:20 | NULL | ✅ Vigente |
| S006 | GILEAD SCIENCES INC | United States | FDA Approved | 2026-11-02 09:05 | NULL | ✅ Vigente (INSERT) |

**Análisis del resultado:**
- **S002** tiene 2 versiones: la original (FDA Approved, `__END_AT` = 2026-11-02 09:00) y la actualizada (FDA + EMA Dual Certified, `__END_AT IS NULL`). El CDC detectó el UPDATE y preservó el historial.
- **S004** tiene `__END_AT` marcado (2026-11-02 09:10) — el DELETE cerró la versión sin crear una nueva. El CDC detectó el DELETE.
- **S006** aparece como nueva versión vigente — el CDC detectó el INSERT.
- **Gold** filtra `__END_AT IS NULL` y obtiene 5 patrocinadores vigentes: S001, S002 (Dual), S003, S005, S006.
- **Total SCD2:** 7 registros = 5 originales + 1 versión nueva del UPDATE + 1 INSERT nuevo.

### 4.2 Prueba de CI/CD entre Ramas

**Flujo validado:**
1. Se abre un Pull Request hacia la rama `dev` (o `main` para producción).
2. GitHub Actions ejecuta `databricks bundle validate -t development` (validación verde).
3. Al hacer merge del PR, el push dispara `databricks bundle deploy -t development` + `databricks bundle run clinical_trials_orchestration_job -t development`.
4. El job ejecuta: pipeline SDP → verify_gold_rows → If/Else → success/abort.
5. Notificación por email a `pao02.vargas@gmail.com` con el resultado.

**Commits de evidencia en GitHub:**
- `5b2b923` — Fix Bug 3 (readStream → batch)
- `821cce5` — Fix Bug 5 (invocar build_research_sponsors_scd2)
- `bc8a433` — Fix Bug 6 (notebook_task en lugar de spark_python_task)
- `07b0543` — Fix Bug 7 (calidad de datos Silver)

### 4.3 Prueba del Quality Gate (If/Else)

**Escenario Gold con datos:**
- `verify_gold_rows` cuenta 12 filas en `trials_gold`.
- `taskValues.set("row_count", 12)` expone el valor.
- `gold_has_rows` evalúa `12 > 0` → TRUE.
- Se ejecuta `gold_published_ok` (job_gate.py, status=success).
- Email de éxito enviado.

**Escenario Gold vacío (simulado):**
- Si `row_count = 0`, `gold_has_rows` evalúa `0 > 0` → FALSE.
- Se ejecuta `gold_empty_abort` (job_gate.py, status=empty).
- `raise RuntimeError(...)` detiene el job con mensaje de error.
- Email de fallo enviado.

---

## 5. Arquitectura del Pipeline

```
John Snow Labs Marketplace (FDA PMR/PMC)
         │
         ▼
    trials_bronze (batch read)          research_sponsors_bronze (Auto Loader JSON)
         │                                       │
         ▼                                       ▼
    trials_silver (tipado + expectations)    research_sponsors_scd2 (Auto CDC SCD2)
         │                                       │
         └────────────── JOIN (vigente) ─────────┘
                          │
                          ▼
                    trials_gold (agregaciones)
                          │
                          ▼
              mv_clinical_trials_metrics (Metric View)
                          │
                          ▼
              Dashboard AI/BI (6 widgets)
```

### Tablas del Pipeline

| Capa | Tabla | Tipo | Filas (dev) |
| --- | --- | --- | --- |
| Bronze | `trials_bronze` | Streaming Table (batch) | 2,783 |
| Bronze | `research_sponsors_bronze` | Streaming Table (Auto Loader) | 8 |
| Silver | `trials_silver` | Materialized View (batch) | 2,504 |
| Silver | `research_sponsors_scd2` | Streaming Table (SCD2) | 7 |
| Gold | `trials_gold` | Materialized View | 12 |
| Semantic | `mv_clinical_trials_metrics` | Metric View | — |

---

## 6. Estructura del Repositorio

```
clinical-trials-dab-repo/
├── databricks.yml              # Bundle config: 2 targets (dev/prod), variables
├── resources/
│   ├── pipeline.yml            # SDP pipeline (serverless, configuration map)
│   └── job.yml                 # Lakeflow Job (pipeline → verify → If/Else + cron)
├── src/
│   ├── transformations.py      # SDP: Bronze/Silver/Gold + Auto CDC SCD2
│   ├── 00_setup_and_data_prep  # Setup: catalogs, volumes, CDC sintético
│   ├── check_gold_rows         # Notebook: COUNT(*) Gold → taskValues
│   ├── job_gate                # Notebook: success/empty abort
│   ├── metric_view.sql         # CREATE METRIC VIEW (2 measures, 2 dimensions)
│   └── grants.sql             # GRANT SELECT ON SCHEMA (jerarquía UC)
├── .github/workflows/
│   ├── dev.yaml                # CI/CD dev: validate PR → deploy+run push
│   └── prod.yaml               # CI/CD prod: validate PR → deploy+run push
└── docs/
    └── DECISIONES_TECNICAS.md  # Este documento
```

---

## 7. Gobernanza de Unity Catalog

### Jerarquía de permisos aplicada

```sql
-- Nivel de SCHEMA (hereda a todas las tablas/views del esquema)
GRANT USE SCHEMA ON SCHEMA dab__prod.biotech_clinical_trials
  TO `juan.morice@ulatina.net`;

GRANT SELECT ON SCHEMA dab__prod.biotech_clinical_trials
  TO `juan.morice@ulatina.net`;
```

**Principio de mínimo privilegio:**
- Solo se otorgan permisos sobre el esquema de **producción** (`dab__prod`).
- No se exponen tablas de **development** (`dab__dev`).
- No se otorgan permisos sobre el **Job** de orquestación.
- El receptor solo puede consultar datos, no modificarlos.

### Catálogos y esquemas

| Catálogo | Schema | Propósito |
| --- | --- | --- |
| `dab__dev` | `biotech_clinical_trials` | Desarrollo y pruebas |
| `dab__prod` | `biotech_clinical_trials` | Producción (lectura externa) |
| `john_snow_labs_biotech...` | `biotech_pharma...` | Marketplace (solo lectura) |

---

## 8. Stack Tecnológico

| Tecnología | Uso en el proyecto |
| --- | --- |
| Databricks Serverless | Compute auto-gestionado, sin infraestructura |
| Delta Lake | Formato de almacenamiento para todas las tablas |
| PySpark | Transformaciones en el SDP (Python) |
| Lakeflow Spark Declarative Pipelines | Pipeline Bronze → Silver → Gold con Auto CDC |
| Lakeflow Jobs | Orquestación con If/Else + cron schedule |
| Databricks Asset Bundles (DABs) | Infraestructura como código |
| GitHub Actions | CI/CD con OAuth M2M |
| Unity Catalog | Gobernanza, permisos a nivel de schema |
| Metric Views | Capa semántica con MEASURE() |
| AI/BI Dashboards | Visualización de KPIs |
| Auto Loader | Ingesta streaming de JSON CDC |
| Auto CDC | Change Data Capture automatizado |
| SCD Type 2 | Dimensión histórica de patrocinadores |

---

## 9. Reflexiones Finales

### 9.1 La importancia de automatizar validaciones

El Quality Gate con If/Else condicional demostró que un pipeline productivo necesita más que mover datos: necesita garantizar que los datos que llegan al dashboard son correctos. La validación automática de filas en Gold, con aborto explícito si la tabla está vacía, previene que los consumidores vean dashboards incompletos o erróneos.

### 9.2 La complejidad oculta de los datos reales

El dataset del Marketplace (John Snow Labs / FDA) presentó filas desalineadas, IDs corruptos, fechas inválidas y valores no numéricos en columnas esperadas numéricas. Esto enseñó que las expectations de calidad no son opcionales — son la primera línea de defensa contra datos que pueden comprometer todo el análisis aguas abajo.

### 9.3 El valor de la infraestructura como código

DABs con `databricks.yml` versionado en Git, desplegado automáticamente por GitHub Actions, convirtió lo que podría ser un proceso manual propenso a errores en un flujo reproducible y auditable. Cada cambio al pipeline se revisa (PR), valida (CI), despliega (CD) y ejecuta (Job) sin intervención humana.

### 9.4 Serverless como habilitador

Databricks Serverless eliminó la necesidad de gestionar clusters. El pipeline escaló automáticamente de 9 a 2,504 registros sin configuración manual. El compute se aprovisiona y libera en segundos, y el costo se asocia al uso real, no a infraestructura idle.

### 9.5 Lección aprendida: SCD2 y la dimensión del tiempo

Auto CDC con SCD Type 2 no solo captura cambios — preserva historial. Cuando un patrocinador cambia de certificación (S002: "FDA Approved" → "FDA + EMA Dual Certified"), el análisis puede referenciar tanto la versión vigente como la histórica. Esto es fundamental para auditorías y análisis temporal en el dominio clínico, donde la certificación del patrocinador en el momento del ensayo puede diferir de la certificación actual.

---

## 10. Métricas Finales del Proyecto

| Métrica | Valor |
| --- | --- |
| Ensayos procesados (Silver) | 2,504 |
| Agregaciones Gold | 12 (phase × certification_level) |
| Patrocinadores SCD2 | 7 (5 originales + 1 update + 1 insert) |
| Expectations de calidad | 4 (2 DROP + 1 WARN + 1 DROP) |
| Bugs resueltos | 7 |
| Commits en GitHub | 4+ |
| Widgets en dashboard | 6 |
| Workflows CI/CD | 2 (dev + prod) |
| Entornos aislados | 2 (development + production) |

---

*Documento generado el 28 de octubre de 2026.*
*Paola Vargas — Universidad Latina de Costa Rica — Técnico en Ingeniería de Datos con Databricks.*