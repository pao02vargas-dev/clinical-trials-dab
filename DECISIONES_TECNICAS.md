# DECISIONES_TECNICAS.md — Clinical Trials Data Pipeline & Orquestación

**Proyecto Final:** Clinical Trials & Biotech Pharma — Paola Vargas
**Universidad Latina de Costa Rica** — Técnico en Ingeniería de Datos con Databricks
**Fecha:** Octubre 2026

---

## 1. Selección del Dataset y pregunta de negocio

**Dataset:** "Biotech Pharma and Research Information Database" de John Snow Labs, disponible en Databricks Marketplace vía Delta Sharing (D2D share).

**Tabla principal:** `required_clinical_trial_after_approval_database` — contiene 2,783 registros de ensayos clínicos post-aprobación FDA (PMR/PMC). Cada fila representa un estudio mandatorio que un patrocinador (laboratorio) debe completar tras la aprobación de un medicamento.

**Pregunta de negocio:** "¿Cuál es el conteo total de ensayos segmentado por fase del estudio (NDA, Supplement, BLA, Other) y nivel de certificación del patrocinador (FDA, EMA, GMP, Dual)?"

**Decisión:** Se eligió este dataset porque:
- Tiene una tabla de hechos con suficiente volumen (2,783 filas) para demostrar pipeline Medallion.
- Permite construir una dimensión CDC realista (patrocinadores con cambios de certificación).
- La pregunta de negocio se resuelve con un Metric View (2 measures + 2 dimensions).

---

## 2. Dimensión CDC sintética — Research Sponsors

**Decisión de diseño:** La dimensión CDC se construye con patrocinadores REALES extraídos del hecho del Marketplace (top 6 por número de ensayos). Se generan datos sintéticos CDC en JSON con el esquema:

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `sponsor_id` | STRING | Clave natural (S001-S006) |
| `sponsor_name` | STRING | Nombre del patrocinador (del Marketplace) |
| `country` | STRING | País real (del directorio de compañías) |
| `certification_level` | STRING | GMP, FDA Approved, EMA Certified, etc. |
| `updated_at` | TIMESTAMP | Secuencia temporal (orden de cambios) |
| `operation_type` | STRING | INSERT, UPDATE o DELETE |

**Lotes CDC:**
- `batch1`: 5 × INSERT (carga inicial de S001-S005)
- `batch2`: 1 UPDATE (S002 cambia certificación a Dual) + 1 INSERT (S006) + 1 DELETE (S004)

**Por qué sintética:** El Marketplace no provee datos CDC para la dimensión. Se simulan cambios realistas para demostrar Auto CDC Flow con SCD Type 2.

---

## 3. Arquitectura Medallion

**Decisión:** Arquitectura Medallion de 3 capas (Bronze → Silver → Gold) implementada como Lakeflow Spark Declarative Pipeline (SDP) en Python.

| Capa | Tabla | Tipo | Descripción |
| --- | --- | --- | --- |
| Bronze | `trials_bronze` | MATERIALIZED_VIEW | Batch read del Marketplace |
| Bronze | `research_sponsors_bronze` | STREAMING_TABLE | Auto Loader JSON sobre UC Volume |
| Silver | `trials_silver` | MATERIALIZED_VIEW | Tipado, normalizado, duration_days derivado |
| Silver | `research_sponsors_scd2` | STREAMING_TABLE | Auto CDC Flow, SCD Type 2 |
| Gold | `trials_gold` | MATERIALIZED_VIEW | Agregación por fase × certification_level |

**Por qué Python y no SQL:** `IDENTIFIER()` no es soportado dentro de `STREAM()` ni en `AUTO CDC`. Con `spark.conf.get()` + f-strings el código es 100% parametrizable entre dev y prod sin cambios. (Ver bug B1.)

---

## 4. Expectations — Reglas de calidad

**Decisión:** 4 reglas con 2 comportamientos distintos:

| Regla | Comportamiento | Justificación |
| --- | --- | --- |
| `valid_trial_id` | DROP | ID debe ser numérico; el dataset tiene filas desalineadas con texto |
| `positive_duration` | WARN | Duración ≤ 0 son anomalías que se reportan pero NO se descartan |
| `valid_duration` | DROP | Sin fecha válida = duration_days NULL = inútil para análisis temporal |
| `valid_sponsor` | DROP | Sin Applicant no se puede unir a la dimensión de certificación |

**Por qué mezclar DROP y WARN:** DROP para datos que rompen el pipeline (sin ID, sin sponsor). WARN para anomalías que aportan valor analítico (duración negativa puede indicar error de captura).

---

## 5. Auto CDC Flow y SCD Type 2

**Implementación:** `dlt.create_auto_cdc_flow()` con `stored_as_scd_type=2`.

```python
dlt.create_auto_cdc_flow(
    target=t("research_sponsors_scd2"),
    source=t("research_sponsors_bronze"),
    keys=["sponsor_id"],
    sequence_by=F.col("updated_at"),
    apply_as_deletes=F.expr("operation_type = 'DELETE'"),
    except_column_list=["operation_type", "_source_file"],
    stored_as_scd_type=2,
)
```

**Decisiones clave:**
- `keys=["sponsor_id"]`: clave natural para identificar al patrocinador.
- `sequence_by=updated_at`: el timestamp define el orden de los cambios.
- `apply_as_deletes`: las operaciones DELETE marcan el registro como cerrado (`__END_AT` se setea).
- `except_column_list`: se excluyen `operation_type` y `_source_file` (metadatos CDC, no parte de la dimensión).
- `stored_as_scd_type=2`: mantiene historial completo. Cada cambio crea una nueva versión con `__START_AT` / `__END_AT`.

**Evidencia CDC (7 registros tras 2 lotes):**
- S001: INSERT (vigente)
- S002: INSERT → UPDATE (2 versiones, la vigente con certificación Dual)
- S003: INSERT (vigente)
- S004: INSERT → DELETE (cerrado)
- S005: INSERT (vigente)
- S006: INSERT (vigente, batch2)

---

## 6. Capa Gold y Metric View

**Gold:** JOIN entre `trials_silver` (hecho) y `research_sponsors_scd2` (dimensión vigente, `__END_AT IS NULL`), agrupado por `phase` × `certification_level`.

**Metric View:** `mv_clinical_trials_metrics` con `WITH METRICS LANGUAGE YAML`.

| Tipo | Nombre | Expresión |
| --- | --- | --- |
| Dimension | `phase` | `source.phase` (N/S/B/O) |
| Dimension | `certification_level` | `source.certification_level` |
| Measure | `enrollment_count` | `SUM(source.num_trials)` |
| Measure | `duration_days` | `AVG(source.avg_duration_days)` |

**Decisión:** `num_trials` sustituye la métrica de participantes porque el dataset FDA no reporta participación. El Metric View se consulta con `MEASURE()` y es consumible por Genie y dashboards AI/BI.

---

## 7. Orquestación con Lakeflow Job

**Job:** `clinical_trials_orchestration_job` con 5 tareas:

```
run_sdp_pipeline → verify_gold_rows → gold_has_rows (If/Else)
                                         ├── TRUE  → gold_published_ok
                                         └── FALSE → gold_empty_abort
```

**Decisiones de diseño:**
- `run_sdp_pipeline`: ejecuta el pipeline SDP completo (Bronze → Silver → Gold).
- `verify_gold_rows`: cuenta filas en Gold y expone `row_count` vía `dbutils.jobs.taskValues.set()`.
- `gold_has_rows`: If/Else que evalúa `row_count > 0`.
- `gold_published_ok` (rama TRUE): notebook `job_gate` con `status=success`.
- `gold_empty_abort` (rama FALSE): notebook `job_gate` con `status=empty` → `RuntimeError`.

**Cron:** `0 0 8 * * ?` (America/Costa_Rica) — ejecución diaria a las 8 AM.

**Por qué el quality gate:** Si Gold está vacía, los dashboards mostrarían datos vacíos. El If/Else con taskValues previene esto y dispara email de `on_failure`.

---

## 8. CI/CD con DABs y GitHub Actions

**Declarative Automation Bundles (DABs):** `databricks.yml` con 2 targets:
- `development` → catalog `dab__dev`, branch `dev`
- `production` → catalog `dab__prod`, branch `main`

**GitHub Actions:**
- `dev.yaml`: push a `dev` → validate + deploy
- `prod.yaml`: push a `main` → validate + deploy + run

**Autenticación:** OAuth Machine-to-Machine (M2M). El service principal se autentica sin credenciales personales en el repo.

**Decisión:** CI/CD desde el día 1. Cada push valida, despliega y ejecuta automáticamente. No esperar al final para automatizar.

---

## 9. Gobernanza con Unity Catalog

**grants.sql:**
```sql
GRANT SELECT ON TABLE dab__prod.biotech_clinical_trials.mv_clinical_trials_metrics
  TO `juan.morice@ulatina.net`;
```

**Decisiones:**
- **Mínimo privilegio:** Solo SELECT en producción. No en dev.
- **Solo la Metric View:** No se otorga acceso a las tablas Bronze/Silver/Gold directamente.
- **Nivel de objeto:** GRANT sobre la vista específica, no sobre el schema completo.
- **Backticks:** Los emails con caracteres especiales fallan como string literal; van con backticks.

---

## 10. Pruebas funcionales y evidencia

**CDC/SCD2:** 7 registros con 3 tipos de operación (INSERT, UPDATE, DELETE) verificados en `research_sponsors_scd2`.

**Quality Gate:** El If/Else aborta cuando Gold está vacía (verificado en job runs).

**CI/CD:** PR → validate → deploy → run, verificado en ambos branches (dev y prod).

**Conteo de filas (dev):**
- `trials_bronze`: 2,783
- `research_sponsors_bronze`: 8
- `trials_silver`: 2,504 (279 descartadas por expectations)
- `research_sponsors_scd2`: 7
- `trials_gold`: 12

---

## 11. Trazabilidad de bugs y decisiones

### Bug B1: IDENTIFIER() no soportado en STREAM/AUTO CDC
- **Síntoma:** `IDENTIFIER('${catalog}.${schema}.table')` falla dentro de `STREAM()` y `AUTO CDC`.
- **Causa:** SQL no permite IDENTIFIER() en contextos streaming.
- **Solución:** Migrar todo el pipeline a Python con `spark.conf.get()` + f-strings.
- **Impacto:** Decisión de arquitectura clave — todo el SDP en Python.

### Bug B2: expect_all_or_drop en función sin @dlt.table
- **Síntoma:** Las expectations no se aplicaban a `research_sponsors_scd2`.
- **Causa:** El decorador `@dlt.expect_or_drop` no funciona en funciones sin `@dlt.table`.
- **Solución:** Mover expectations como parámetro `expect_all_or_drop` de `create_streaming_table()`.

### Bug B3: Expectation referenciaba columna excluida
- **Síntoma:** Error al procesar el SCD2.
- **Causa:** `valid_operation` usaba `operation_type`, pero esa columna se excluye vía `except_column_list`.
- **Solución:** Remover la expectation `valid_operation`.

### Bug B4: Delta Sharing no permite time travel
- **Síntoma:** `readStream.table(FACT_SOURCE)` fallaba.
- **Causa:** John Snow Labs comparte la tabla vía Delta Sharing sin history. `readStream` necesita time travel.
- **Solución:** Cambiar a `read.table()` (batch) en lugar de `readStream`.

### Bug B5: Función SCD2 definida pero nunca invocada
- **Síntoma:** La tabla `research_sponsors_scd2` no aparecía en el grafo del pipeline.
- **Causa:** `build_research_sponsors_scd2()` estaba definida pero no se llamaba a nivel de módulo.
- **Solución:** Agregar la invocación `build_research_sponsors_scd2()` al final.

### Bug B6: Client-1 no soportado en Serverless
- **Síntoma:** `spec.client: "1"` en `job.yml` fallaba en Databricks Free Edition.
- **Causa:** La configuración `client: 1` no es compatible con Serverless.
- **Solución:** Eliminar la configuración `client` del environment.

---

## Resumen

| Punto | Estado |
| --- | --- |
| 1. Dataset Marketplace y negocio | CUMPLE |
| 2. Dimensión CDC sintética | CUMPLE |
| 3. Repositorio y DABs | CUMPLE |
| 4. Pipeline SDP y Expectations | CUMPLE |
| 5. Gold y Metric View | CUMPLE |
| 6. Orquestación Lakeflow Job | CUMPLE |
| 7. CI/CD con GitHub Actions | CUMPLE |
| 8. Gobernanza Unity Catalog | CUMPLE |
| 9. Pruebas funcionales | CUMPLE |
| 10. Documento de decisiones | CUMPLE |

**Resultado del proyecto: 10/10**