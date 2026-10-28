 🧬 Clinical Trials Data Pipeline & Lakeflow Orchestration

Proyecto final desarrollado para el **Técnico en Ingeniería de Datos con Databricks** (Universidad Latina de Costa Rica).

Este repositorio implementa una arquitectura **Medallion** automatizada para el procesamiento de datos de ensayos clínicos (provenientes del Marketplace / John Snow Labs), aplicando **Change Data Capture (CDC) con SCD Tipo 2**, validaciones de calidad de datos y orquestación inteligente mediante **Databricks Asset Bundles (DABs)** y **Databricks Serverless**.

---

## 🏗️ Arquitectura y Caso de Negocio (CDC)

El pipeline procesa datos en tres capas (`Bronze` → `Silver` → `Gold`). Para cumplir con el modelo de dimensiones dinámicas, se implementó una estrategia de **Change Data Capture (SCD Tipo 2)** sobre la dimensión de **Sustancias y Fármacos Ensayados (`dim_clinical_substances`)**, derivada directamente de los datasets de ensayos clínicos.

### 📋 Estructura de la Dimensión CDC (`bronze_substances_cdc`)
La tabla de ingesta CDC incluye las columnas de negocio, la secuencia temporal y el tipo de operación:

| Columna | Tipo | Descripción | Ejemplo |
| :--- | :--- | :--- | :--- |
| `substance_id` | `STRING` | **Clave de Negocio (Business Key)** única del fármaco/sustancia. | `SUB-9042` |
| `substance_name` | `STRING` | Nombre comercial o genérico de la sustancia activa. | `Pembrolizumab` |
| `therapeutic_area` | `STRING` | Área terapéutica asociada al tratamiento. | `Oncology` |
| `approval_status` | `STRING` | Estado reglamentario actual del compuesto. | `Phase 3 Trial` |
| `cdc_timestamp` | `TIMESTAMP` | **Columna de Secuencia**: Control de orden cronológico del evento. | `2026-10-28 14:00:00` |
| `cdc_operation` | `STRING` | **Columna de Operación**: Tipo de cambio en origen (`INSERT`, `UPDATE`, `DELETE`). | `UPDATE` |

---

## 🛠️ Requisitos Previos

Antes de desplegar el bundle, asegúrate de contar con lo siguiente en tu entorno local:
1. **Python 3.10+** instalado.
2. **Databricks CLI** (`databricks`) configurado y autenticado en tu workspace.
3. Un workspace de Databricks con soporte para **Serverless Compute** (como Databricks Free Edition).

---

## 🚀 Instrucciones de Despliegue y Ejecución

Sigue estos pasos en tu terminal (CMD o PowerShell) para validar, desplegar y ejecutar el proyecto mediante Databricks Asset Bundles (DABs).

### 1. Configurar la autenticación (si usas autenticación local en texto plano)
```cmd
set DATABRICKS_AUTH_STORAGE=plaintext
2. Validar la configuración del BundleVerifica que la estructura YAML y las dependencias del DAB estén correctas:DOSdatabricks bundle validate --target development
3. Desplegar el Bundle al WorkspaceSube los archivos fuente, notebooks de control y configura los recursos (Jobs y Pipelines) en tu workspace de desarrollo:DOSdatabricks bundle deploy --target development
4. Ejecutar el Job de OrquestaciónDispara la ejecución completa del flujo automatizado (Pipeline SDP $\rightarrow$ Verificación de filas $\rightarrow$ Condición If/Else $\rightarrow$ Publicación o Aborte):DOSdatabricks bundle run clinical_trials_orchestration_job --target development
🧩 Estructura del RepositorioPlaintextclinical-trials-dab/
├── databricks.yml              # Configuración principal del DAB y variables globales
├── resources/
│   └── job.yml                 # Definición del Job de orquestación y tareas condicionales
├── src/
│   ├── check_gold_rows.py      # Notebook Python para verificar conteo de filas (Task Values)
│   ├── job_gate.py             # Notebook Python de control (éxito o parada de emergencia)
│   └── pipeline_code.py        # Lógica del pipeline Delta Live Tables (Bronze -> Silver -> Gold)
└── README.md
🛡️ Orquestación & Quality GateEl Job principal (clinical_trials_orchestration_job) implementa un flujo inteligente de control:run_sdp_pipeline: Ejecuta la ingesta y transformación en las capas Bronze, Silver (aplicando APPLY CHANGES INTO para SCD Tipo 2) y Gold.verify_gold_rows: Cuenta los registros publicados en la tabla Gold y los expone mediante dbutils.jobs.taskValues.set().gold_has_rows (If/Else Condition): Evalúa si el conteo es mayor a 0.Rama TRUE: Ejecuta job_gate.py confirmando el éxito (success) y publicando la capa.Rama FALSE: Ejecuta gold_empty_abort, generando un fallo controlado (RuntimeError) para evitar reportar datos vacíos en producción o desarrollo.👩‍💻 AutoraPaola VargasTécnico en Ingeniería de Datos con Databricks – Universidad Latina de Costa Rica