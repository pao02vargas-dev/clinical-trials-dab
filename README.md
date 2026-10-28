# Clinical Trials Biotech - Databricks Asset Bundle (DAB)

Este proyecto implementa una arquitectura de datos **Medallion Architecture (Bronze, Silver, Gold)** en **Databricks** utilizando **Databricks Asset Bundles (DABs)** y un flujo de **CI/CD con GitHub Actions**.

---

## 🏗️ Arquitectura del Proyecto

El pipeline de procesamiento de datos se organiza en las siguientes capas de Delta Lake en **Unity Catalog**:

1. **Bronze (`bronze_clinical_trials`)**: Ingesta en bruto de datos de ensayos clínicos desde la capa landing mediante **Auto Loader**.
2. **Silver (`silver_clinical_trials`)**: Limpieza, validación de calidad de datos, tipado estricto y filtrado de registros duplicados o nulos.
3. **Gold (`gold_clinical_trials`)**: Agregaciones del negocio y métricas clave preparadas para consumo analítico y tableros en Power BI.

---

## 📁 Estructura del Repositorio

```text
clinical-trials-dab/
├── .github/
│   └── workflows/
│       ├── dev.yaml          # Pipeline de CI/CD para el entorno de desarrollo
│       └── prod.yaml         # Pipeline de CI/CD para el entorno de producción
├── src/
│   ├── bronze/               # Notebooks o scripts de ingesta Bronze
│   ├── silver/               # Notebooks de transformación y calidad Silver
│   └── gold/                 # Notebooks de modelos agregados Gold
├── databricks.yml            # Configuración principal del Databricks Asset Bundle
├── resources/                # Definición de Pipelines DLT y Jobs en YAML
│   ├── clinical_trials_pipeline.yml
│   └── clinical_trials_job.yml
└── README.md