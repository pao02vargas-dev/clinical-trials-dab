# Databricks notebook source
# DBTITLE 1,Titulo - Preparacion del Entorno
# MAGIC %md
# MAGIC # Preparacion del Entorno y Datos CDC
# MAGIC
# MAGIC Este notebook prepara toda la infraestructura de **Unity Catalog** y genera los datos **CDC sinteticos** para la dimension **Research Sponsors** (patrocinadores de ensayos clinicos).
# MAGIC
# MAGIC ## Que hace?
# MAGIC
# MAGIC 1. **Crea catalogos, schemas y volumes** en `dab__dev` y `dab__prod` (idempotente)
# MAGIC 2. **Verifica acceso** a la tabla del Marketplace de John Snow Labs (hecho principal con 2,783 ensayos)
# MAGIC 3. **Genera datos CDC** con 2 lotes: batch1 (5 INSERT) y batch2 (UPDATE + INSERT + DELETE)
# MAGIC
# MAGIC ## Como usarlo
# MAGIC
# MAGIC | Argumento | Que hace |
# MAGIC | --- | --- |
# MAGIC | `all` (default) | Ejecuta todo: batch1 + batch2 |
# MAGIC | `1` | Solo carga inicial (batch1) |
# MAGIC | `2` | Solo lote de cambios (batch2) |
# MAGIC
# MAGIC **Demo paso a paso:** correr `1` -> update del SDP -> correr `2` -> update del SDP

# COMMAND ----------

# =====================================================================
# 00_setup_and_data_prep.py — Preparación automática de entorno y datos CDC
# Proyecto Final: Clinical Trials & Biotech Pharma — Paola Vargas
# Ejecutar en notebook serverless. Modo: sin argumentos = todo;
# "1" = solo carga inicial; "2" = solo lote de cambios.
# Demo paso a paso: correr "1" -> update del SDP -> "2" -> update del SDP.
# =====================================================================
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOGS = ["dab__dev", "dab__prod"]
SCHEMA = "biotech_clinical_trials"
BATCH = sys.argv[1] if len(sys.argv) > 1 else "all"

# ---------------------------------------------------------------------
# 1. Creación de catálogos, esquemas y volumes (idempotente)
# ---------------------------------------------------------------------
for catalog in CATALOGS:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception as e:
        print(f"[AVISO] Catálogo `{catalog}` no creado (¿ya existe?): {e}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.{SCHEMA}.cdc_source")
    print(f"[OK] Entorno listo: {catalog}.{SCHEMA} + volume cdc_source")

# ---------------------------------------------------------------------
# 2. Verificación de la tabla del Marketplace (hecho principal)
# ---------------------------------------------------------------------
FACT_TABLE = ("john_snow_labs_biotech_pharma_and_research_information_database."
              "biotech_pharma_and_research_information_database."
              "required_clinical_trial_after_approval_database")
DIRECTORY_TABLE = ("john_snow_labs_biotech_pharma_and_research_information_database."
                   "biotech_pharma_and_research_information_database."
                   "directory_of_us_and_eu_pharmaceutical_companies")

n_rows = spark.read.table(FACT_TABLE).count()
print(f"[OK] Hecho del Marketplace accesible: {FACT_TABLE} ({n_rows} filas)")

# ---------------------------------------------------------------------
# 3. Datos CDC — dimensión Research Sponsors (Applicants REALES del hecho)
#    batch1 = 5 patrocinadores INSERT | batch2 = UPDATE + INSERT + DELETE
# ---------------------------------------------------------------------
SPONSOR_SCHEMA = StructType([
    StructField("sponsor_id",           StringType()),
    StructField("sponsor_name",         StringType()),
    StructField("country",              StringType()),
    StructField("certification_level",  StringType()),
    StructField("updated_at",           StringType()),   # ISO 8601 -> timestamp
    StructField("operation_type",       StringType()),
])

top_sponsors = [r["Applicant"] for r in spark.sql(f"""
    SELECT Applicant, COUNT(*) AS n
    FROM {FACT_TABLE}
    WHERE Applicant IS NOT NULL AND TRIM(Applicant) != ''
    GROUP BY Applicant ORDER BY n DESC, Applicant LIMIT 6
""").collect()]
print(f"[OK] Patrocinadores seleccionados: {top_sponsors}")

def country_of(name: str) -> str:
    """País real desde el directorio de compañías; fallback United States."""
    r = spark.sql(f"SELECT Country FROM {DIRECTORY_TABLE} "
                  f"WHERE UPPER(TRIM(Pharmaceutical_Company_Name)) = "
                  f"'{name.upper().replace(chr(39), chr(39)*2)}' LIMIT 1").collect()
    return r[0]["Country"] if r and r[0]["Country"] else "United States"

CERT_LEVELS = ["GMP", "FDA Approved", "EMA Certified", "ISO 9001", "GMP", "FDA Approved"]

def sponsor_row(i, name, op, ts):
    return (f"S{i:03d}", name, country_of(name), CERT_LEVELS[i - 1], ts, op)

# BATCH 1: carga inicial — 5 patrocinadores (INSERT)
batch1_rows = [sponsor_row(i, top_sponsors[i - 1], "INSERT", f"2026-11-01T09:{(i-1)*5:02d}:00")
               for i in range(1, 6)]

# BATCH 2: lote de cambios — UPDATE de certificación + INSERT nuevo + DELETE
batch2_rows = [
    ("S002", top_sponsors[1], country_of(top_sponsors[1]), "FDA + EMA Dual Certified",
     "2026-11-02T09:00:00", "UPDATE"),
    sponsor_row(6, top_sponsors[5], "INSERT", "2026-11-02T09:05:00"),
    ("S004", None, None, None, "2026-11-02T09:10:00", "DELETE"),
]

def write_batch(rows, batch_dir):
    df = (spark.createDataFrame(rows, SPONSOR_SCHEMA)
              .withColumn("updated_at", F.to_timestamp("updated_at")))
    for catalog in CATALOGS:
        path = f"/Volumes/{catalog}/{SCHEMA}/cdc_source/{batch_dir}/"
        df.coalesce(1).write.mode("overwrite").json(path)
        print(f"[OK] Lote '{batch_dir}' escrito en {path} ({df.count()} registros)")

if BATCH in ("1", "all"): write_batch(batch1_rows, "batch1")
if BATCH in ("2", "all"): write_batch(batch2_rows, "batch2")
print(f"\n[LISTO] Datos CDC preparados (modo: {BATCH}). Ejecuta el update del SDP.")

# COMMAND ----------

# DBTITLE 1,Explicacion Seccion 1
# MAGIC %md
# MAGIC ## Seccion 1: Creacion de Catalogos, Schemas y Volumes
# MAGIC
# MAGIC Esta seccion crea la infraestructura en **Unity Catalog** de forma **idempotente** (se puede ejecutar multiples veces sin error).
# MAGIC
# MAGIC ### Que crea?
# MAGIC
# MAGIC | Objeto | Dev | Prod |
# MAGIC | --- | --- | --- |
# MAGIC | **Catalog** | `dab__dev` | `dab__prod` |
# MAGIC | **Schema** | `biotech_clinical_trials` | `biotech_clinical_trials` |
# MAGIC | **Volume** | `cdc_source` | `cdc_source` |
# MAGIC
# MAGIC El volume `cdc_source` es donde se guardan los archivos JSON con los datos CDC que Auto Loader leera en el pipeline SDP.
# MAGIC
# MAGIC > **Por que idempotente?** Los `IF NOT EXISTS` permiten re-ejecutar sin romper. Si el catalogo ya existe, lo deja intacto.

# COMMAND ----------

# DBTITLE 1,Explicacion Seccion 2
# MAGIC %md
# MAGIC ## Seccion 2: Verificacion de la tabla del Marketplace
# MAGIC
# MAGIC Antes de generar datos CDC, se valida que la tabla del **Databricks Marketplace** (John Snow Labs - FDA PMR/PMC) este accesible.
# MAGIC
# MAGIC ### Tablas del Marketplace
# MAGIC
# MAGIC | Variable | Tabla | Uso |
# MAGIC | --- | --- | --- |
# MAGIC | `FACT_TABLE` | `required_clinical_trial_after_approval_database` | Hecho principal: ensayos clinicos post-aprobacion (2,783 filas) |
# MAGIC | `DIRECTORY_TABLE` | `directory_of_us_and_eu_pharmaceutical_companies` | Directorio de companias: se usa para obtener el pais real de cada patrocinador |
# MAGIC
# MAGIC > **Por que verificar?** Si el dataset del Marketplace no esta instalado o el usuario no tiene permisos, el pipeline fallaria despues. Mejor detectarlo aqui.

# COMMAND ----------

# DBTITLE 1,Explicacion Seccion 3
# MAGIC %md
# MAGIC ## Seccion 3: Generacion de datos CDC - Research Sponsors
# MAGIC
# MAGIC La dimension CDC se construye con **patrocinadores reales** extraidos del hecho del Marketplace (top 6 por numero de ensayos).
# MAGIC
# MAGIC ### Esquema CDC (6 columnas)
# MAGIC
# MAGIC | Columna | Tipo | Descripcion |
# MAGIC | --- | --- | --- |
# MAGIC | `sponsor_id` | STRING | Clave natural (S001-S006) |
# MAGIC | `sponsor_name` | STRING | Nombre del patrocinador (del Marketplace) |
# MAGIC | `country` | STRING | Pais real (del directorio de companias) |
# MAGIC | `certification_level` | STRING | GMP, FDA Approved, EMA Certified, etc. |
# MAGIC | `updated_at` | TIMESTAMP | Secuencia temporal (define el orden de los cambios) |
# MAGIC | `operation_type` | STRING | INSERT, UPDATE o DELETE |
# MAGIC
# MAGIC ### Lotes CDC
# MAGIC
# MAGIC | Lote | Operaciones | Detalle |
# MAGIC | --- | --- | --- |
# MAGIC | `batch1` | 5 x INSERT | Carga inicial de patrocinadores S001-S005 |
# MAGIC | `batch2` | 1 UPDATE + 1 INSERT + 1 DELETE | S002 cambia certificacion a Dual, S006 nuevo, S004 eliminado |
# MAGIC
# MAGIC ### Funcionamiento
# MAGIC
# MAGIC 1. **`top_sponsors`**: consulta el hecho del Marketplace y obtiene los 6 patrocinadores con mas ensayos
# MAGIC 2. **`country_of(name)`**: busca el pais real de cada patrocinador en el directorio de companias
# MAGIC 3. **`sponsor_row(i, name, op, ts)`**: construye una tupla con el esquema CDC
# MAGIC 4. **`write_batch(rows, dir)`**: escribe los datos como **JSON en UC Volumes** (`cdc_source/batch1/` y `cdc_source/batch2/`)
# MAGIC
# MAGIC ### Que pasa despues?
# MAGIC
# MAGIC El pipeline SDP (Lakeflow Spark Declarative Pipeline) lee estos JSON via **Auto Loader** y los procesa con **Auto CDC Flow** (`create_auto_cdc_flow`) para mantener la dimension `research_sponsors_scd2` con historial completo de cambios (SCD Type 2).
# MAGIC
# MAGIC > **Por que JSON en Volumes?** Auto Loader detecta archivos nuevos automaticamente. Es el patron recomendado para ingesta CDC en Databricks.