# Databricks notebook source
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
