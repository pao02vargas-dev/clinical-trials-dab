# Databricks notebook source
# DBTITLE 1,Explicacion - check_gold_rows
# MAGIC %md
# MAGIC # check_gold_rows — Verificador de Calidad Gold
# MAGIC
# MAGIC Este notebook es el **quality gate** del job de orquestacion. Se ejecuta despues del pipeline SDP y cuenta las filas de la tabla Gold.
# MAGIC
# MAGIC ## Que hace?
# MAGIC
# MAGIC 1. Recibe el parametro `gold_table` (nombre completo de la tabla Gold)
# MAGIC 2. Ejecuta `spark.read.table(gold_table).count()` para contar las filas
# MAGIC 3. Guarda el resultado con `dbutils.jobs.taskValues.set("row_count", count)`
# MAGIC 4. La siguiente tarea (`gold_has_rows`) evalua si `row_count > 0` con un If/Else
# MAGIC
# MAGIC ## En el flujo del job
# MAGIC
# MAGIC ```
# MAGIC run_sdp_pipeline -> verify_gold_rows (este notebook) -> gold_has_rows (If/Else)
# MAGIC                                                      -> TRUE:  gold_published_ok
# MAGIC                                                      -> FALSE: gold_empty_abort
# MAGIC ```
# MAGIC
# MAGIC ## Por que taskValues?
# MAGIC
# MAGIC `dbutils.jobs.taskValues.set()` permite pasar el conteo de filas entre tareas del job sin escribirlo en una tabla temporal. Es el mecanismo nativo de Databricks para comunicar valores entre tareas. La siguiente tarea lo lee con `{{tasks.verify_gold_rows.values.row_count}}`.

# COMMAND ----------

from databricks.sdk.runtime import dbutils
import sys

# Obtener parámetro desde base_parameters (Serverless) o sys.argv (respaldo)
try:
    gold_table = dbutils.widgets.get("gold_table")
except Exception:
    gold_table = sys.argv[1] if len(sys.argv) > 1 else ""

row_count = spark.read.table(gold_table).count()
dbutils.jobs.taskValues.set(key="row_count", value=int(row_count))
print(f"[verify_gold_rows] {gold_table}: {row_count} filas")