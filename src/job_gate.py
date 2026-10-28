# Databricks notebook source
# DBTITLE 1,Explicacion - job_gate
# MAGIC %md
# MAGIC # job_gate — Publicacion o Aborto
# MAGIC
# MAGIC Este notebook es la **tarea final** del job de orquestacion. Se ejecuta en una de dos ramas dependiendo del If/Else:
# MAGIC
# MAGIC | Rama | Parametro `status` | Que hace |
# MAGIC | --- | --- | --- |
# MAGIC | TRUE (Gold tiene filas) | `success` | Imprime mensaje de publicacion OK |
# MAGIC | FALSE (Gold vacia) | `empty` | Lanza `RuntimeError` y aborta el job |
# MAGIC
# MAGIC ## En el flujo del job
# MAGIC
# MAGIC ```
# MAGIC gold_has_rows (If/Else: row_count > 0)
# MAGIC   |-- TRUE  -> gold_published_ok (status=success) -> job exitoso + email on_success
# MAGIC   +-- FALSE -> gold_empty_abort  (status=empty)    -> job falla + email on_failure
# MAGIC ```
# MAGIC
# MAGIC ## Por que abortar si Gold esta vacia?
# MAGIC
# MAGIC Si Gold esta vacia, los dashboards mostrarian datos vacios o errores. Mejor fallar controladamente con un `RuntimeError` que:
# MAGIC 1. Detiene el job inmediatamente
# MAGIC 2. Dispara el email de `on_failure` al equipo
# MAGIC 3. Deja un mensaje claro en los logs para investigar
# MAGIC
# MAGIC Esto convierte un pipeline academico en uno **productivo**: no asume que siempre hay datos, lo **verifica**.

# COMMAND ----------

from databricks.sdk.runtime import dbutils
import sys

# Obtener parámetros desde base_parameters (Serverless) o sys.argv (respaldo)
try:
    mode = dbutils.widgets.get("status")
    gold_table = dbutils.widgets.get("gold_table")
except Exception:
    mode = sys.argv[1] if len(sys.argv) > 1 else "success"
    gold_table = sys.argv[2] if len(sys.argv) > 2 else ""

if mode == "empty":
    raise RuntimeError(
        f"ABORTADO: la tabla Gold '{gold_table}' está vacía tras el update "
        "del pipeline. Revisar el pipeline SDP y las expectations antes de reintentar."
    )
print(f"[OK] Capa Gold publicada correctamente: {gold_table}")