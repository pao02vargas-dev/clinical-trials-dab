# Databricks notebook source
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
