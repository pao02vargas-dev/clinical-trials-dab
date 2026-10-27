# Databricks notebook source
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
