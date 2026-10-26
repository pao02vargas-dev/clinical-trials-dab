# Verifica el conteo de filas en Gold y lo expone como task value
# para la tarea condicional (If/Else) del job.
import sys
from databricks.sdk.runtime import dbutils

gold_table = sys.argv[1]
row_count = spark.read.table(gold_table).count()
dbutils.jobs.taskValues.set(key="row_count", value=int(row_count))
print(f"[verify_gold_rows] {gold_table}: {row_count} filas")
