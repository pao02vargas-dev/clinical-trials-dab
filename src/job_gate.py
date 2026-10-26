# Ramas del If/Else del job de orquestación.
import sys

mode, gold_table = sys.argv[1], sys.argv[2]

if mode == "empty":
    raise RuntimeError(
        f"ABORTADO: la tabla Gold '{gold_table}' está vacía tras el update "
        "del pipeline. Revisar el pipeline SDP y las expectations antes de reintentar."
    )
print(f"[OK] Capa Gold publicada correctamente: {gold_table}")
