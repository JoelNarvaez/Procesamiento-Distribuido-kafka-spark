"""
Job: job_json.py   (PROCESAMIENTO LOCAL)
Fuente: archivo JSONL local en data/raw/eventos.jsonl

Corre en modo LOCAL (local[*]) en el master: NO usa los workers ni necesita
archivos compartidos. Sirve para análisis local y como base de comparación
contra el procesamiento distribuido (job_sql / job_comparacion).

  >>> PARA MODIFICAR LOS ANALISIS, edita la lista ANALISIS de abajo. <<<

Ejecutar:
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh json
"""

import os
import csv
import time
from pyspark.sql import SparkSession, functions as F

JOB_MASTER = os.getenv("JOB_MASTER", "local[*]")   # local por defecto
DATA_PATH  = os.getenv("DATA_PATH",  "/opt/spark/data/raw/eventos.jsonl")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]

# ============================================================
#  ZONA EDITABLE — define aqui tus analisis (agrupaciones/busquedas).
#   nombre   : titulo que se imprime
#   filtro   : condicion tipo SQL para buscar/filtrar, o None  (ej: "nivel = 'CRITICAL'")
#   group_by : lista de columnas para agrupar
#   agg      : lista de (funcion, columna, alias)
#              funciones: count, avg, min, max, sum, stddev
#              usa "*" como columna con count para contar filas
#   orden    : (alias, "desc"|"asc") o None
#   guardar  : nombre de archivo CSV de salida, o None
# ============================================================
ANALISIS = [
    {"nombre": "Conteo por nivel",
     "filtro": None, "group_by": ["nivel"],
     "agg": [("count", "*", "total")],
     "orden": ("total", "desc"), "guardar": None},

    {"nombre": "CPU / RAM por servidor (avg, min, max, stddev)",
     "filtro": None, "group_by": ["servidor"],
     "agg": [("count", "*", "total"),
             ("avg", "uso_cpu_porcentaje", "cpu_avg"),
             ("min", "uso_cpu_porcentaje", "cpu_min"),
             ("max", "uso_cpu_porcentaje", "cpu_max"),
             ("stddev", "uso_cpu_porcentaje", "cpu_stddev"),
             ("avg", "uso_ram_porcentaje", "ram_avg")],
     "orden": ("cpu_avg", "desc"), "guardar": "cpu_ram_por_servidor"},

    {"nombre": "RAM promedio por servicio",
     "filtro": None, "group_by": ["servicio"],
     "agg": [("avg", "uso_ram_porcentaje", "ram_avg"), ("count", "*", "total")],
     "orden": ("ram_avg", "desc"), "guardar": None},

    {"nombre": "Maximo uso de disco por servidor",
     "filtro": None, "group_by": ["servidor"],
     "agg": [("max", "uso_disco_porcentaje", "disco_max"),
             ("avg", "uso_disco_porcentaje", "disco_avg")],
     "orden": ("disco_max", "desc"), "guardar": None},

    {"nombre": "Eventos por zona",
     "filtro": None, "group_by": ["zona"],
     "agg": [("count", "*", "total")],
     "orden": ("total", "desc"), "guardar": None},

    {"nombre": "Servidores en estado critico (CRITICAL/ERROR)",
     "filtro": "nivel IN ('CRITICAL','ERROR')", "group_by": ["servidor"],
     "agg": [("count", "*", "eventos_criticos"),
             ("avg", "uso_cpu_porcentaje", "cpu_avg"),
             ("avg", "temperatura_cpu", "temp_avg")],
     "orden": ("eventos_criticos", "desc"), "guardar": "servidores_criticos"},
]
# ============================================================

FUN = {"count": F.count, "avg": F.avg, "min": F.min, "max": F.max, "sum": F.sum, "stddev": F.stddev}


def construir_expr(fn, columna, alias):
    expr = F.count(F.lit(1)) if (fn == "count" and columna == "*") else FUN[fn](F.col(columna))
    if fn in ("avg", "stddev"):
        expr = F.round(expr, 2)
    return expr.alias(alias)


def ejecutar(df, a):
    base = df.where(a["filtro"]) if a.get("filtro") else df
    exprs = [construir_expr(fn, c, al) for (fn, c, al) in a["agg"]]
    res = base.groupBy(*a["group_by"]).agg(*exprs)
    if a.get("orden"):
        col_o, dir_o = a["orden"]
        res = res.orderBy(F.col(col_o).desc() if dir_o == "desc" else F.col(col_o).asc())
    return res


def guardar_csv(df, ruta):
    filas = df.collect()
    cols = df.columns
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in filas:
            w.writerow([r[c] for c in cols])
    print(f"  -> guardado: {ruta}")


spark = SparkSession.builder.appName("Job_JSON_LogsServidores").master(JOB_MASTER).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"\n=== JOB JSON — LOGS DE SERVIDORES (modo {JOB_MASTER}) ===\n")
inicio = time.time()

df = spark.read.json(DATA_PATH).drop(*KAFKA_META)
df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

for a in ANALISIS:
    print(f"\n--- {a['nombre']} ---")
    res = ejecutar(df, a)
    res.show()
    if a.get("guardar"):
        guardar_csv(res, f"{OUTPUT_DIR}/resultado_json/{a['guardar']}.csv")

print(f"\nTiempo total (local): {time.time() - inicio:.2f} s | Registros: {total:,}")
spark.stop()
