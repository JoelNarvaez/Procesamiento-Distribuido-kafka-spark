"""
Job: job_csv.py   (PROCESAMIENTO LOCAL)
Fuente: archivo CSV local en data/raw/eventos.csv

Corre en modo LOCAL (local[*]) en el master: NO usa los workers ni necesita
archivos compartidos. Sirve para análisis local y como base de comparación
contra el procesamiento distribuido (job_sql / job_comparacion).

  >>> PARA MODIFICAR LOS ANALISIS, edita la lista ANALISIS de abajo. <<<

Ejecutar:
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh csv
"""

import os
import csv as csvmod
import time
from pyspark.sql import SparkSession, functions as F

JOB_MASTER = os.getenv("JOB_MASTER", "local[*]")   # local por defecto
DATA_PATH  = os.getenv("DATA_PATH",  "/opt/spark/data/raw/eventos.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]

# ============================================================
#  ZONA EDITABLE — define aqui tus analisis (agrupaciones/busquedas).
#   nombre   : titulo que se imprime
#   filtro   : condicion tipo SQL para buscar/filtrar, o None
#   group_by : lista de columnas para agrupar
#   agg      : lista de (funcion, columna, alias)
#              funciones: count, avg, min, max, sum, stddev
#              usa "*" como columna con count para contar filas
#   orden    : (alias, "desc"|"asc") o None
#   guardar  : nombre de archivo CSV de salida, o None
# ============================================================
ANALISIS = [
    {"nombre": "Conteo por tipo de evento",
     "filtro": None, "group_by": ["tipo_evento"],
     "agg": [("count", "*", "total")],
     "orden": ("total", "desc"), "guardar": None},

    {"nombre": "Tiempo de respuesta por servicio (avg, min, max, stddev)",
     "filtro": None, "group_by": ["servicio"],
     "agg": [("count", "*", "total_peticiones"),
             ("avg", "tiempo_respuesta_ms", "resp_avg_ms"),
             ("min", "tiempo_respuesta_ms", "resp_min_ms"),
             ("max", "tiempo_respuesta_ms", "resp_max_ms"),
             ("stddev", "tiempo_respuesta_ms", "resp_stddev_ms"),
             ("avg", "peticiones_por_minuto", "peticiones_min_avg")],
     "orden": ("resp_avg_ms", "desc"), "guardar": "respuesta_por_servicio"},

    {"nombre": "Latencia de red promedio por servidor",
     "filtro": None, "group_by": ["servidor"],
     "agg": [("avg", "latencia_red_ms", "latencia_avg_ms"),
             ("max", "latencia_red_ms", "latencia_max_ms")],
     "orden": ("latencia_avg_ms", "desc"), "guardar": None},

    {"nombre": "Total de bytes por servidor (entrada/salida)",
     "filtro": None, "group_by": ["servidor"],
     "agg": [("sum", "bytes_entrada", "total_bytes_entrada"),
             ("sum", "bytes_salida", "total_bytes_salida")],
     "orden": ("total_bytes_salida", "desc"), "guardar": None},

    {"nombre": "Errores por servidor",
     "filtro": None, "group_by": ["servidor"],
     "agg": [("avg", "errores_minuto", "errores_min_avg"),
             ("sum", "errores_minuto", "errores_total")],
     "orden": ("errores_total", "desc"), "guardar": "errores_por_servidor"},
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
        w = csvmod.writer(f)
        w.writerow(cols)
        for r in filas:
            w.writerow([r[c] for c in cols])
    print(f"  -> guardado: {ruta}")


spark = SparkSession.builder.appName("Job_CSV_LogsServidores").master(JOB_MASTER).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"\n=== JOB CSV — LOGS DE SERVIDORES (modo {JOB_MASTER}) ===\n")
inicio = time.time()

df = spark.read.option("header", True).option("inferSchema", True).csv(DATA_PATH).drop(*KAFKA_META)
df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

for a in ANALISIS:
    print(f"\n--- {a['nombre']} ---")
    res = ejecutar(df, a)
    res.show()
    if a.get("guardar"):
        guardar_csv(res, f"{OUTPUT_DIR}/resultado_csv/{a['guardar']}.csv")

print(f"\nTiempo total (local): {time.time() - inicio:.2f} s | Registros: {total:,}")
spark.stop()
