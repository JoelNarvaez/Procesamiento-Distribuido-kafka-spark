"""
Job: job_csv.py
Fuente: archivo CSV en data/raw/eventos.csv

Analisis (sobre datos CSV):
  - Conteo por tipo de evento
  - Tiempo de respuesta por servicio (avg, min, max, stddev)
  - Peticiones por minuto promedio por servicio
  - Latencia de red promedio por servidor
  - Total de bytes enviados y recibidos
  - Errores por minuto y total de errores por servidor

Ejecutar (recomendado, via submit_jobs.sh):
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh csv
"""

import os
import csv as csvmod
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min as smin, max as smax, stddev, round as rnd, desc, sum as ssum
)

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.1.65:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.csv")
OUTPUT_DIR   = os.getenv("OUTPUT_DIR",   "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]


def guardar_csv(df, ruta):
    """Recolecta en el driver (master) y escribe un unico CSV."""
    filas = df.collect()
    columnas = df.columns
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="") as f:
        w = csvmod.writer(f)
        w.writerow(columnas)
        for r in filas:
            w.writerow([r[c] for c in columnas])
    print(f"  -> guardado: {ruta}")


spark = SparkSession.builder \
    .appName("Job_CSV_LogsServidores") \
    .master(SPARK_MASTER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB CSV - LOGS DE SERVIDORES ===\n")
inicio = time.time()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(DATA_PATH) \
    .drop(*KAFKA_META)
df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

# 1) Conteo por tipo de evento
print("\n--- Conteo por tipo de evento ---")
tipos = df.groupBy("tipo_evento").agg(count("*").alias("total")).orderBy(desc("total"))
tipos.show()

# 2) Tiempo de respuesta por servicio: avg, min, max, stddev
print("\n--- Tiempo de respuesta por servicio (avg, min, max, stddev) ---")
respuesta = df.groupBy("servicio").agg(
    count("*").alias("total_peticiones"),
    rnd(avg("tiempo_respuesta_ms"), 2).alias("resp_avg_ms"),
    smin("tiempo_respuesta_ms").alias("resp_min_ms"),
    smax("tiempo_respuesta_ms").alias("resp_max_ms"),
    rnd(stddev("tiempo_respuesta_ms"), 2).alias("resp_stddev_ms"),
    rnd(avg("peticiones_por_minuto"), 2).alias("peticiones_min_avg")
).orderBy(desc("resp_avg_ms"))
respuesta.show()

# 3) Latencia de red promedio por servidor
print("\n--- Latencia de red promedio por servidor ---")
latencia = df.groupBy("servidor").agg(
    rnd(avg("latencia_red_ms"), 2).alias("latencia_avg_ms"),
    rnd(smax("latencia_red_ms"), 2).alias("latencia_max_ms")
).orderBy(desc("latencia_avg_ms"))
latencia.show()

# 4) Total de bytes enviados y recibidos (global)
print("\n--- Total de bytes (entrada/salida) ---")
bytes_tot = df.agg(
    ssum("bytes_entrada").alias("total_bytes_entrada"),
    ssum("bytes_salida").alias("total_bytes_salida")
)
bytes_tot.show()

# 5) Errores por minuto y total de errores por servidor
print("\n--- Errores por servidor ---")
errores = df.groupBy("servidor").agg(
    rnd(avg("errores_minuto"), 2).alias("errores_min_avg"),
    ssum("errores_minuto").alias("errores_total")
).orderBy(desc("errores_total"))
errores.show()

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} s | Registros: {total:,}")

guardar_csv(respuesta, f"{OUTPUT_DIR}/resultado_csv/respuesta_por_servicio.csv")
guardar_csv(errores,   f"{OUTPUT_DIR}/resultado_csv/errores_por_servidor.csv")

spark.stop()
