"""
Job: job_comparacion.py
Compara el MISMO procesamiento ejecutado en:
  - LOCAL 1 nucleo   (local[1])   -> simula una sola maquina, sin paralelismo
  - LOCAL n nucleos  (local[*])   -> una sola maquina, todos sus nucleos
  - DISTRIBUIDO      (cluster)    -> los 3 nodos (master + 2 workers)

Imprime los tiempos y el speedup, y guarda un resumen CSV en el master.
Demuestra el beneficio del procesamiento distribuido.

Ejecutar:
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh comparacion
"""

import os
import csv as csvmod
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, stddev, round as rnd, desc

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://100.124.245.95:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.jsonl")
OUTPUT_DIR   = os.getenv("OUTPUT_DIR",   "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]


def procesar(spark):
    """Mismo trabajo en los 3 modos: lectura + 3 agregaciones forzadas."""
    df = spark.read.json(DATA_PATH).drop(*KAFKA_META)
    total = df.count()

    df.groupBy("nivel").agg(count("*").alias("total")).collect()

    df.groupBy("servidor").agg(
        rnd(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
        rnd(stddev("uso_cpu_porcentaje"), 2).alias("cpu_stddev")
    ).collect()

    df.groupBy("zona", "tipo_evento").agg(count("*").alias("total")) \
      .orderBy(desc("total")).collect()

    return total


def medir(nombre, master_url):
    print("\n" + "=" * 50)
    print(f"  {nombre}")
    print("=" * 50)
    spark = SparkSession.builder \
        .appName(f"Comparacion_{nombre}") \
        .master(master_url) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    total = procesar(spark)
    elapsed = time.time() - t0
    spark.stop()
    print(f"  Registros: {total:,}")
    print(f"  Tiempo:    {elapsed:.2f} s")
    return total, elapsed


total, t_local1 = medir("MODO LOCAL (1 nucleo)",  "local[1]")
_,     t_localn = medir("MODO LOCAL (n nucleos)", "local[*]")
_,     t_dist   = medir("MODO DISTRIBUIDO (3 nodos)", SPARK_MASTER)

speedup_1 = t_local1 / t_dist if t_dist > 0 else 0
speedup_n = t_localn / t_dist if t_dist > 0 else 0

print("\n" + "=" * 50)
print("  COMPARATIVA")
print("=" * 50)
print(f"  Local 1 nucleo:   {t_local1:.2f} s")
print(f"  Local n nucleos:  {t_localn:.2f} s")
print(f"  Distribuido:      {t_dist:.2f} s")
print(f"  Speedup vs 1 nucleo:  {speedup_1:.2f}x")
print(f"  Speedup vs n nucleos: {speedup_n:.2f}x")
print(f"  Registros:        {total:,}")
print("=" * 50 + "\n")

ruta = f"{OUTPUT_DIR}/resultado_comparacion/comparacion.csv"
os.makedirs(os.path.dirname(ruta), exist_ok=True)
with open(ruta, "w", newline="") as f:
    w = csvmod.writer(f)
    w.writerow(["modo", "tiempo_segundos", "registros"])
    w.writerow(["local_1_nucleo",  round(t_local1, 2), total])
    w.writerow(["local_n_nucleos", round(t_localn, 2), total])
    w.writerow(["distribuido",     round(t_dist, 2),   total])
print(f"Resumen guardado en: {ruta}")
