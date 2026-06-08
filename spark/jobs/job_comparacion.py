"""
Job: job_comparacion.py
Descripción: Corre el mismo procesamiento en modo LOCAL (1 núcleo)
             y en modo CLÚSTER, imprimiendo los tiempos para comparar.
             Esto demuestra el beneficio del procesamiento distribuido.

Ejecutar:
  spark-submit --master spark://192.168.1.65:7077 \
               /opt/spark/jobs/job_comparacion.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.1.65:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.jsonl")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]

# ─── Función reutilizable con el mismo procesamiento ──────────
def procesar(spark, label):
    df = spark.read.json(DATA_PATH).drop(*KAFKA_META)
    total = df.count()

    # Agregación 1
    df.groupBy("nivel").agg(count("*").alias("total")).collect()

    # Agregación 2
    df.groupBy("servidor") \
      .agg(
          spark_round(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
          spark_round(avg("uso_ram_porcentaje"), 2).alias("ram_avg")
      ).collect()

    # Agregación 3
    df.groupBy("zona", "tipo_evento") \
      .agg(count("*").alias("total")) \
      .orderBy(desc("total")) \
      .collect()

    return total

# ──────────────────────────────────────────────────────────────
# 1. Ejecución LOCAL (simula una sola máquina)
# ──────────────────────────────────────────────────────────────
print("\n" + "="*50)
print("  MODO LOCAL (1 máquina, 1 núcleo)")
print("="*50)

spark_local = SparkSession.builder \
    .appName("Comparacion_Local") \
    .master("local[1]") \
    .getOrCreate()
spark_local.sparkContext.setLogLevel("WARN")

t0 = time.time()
total = procesar(spark_local, "local")
tiempo_local = time.time() - t0

print(f"  Registros: {total:,}")
print(f"  Tiempo:    {tiempo_local:.2f} segundos")

spark_local.stop()

# ──────────────────────────────────────────────────────────────
# 2. Ejecución DISTRIBUIDA (3 nodos)
# ──────────────────────────────────────────────────────────────
print("\n" + "="*50)
print("  MODO DISTRIBUIDO (3 nodos)")
print("="*50)

spark_cluster = SparkSession.builder \
    .appName("Comparacion_Distribuido") \
    .master(SPARK_MASTER) \
    .getOrCreate()
spark_cluster.sparkContext.setLogLevel("WARN")

t0 = time.time()
total = procesar(spark_cluster, "cluster")
tiempo_cluster = time.time() - t0

print(f"  Registros: {total:,}")
print(f"  Tiempo:    {tiempo_cluster:.2f} segundos")

spark_cluster.stop()

# ──────────────────────────────────────────────────────────────
# 3. Resultado comparativo
# ──────────────────────────────────────────────────────────────
mejora = tiempo_local / tiempo_cluster if tiempo_cluster > 0 else 0

print("\n" + "="*50)
print("  COMPARATIVA")
print("="*50)
print(f"  Local (1 núcleo):   {tiempo_local:.2f}s")
print(f"  Distribuido:        {tiempo_cluster:.2f}s")
print(f"  Mejora:             {mejora:.2f}x más rápido")
print(f"  Registros:          {total:,}")
print("="*50 + "\n")
