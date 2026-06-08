"""
Job: job_csv.py
Descripción: Procesa el archivo CSV con los logs de monitoreo.
             Realiza conteos, filtros y agregaciones básicas.

Ejecutar:
  spark-submit --master spark://192.168.1.65:7077 \
               /opt/spark/jobs/job_csv.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.1.65:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.csv")
OUTPUT_DIR   = os.getenv("OUTPUT_DIR",   "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]

spark = SparkSession.builder \
    .appName("Job_CSV_LogsServidores") \
    .master(SPARK_MASTER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB CSV — LOGS DE SERVIDORES ===\n")

inicio = time.time()

# Leer CSV con encabezado y quitar columnas de metadatos Kafka
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(DATA_PATH) \
    .drop(*KAFKA_META)

total = df.count()
print(f"Registros cargados: {total:,}")

# Agregación 1: conteo por tipo de evento
print("\n--- Conteo por tipo de evento ---")
df.groupBy("tipo_evento") \
  .agg(count("*").alias("total")) \
  .orderBy(desc("total")) \
  .show()

# Agregación 2: tiempo de respuesta promedio por servicio
print("\n--- Tiempo de respuesta promedio por servicio ---")
df.groupBy("servicio") \
  .agg(
      spark_round(avg("tiempo_respuesta_ms"), 2).alias("respuesta_avg_ms"),
      count("*").alias("total_peticiones")
  ) \
  .orderBy(desc("respuesta_avg_ms")) \
  .show()

# Filtro: solo errores críticos
print("\n--- Conteo de eventos CRITICAL por servidor ---")
df.filter(col("nivel") == "CRITICAL") \
  .groupBy("servidor") \
  .agg(count("*").alias("total_criticos")) \
  .orderBy(desc("total_criticos")) \
  .show()

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} segundos")
print(f"Registros procesados: {total:,}")

# Guardar resultado principal
df.groupBy("servicio") \
  .agg(
      spark_round(avg("tiempo_respuesta_ms"), 2).alias("respuesta_avg_ms"),
      count("*").alias("total_peticiones")
  ) \
  .coalesce(1).write.mode("overwrite") \
  .option("header", True) \
  .csv(f"{OUTPUT_DIR}/resultado_csv")

print(f"Resultado guardado en: {OUTPUT_DIR}/resultado_csv")

spark.stop()
