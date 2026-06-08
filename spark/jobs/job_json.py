"""
Job: job_json.py
Descripción: Procesa el archivo JSON con los logs de monitoreo.
             Realiza agregaciones por nivel, servidor y zona.

Ejecutar:
  spark-submit --master spark://192.168.1.65:7077 \
               /opt/spark/jobs/job_json.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.1.65:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.jsonl")
OUTPUT_DIR   = os.getenv("OUTPUT_DIR",   "/opt/spark/output")

# Columnas de metadatos Kafka que no forman parte del schema real
KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]

spark = SparkSession.builder \
    .appName("Job_JSON_LogsServidores") \
    .master(SPARK_MASTER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB JSON — LOGS DE SERVIDORES ===\n")

inicio = time.time()

# Leer JSONL (una línea por evento) y quitar columnas de metadatos Kafka
df = spark.read.json(DATA_PATH).drop(*KAFKA_META)
total = df.count()
print(f"Registros cargados: {total:,}")

# Agregación 1: conteo por nivel de log
print("\n--- Conteo por nivel ---")
df.groupBy("nivel") \
  .agg(count("*").alias("total")) \
  .orderBy(desc("total")) \
  .show()

# Agregación 2: promedio de CPU y RAM por servidor
print("\n--- CPU y RAM promedio por servidor ---")
df.groupBy("servidor") \
  .agg(
      spark_round(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
      spark_round(avg("uso_ram_porcentaje"), 2).alias("ram_avg"),
      count("*").alias("total_eventos")
  ) \
  .orderBy(desc("cpu_avg")) \
  .show()

# Agregación 3: conteo por zona
print("\n--- Eventos por zona ---")
df.groupBy("zona") \
  .agg(count("*").alias("total")) \
  .orderBy(desc("total")) \
  .show()

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} segundos")
print(f"Registros procesados: {total:,}")

# Guardar resultado principal
df.groupBy("servidor") \
  .agg(
      spark_round(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
      spark_round(avg("uso_ram_porcentaje"), 2).alias("ram_avg"),
      count("*").alias("total_eventos")
  ) \
  .coalesce(1).write.mode("overwrite") \
  .option("header", True) \
  .csv(f"{OUTPUT_DIR}/resultado_json")

print(f"Resultado guardado en: {OUTPUT_DIR}/resultado_json")

spark.stop()
