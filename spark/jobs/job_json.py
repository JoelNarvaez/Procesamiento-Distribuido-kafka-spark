"""
Job: job_json.py
Fuente: archivo JSONL (una linea por evento) en data/raw/eventos.jsonl

Analisis (sobre datos JSON):
  - Conteo por nivel de log
  - CPU/RAM por servidor (avg, min, max, desviacion estandar)
  - RAM promedio por servicio
  - Maximo uso de disco por servidor
  - Eventos por zona
  - Deteccion de servidores en estado critico

Ejecutar (recomendado, via submit_jobs.sh):
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh json
"""

import os
import csv
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, min as smin, max as smax, stddev, round as rnd, desc, sum as ssum
)

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.1.65:7077")
DATA_PATH    = os.getenv("DATA_PATH",    "/opt/spark/data/raw/eventos.jsonl")
OUTPUT_DIR   = os.getenv("OUTPUT_DIR",   "/opt/spark/output")

KAFKA_META = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_consumer_ts"]


def guardar_csv(df, ruta):
    """Recolecta el resultado en el driver (master) y lo escribe como un solo
    CSV. Asi el resultado SIEMPRE queda en el master, sin importar en que
    worker se haya ejecutado el calculo."""
    filas = df.collect()
    columnas = df.columns
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(columnas)
        for r in filas:
            w.writerow([r[c] for c in columnas])
    print(f"  -> guardado: {ruta}")


spark = SparkSession.builder \
    .appName("Job_JSON_LogsServidores") \
    .master(SPARK_MASTER) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB JSON - LOGS DE SERVIDORES ===\n")
inicio = time.time()

df = spark.read.json(DATA_PATH).drop(*KAFKA_META)
df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

# 1) Conteo por nivel
print("\n--- Conteo por nivel ---")
nivel = df.groupBy("nivel").agg(count("*").alias("total")).orderBy(desc("total"))
nivel.show()

# 2) CPU y RAM por servidor: avg, min, max y desviacion estandar
print("\n--- CPU / RAM por servidor (avg, min, max, stddev) ---")
por_servidor = df.groupBy("servidor").agg(
    count("*").alias("total_eventos"),
    rnd(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
    rnd(smin("uso_cpu_porcentaje"), 2).alias("cpu_min"),
    rnd(smax("uso_cpu_porcentaje"), 2).alias("cpu_max"),
    rnd(stddev("uso_cpu_porcentaje"), 2).alias("cpu_stddev"),
    rnd(avg("uso_ram_porcentaje"), 2).alias("ram_avg"),
).orderBy(desc("cpu_avg"))
por_servidor.show()

# 3) RAM promedio por servicio
print("\n--- RAM promedio por servicio ---")
ram_servicio = df.groupBy("servicio").agg(
    rnd(avg("uso_ram_porcentaje"), 2).alias("ram_avg"),
    count("*").alias("total")
).orderBy(desc("ram_avg"))
ram_servicio.show()

# 4) Maximo uso de disco por servidor
print("\n--- Maximo uso de disco por servidor ---")
disco = df.groupBy("servidor").agg(
    rnd(smax("uso_disco_porcentaje"), 2).alias("disco_max"),
    rnd(avg("uso_disco_porcentaje"), 2).alias("disco_avg")
).orderBy(desc("disco_max"))
disco.show()

# 5) Eventos por zona
print("\n--- Eventos por zona ---")
zona = df.groupBy("zona").agg(count("*").alias("total")).orderBy(desc("total"))
zona.show()

# 6) Deteccion de servidores en estado critico
print("\n--- Servidores en estado critico (eventos CRITICAL/ERROR) ---")
criticos = df.filter(col("nivel").isin("CRITICAL", "ERROR")) \
    .groupBy("servidor").agg(
        count("*").alias("eventos_criticos"),
        rnd(avg("uso_cpu_porcentaje"), 2).alias("cpu_avg"),
        rnd(avg("temperatura_cpu"), 2).alias("temp_avg")
    ).orderBy(desc("eventos_criticos"))
criticos.show()

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} s | Registros: {total:,}")

guardar_csv(por_servidor, f"{OUTPUT_DIR}/resultado_json/cpu_ram_por_servidor.csv")
guardar_csv(criticos,     f"{OUTPUT_DIR}/resultado_json/servidores_criticos.csv")

spark.stop()
