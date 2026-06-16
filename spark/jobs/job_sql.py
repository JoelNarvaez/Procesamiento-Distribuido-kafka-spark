"""
Job: job_sql.py
Fuente: tabla MySQL logs_metricas_servidores (leida por JDBC).

Este job es el mejor ejemplo de procesamiento DISTRIBUIDO real: la lectura
viaja por la red desde MySQL (en el master) hacia los executors de los workers,
asi que no necesita un sistema de archivos compartido.

Analisis (Spark SQL):
  - Estadisticas globales (min/max/avg/stddev de las metricas)
  - Metricas promedio por ambiente
  - Top servidores con mas errores
  - Eventos por servicio y nivel
  - Servidores en estado critico

Ejecutar (recomendado, via submit_jobs.sh):
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh sql
"""

import os
import csv as csvmod
import time
from pyspark.sql import SparkSession

SPARK_MASTER   = os.getenv("SPARK_MASTER",   "spark://100.124.245.95:7077")
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "100.124.245.95")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "monitoreo_servidores")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")
OUTPUT_DIR     = os.getenv("OUTPUT_DIR",     "/opt/spark/output")

JDBC_URL    = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_JAR    = "/opt/spark/jars/mysql-connector-j-8.0.33.jar"


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
    .appName("Job_SQL_LogsServidores") \
    .master(SPARK_MASTER) \
    .config("spark.jars", JDBC_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB SQL - LOGS DE SERVIDORES ===\n")
inicio = time.time()

# Lectura distribuida desde MySQL via JDBC.
# partitionColumn + numPartitions reparte la lectura entre los workers.
df = spark.read \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "logs_metricas_servidores") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", JDBC_DRIVER) \
    .option("fetchsize", "5000") \
    .load()

df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

df.createOrReplaceTempView("logs")

# 1) Estadisticas globales (min/max/avg/stddev)
print("\n--- Estadisticas globales de metricas ---")
estad = spark.sql("""
    SELECT
        ROUND(AVG(uso_cpu_porcentaje), 2)    AS cpu_avg,
        ROUND(MIN(uso_cpu_porcentaje), 2)    AS cpu_min,
        ROUND(MAX(uso_cpu_porcentaje), 2)    AS cpu_max,
        ROUND(STDDEV(uso_cpu_porcentaje), 2) AS cpu_stddev,
        ROUND(AVG(uso_ram_porcentaje), 2)    AS ram_avg,
        ROUND(AVG(uso_disco_porcentaje), 2)  AS disco_avg,
        ROUND(AVG(temperatura_cpu), 2)       AS temp_avg,
        ROUND(AVG(latencia_red_ms), 2)       AS latencia_avg,
        ROUND(AVG(tiempo_respuesta_ms), 2)   AS respuesta_avg
    FROM logs
""")
estad.show()

# 2) Metricas promedio por ambiente
print("\n--- Metricas promedio por ambiente ---")
ambiente = spark.sql("""
    SELECT ambiente,
           COUNT(*)                              AS total,
           ROUND(AVG(uso_cpu_porcentaje),  2)    AS cpu_avg,
           ROUND(AVG(uso_ram_porcentaje),  2)    AS ram_avg,
           ROUND(AVG(tiempo_respuesta_ms), 2)    AS respuesta_avg_ms,
           ROUND(AVG(errores_minuto),      2)    AS errores_avg
    FROM logs
    GROUP BY ambiente
    ORDER BY total DESC
""")
ambiente.show()

# 3) Top 10 servidores con mas errores
print("\n--- Top 10 servidores con mas errores ---")
top_err = spark.sql("""
    SELECT servidor, zona,
           COUNT(*) AS total_errores,
           ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg
    FROM logs
    WHERE nivel IN ('ERROR', 'CRITICAL')
    GROUP BY servidor, zona
    ORDER BY total_errores DESC
    LIMIT 10
""")
top_err.show()

# 4) Eventos por servicio y nivel
print("\n--- Eventos por servicio y nivel ---")
spark.sql("""
    SELECT servicio, nivel, COUNT(*) AS total
    FROM logs
    GROUP BY servicio, nivel
    ORDER BY servicio, total DESC
""").show(40)

# 5) Servidores en estado critico (umbrales sobre el promedio)
print("\n--- Servidores en estado critico ---")
criticos = spark.sql("""
    SELECT servidor,
           COUNT(*) AS eventos,
           ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg,
           ROUND(AVG(uso_ram_porcentaje), 2) AS ram_avg,
           ROUND(AVG(temperatura_cpu), 2)    AS temp_avg
    FROM logs
    GROUP BY servidor
    HAVING cpu_avg > 70 OR ram_avg > 70 OR temp_avg > 75
    ORDER BY cpu_avg DESC
""")
criticos.show()

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} s | Registros: {total:,}")

guardar_csv(estad,    f"{OUTPUT_DIR}/resultado_sql/estadisticas_globales.csv")
guardar_csv(ambiente, f"{OUTPUT_DIR}/resultado_sql/por_ambiente.csv")
guardar_csv(top_err,  f"{OUTPUT_DIR}/resultado_sql/top_servidores_errores.csv")

spark.stop()
