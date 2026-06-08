"""
Job: job_sql.py
Descripción: Lee los logs desde MySQL y realiza agregaciones
             usando Spark SQL sobre la tabla logs_metricas_servidores.

Ejecutar:
  spark-submit --master spark://192.168.1.65:7077 \
               --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
               /opt/spark/jobs/job_sql.py
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, desc

SPARK_MASTER   = os.getenv("SPARK_MASTER",   "spark://192.168.1.65:7077")
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "192.168.1.65")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "monitoreo_servidores")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")
OUTPUT_DIR     = os.getenv("OUTPUT_DIR",     "/opt/spark/output")

JDBC_URL    = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?useSSL=false&serverTimezone=UTC"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_JAR    = "/opt/spark/jars/mysql-connector-j-8.0.33.jar"

spark = SparkSession.builder \
    .appName("Job_SQL_LogsServidores") \
    .master(SPARK_MASTER) \
    .config("spark.jars", JDBC_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n=== JOB SQL — LOGS DE SERVIDORES ===\n")

inicio = time.time()

# Leer desde MySQL vía JDBC
df = spark.read \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "logs_metricas_servidores") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", JDBC_DRIVER) \
    .option("fetchsize", "5000") \
    .load()

total = df.count()
print(f"Registros cargados: {total:,}")

# Registrar como vista temporal para usar Spark SQL
df.createOrReplaceTempView("logs")

# Consulta 1: resumen de métricas por ambiente
print("\n--- Métricas promedio por ambiente ---")
spark.sql("""
    SELECT ambiente,
           COUNT(*)                              AS total,
           ROUND(AVG(uso_cpu_porcentaje),  2)    AS cpu_avg,
           ROUND(AVG(uso_ram_porcentaje),  2)    AS ram_avg,
           ROUND(AVG(tiempo_respuesta_ms), 2)    AS respuesta_avg_ms,
           ROUND(AVG(errores_minuto),      2)    AS errores_avg
    FROM logs
    GROUP BY ambiente
    ORDER BY total DESC
""").show()

# Consulta 2: top 10 servidores con más errores
print("\n--- Top 10 servidores con más errores ---")
spark.sql("""
    SELECT servidor, zona,
           COUNT(*) AS total_errores,
           ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg
    FROM logs
    WHERE nivel IN ('ERROR', 'CRITICAL')
    GROUP BY servidor, zona
    ORDER BY total_errores DESC
    LIMIT 10
""").show()

# Consulta 3: conteo de mensajes por servicio y nivel
print("\n--- Eventos por servicio y nivel ---")
spark.sql("""
    SELECT servicio, nivel, COUNT(*) AS total
    FROM logs
    GROUP BY servicio, nivel
    ORDER BY servicio, total DESC
""").show(30)

fin = time.time()
print(f"\nTiempo total (distribuido): {fin - inicio:.2f} segundos")
print(f"Registros procesados: {total:,}")

# Guardar resultado principal
spark.sql("""
    SELECT ambiente,
           COUNT(*)                           AS total,
           ROUND(AVG(uso_cpu_porcentaje), 2)  AS cpu_avg,
           ROUND(AVG(uso_ram_porcentaje), 2)  AS ram_avg,
           ROUND(AVG(tiempo_respuesta_ms), 2) AS respuesta_avg_ms
    FROM logs
    GROUP BY ambiente
""").coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv(f"{OUTPUT_DIR}/resultado_sql")

print(f"Resultado guardado en: {OUTPUT_DIR}/resultado_sql")

spark.stop()
