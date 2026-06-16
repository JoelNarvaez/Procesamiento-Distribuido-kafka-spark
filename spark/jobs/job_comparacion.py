"""
Job: job_comparacion.py   (COMPARACION LOCAL vs DISTRIBUIDO)
Fuente: tabla MySQL (JDBC) — el mismo origen en los 3 modos para una
comparacion justa.

Corre EXACTAMENTE el mismo trabajo en:
  - LOCAL 1 nucleo   (local[1])  -> una maquina, sin paralelismo
  - LOCAL n nucleos  (local[*])  -> una maquina, todos sus nucleos
  - DISTRIBUIDO      (cluster)   -> master + 2 workers
e imprime los tiempos y el speedup.

Se usa MySQL (no archivos) porque es el unico origen que corre distribuido
sin almacenamiento compartido entre nodos.

Ejecutar:
  docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh comparacion
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


def leer_mysql(spark):
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "logs_metricas_servidores") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", JDBC_DRIVER) \
        .option("fetchsize", "5000") \
        .load()


def procesar(spark):
    """El mismo trabajo en los 3 modos: lectura + 3 agregaciones forzadas."""
    df = leer_mysql(spark)
    total = df.count()
    df.createOrReplaceTempView("logs")

    spark.sql("SELECT nivel, COUNT(*) FROM logs GROUP BY nivel").collect()
    spark.sql("""SELECT servidor, AVG(uso_cpu_porcentaje), STDDEV(uso_cpu_porcentaje)
                 FROM logs GROUP BY servidor""").collect()
    spark.sql("""SELECT zona, tipo_evento, COUNT(*) AS t
                 FROM logs GROUP BY zona, tipo_evento ORDER BY t DESC""").collect()
    return total


def medir(nombre, master_url):
    print("\n" + "=" * 50)
    print(f"  {nombre}")
    print("=" * 50)
    spark = SparkSession.builder \
        .appName(f"Comparacion_{nombre}") \
        .master(master_url) \
        .config("spark.jars", JDBC_JAR) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    total = procesar(spark)
    elapsed = time.time() - t0
    spark.stop()
    print(f"  Registros: {total:,}")
    print(f"  Tiempo:    {elapsed:.2f} s")
    return total, elapsed


total, t_local1 = medir("MODO LOCAL (1 nucleo)",      "local[1]")
_,     t_localn = medir("MODO LOCAL (n nucleos)",     "local[*]")
_,     t_dist   = medir("MODO DISTRIBUIDO (3 nodos)", SPARK_MASTER)

speedup_1 = t_local1 / t_dist if t_dist > 0 else 0
speedup_n = t_localn / t_dist if t_dist > 0 else 0

print("\n" + "=" * 50)
print("  COMPARATIVA")
print("=" * 50)
print(f"  Local 1 nucleo:        {t_local1:.2f} s")
print(f"  Local n nucleos:       {t_localn:.2f} s")
print(f"  Distribuido (3 nodos): {t_dist:.2f} s")
print(f"  Speedup vs 1 nucleo:   {speedup_1:.2f}x")
print(f"  Speedup vs n nucleos:  {speedup_n:.2f}x")
print(f"  Registros:             {total:,}")
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
