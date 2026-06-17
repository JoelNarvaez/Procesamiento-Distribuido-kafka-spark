"""
Job: job_sql.py   (PROCESAMIENTO DISTRIBUIDO)
Fuente: tabla MySQL logs_metricas_servidores (leida por JDBC).

Es el unico job realmente distribuido: la lectura viaja por la red desde MySQL
(en el master) hacia los executors de los 2 workers. No necesita archivos
compartidos.

  >>> PARA MODIFICAR LOS ANALISIS, edita la lista CONSULTAS de abajo. <<<
  Cada consulta es SQL normal sobre la vista `logs`. Cambia los GROUP BY,
  los WHERE (busquedas) o las agregaciones libremente.

Ejecutar:
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

# ============================================================
#  ZONA EDITABLE — define aqui tus consultas/agrupaciones.
#  - nombre  : titulo que se imprime
#  - sql     : consulta Spark SQL sobre la vista `logs`
#  - guardar : nombre de archivo CSV de salida, o None para no guardar
#  Agrega, quita o edita libremente. Funciones utiles:
#    COUNT(*), AVG(x), MIN(x), MAX(x), SUM(x), STDDEV(x), ROUND(x,2)
# ============================================================
CONSULTAS = [
    {
        "nombre": "Estadisticas globales (min/max/avg/stddev)",
        "guardar": "estadisticas_globales",
        "sql": """
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
        """,
    },
    {
        "nombre": "Metricas promedio por ambiente",
        "guardar": "por_ambiente",
        "sql": """
            SELECT ambiente,
                   COUNT(*)                           AS total,
                   ROUND(AVG(uso_cpu_porcentaje),  2) AS cpu_avg,
                   ROUND(AVG(uso_ram_porcentaje),  2) AS ram_avg,
                   ROUND(AVG(tiempo_respuesta_ms), 2) AS respuesta_avg_ms,
                   ROUND(AVG(errores_minuto),      2) AS errores_avg
            FROM logs
            GROUP BY ambiente
            ORDER BY total DESC
        """,
    },
    {
        "nombre": "Top 10 servidores con mas errores",
        "guardar": "top_servidores_errores",
        "sql": """
            SELECT servidor, zona,
                   COUNT(*) AS total_errores,
                   ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg
            FROM logs
            WHERE nivel IN ('ERROR', 'CRITICAL')
            GROUP BY servidor, zona
            ORDER BY total_errores DESC
            LIMIT 10
        """,
    },
    {
        "nombre": "Eventos por servicio y nivel",
        "guardar": None,
        "sql": """
            SELECT servicio, nivel, COUNT(*) AS total
            FROM logs
            GROUP BY servicio, nivel
            ORDER BY servicio, total DESC
        """,
    },
    {
        "nombre": "Servidores en estado critico",
        "guardar": None,
        "sql": """
            SELECT servidor,
                   COUNT(*) AS eventos,
                   ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg,
                   ROUND(AVG(uso_ram_porcentaje), 2) AS ram_avg,
                   ROUND(AVG(temperatura_cpu), 2)    AS temp_avg
            FROM logs
            GROUP BY servidor
            HAVING cpu_avg > 70 OR ram_avg > 70 OR temp_avg > 75
            ORDER BY cpu_avg DESC
        """,
    },

    # ---------- Consultas mas complejas ----------
    {
        "nombre": "Tasa de error por servicio (%)",
        "guardar": "tasa_error_por_servicio",
        "sql": """
            SELECT servicio,
                   COUNT(*) AS total,
                   SUM(CASE WHEN nivel IN ('ERROR','CRITICAL') THEN 1 ELSE 0 END) AS errores,
                   ROUND(100.0 * SUM(CASE WHEN nivel IN ('ERROR','CRITICAL') THEN 1 ELSE 0 END)
                         / COUNT(*), 2) AS pct_error
            FROM logs
            GROUP BY servicio
            ORDER BY pct_error DESC
        """,
    },
    {
        "nombre": "Percentiles de tiempo de respuesta por servicio (p50/p90/p95/p99)",
        "guardar": "percentiles_respuesta",
        "sql": """
            SELECT servicio,
                   COUNT(*) AS total,
                   ROUND(percentile_approx(tiempo_respuesta_ms, 0.50), 0) AS p50_ms,
                   ROUND(percentile_approx(tiempo_respuesta_ms, 0.90), 0) AS p90_ms,
                   ROUND(percentile_approx(tiempo_respuesta_ms, 0.95), 0) AS p95_ms,
                   ROUND(percentile_approx(tiempo_respuesta_ms, 0.99), 0) AS p99_ms,
                   MAX(tiempo_respuesta_ms) AS max_ms
            FROM logs
            GROUP BY servicio
            ORDER BY p99_ms DESC
        """,
    },
    {
        "nombre": "Ranking de servidores por CPU (window RANK)",
        "guardar": "ranking_servidores_cpu",
        "sql": """
            WITH agg AS (
                SELECT servidor,
                       COUNT(*) AS eventos,
                       ROUND(AVG(uso_cpu_porcentaje), 2) AS cpu_avg,
                       ROUND(AVG(uso_ram_porcentaje), 2) AS ram_avg
                FROM logs
                GROUP BY servidor
            )
            SELECT servidor, eventos, cpu_avg, ram_avg,
                   RANK() OVER (ORDER BY cpu_avg DESC) AS ranking_cpu
            FROM agg
            ORDER BY ranking_cpu
        """,
    },
    {
        "nombre": "Servicio mas problematico por zona (window ROW_NUMBER)",
        "guardar": None,
        "sql": """
            SELECT zona, servicio, errores
            FROM (
                SELECT zona, servicio,
                       COUNT(*) AS errores,
                       ROW_NUMBER() OVER (PARTITION BY zona ORDER BY COUNT(*) DESC) AS rn
                FROM logs
                WHERE nivel IN ('ERROR','CRITICAL')
                GROUP BY zona, servicio
            )
            WHERE rn = 1
            ORDER BY errores DESC
        """,
    },
    {
        "nombre": "Distribucion de codigos de estado por clase (2xx/3xx/4xx/5xx)",
        "guardar": None,
        "sql": """
            SELECT clase_estado, COUNT(*) AS total
            FROM (
                SELECT CASE
                         WHEN codigo_estado BETWEEN 200 AND 299 THEN '2xx OK'
                         WHEN codigo_estado BETWEEN 300 AND 399 THEN '3xx Redireccion'
                         WHEN codigo_estado BETWEEN 400 AND 499 THEN '4xx Cliente'
                         WHEN codigo_estado BETWEEN 500 AND 599 THEN '5xx Servidor'
                         ELSE 'Otro'
                       END AS clase_estado
                FROM logs
            )
            GROUP BY clase_estado
            ORDER BY total DESC
        """,
    },
    {
        "nombre": "Throughput por hora (eventos y respuesta promedio)",
        "guardar": None,
        "sql": """
            SELECT HOUR(timestamp_evento) AS hora,
                   COUNT(*) AS eventos,
                   ROUND(AVG(tiempo_respuesta_ms), 2) AS resp_avg_ms,
                   SUM(CASE WHEN nivel IN ('ERROR','CRITICAL') THEN 1 ELSE 0 END) AS errores
            FROM logs
            GROUP BY HOUR(timestamp_evento)
            ORDER BY hora
        """,
    },
]
# ============================================================


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

print("\n=== JOB SQL — LOGS DE SERVIDORES (distribuido) ===\n")
inicio = time.time()

# Lectura DISTRIBUIDA: particionamos por CRC32(id_log) (numero uniforme 0..2^32)
# para que Spark abra varias consultas en paralelo y reparta la carga entre los
# 2 workers. Sin esto, JDBC lee todo en UNA sola particion (un solo executor).
NUM_PARTICIONES = os.getenv("JDBC_NUM_PARTITIONS", "8")

df = spark.read \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "(SELECT *, CRC32(id_log) AS pcol FROM logs_metricas_servidores) AS sub") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", JDBC_DRIVER) \
    .option("partitionColumn", "pcol") \
    .option("lowerBound", "0") \
    .option("upperBound", "4294967295") \
    .option("numPartitions", NUM_PARTICIONES) \
    .option("fetchsize", "5000") \
    .load() \
    .drop("pcol")

df.cache()
total = df.count()
print(f"Registros cargados: {total:,}")

df.createOrReplaceTempView("logs")

for consulta in CONSULTAS:
    print(f"\n--- {consulta['nombre']} ---")
    res = spark.sql(consulta["sql"])
    res.show(50, truncate=False)
    if consulta.get("guardar"):
        guardar_csv(res, f"{OUTPUT_DIR}/resultado_sql/{consulta['guardar']}.csv")

print(f"\nTiempo total (distribuido): {time.time() - inicio:.2f} s | Registros: {total:,}")

spark.stop()
