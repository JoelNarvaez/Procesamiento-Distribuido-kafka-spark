#!/bin/bash
# submit_jobs.sh — Lanza los jobs de Spark desde el Master
#
# Uso:
#   ./submit_jobs.sh              # corre los 4 jobs
#   ./submit_jobs.sh json         # solo job_json
#   ./submit_jobs.sh csv          # solo job_csv
#   ./submit_jobs.sh sql          # solo job_sql
#   ./submit_jobs.sh comparacion  # solo comparación local vs clúster
#
# Desde fuera del contenedor:
#   docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh

export SPARK_MASTER="${SPARK_MASTER:-spark://100.124.245.95:7077}"
export MYSQL_HOST="${MYSQL_HOST:-100.124.245.95}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-monitoreo_servidores}"
export MYSQL_USER="${MYSQL_USER:-root}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-root123}"
export OUTPUT_DIR="${OUTPUT_DIR:-/opt/spark/output}"

# OJO: ya NO se exporta un DATA_PATH global. Antes se exportaba
# DATA_PATH=/opt/spark/data/logs.json y ese mismo valor lo leían
# job_csv.py (que espera un CSV) y job_json.py (que espera un JSONL),
# por lo que al menos uno de los dos siempre leía el archivo equivocado.
# Cada job usa su propio default; si quieres otro archivo, pásalo por job:
DATA_JSON="${DATA_JSON:-/opt/spark/data/raw/eventos.jsonl}"
DATA_CSV="${DATA_CSV:-/opt/spark/data/raw/eventos.csv}"

JOBS_DIR="/opt/spark/jobs"
SUBMIT="/opt/spark/bin/spark-submit"
JAR="/opt/spark/jars/mysql-connector-j-8.0.33.jar"

# El driver corre en este contenedor (modo cliente). Con network_mode: host
# se anuncia con la IP de la máquina para que los executors de los workers
# puedan conectarse de regreso.
DRIVER_HOST="${DRIVER_HOST:-100.124.245.95}"
EVENTLOG_DIR="${EVENTLOG_DIR:-/opt/spark/eventlog}"
COMMON_CONF="--conf spark.driver.host=${DRIVER_HOST} --conf spark.driver.bindAddress=0.0.0.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=${EVENTLOG_DIR}"

# run <titulo> <archivo.py> <data_path> [jars]
# Solo el job SQL necesita el conector JDBC; los demas se ejecutan sin --jars.
run() {
  echo ""
  echo ">>> Ejecutando: $1"
  local extra=""
  [ -n "$4" ] && extra="--jars $4"
  DATA_PATH="$3" $SUBMIT --master "$SPARK_MASTER" $COMMON_CONF $extra "$JOBS_DIR/$2"
  [ $? -eq 0 ] && echo "    OK: $1" || { echo "    FALLO: $1"; exit 1; }
}

case "${1:-todos}" in
  json)        run "Job JSON"    "job_json.py"        "$DATA_JSON" ;;
  csv)         run "Job CSV"     "job_csv.py"         "$DATA_CSV"  ;;
  sql)         run "Job SQL"     "job_sql.py"         ""           "$JAR" ;;
  comparacion) run "Comparación" "job_comparacion.py" "$DATA_JSON" ;;
  todos)
    run "Job JSON"    "job_json.py"        "$DATA_JSON"
    run "Job CSV"     "job_csv.py"         "$DATA_CSV"
    run "Job SQL"     "job_sql.py"         ""           "$JAR"
    run "Comparación" "job_comparacion.py" "$DATA_JSON"
    ;;
  *) echo "Uso: $0 [json|csv|sql|comparacion|todos]"; exit 1 ;;
esac

echo ""
echo "Resultados en: $OUTPUT_DIR"
