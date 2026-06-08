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

export SPARK_MASTER="${SPARK_MASTER:-spark://192.168.1.65:7077}"
export MYSQL_HOST="${MYSQL_HOST:-192.168.1.65}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-monitoreo_servidores}"
export MYSQL_USER="${MYSQL_USER:-root}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-root123}"
export DATA_PATH="${DATA_PATH:-/opt/spark/data/logs.json}"
export OUTPUT_DIR="${OUTPUT_DIR:-/opt/spark/output}"

JOBS_DIR="/opt/spark/jobs"
SUBMIT="/opt/spark/bin/spark-submit"
JAR="/opt/spark/jars/mysql-connector-j-8.0.33.jar"

run() {
  echo ""
  echo ">>> Ejecutando: $1"
  $SUBMIT --master "$SPARK_MASTER" --jars "$JAR" "$JOBS_DIR/$2"
  [ $? -eq 0 ] && echo "    OK: $1" || { echo "    FALLO: $1"; exit 1; }
}

case "${1:-todos}" in
  json)        run "Job JSON"         "job_json.py" ;;
  csv)         run "Job CSV"          "job_csv.py" ;;
  sql)         run "Job SQL"          "job_sql.py" ;;
  comparacion) run "Comparación"      "job_comparacion.py" ;;
  todos)
    run "Job JSON"    "job_json.py"
    run "Job CSV"     "job_csv.py"
    run "Job SQL"     "job_sql.py"
    run "Comparación" "job_comparacion.py"
    ;;
  *) echo "Uso: $0 [json|csv|sql|comparacion|todos]"; exit 1 ;;
esac

echo ""
echo "Resultados en: $OUTPUT_DIR"
