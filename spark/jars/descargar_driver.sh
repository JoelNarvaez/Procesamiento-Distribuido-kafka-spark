#!/bin/bash
# =============================================================
#  descargar_driver.sh
#  Descarga el conector JDBC de MySQL que necesita job_sql.py.
#  El .jar NO se versiona en git, por eso hay que descargarlo
#  en CADA nodo (master y los 2 workers) antes de levantar Spark.
#
#  Ejecutar:
#    bash spark/jars/descargar_driver.sh
# =============================================================
set -e

VERSION="8.0.33"
JAR="mysql-connector-j-${VERSION}.jar"
URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${VERSION}/${JAR}"
DEST_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "${DEST_DIR}/${JAR}" ]; then
  echo "[OK] El driver ya existe: ${DEST_DIR}/${JAR}"
  exit 0
fi

# Docker crea una CARPETA con este nombre si se hizo 'compose up' antes de
# tener el .jar. Hay que bajar el compose y borrarla antes de descargar.
if [ -d "${DEST_DIR}/${JAR}" ]; then
  echo "[ERROR] '${DEST_DIR}/${JAR}' es una CARPETA (la creó Docker)."
  echo "        Soluciona asi:"
  echo "          docker compose -f docker/cluster/nodoN/docker-compose.yml down"
  echo "          rm -rf '${DEST_DIR}/${JAR}'"
  echo "          bash spark/jars/descargar_driver.sh"
  exit 1
fi

echo "Descargando ${JAR}..."
if command -v curl >/dev/null 2>&1; then
  curl -fSL "$URL" -o "${DEST_DIR}/${JAR}"
elif command -v wget >/dev/null 2>&1; then
  wget -O "${DEST_DIR}/${JAR}" "$URL"
else
  echo "[ERROR] Necesitas curl o wget instalado."
  exit 1
fi

echo "[OK] Descargado en: ${DEST_DIR}/${JAR}"
