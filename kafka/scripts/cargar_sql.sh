# #!/bin/bash

# # =============================================================
# #  cargar_sql.sh
# #  Carga el archivo data/raw/eventos.sql en MySQL
# #  Ejecutar desde la raíz del proyecto:
# #    bash kafka/scripts/cargar_sql.sh
# # =============================================================

# # ── Configuración (usa .env si existe, si no usa defaults) ───
# if [ -f ".env" ]; then
#   export $(grep -v '^#' .env | xargs)
# fi

# MYSQL_HOST="${MYSQL_HOST:-192.168.10.101}"
# MYSQL_PORT="${MYSQL_PORT:-3306}"
# MYSQL_USER="${MYSQL_USER:-usuario}"
# MYSQL_PASSWORD="${MYSQL_PASSWORD:-usuario123}"
# MYSQL_DATABASE="${MYSQL_DATABASE:-monitoreo_servidores}"
# SQL_FILE="${SQL_FILE:-data/raw/eventos.sql}"

# # ── Validaciones ─────────────────────────────────────────────
# echo "=========================================="
# echo " CARGA DE DATOS SQL A MYSQL"
# echo "=========================================="
# echo " Host     : $MYSQL_HOST:$MYSQL_PORT"
# echo " Base     : $MYSQL_DATABASE"
# echo " Archivo  : $SQL_FILE"
# echo "=========================================="
# echo ""

# # Verificar que el archivo SQL existe
# if [ ! -f "$SQL_FILE" ]; then
#   echo "[ERROR] No se encontró el archivo: $SQL_FILE"
#   echo "        Asegúrate de haber ejecutado consumer_sql.js primero."
#   exit 1
# fi

# # Verificar que el archivo no está vacío
# if [ ! -s "$SQL_FILE" ]; then
#   echo "[ERROR] El archivo $SQL_FILE está vacío."
#   echo "        Asegúrate de haber consumido mensajes con consumer_sql.js."
#   exit 1
# fi

# # Contar cuántos INSERTs tiene el archivo
# TOTAL_INSERTS=$(grep -c "^INSERT" "$SQL_FILE" || true)
# echo "[INFO] INSERTs detectados en el archivo: $TOTAL_INSERTS"
# echo ""

# # ── Ejecutar carga ───────────────────────────────────────────
# echo "[INFO] Iniciando carga a MySQL..."
# START_TIME=$(date +%s)

# mysql \
#   -h "$MYSQL_HOST" \
#   -P "$MYSQL_PORT" \
#   -u "$MYSQL_USER" \
#   -p"$MYSQL_PASSWORD" \
#   "$MYSQL_DATABASE" < "$SQL_FILE"

# EXIT_CODE=$?
# END_TIME=$(date +%s)
# ELAPSED=$((END_TIME - START_TIME))

# echo ""
# if [ $EXIT_CODE -eq 0 ]; then
#   echo "=========================================="
#   echo " Carga completada exitosamente"
#   echo " Tiempo transcurrido: ${ELAPSED}s"
#   echo "=========================================="
# else
#   echo "=========================================="
#   echo " [ERROR] La carga falló (código: $EXIT_CODE)"
#   echo " Revisa la conexión a MySQL y vuelve a intentarlo."
#   echo "=========================================="
#   exit $EXIT_CODE
# fi