#!/bin/bash

set -e

WORKERS=("100.126.190.35" "100.87.252.100")
SSH_USER="${SSH_USER:-$USER}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ORIGEN="$REPO_DIR/data/raw/"

if [ ! -f "$ORIGEN/eventos.jsonl" ] && [ ! -f "$ORIGEN/eventos.csv" ]; then
  echo "[ERROR] No hay datos en $ORIGEN"
  echo "        Genera primero: node generar_datos_prueba.js"
  echo "        (o consume con los consumers de Kafka)."
  exit 1
fi

echo "=========================================="
echo " SINCRONIZACION DE DATOS A LOS WORKERS"
echo "=========================================="
echo " Origen : $ORIGEN"
echo " Destino: $REPO_DIR/data/raw/ en cada worker"
echo "=========================================="

for w in "${WORKERS[@]}"; do
  echo ""
  echo ">>> Sincronizando hacia $SSH_USER@$w ..."
  ssh "$SSH_USER@$w" "mkdir -p '$REPO_DIR/data/raw'"
  rsync -avz --progress \
    "$ORIGEN" \
    "$SSH_USER@$w:$REPO_DIR/data/raw/"
  echo "    OK: $w"
done

echo ""
echo "Listo. Los 3 nodos tienen los mismos archivos de datos."
