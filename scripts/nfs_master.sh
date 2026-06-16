#!/bin/bash
# =============================================================
#  nfs_master.sh  (ejecutar SOLO en el MASTER, nodo 1)
#  Exporta la carpeta data/ del repo por NFS hacia la red Tailscale,
#  para que los workers lean los mismos eventos.jsonl / eventos.csv.
#
#  Solo aplica a los jobs de ARCHIVO (json / csv). El job SQL no lo usa.
#
#  Uso:
#    bash scripts/nfs_master.sh
# =============================================================
set -e

# Rango CGNAT de Tailscale (todas las IPs 100.x del tailnet).
TAILSCALE_CIDR="${TAILSCALE_CIDR:-100.64.0.0/10}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
EXPORT_DIR="$REPO_DIR/data"
# Solo lectura: los workers únicamente LEEN los datos.
OPTS="ro,sync,no_subtree_check"

echo "=========================================="
echo " NFS MASTER - exportar datos por Tailscale"
echo " Carpeta : $EXPORT_DIR"
echo " Red     : $TAILSCALE_CIDR"
echo " Opciones: $OPTS"
echo "=========================================="

echo "[1/4] Instalando nfs-kernel-server..."
sudo apt-get update -y
sudo apt-get install -y nfs-kernel-server

echo "[2/4] Configurando /etc/exports..."
LINE="$EXPORT_DIR $TAILSCALE_CIDR($OPTS)"
# Quitar cualquier export previo de la misma carpeta y agregar el nuevo.
sudo sed -i "\#^$EXPORT_DIR #d" /etc/exports
echo "$LINE" | sudo tee -a /etc/exports

echo "[3/4] Aplicando exports..."
sudo exportfs -ra
sudo systemctl enable --now nfs-server

echo "[4/4] Exports activos:"
sudo exportfs -v

echo ""
echo "LISTO. Ahora en CADA worker ejecuta:"
echo "  MASTER_IP=100.124.245.95 MASTER_EXPORT=$EXPORT_DIR bash scripts/nfs_worker.sh"
