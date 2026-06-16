#!/bin/bash
# =============================================================
#  nfs_worker.sh  (ejecutar en CADA WORKER, nodos 2 y 3)
#  Monta la carpeta data/ del master (por NFS, vía Tailscale) sobre la
#  carpeta data/ del repo local, para que el docker-compose la vea sin
#  cambios (el volumen relativo ../../../data sigue apuntando ahí).
#
#  Solo aplica a los jobs de ARCHIVO (json / csv).
#
#  Uso:
#    MASTER_IP=100.124.245.95 MASTER_EXPORT=/ruta/del/master/data bash scripts/nfs_worker.sh
# =============================================================
set -e

MASTER_IP="${MASTER_IP:-100.124.245.95}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_MOUNT="$REPO_DIR/data"
# Ruta ABSOLUTA de la carpeta data/ EN EL MASTER (la que imprime nfs_master.sh).
MASTER_EXPORT="${MASTER_EXPORT:?Define MASTER_EXPORT con la ruta data del master (ej: /home/usuario/.../proyecto/data)}"

echo "=========================================="
echo " NFS WORKER - montar datos del master"
echo " Master : ${MASTER_IP}:${MASTER_EXPORT}"
echo " Local  : ${LOCAL_MOUNT}"
echo "=========================================="

echo "[1/4] Instalando nfs-common..."
sudo apt-get update -y
sudo apt-get install -y nfs-common

echo "[2/4] Preparando punto de montaje..."
sudo mkdir -p "$LOCAL_MOUNT"
# Si ya estaba montado, lo desmontamos para remontar limpio.
if mountpoint -q "$LOCAL_MOUNT"; then
  sudo umount "$LOCAL_MOUNT" || sudo umount -l "$LOCAL_MOUNT"
fi

echo "[3/4] Montando NFS..."
if ! sudo mount -t nfs "${MASTER_IP}:${MASTER_EXPORT}" "$LOCAL_MOUNT"; then
  echo "    (reintentando con NFSv3...)"
  sudo mount -t nfs -o vers=3 "${MASTER_IP}:${MASTER_EXPORT}" "$LOCAL_MOUNT"
fi

echo "[4/4] Verificando contenido..."
mountpoint "$LOCAL_MOUNT"
ls -la "$LOCAL_MOUNT/raw" || true

echo ""
echo "OK. IMPORTANTE: reinicia el contenedor del worker para que tome el montaje:"
echo "  docker compose -f docker/cluster/nodoN/docker-compose.yml up -d --force-recreate spark-worker"
echo ""
echo "Para que el montaje persista tras reiniciar la VM, agrega a /etc/fstab:"
echo "  ${MASTER_IP}:${MASTER_EXPORT}  ${LOCAL_MOUNT}  nfs  defaults,_netdev  0  0"
