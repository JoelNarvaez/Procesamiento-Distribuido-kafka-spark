#!/bin/bash

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-100.124.245.95:9092}"

TOPICS=(
  "metricas_recursos"
  "logs_http"
  "logs_errores"
  "metricas_red"
  "logs_seguridad"
)

echo "=========================================="
echo " TOPICOS KAFKA - CLUSTER"
echo "=========================================="
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo ""

for TOPIC in "${TOPICS[@]}"
do
  echo "Eliminando tópico si existe: $TOPIC"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --delete \
    --topic "$TOPIC" 2>/dev/null

  sleep 5

  echo "Creando tópico: $TOPIC"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3 \
    --config min.insync.replicas=1

  echo ""
done

echo "=========================================="
echo " LISTA DE TOPICOS"
echo "=========================================="

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list

echo ""
echo "=========================================="
echo " DESCRIPCION DE TOPICOS"
echo "=========================================="

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --describe