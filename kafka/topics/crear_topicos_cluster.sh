#!/bin/bash

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-192.168.1.65:9092}"

TOPICS=(
  "metricas_recursos"
  "logs_http"
  "logs_errores"
  "metricas_red"
  "logs_seguridad"
)

echo "=========================================="
echo " CREACION DE TOPICOS KAFKA - CLUSTER"
echo "=========================================="
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo ""

for TOPIC in "${TOPICS[@]}"
do
  echo "Creando tópico: $TOPIC"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 3

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