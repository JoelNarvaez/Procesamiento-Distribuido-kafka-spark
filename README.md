# Sistema distribuido de monitoreo de logs y métricas (Kafka + Spark)

Monitoreo distribuido de logs y métricas de servidores sobre **3 nodos** conectados
en una **red LAN con IPs fijas**, usando **Apache Kafka** (clúster KRaft) y
**Apache Spark** (1 master + 2 workers), todo en Docker.

## Arquitectura

| Nodo | IP fija | Rol Kafka | Rol Spark | SO |
|------|---------|-----------|-----------|----|
| Nodo 1 | `192.168.1.65` | Broker + Controller 1 | **Master** + MySQL + productores | Linux (físico) |
| Nodo 2 | `192.168.1.66` | Broker + Controller 2 | Worker 1 | **VM Ubuntu (bridged)** sobre Windows |
| Nodo 3 | `192.168.1.67` | Broker + Controller 3 | Worker 2 | **VM Ubuntu (bridged)** sobre Windows |

> **Importante (Windows):** los nodos 2 y 3 son máquinas Windows. Spark necesita
> `network_mode: host` para que los executors abran sus puertos sobre la LAN, y eso
> **solo funciona en Linux**. Por eso los workers corren dentro de una **VM Ubuntu
> con adaptador en modo puente (bridged, NO NAT)**, de modo que cada VM obtiene su
> propia IP en la red física (`192.168.1.66` / `192.168.1.67`).

```
                 LAN  192.168.1.0/24
   +----------------+----------------+----------------+
   | Nodo1 (.65)    | Nodo2 (.66)    | Nodo3 (.67)    |
   | Linux fisico   | VM Ubuntu      | VM Ubuntu      |
   | Kafka b+c 1    | Kafka b+c 2    | Kafka b+c 3    |
   | Spark Master   | Spark Worker 1 | Spark Worker 2 |
   | MySQL          |                |                |
   +----------------+----------------+----------------+
```

## Requisitos en cada nodo

- Docker + Docker Compose
- Node.js (solo nodo 1, para productores/consumidores)
- El repo **clonado en la misma ruta absoluta** en los 3 nodos
- IPs fijas configuradas (`.65`, `.66`, `.67`) y los nodos viéndose entre sí (`ping`)

## Puesta en marcha

### 1. Descargar el conector JDBC de MySQL (en los 3 nodos)
El `.jar` no se versiona en git. **Antes** de levantar Spark:
```bash
bash spark/jars/descargar_driver.sh
```

### 2. Levantar los servicios (en cada nodo el suyo)
```bash
# Nodo 1
docker compose -f docker/cluster/nodo1/docker-compose.yml up -d
# Nodo 2
docker compose -f docker/cluster/nodo2/docker-compose.yml up -d
# Nodo 3
docker compose -f docker/cluster/nodo3/docker-compose.yml up -d
```
Verificar:
- Kafka: los 3 brokers arriba.
- Spark Master UI: http://192.168.1.65:8080 -> deben aparecer **2 workers ALIVE**.

### 3. Crear los tópicos (desde el nodo 1)
```bash
docker exec -i kafka-nodo1 bash -s < kafka/topics/crear_topicos_cluster.sh
```
Crea 5 tópicos con 3 particiones y factor de replicación 3.

### 4. Datos para Spark
**Opción A — generar datos sintéticos (rápido, recomendado para la prueba de 100k):**
```bash
npm install
node generar_datos_prueba.js 100000     # crea data/raw/eventos.jsonl y .csv
```
**Opción B — datos reales desde Kafka:**
```bash
cd kafka && npm install && cd ..
# Productores (envían eventos en continuo):
bash kafka/producers/ejecutar.sh
# Consumidores (en otras terminales):
node kafka/consumers/consumer_json.js   # -> data/raw/eventos.jsonl
node kafka/consumers/consumer_csv.js    # -> data/raw/eventos.csv
node kafka/consumers/consumer_sql.js    # -> MySQL (inserción por lotes)
```

### 5. Sincronizar los archivos a los workers
Los jobs de **archivos** (JSON/CSV) leen desde el disco local de cada executor,
así que el archivo debe existir en los 3 nodos en la misma ruta:
```bash
bash scripts/sync_datos.sh
```
> El job **SQL no necesita esto**: lee por red desde MySQL.

### 6. Ejecutar los jobs de Spark (desde el nodo 1)
```bash
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh           # todos
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh json
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh csv
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh sql
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh comparacion
```
Los resultados quedan siempre en el master, en `spark/output/`.

## Análisis incluidos
- **job_json.py**: conteo por nivel; CPU/RAM por servidor (avg/min/max/stddev);
  RAM por servicio; máximo de disco por servidor; eventos por zona; servidores críticos.
- **job_csv.py**: conteo por tipo; tiempo de respuesta por servicio (avg/min/max/stddev);
  latencia de red por servidor; total de bytes entrada/salida; errores por servidor.
- **job_sql.py**: estadísticas globales (min/max/avg/stddev); métricas por ambiente;
  top servidores con más errores; eventos por servicio y nivel; servidores críticos.
- **job_comparacion.py**: mismo trabajo en `local[1]`, `local[*]` y distribuido,
  con tiempos y speedup.

## Pruebas de tolerancia a fallos (Kafka)
```bash
# Apagar un broker y verificar que el clúster sigue
docker stop kafka-nodo3
docker exec kafka-nodo1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 192.168.1.65:9092 --describe --topic logs_http
# Reincorporarlo
docker start kafka-nodo3
```
Con replicación 3 y `min.insync.replicas=1`, el clúster sigue produciendo y
consumiendo aunque caiga un nodo.

## Solución de problemas
- **Los workers no aparecen ALIVE**: la VM no está en modo *bridged* o falta
  `network_mode: host`. Revisa que `ping 192.168.1.66` funcione desde el master.
- **`job_sql` falla por driver JDBC**: corre `spark/jars/descargar_driver.sh` en ese nodo.
- **`FileNotFoundException` en JSON/CSV**: faltó `scripts/sync_datos.sh`.
