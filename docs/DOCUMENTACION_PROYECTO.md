# Documentación del Proyecto
## Sistema distribuido de monitoreo de logs y métricas (Tailscale + Kafka + Spark)

> **Reporte de configuración y pruebas** — del montaje de la red hasta la ejecución de los jobs de Spark.

| Campo | Valor |
|-------|-------|
| Proyecto | Monitoreo distribuido de logs y métricas de servidores |
| Tecnologías | Tailscale · Docker · Apache Kafka (KRaft) · Apache Spark Standalone · MySQL · Node.js |
| Nodos | 3 (1 Linux físico + 2 VM Ubuntu) |
| Fecha del reporte | 17/06/2026 |
| Integrantes | _(completar)_ |

---

## Índice

1. [Objetivo del proyecto](#1-objetivo-del-proyecto)
2. [Arquitectura general](#2-arquitectura-general)
3. [Configuración de la red con Tailscale](#3-configuración-de-la-red-con-tailscale)
4. [Configuración de Docker en cada nodo](#4-configuración-de-docker-en-cada-nodo)
5. [Configuración del clúster Kafka (KRaft)](#5-configuración-del-clúster-kafka-kraft)
6. [Productores y consumidores (Node.js)](#6-productores-y-consumidores-nodejs)
7. [Base de datos MySQL](#7-base-de-datos-mysql)
8. [Compartición de datos (NFS / rsync)](#8-compartición-de-datos-nfs--rsync)
9. [Configuración del clúster Spark](#9-configuración-del-clúster-spark)
10. [Jobs de análisis en Spark](#10-jobs-de-análisis-en-spark)
11. [Pruebas y resultados](#11-pruebas-y-resultados)
12. [Pruebas de tolerancia a fallos](#12-pruebas-de-tolerancia-a-fallos)
13. [Solución de problemas](#13-solución-de-problemas)
14. [Conclusiones](#14-conclusiones)

---

## 1. Objetivo del proyecto

Construir un sistema **distribuido** de monitoreo de logs y métricas de servidores
sobre **3 nodos** conectados mediante **Tailscale** (VPN mesh con IPs fijas `100.x`),
empleando:

- **Apache Kafka** (clúster KRaft, sin ZooKeeper) como bus de mensajería tolerante a fallos.
- **Apache Spark** (1 master + 2 workers) para el procesamiento distribuido y analítica.
- **MySQL** como almacén persistente y fuente para el procesamiento distribuido real.
- **Node.js** para los productores (generación de eventos) y consumidores.

El sistema simula eventos de 5 categorías (recursos, HTTP, errores, red y seguridad)
y permite analizarlos en tres modos comparables: **local 1 núcleo**, **local n núcleos**
y **distribuido (3 nodos)**.

---

## 2. Arquitectura general

| Nodo | IP Tailscale | Rol Kafka | Rol Spark | SO |
|------|--------------|-----------|-----------|----|
| Nodo 1 | `100.124.245.95` | Broker + Controller 1 | **Master** + MySQL + productores | Linux (físico) |
| Nodo 2 | `100.126.190.35` | Broker + Controller 2 | Worker 1 | **VM Ubuntu** sobre Windows |
| Nodo 3 | `100.87.252.100` | Broker + Controller 3 | Worker 2 | **VM Ubuntu** sobre Windows |

```
                 Tailnet  100.x (Tailscale)
   +-------------------+-------------------+-------------------+
   | Nodo1 .124.245.95 | Nodo2 .126.190.35 | Nodo3 .87.252.100 |
   | Linux fisico      | VM Ubuntu         | VM Ubuntu         |
   | Kafka b+c 1       | Kafka b+c 2       | Kafka b+c 3       |
   | Spark Master      | Spark Worker 1    | Spark Worker 2    |
   | MySQL             |                   |                   |
   +-------------------+-------------------+-------------------+
```

**Flujo de datos:**

```
Productores Node.js --> Topicos Kafka --> Consumidores Node.js
                                          |-> data/raw/eventos.jsonl  (JSON)
                                          |-> data/raw/eventos.csv    (CSV)
                                          |-> MySQL                   (SQL)
                                                      |
                                         Spark (master + 2 workers)
                                         |- job_json        (lee JSONL local)
                                         |- job_csv         (lee CSV local)
                                         |- job_sql         (lee MySQL por JDBC/red)
                                         |- job_comparacion (local vs distribuido)
                                                      |
                                         spark/output/  (resultados en el master)
```

**Decisiones de diseño clave:**

- **Por qué VMs en los nodos Windows:** Spark necesita `network_mode: host` para que
  los executors abran sus puertos sobre la red, y eso **solo funciona en Linux**. Por eso
  los workers corren dentro de una VM Ubuntu con Tailscale instalado **dentro** de la VM.
- **Datos de archivo (JSON/CSV):** cada executor lee de su disco local, por lo que el
  archivo debe existir en los 3 nodos en la misma ruta (NFS o `rsync`).
- **Datos SQL:** la lectura JDBC viaja por red desde MySQL hacia los executors → es el
  mejor ejemplo de procesamiento distribuido real, sin almacenamiento compartido.

> 📷 **[IMAGEN 1]** — Diagrama de arquitectura completo (red, nodos, roles).

---

## 3. Configuración de la red con Tailscale

Los 3 nodos están en la misma cuenta de Tailscale y se ven por sus IPs `100.x`.
Tailscale funciona sobre cualquier red (incluso detrás de NAT), así que las VM
pueden usar el adaptador **NAT por defecto** (no requiere modo puente).

### 3.1 Instalación

En el **Linux físico** y **dentro de cada VM Ubuntu**:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

> ⚠️ **Importante:** en los nodos Windows, Tailscale se instala **DENTRO de la VM Ubuntu**,
> NO en el Windows host. Así el contenedor con `network_mode: host` se enlaza a la IP `100.x` de la VM.

### 3.2 Verificación

```bash
tailscale status      # los 3 nodos deben aparecer
tailscale ip -4       # muestra la IP 100.x del nodo
```

Comprobar conectividad entre nodos (desde el master):

```bash
ping 100.126.190.35   # Nodo 2
ping 100.87.252.100   # Nodo 3
```

> 📷 **[IMAGEN 2]** — Salida de `tailscale status` mostrando los 3 nodos conectados.

> 📷 **[IMAGEN 3]** — Panel de administración de Tailscale (admin console) con los 3 dispositivos.

> 📷 **[IMAGEN 4]** — `ping` exitoso entre nodos por IP `100.x`.

---

## 4. Configuración de Docker en cada nodo

### 4.1 Requisitos por nodo

- Docker + Docker Compose
- Tailscale instalado y autenticado con la misma cuenta
- El repositorio **clonado en la misma ruta absoluta** en los 3 nodos
- Node.js (solo Nodo 1, para productores/consumidores)

### 4.2 Descargar el conector JDBC de MySQL (en los 3 nodos)

El `.jar` no se versiona en git. **Antes** de levantar Spark:

```bash
bash spark/jars/descargar_driver.sh
```

Descarga `mysql-connector-j-8.0.33.jar` desde Maven Central.

### 4.3 Levantar los servicios (cada nodo el suyo)

```bash
# Nodo 1
docker compose -f docker/cluster/nodo1/docker-compose.yml up -d
# Nodo 2
docker compose -f docker/cluster/nodo2/docker-compose.yml up -d
# Nodo 3
docker compose -f docker/cluster/nodo3/docker-compose.yml up -d
```

**Servicios por nodo:**

| Nodo | Contenedores |
|------|--------------|
| Nodo 1 | `kafka-nodo1`, `spark-master-cluster`, `spark-history-cluster`, `mysql-cluster` |
| Nodo 2 | `kafka-nodo2`, `spark-worker-1-cluster` |
| Nodo 3 | `kafka-nodo3`, `spark-worker-2-cluster` |

> 📷 **[IMAGEN 5]** — `docker ps` en el Nodo 1 (master) con sus 4 contenedores arriba.

> 📷 **[IMAGEN 6]** — `docker ps` en un Worker (Nodo 2 o 3) con sus 2 contenedores.

---

## 5. Configuración del clúster Kafka (KRaft)

Kafka corre en modo **KRaft** (sin ZooKeeper): los 3 brokers son también controllers.

### 5.1 Parámetros clave (docker-compose)

```yaml
KAFKA_NODE_ID: 1                       # 1, 2, 3 según el nodo
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://100.124.245.95:9092   # IP Tailscale del nodo
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@100.124.245.95:9093,2@100.126.190.35:9093,3@100.87.252.100:9093
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

| Parámetro | Valor | Por qué |
|-----------|-------|---------|
| Listener cliente | `9092` | conexión de productores/consumidores |
| Listener control | `9093` | quórum entre controllers |
| `advertised.listeners` | IP `100.x` del nodo | clave para que clientes de otros nodos se conecten |
| Réplica de offsets/tx | `3` | tolerancia a fallos |

### 5.2 Crear los tópicos (desde el Nodo 1)

```bash
docker exec -i kafka-nodo1 bash -s < kafka/topics/crear_topicos_cluster.sh
```

Crea **5 tópicos**, cada uno con **3 particiones**, **factor de replicación 3** y
`min.insync.replicas=1`:

| Tópico | Categoría de eventos |
|--------|----------------------|
| `metricas_recursos` | CPU, RAM, disco, temperatura |
| `logs_http` | peticiones HTTP, códigos de estado |
| `logs_errores` | errores y excepciones de servicios |
| `metricas_red` | latencia, paquetes perdidos, conexiones |
| `logs_seguridad` | logins, IPs sospechosas, accesos |

> 📷 **[IMAGEN 7]** — Salida de `crear_topicos_cluster.sh`: lista y `--describe` de los 5 tópicos.

> 📷 **[IMAGEN 8]** — `kafka-topics.sh --describe` mostrando réplicas (3) y partición líder por tópico.

---

## 6. Productores y consumidores (Node.js)

### 6.1 Variables de entorno (`kafka/.env`)

```env
KAFKA_BROKERS=100.124.245.95:9092,100.126.190.35:9092,100.87.252.100:9092
MYSQL_HOST=100.124.245.95
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=root123
MYSQL_DATABASE=monitoreo_servidores
PRODUCER_DELAY_MS=100
```

### 6.2 Generación de eventos

El generador (`kafka/shared/event_generator.js`) simula **5 servidores** distribuidos
en zonas (`rack-a/b/c`) y ambientes (`produccion`, `pruebas`, `desarrollo`). Cada evento
incluye métricas realistas correlacionadas con el nivel (`INFO`, `WARNING`, `ERROR`, `CRITICAL`),
con picos y caídas aleatorios para hacer la analítica interesante.

**Opción A — generar datos sintéticos (recomendado para la prueba de 100k):**

```bash
npm install
node generar_datos_prueba.js 100000   # crea data/raw/eventos.jsonl y .csv
```

**Opción B — datos reales desde Kafka:**

```bash
cd kafka && npm install && cd ..
bash kafka/producers/ejecutar.sh        # lanza los 5 productores en paralelo
# En otras terminales, los consumidores:
node kafka/consumers/consumer_json.js   # -> data/raw/eventos.jsonl
node kafka/consumers/consumer_csv.js    # -> data/raw/eventos.csv
node kafka/consumers/consumer_sql.js    # -> MySQL (inserción por lotes)
```

> 📷 **[IMAGEN 9]** — Productores corriendo (`ejecutar.sh`) enviando eventos a Kafka.

> 📷 **[IMAGEN 10]** — Consumidores escribiendo a JSONL / CSV / MySQL.

---

## 7. Base de datos MySQL

Corre en el Nodo 1 (`mysql-cluster`, puerto `3306`). El esquema se carga automáticamente
al primer arranque (`database/schema.sql`).

| Parámetro | Valor |
|-----------|-------|
| Base de datos | `monitoreo_servidores` |
| Tabla | `logs_metricas_servidores` |
| Clave primaria | `id_log` (UUID) |
| Usuario root | `root` / `root123` |

La tabla almacena ~40 columnas: identificación del servidor (servidor, IP, zona, SO,
ambiente), datos del evento (servicio, tipo, nivel, código de estado), datos HTTP/seguridad
y métricas numéricas (CPU, RAM, disco, tiempo de respuesta, latencia, bytes, etc.).

> 📷 **[IMAGEN 11]** — `SELECT COUNT(*)` y muestra de filas en MySQL (`logs_metricas_servidores`).

---

## 8. Compartición de datos (NFS / rsync)

Los jobs de **archivos** (JSON/CSV) leen desde el disco local de cada executor, así que
el archivo debe existir en los 3 nodos en la misma ruta. Hay dos opciones:

### 8.1 Opción A — Sincronización por rsync

```bash
bash scripts/sync_datos.sh
```

Copia `data/raw/` del master a los 2 workers vía SSH/rsync.

### 8.2 Opción B — NFS exportado por el master (recomendado)

En el **master**:

```bash
bash scripts/nfs_master.sh
# Exporta data/ en modo solo-lectura a toda la red Tailscale (100.64.0.0/10)
```

En **cada worker**:

```bash
MASTER_IP=100.124.245.95 \
MASTER_EXPORT=/ruta/absoluta/al/proyecto/data \
bash scripts/nfs_worker.sh
# Luego recrear el contenedor del worker:
docker compose -f docker/cluster/nodoN/docker-compose.yml up -d --force-recreate spark-worker
```

> El job **SQL no necesita esto**: lee por red desde MySQL.

> 📷 **[IMAGEN 12]** — `exportfs -v` en el master mostrando la carpeta `data/` exportada.

> 📷 **[IMAGEN 13]** — Worker con el NFS montado (`mountpoint` + `ls data/raw`).

---

## 9. Configuración del clúster Spark

Spark Standalone: **1 Master (Nodo 1) + 2 Workers (Nodos 2 y 3)**. Todos los servicios
usan `network_mode: host` para exponer los puertos aleatorios de los executors sobre Tailscale.

### 9.1 Master (Nodo 1)

```yaml
SPARK_LOCAL_IP: 100.124.245.95
SPARK_MASTER_HOST: 100.124.245.95
command: spark-class org.apache.spark.deploy.master.Master
         --host 100.124.245.95 --port 7077 --webui-port 8080
```

### 9.2 Workers (Nodos 2 y 3)

```yaml
SPARK_LOCAL_IP: 100.126.190.35        # IP Tailscale del worker
SPARK_PUBLIC_DNS: 100.126.190.35
command: spark-class org.apache.spark.deploy.worker.Worker
         spark://100.124.245.95:7077  # se registra contra el master
         --host 100.126.190.35 --port 7078 --webui-port 8081
```

### 9.3 Verificación

Abrir la **UI del Master**: `http://100.124.245.95:8080` → deben aparecer **2 workers ALIVE**.

| Puerto | Servicio |
|--------|----------|
| `8080` | Spark Master UI |
| `7077` | Spark Master (RPC) |
| `8081` | Spark Worker UI |
| `18080` | Spark History Server |

> 📷 **[IMAGEN 14]** — Spark Master UI (`:8080`) con los 2 workers en estado **ALIVE**.

> 📷 **[IMAGEN 15]** — Detalle de un worker en la UI de Spark.

---

## 10. Jobs de análisis en Spark

Ejecución desde el Nodo 1 (`submit_jobs.sh`). Los resultados quedan siempre en el master,
en `spark/output/`.

```bash
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh             # todos
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh json
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh csv
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh sql
docker exec spark-master-cluster bash /opt/spark/jobs/submit_jobs.sh comparacion
```

| Job | Fuente | Modo | Análisis |
|-----|--------|------|----------|
| `job_json.py` | JSONL local | `local[*]` | conteo por nivel; CPU/RAM por servidor (avg/min/max/stddev); RAM por servicio; máximo de disco; eventos por zona; servidores críticos |
| `job_csv.py` | CSV local | `local[*]` | conteo por tipo; tiempo de respuesta por servicio; latencia de red por servidor; bytes entrada/salida; errores por servidor |
| `job_sql.py` | MySQL (JDBC) | **distribuido** | estadísticas globales; métricas por ambiente; top errores; tasa de error por servicio; percentiles p50/p90/p95/p99; ranking por CPU (window RANK); distribución de códigos de estado; throughput por hora |
| `job_comparacion.py` | MySQL (JDBC) | local[1] / local[*] / cluster | mismo trabajo en los 3 modos + speedup |

**Lectura distribuida JDBC:** `job_sql` y `job_comparacion` particionan por `CRC32(id_log)`
en 8 particiones para que la lectura se reparta entre los 2 workers (sin esto, JDBC leería
todo en una sola partición).

> 📷 **[IMAGEN 16]** — Ejecución de `submit_jobs.sh sql` en la terminal (logs del job).

> 📷 **[IMAGEN 17]** — Tablas de resultados impresas por `job_sql.py` (`res.show()`).

---

## 11. Pruebas y resultados

### 11.1 Volumen de datos procesado

Prueba realizada con **~315 000 registros** en la tabla MySQL.

### 11.2 Comparación local vs distribuido (`job_comparacion.py`)

| Modo | Tiempo (s) | Registros |
|------|-----------|-----------|
| Local 1 núcleo (`local[1]`) | 12.24 | 315 453 |
| Local n núcleos (`local[*]`) | 2.44 | 315 453 |
| Distribuido (3 nodos) | 19.01 | 315 453 |

> **Observación:** en este volumen, `local[*]` es el más rápido porque el costo de red de
> Tailscale (envío de tareas y resultados entre nodos) supera la ganancia de paralelismo.
> El modo distribuido demuestra su ventaja con volúmenes mucho mayores; con 315k registros
> el overhead de coordinación domina. _Conviene comentar este efecto en la defensa._

> 📷 **[IMAGEN 18]** — Salida de `job_comparacion.py` con la tabla COMPARATIVA y los speedups.

### 11.3 Estadísticas globales (`estadisticas_globales.csv`)

| cpu_avg | cpu_min | cpu_max | cpu_stddev | ram_avg | disco_avg | temp_avg | latencia_avg | respuesta_avg |
|---------|---------|---------|------------|---------|-----------|----------|--------------|---------------|
| 54.52 | 0.73 | 100.00 | 27.17 | 57.59 | 57.98 | 65.14 | 261.78 | 1119.07 |

### 11.4 Métricas por ambiente (`por_ambiente.csv`)

| ambiente | total | cpu_avg | ram_avg | respuesta_avg_ms | errores_avg |
|----------|-------|---------|---------|------------------|-------------|
| produccion | 198 299 | 54.55 | 57.65 | 1120.76 | 17.14 |
| desarrollo | 66 749 | 54.45 | 57.47 | 1119.57 | 17.15 |
| pruebas | 66 189 | 54.48 | 57.53 | 1113.47 | 17.03 |

### 11.5 Otros archivos de salida (`spark/output/resultado_sql/`)

- `top_servidores_errores.csv` — top 10 servidores con más errores
- `tasa_error_por_servicio.csv` — % de error por servicio
- `percentiles_respuesta.csv` — p50/p90/p95/p99 de tiempo de respuesta
- `ranking_servidores_cpu.csv` — ranking de servidores por CPU

> 📷 **[IMAGEN 19]** — Spark History Server (`:18080`) con los jobs ejecutados.

> 📷 **[IMAGEN 20]** — DAG / etapas de un job en la UI de Spark (tareas repartidas entre workers).

> 📷 **[IMAGEN 21]** — Contenido de los CSV de resultados (abiertos en editor o tabla).

---

## 12. Pruebas de tolerancia a fallos

Con replicación 3 y `min.insync.replicas=1`, el clúster Kafka sigue produciendo y
consumiendo aunque caiga un nodo.

```bash
# Apagar un broker y verificar que el clúster sigue
docker stop kafka-nodo3
docker exec kafka-nodo1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 100.124.245.95:9092 --describe --topic logs_http

# Reincorporar el nodo
docker start kafka-nodo3
```

**Resultado esperado:** tras detener `kafka-nodo3`, los tópicos siguen disponibles; las
particiones cuyo líder estaba en el nodo 3 eligen un nuevo líder entre las réplicas. Spark
también redistribuye tareas si un worker se cae.

> 📷 **[IMAGEN 22]** — `--describe` del tópico ANTES de apagar el broker (3 réplicas, ISR completo).

> 📷 **[IMAGEN 23]** — `--describe` DESPUÉS de `docker stop kafka-nodo3`: nuevo líder, ISR reducido pero tópico operativo.

> 📷 **[IMAGEN 24]** — Spark Master UI con un worker caído / recuperado.

---

## 13. Solución de problemas

| Síntoma | Causa | Solución |
|---------|-------|----------|
| Los workers no aparecen ALIVE | Tailscale inactivo en la VM o falta `network_mode: host` | Verificar `ping 100.x` y `tailscale status` |
| `job_sql` falla por driver JDBC | Falta el `.jar` en ese nodo | `bash spark/jars/descargar_driver.sh` |
| `FileNotFoundException` en JSON/CSV | No se sincronizaron los datos | `bash scripts/sync_datos.sh` o montar NFS |
| El `.jar` aparece como carpeta | Docker la creó al hacer `up` sin el jar | `down`, `rm -rf` la carpeta, volver a descargar |
| Clientes no conectan a Kafka | `advertised.listeners` mal configurado | Debe tener la IP `100.x` real del nodo |

---

## 14. Conclusiones

- Tailscale permitió montar una red mesh con IPs fijas entre máquinas físicas y VMs sin
  configurar puertos en el router, funcionando incluso detrás de NAT.
- Kafka en modo KRaft con replicación 3 ofrece tolerancia a fallos verificable: el clúster
  sigue operando con un nodo caído.
- Spark distribuido demuestra el reparto de carga (lectura JDBC particionada entre workers);
  la comparación de modos evidencia que el beneficio del clúster depende del volumen de datos
  frente al overhead de red.
- El diseño separa correctamente las fuentes: archivos (requieren datos replicados/NFS) y
  base de datos (lectura distribuida por red sin almacenamiento compartido).

---

_Documento generado como reporte de configuración y pruebas del proyecto._
_Reemplazar los marcadores **[IMAGEN N]** por las capturas correspondientes._
