# Arquitectura del sistema

## Nodos y red
- 3 nodos conectados por **Tailscale** (VPN mesh) con IPs fijas:
  `100.124.245.95`, `100.126.190.35`, `100.87.252.100`.
- Nodo 1: Linux físico. Nodos 2 y 3: VM Ubuntu sobre Windows.
- Tailscale se instala DENTRO de cada VM (no en el Windows host) para que el
  contenedor con `network_mode: host` se enlace a la IP `100.x` de la VM.
- Tailscale funciona sobre cualquier red (incluso NAT), por lo que la VM no
  necesita modo puente.
- Nota de entrega: la propuesta deja Tailscale como auxiliar; aquí es la red
  principal y conviene justificarlo en la documentación.

## Kafka (KRaft, sin ZooKeeper)
- 3 brokers que también son controllers (`KAFKA_PROCESS_ROLES=broker,controller`).
- Quórum de controllers: `1@100.124.245.95:9093, 2@100.126.190.35:9093, 3@100.87.252.100:9093`.
- Listener cliente `9092`, listener de control `9093`.
- `advertised.listeners` con la IP de Tailscale de cada nodo (clave para que los
  clientes de otros nodos se conecten).
- 5 tópicos, 3 particiones, factor de replicación 3, `min.insync.replicas=1`.
- Kafka usa puertos fijos publicados -> funciona bien aun en Docker dentro de VM.

## Spark (Standalone)
- 1 Master (nodo 1) + 2 Workers (nodos 2 y 3).
- Todos los servicios de Spark usan `network_mode: host` para exponer los
  puertos aleatorios de los executors sobre Tailscale. Requiere Linux (de ahí las VMs).
- El driver corre en el contenedor del master en modo cliente
  (`spark.driver.host=100.124.245.95`, `bindAddress=0.0.0.0`).

## Flujo de datos
```
Productores Node.js --> Topicos Kafka --> Consumidores Node.js
                                          |-> data/raw/eventos.jsonl  (JSON)
                                          |-> data/raw/eventos.csv    (CSV)
                                          |-> MySQL                   (SQL)
                                                      |
                                         Spark (master + 2 workers)
                                         |- job_json   (lee JSONL local)
                                         |- job_csv    (lee CSV local)
                                         |- job_sql    (lee MySQL por JDBC/red)
                                         |- job_comparacion (local vs distribuido)
                                                      |
                                         spark/output/  (resultados en el master)
```

## Decisiones de diseño relevantes
- **Datos de archivo (JSON/CSV):** Spark reparte la lectura entre executors; cada
  uno lee de su disco local, por lo que el archivo debe existir en los 3 nodos en
  la misma ruta (`scripts/sync_datos.sh`). Alternativa: un NFS exportado por el master.
- **Datos SQL:** la lectura JDBC viaja por la red desde MySQL hacia los executors,
  así que NO requiere almacenamiento compartido. Es el mejor ejemplo de
  procesamiento distribuido real.
- **Resultados:** cada job recolecta el resultado agregado en el driver (master) y
  lo escribe a `spark/output/`, garantizando que la salida quede siempre en el master
  sin importar en qué worker se ejecutó el cálculo.
- **Tolerancia a fallos:** con replicación 3 el clúster Kafka sobrevive a la caída
  de un nodo; Spark redistribuye tareas si un worker se cae.
