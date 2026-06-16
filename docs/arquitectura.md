# Arquitectura del sistema

## Nodos y red
- 3 nodos en LAN `192.168.1.0/24` con IPs fijas: `.65`, `.66`, `.67`.
- Nodo 1: Linux físico. Nodos 2 y 3: VM Ubuntu (bridged) sobre Windows.
- El modo *bridged* hace que cada VM tenga su propia IP en la red física y
  aparezca como un host independiente en la LAN.

## Kafka (KRaft, sin ZooKeeper)
- 3 brokers que también son controllers (`KAFKA_PROCESS_ROLES=broker,controller`).
- Quórum de controllers: `1@.65:9093, 2@.66:9093, 3@.67:9093`.
- Listener cliente `9092`, listener de control `9093`.
- `advertised.listeners` con la IP de la LAN de cada nodo (clave para que los
  clientes externos se conecten).
- 5 tópicos, 3 particiones, factor de replicación 3, `min.insync.replicas=1`.
- Kafka usa puertos fijos publicados -> funciona bien aun en Docker dentro de VM.

## Spark (Standalone)
- 1 Master (nodo 1) + 2 Workers (nodos 2 y 3).
- Todos los servicios de Spark usan `network_mode: host` para exponer los
  puertos aleatorios de los executors sobre la LAN. Requiere Linux (de ahí las VMs).
- El driver corre en el contenedor del master en modo cliente
  (`spark.driver.host=192.168.1.65`, `bindAddress=0.0.0.0`).

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
