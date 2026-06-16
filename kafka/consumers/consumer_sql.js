/**
 * consumer_sql.js
 * Consume los 5 tópicos de Kafka e inserta cada mensaje
 * directamente en MySQL, uno por uno.
 */

const { createKafkaClient } = require("../shared/kafka_client");
const mysql = require("mysql2/promise");
require("dotenv").config();

const TOPICS = [
  "metricas_recursos",
  "logs_http",
  "logs_errores",
  "metricas_red",
  "logs_seguridad"
];

const DB_COLUMNS = [
  "id_log",
  "timestamp_evento",
  "servidor",
  "ip_servidor",
  "zona",
  "sistema_operativo",
  "ambiente",
  "servicio",
  "tipo_evento",
  "nivel",
  "codigo_estado",
  "endpoint",
  "metodo_http",
  "usuario",
  "correo_usuario",
  "ciudad",
  "ip_origen",
  "user_agent",
  "tiempo_respuesta_ms",
  "uso_cpu_porcentaje",
  "uso_ram_porcentaje",
  "uso_disco_porcentaje",
  "bytes_entrada",
  "bytes_salida",
  "peticiones_por_minuto",
  "conexiones_activas",
  "errores_minuto",
  "latencia_red_ms",
  "temperatura_cpu",
  "paquetes_perdidos",
  "intento_login",
  "ip_sospechosa",
  "pais_origen",
  "trace_id",
  "session_id",
  "proceso",
  "mensaje"
];

const TABLE_NAME = process.env.SQL_TABLE || "logs_metricas_servidores";
const DB_NAME = process.env.MYSQL_DATABASE || "monitoreo_servidores";
const GROUP_ID = process.env.CONSUMER_GROUP_SQL || "consumer-group-sql";

// Inserción por lotes: mucho más rápido que insertar uno por uno.
const BATCH_SIZE = Number(process.env.SQL_BATCH_SIZE || 500);
const FLUSH_MS = Number(process.env.SQL_FLUSH_MS || 2000);

function timestamp() {
  return new Date().toISOString();
}

function toMysqlDatetime(value) {
  if (!value) return null;
  return String(value).replace("T", " ").replace(/\.\d{3}Z$/, "");
}

function eventoAValores(evento) {
  return DB_COLUMNS.map((col) => {
    const value = evento[col];

    if (col === "timestamp_evento") return toMysqlDatetime(value);
    if (typeof value === "boolean") return value ? 1 : 0;
    if (value === null || value === undefined) return null;

    return value;
  });
}

function crearPool() {
  return mysql.createPool({
    host: process.env.MYSQL_HOST || "192.168.1.65",
    port: Number(process.env.MYSQL_PORT || 3306),
    user: process.env.MYSQL_USER || "usuario",
    password: process.env.MYSQL_PASSWORD || "usuario123",
    database: DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });
}

const columnasSql = DB_COLUMNS.map((c) => `\`${c}\``).join(", ");
const filaPlaceholder = `(${DB_COLUMNS.map(() => "?").join(", ")})`;

// Inserta un lote completo en una sola consulta multi-fila.
async function insertarLote(pool, eventos) {
  if (eventos.length === 0) return;

  const groups = eventos.map(() => filaPlaceholder).join(", ");
  const sql =
    `INSERT IGNORE INTO \`${TABLE_NAME}\` (${columnasSql}) VALUES ${groups}`;

  const valores = [];
  for (const evento of eventos) {
    valores.push(...eventoAValores(evento));
  }

  await pool.query(sql, valores);
}

async function runConsumerSql() {
  const pool = crearPool();
  const kafka = createKafkaClient("consumer-sql");

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000
  });

  let totalProcesados = 0;
  let cerrando = false;

  // Buffer de inserción por lotes + serialización de los flush.
  let buffer = [];
  let flushChain = Promise.resolve();

  function flush() {
    flushChain = flushChain.then(async () => {
      if (buffer.length === 0) return;
      const lote = buffer;
      buffer = [];
      try {
        await insertarLote(pool, lote);
        totalProcesados += lote.length;
        console.log(`[${timestamp()}] Lote insertado: ${lote.length} (total: ${totalProcesados})`);
      } catch (error) {
        console.error(
          `[${timestamp()}] Error insertando lote:`,
          error.code || "",
          error.sqlMessage || error.message
        );
      }
    });
    return flushChain;
  }

  try {
    const conn = await pool.getConnection();
    console.log(`[${timestamp()}] Conexión a MySQL establecida correctamente`);
    conn.release();
  } catch (dbError) {
    console.error(`[${timestamp()}] No se pudo conectar a MySQL:`, dbError.message);
    process.exit(1);
  }

  const flushInterval = setInterval(flush, FLUSH_MS);

  const shutdown = async (signal) => {
    if (cerrando) return;
    cerrando = true;

    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer SQL...`);

    try {
      clearInterval(flushInterval);
      await consumer.stop();
      await flush();        // vaciar lo que quede en el buffer
      await consumer.disconnect();
      await pool.end();

      console.log(`[${timestamp()}] Consumer SQL detenido. Total procesados: ${totalProcesados}`);
      process.exit(0);
    } catch (error) {
      console.error(`[${timestamp()}] Error al cerrar consumer SQL:`, error.message);
      process.exit(1);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await consumer.connect();

    for (const topic of TOPICS) {
      await consumer.subscribe({
        topic,
        fromBeginning: false
      });
    }

    console.log("==========================================");
    console.log(" Consumer SQL iniciado");
    console.log(` Grupo      : ${GROUP_ID}`);
    console.log(` Tópicos    : ${TOPICS.join(", ")}`);
    console.log(` Base datos : ${DB_NAME}.${TABLE_NAME}`);
    console.log(` Host MySQL : ${process.env.MYSQL_HOST || "192.168.1.65"}`);
    console.log(` Modo       : inserción por lotes (BATCH_SIZE=${BATCH_SIZE}, FLUSH_MS=${FLUSH_MS})`);
    console.log("==========================================\n");

    await consumer.run({
      autoCommit: true,

      eachMessage: async ({ message }) => {
        try {
          const raw = message.value?.toString();
          if (!raw) return;

          const evento = JSON.parse(raw);
          buffer.push(evento);

          if (buffer.length >= BATCH_SIZE) {
            await flush();
          }
        } catch (error) {
          console.error(
            `[${timestamp()}] Error procesando mensaje:`,
            error.code || "",
            error.sqlMessage || error.message
          );
        }
      }
    });
  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer SQL:`, error.message);

    try {
      await consumer.disconnect();
    } catch (_) {}

    await pool.end();
    process.exit(1);
  }
}

runConsumerSql();