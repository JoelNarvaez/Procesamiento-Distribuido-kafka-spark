// /**
//  * consumer_sql.js
//  * Consume los 5 tópicos de Kafka y guarda cada mensaje
//  * como un INSERT INTO en data/raw/eventos.sql
//  * Compatible con el schema definido en database/schema.sql
//  */

// const { createKafkaClient } = require("../shared/kafka_client");
// const fs   = require("fs");
// const path = require("path");
// require("dotenv").config();

// // ── Configuración ────────────────────────────────────────────────────────────
// const TOPICS = [
//   "metricas_recursos",
//   "logs_http",
//   "logs_errores",
//   "metricas_red",
//   "logs_seguridad"
// ];

// // Columnas en el mismo orden que el schema de la BD
// const DB_COLUMNS = [
//   "id_log",
//   "timestamp_evento",
//   "servidor",
//   "ip_servidor",
//   "zona",
//   "sistema_operativo",
//   "ambiente",
//   "servicio",
//   "tipo_evento",
//   "nivel",
//   "codigo_estado",
//   "endpoint",
//   "metodo_http",
//   "usuario",
//   "correo_usuario",
//   "ciudad",
//   "ip_origen",
//   "user_agent",
//   "tiempo_respuesta_ms",
//   "uso_cpu_porcentaje",
//   "uso_ram_porcentaje",
//   "uso_disco_porcentaje",
//   "bytes_entrada",
//   "bytes_salida",
//   "peticiones_por_minuto",
//   "conexiones_activas",
//   "errores_minuto",
//   "latencia_red_ms",
//   "temperatura_cpu",
//   "paquetes_perdidos",
//   "intento_login",
//   "ip_sospechosa",
//   "pais_origen",
//   "trace_id",
//   "session_id",
//   "proceso",
//   "mensaje"
// ];

// const TABLE_NAME  = process.env.SQL_TABLE     || "logs_metricas_servidores";
// const DB_NAME     = process.env.SQL_DATABASE  || "monitoreo_servidores";
// const GROUP_ID    = process.env.CONSUMER_GROUP_SQL || "consumer-group-sql";
// const OUTPUT_DIR  = path.resolve(__dirname, "../../data/raw");
// const OUTPUT_FILE = path.join(OUTPUT_DIR, "eventos.sql");
// const LOG_EVERY   = Number(process.env.LOG_EVERY    || 1000);
// // Cuántos INSERTs agrupar en un solo VALUES (...),(...)
// const BATCH_SIZE  = Number(process.env.SQL_BATCH_SIZE || 500);

// // ── Helpers ──────────────────────────────────────────────────────────────────
// function ensureDir(dir) {
//   if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
// }

// function timestamp() {
//   return new Date().toISOString();
// }

// /**
//  * Escapa un valor para uso seguro dentro de un INSERT SQL.
//  * - null / undefined / false → NULL
//  * - true → 1  (MySQL tinyint)
//  * - números → sin comillas
//  * - strings  → comillas simples con escape de comillas internas
//  */
// function sqlEscape(value) {
//   if (value === null || value === undefined) return "NULL";
//   if (typeof value === "boolean")            return value ? "1" : "0";
//   if (typeof value === "number")             return String(value);

//   // Convertir DATETIME ISO a formato MySQL
//   if (
//     typeof value === "string" &&
//     /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)
//   ) {
//     // "2024-05-01T13:45:00.000Z" → "2024-05-01 13:45:00"
//     const mysqlDate = value.replace("T", " ").replace(/\.\d{3}Z$/, "");
//     return `'${mysqlDate}'`;
//   }

//   // Escape de comillas simples
//   return `'${String(value).replace(/'/g, "''")}'`;
// }

// /**
//  * Convierte un objeto evento en la parte VALUES (...) de un INSERT.
//  */
// function eventoAValuesTuple(evento) {
//   const valores = DB_COLUMNS.map((col) => sqlEscape(evento[col]));
//   return `(${valores.join(", ")})`;
// }

// // ── Buffer de lotes ──────────────────────────────────────────────────────────
// let buffer = [];

// function flushBuffer(stream) {
//   if (buffer.length === 0) return;

//   const columnasSql = DB_COLUMNS.map((c) => `\`${c}\``).join(", ");
//   const values      = buffer.join(",\n  ");
//   const sql =
//     `INSERT INTO \`${TABLE_NAME}\`\n` +
//     `  (${columnasSql})\nVALUES\n  ${values};\n\n`;

//   stream.write(sql);
//   buffer = [];
// }

// // ── Main ─────────────────────────────────────────────────────────────────────
// async function runConsumerSql() {
//   ensureDir(OUTPUT_DIR);

//   const kafka    = createKafkaClient("consumer-sql");
//   const consumer = kafka.consumer({ groupId: GROUP_ID });

//   // Cabecera del archivo SQL
//   const stream = fs.createWriteStream(OUTPUT_FILE, { flags: "a" });
//   const fileExists = fs.existsSync(OUTPUT_FILE);

//   if (!fileExists) {
//     stream.write(
//       `-- =====================================================\n` +
//       `-- Archivo generado por consumer_sql.js\n` +
//       `-- Tabla destino: ${DB_NAME}.${TABLE_NAME}\n` +
//       `-- =====================================================\n\n` +
//       `USE \`${DB_NAME}\`;\n\n`
//     );
//   }

//   let totalRecibidos = 0;
//   let totalEscritos  = 0;

//   // Cierre limpio
//   const shutdown = async (signal) => {
//     console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer SQL...`);
//     flushBuffer(stream);   // Escribir lo que quede en el buffer
//     stream.end();
//     await consumer.disconnect();
//     console.log(`[${timestamp()}] Consumer SQL detenido. Total escritos: ${totalEscritos}`);
//     process.exit(0);
//   };

//   process.on("SIGINT",  () => shutdown("SIGINT"));
//   process.on("SIGTERM", () => shutdown("SIGTERM"));

//   try {
//     await consumer.connect();
//     await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

//     console.log("==========================================");
//     console.log(" Consumer SQL iniciado");
//     console.log(` Grupo        : ${GROUP_ID}`);
//     console.log(` Tópicos      : ${TOPICS.join(", ")}`);
//     console.log(` Salida       : ${OUTPUT_FILE}`);
//     console.log(` Tabla        : ${DB_NAME}.${TABLE_NAME}`);
//     console.log(` Batch size   : ${BATCH_SIZE} registros por INSERT`);
//     console.log("==========================================\n");

//     await consumer.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         totalRecibidos++;

//         try {
//           const raw = message.value?.toString();
//           if (!raw) return;

//           const evento = JSON.parse(raw);

//           buffer.push(eventoAValuesTuple(evento));
//           totalEscritos++;

//           // Vaciar buffer cuando alcanza el tamaño de lote
//           if (buffer.length >= BATCH_SIZE) {
//             flushBuffer(stream);
//           }

//           if (totalEscritos % LOG_EVERY === 0) {
//             console.log(`[${timestamp()}] [SQL] Escritos: ${totalEscritos} | Tópico actual: ${topic}`);
//           }
//         } catch (parseError) {
//           console.error(`[${timestamp()}] Error al parsear mensaje (offset ${message.offset}):`, parseError.message);
//         }
//       }
//     });

//   } catch (error) {
//     console.error(`[${timestamp()}] Error crítico en consumer SQL:`, error.message);
//     flushBuffer(stream);
//     stream.end();
//     await consumer.disconnect();
//     process.exit(1);
//   }
// }

// runConsumerSql();

/**
 * consumer_sql.js
 * Consume los 5 tópicos de Kafka e inserta cada mensaje
 * directamente en MySQL usando mysql2 con pool de conexiones.
 * Compatible con el schema definido en database/schema.sql
 */

const { createKafkaClient } = require("../shared/kafka_client");
const mysql = require("mysql2/promise");
require("dotenv").config();

// ── Configuración ────────────────────────────────────────────────────────────
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

const TABLE_NAME = process.env.SQL_TABLE          || "logs_metricas_servidores";
const DB_NAME    = process.env.SQL_DATABASE       || "monitoreo_servidores";
const GROUP_ID   = process.env.CONSUMER_GROUP_SQL || "consumer-group-sql";
const LOG_EVERY  = Number(process.env.LOG_EVERY   || 1000);
const BATCH_SIZE = Number(process.env.SQL_BATCH_SIZE || 100);

// ── Helpers ──────────────────────────────────────────────────────────────────
function timestamp() {
  return new Date().toISOString();
}

/**
 * Convierte timestamp ISO a formato MySQL DATETIME
 * "2024-05-01T13:45:00.000Z" → "2024-05-01 13:45:00"
 */
function toMysqlDatetime(value) {
  if (!value) return null;
  return String(value).replace("T", " ").replace(/\.\d{3}Z$/, "");
}

/**
 * Extrae los valores del evento en el orden de DB_COLUMNS
 * listos para usar con prepared statements (mysql2 escapa automáticamente)
 */
function eventoAValores(evento) {
  return DB_COLUMNS.map((col) => {
    const value = evento[col];
    if (col === "timestamp_evento") return toMysqlDatetime(value);
    if (typeof value === "boolean") return value ? 1 : 0;
    if (value === null || value === undefined) return null;
    return value;
  });
}

// ── Crear pool de conexiones MySQL ───────────────────────────────────────────
function crearPool() {
  return mysql.createPool({
    host               : process.env.MYSQL_HOST     || "192.168.1.65",
    port               : Number(process.env.MYSQL_PORT || 3306),
    user               : process.env.MYSQL_USER     || "usuario",
    password           : process.env.MYSQL_PASSWORD || "usuario123",
    database           : process.env.MYSQL_DATABASE || DB_NAME,
    waitForConnections : true,
    connectionLimit    : 10,
    queueLimit         : 0
  });
}

// ── INSERT por lotes ─────────────────────────────────────────────────────────
const columnasSql  = DB_COLUMNS.map((c) => `\`${c}\``).join(", ");
const placeholders = `(${DB_COLUMNS.map(() => "?").join(", ")})`;

async function flushBuffer(pool, buffer) {
  if (buffer.length === 0) return;
  const batchPlaceholders = buffer.map(() => placeholders).join(", ");
  const batchQuery  = `INSERT INTO \`${TABLE_NAME}\` (${columnasSql}) VALUES ${batchPlaceholders}`;
  const batchValues = buffer.flat();
  await pool.execute(batchQuery, batchValues);
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function runConsumerSql() {
  const pool     = crearPool();
  const kafka    = createKafkaClient("consumer-sql");
  const consumer = kafka.consumer({ groupId: GROUP_ID });

  let totalInsertados = 0;
  let buffer          = [];

  // Verificar conexión a MySQL antes de arrancar
  try {
    const conn = await pool.getConnection();
    console.log(`[${timestamp()}] Conexión a MySQL establecida correctamente`);
    conn.release();
  } catch (dbError) {
    console.error(`[${timestamp()}] No se pudo conectar a MySQL:`, dbError.message);
    process.exit(1);
  }

  // Cierre limpio
  const shutdown = async (signal) => {
    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer SQL...`);
    try {
      if (buffer.length > 0) {
        await flushBuffer(pool, buffer);
        totalInsertados += buffer.length;
        buffer = [];
        console.log(`[${timestamp()}] Buffer final insertado.`);
      }
    } catch (err) {
      console.error(`[${timestamp()}] Error al insertar buffer final:`, err.message);
    }
    await consumer.disconnect();
    await pool.end();
    console.log(`[${timestamp()}] Consumer SQL detenido. Total insertados: ${totalInsertados}`);
    process.exit(0);
  };

  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await consumer.connect();
    await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

    console.log("==========================================");
    console.log(" Consumer SQL iniciado");
    console.log(` Grupo      : ${GROUP_ID}`);
    console.log(` Tópicos    : ${TOPICS.join(", ")}`);
    console.log(` Base datos : ${DB_NAME}.${TABLE_NAME}`);
    console.log(` Host MySQL : ${process.env.MYSQL_HOST || "192.168.1.65"}`);
    console.log(` Batch size : ${BATCH_SIZE} registros por INSERT`);
    console.log("==========================================\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const raw = message.value?.toString();
          if (!raw) return;

          const evento = JSON.parse(raw);
          buffer.push(eventoAValores(evento));

          if (buffer.length >= BATCH_SIZE) {
            await flushBuffer(pool, buffer);
            totalInsertados += buffer.length;
            buffer = [];
          }

          if (totalInsertados % LOG_EVERY === 0 && totalInsertados > 0) {
            console.log(`[${timestamp()}] [SQL] Insertados: ${totalInsertados} | Tópico: ${topic}`);
          }
        } catch (err) {
          console.error(`[${timestamp()}] Error al procesar mensaje (offset ${message.offset}):`, err.message);
        }
      }
    });

  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer SQL:`, error.message);
    try {
      if (buffer.length > 0) await flushBuffer(pool, buffer);
    } catch (_) {}
    await consumer.disconnect();
    await pool.end();
    process.exit(1);
  }
}

runConsumerSql();