/**
 * consumer_csv.js
 * Consume los 5 tópicos de Kafka y guarda cada mensaje
 * como una fila CSV en data/raw/eventos.csv
 * Las columnas siguen el orden definido en kafka/shared/fields.js
 */

const { createKafkaClient } = require("../shared/kafka_client");
const { FIELDS } = require("../shared/fields");
const fs   = require("fs");
const path = require("path");
require("dotenv").config();

// ── Configuración ────────────────────────────────────────────────────────────
const TOPICS = [
  "metricas_recursos",
  "logs_http",
  "logs_errores",
  "metricas_red",
  "logs_seguridad"
];

// Columnas extra de metadatos Kafka al final del CSV
const KAFKA_META_FIELDS = [
  "_kafka_topic",
  "_kafka_partition",
  "_kafka_offset",
  "_kafka_timestamp",
  "_consumer_ts"
];

const ALL_FIELDS = [...FIELDS, ...KAFKA_META_FIELDS];

const GROUP_ID    = process.env.CONSUMER_GROUP_CSV || "consumer-group-csv";
const OUTPUT_DIR  = path.resolve(__dirname, "../../data/raw");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "eventos.csv");
const LOG_EVERY   = Number(process.env.LOG_EVERY || 1000);

// ── Helpers ──────────────────────────────────────────────────────────────────
function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function timestamp() {
  return new Date().toISOString();
}

/**
 * Convierte un valor a string CSV-safe:
 * - null / undefined → cadena vacía
 * - Strings con comas, comillas o saltos de línea → envueltos en comillas dobles
 * - Booleanos → "true" / "false"
 */
function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Convierte un objeto evento en una línea CSV usando ALL_FIELDS como orden.
 */
function objetoALineaCsv(obj) {
  return ALL_FIELDS.map((field) => csvEscape(obj[field])).join(",");
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function runConsumerCsv() {
  ensureDir(OUTPUT_DIR);

  const kafka    = createKafkaClient("consumer-csv");
  const consumer = kafka.consumer({ groupId: GROUP_ID });

  // Si el archivo no existe, escribir cabecera; si ya existe, solo append
  const fileExists = fs.existsSync(OUTPUT_FILE);
  const stream     = fs.createWriteStream(OUTPUT_FILE, { flags: "a" });

  if (!fileExists) {
    stream.write(ALL_FIELDS.join(",") + "\n");
  }

  let totalRecibidos = 0;
  let totalEscritos  = 0;

  // Cierre limpio
  const shutdown = async (signal) => {
    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer CSV...`);
    stream.end();
    await consumer.disconnect();
    console.log(`[${timestamp()}] Consumer CSV detenido. Total escritos: ${totalEscritos}`);
    process.exit(0);
  };

  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await consumer.connect();
    await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

    console.log("==========================================");
    console.log(" Consumer CSV iniciado");
    console.log(` Grupo        : ${GROUP_ID}`);
    console.log(` Tópicos      : ${TOPICS.join(", ")}`);
    console.log(` Salida       : ${OUTPUT_FILE}`);
    console.log(` Columnas     : ${ALL_FIELDS.length}`);
    console.log("==========================================\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        totalRecibidos++;

        try {
          const raw = message.value?.toString();
          if (!raw) return;

          const evento = JSON.parse(raw);

          // Adjuntar metadatos Kafka
          const registro = {
            ...evento,
            _kafka_topic     : topic,
            _kafka_partition : partition,
            _kafka_offset    : message.offset,
            _kafka_timestamp : message.timestamp,
            _consumer_ts     : timestamp()
          };

          stream.write(objetoALineaCsv(registro) + "\n");
          totalEscritos++;

          if (totalEscritos % LOG_EVERY === 0) {
            console.log(`[${timestamp()}] [CSV] Escritos: ${totalEscritos} | Tópico actual: ${topic}`);
          }
        } catch (parseError) {
          console.error(`[${timestamp()}] Error al parsear mensaje (offset ${message.offset}):`, parseError.message);
        }
      }
    });

  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer CSV:`, error.message);
    stream.end();
    await consumer.disconnect();
    process.exit(1);
  }
}

runConsumerCsv();