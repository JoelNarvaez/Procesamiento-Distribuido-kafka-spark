/**
 * consumer_csv.js
 * Consume los 5 tópicos de Kafka y guarda cada mensaje
 * como una fila CSV en data/raw/eventos.csv
 */

const { createKafkaClient } = require("../shared/kafka_client");
const { FIELDS } = require("../shared/fields");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const TOPICS = [
  "metricas_recursos",
  "logs_http",
  "logs_errores",
  "metricas_red",
  "logs_seguridad"
];

const KAFKA_META_FIELDS = [
  "_kafka_topic",
  "_kafka_partition",
  "_kafka_offset",
  "_kafka_timestamp",
  "_consumer_ts"
];

const ALL_FIELDS = [...FIELDS, ...KAFKA_META_FIELDS];

const GROUP_ID = process.env.CONSUMER_GROUP_CSV || "consumer-group-csv";
const OUTPUT_DIR = path.resolve(__dirname, "../../data/raw");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "eventos.csv");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function timestamp() {
  return new Date().toISOString();
}

function csvEscape(value) {
  if (value === null || value === undefined) return "";

  const str = String(value);

  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }

  return str;
}

function objetoALineaCsv(obj) {
  return ALL_FIELDS.map((field) => csvEscape(obj[field])).join(",");
}

async function runConsumerCsv() {
  ensureDir(OUTPUT_DIR);

  const kafka = createKafkaClient("consumer-csv");

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000
  });

  const fileExists = fs.existsSync(OUTPUT_FILE);
  const stream = fs.createWriteStream(OUTPUT_FILE, { flags: "a" });

  if (!fileExists) {
    stream.write(ALL_FIELDS.join(",") + "\n");
  }

  let totalEscritos = 0;
  let cerrando = false;

  const shutdown = async (signal) => {
    if (cerrando) return;
    cerrando = true;

    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer CSV...`);

    try {
      await consumer.stop();
      await consumer.disconnect();

      stream.end(() => {
        console.log("==========================================");
        console.log(`[${timestamp()}] Consumer CSV detenido.`);
        console.log(` TOTAL DE EVENTOS CONSUMIDOS (CSV): ${totalEscritos}`);
        console.log("==========================================");
        process.exit(0);
      });
    } catch (error) {
      console.error(`[${timestamp()}] Error al cerrar consumer CSV:`, error.message);
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
    console.log(" Consumer CSV iniciado");
    console.log(` Grupo    : ${GROUP_ID}`);
    console.log(` Tópicos  : ${TOPICS.join(", ")}`);
    console.log(` Salida   : ${OUTPUT_FILE}`);
    console.log(` Columnas : ${ALL_FIELDS.length}`);
    console.log("==========================================\n");

    await consumer.run({
      autoCommit: true,

      eachMessage: async ({ topic, partition, message }) => {
        try {
          const raw = message.value?.toString();
          if (!raw) return;

          const evento = JSON.parse(raw);

          const registro = {
            ...evento,
            _kafka_topic: topic,
            _kafka_partition: partition,
            _kafka_offset: message.offset,
            _kafka_timestamp: message.timestamp,
            _consumer_ts: timestamp()
          };

          const ok = stream.write(objetoALineaCsv(registro) + "\n");

          if (!ok) {
            await new Promise((resolve) => stream.once("drain", resolve));
          }

          totalEscritos++;

          if (totalEscritos % 1000 === 0) {
            console.log(`[${timestamp()}] Eventos consumidos (CSV): ${totalEscritos}`);
          }
        } catch (error) {
          console.error(`[${timestamp()}] Error procesando mensaje:`, error.message);
        }
      }
    });
  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer CSV:`, error.message);

    try {
      await consumer.disconnect();
    } catch (_) {}

    stream.end();
    process.exit(1);
  }
}

runConsumerCsv();