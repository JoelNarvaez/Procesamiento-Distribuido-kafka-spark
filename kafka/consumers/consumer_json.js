/**
 * consumer_json.js
 * Consume los 5 tópicos de Kafka y guarda cada mensaje
 * como una línea JSON en data/raw/eventos.jsonl
 */

const { createKafkaClient } = require("../shared/kafka_client");
const fs = require("fs");
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

const GROUP_ID    = process.env.CONSUMER_GROUP_JSON || "consumer-group-json";
const OUTPUT_DIR  = path.resolve(__dirname, "../../data/raw");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "eventos.jsonl");
const LOG_EVERY   = Number(process.env.LOG_EVERY || 1000);

// ── Helpers ──────────────────────────────────────────────────────────────────
function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function timestamp() {
  return new Date().toISOString();
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function runConsumerJson() {
  ensureDir(OUTPUT_DIR);

  const kafka    = createKafkaClient("consumer-json");
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  const stream   = fs.createWriteStream(OUTPUT_FILE, { flags: "a" });

  let totalRecibidos = 0;
  let totalEscritos  = 0;

  // Cierre limpio ante señales del SO
  const shutdown = async (signal) => {
    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer JSON...`);
    stream.end();
    await consumer.disconnect();
    console.log(`[${timestamp()}] Consumer JSON detenido. Total escritos: ${totalEscritos}`);
    process.exit(0);
  };

  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await consumer.connect();

    // Suscribirse a todos los tópicos desde el inicio
    await consumer.subscribe({ topics: TOPICS, fromBeginning: true });

    console.log("==========================================");
    console.log(" Consumer JSON iniciado");
    console.log(` Grupo        : ${GROUP_ID}`);
    console.log(` Tópicos      : ${TOPICS.join(", ")}`);
    console.log(` Salida       : ${OUTPUT_FILE}`);
    console.log("==========================================\n");

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        totalRecibidos++;

        try {
          const raw    = message.value?.toString();
          if (!raw) return;

          const evento = JSON.parse(raw);

          // Enriquecer con metadatos de Kafka para trazabilidad
          const registro = {
            ...evento,
            _kafka_topic     : topic,
            _kafka_partition : partition,
            _kafka_offset    : message.offset,
            _kafka_timestamp : message.timestamp,
            _consumer_ts     : timestamp()
          };

          // Escribir una línea JSON por evento
          stream.write(JSON.stringify(registro) + "\n");
          totalEscritos++;

          if (totalEscritos % LOG_EVERY === 0) {
            console.log(`[${timestamp()}] [JSON] Escritos: ${totalEscritos} | Tópico actual: ${topic}`);
          }
        } catch (parseError) {
          console.error(`[${timestamp()}] Error al parsear mensaje (offset ${message.offset}):`, parseError.message);
        }
      }
    });

  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer JSON:`, error.message);
    stream.end();
    await consumer.disconnect();
    process.exit(1);
  }
}

runConsumerJson();