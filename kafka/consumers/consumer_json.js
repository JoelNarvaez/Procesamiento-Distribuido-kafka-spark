/**
 * consumer_json.js
 * Consume los 5 tópicos de Kafka y guarda cada mensaje
 * como una línea JSON en data/raw/eventos.jsonl
 */

const { createKafkaClient } = require("../shared/kafka_client");
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

const GROUP_ID = process.env.CONSUMER_GROUP_JSON || "consumer-group-json";
const OUTPUT_DIR = path.resolve(__dirname, "../../data/raw");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "eventos.jsonl");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function timestamp() {
  return new Date().toISOString();
}

async function runConsumerJson() {
  ensureDir(OUTPUT_DIR);

  const kafka = createKafkaClient("consumer-json");

  const consumer = kafka.consumer({
    groupId: GROUP_ID,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000
  });

  const stream = fs.createWriteStream(OUTPUT_FILE, { flags: "a" });

  let totalEscritos = 0;
  let cerrando = false;

  const shutdown = async (signal) => {
    if (cerrando) return;
    cerrando = true;

    console.log(`\n[${timestamp()}] Señal ${signal} recibida. Cerrando consumer JSON...`);

    try {
      await consumer.stop();
      await consumer.disconnect();

      stream.end(() => {
        console.log("==========================================");
        console.log(`[${timestamp()}] Consumer JSON detenido.`);
        console.log(` TOTAL DE EVENTOS CONSUMIDOS (JSON): ${totalEscritos}`);
        console.log("==========================================");
        process.exit(0);
      });
    } catch (error) {
      console.error(`[${timestamp()}] Error al cerrar consumer JSON:`, error.message);
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
    console.log(" Consumer JSON iniciado");
    console.log(` Grupo   : ${GROUP_ID}`);
    console.log(` Tópicos : ${TOPICS.join(", ")}`);
    console.log(` Salida  : ${OUTPUT_FILE}`);
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

          const ok = stream.write(JSON.stringify(registro) + "\n");

          if (!ok) {
            await new Promise((resolve) => stream.once("drain", resolve));
          }

          totalEscritos++;

          if (totalEscritos % 1000 === 0) {
            console.log(`[${timestamp()}] Eventos consumidos (JSON): ${totalEscritos}`);
          }
        } catch (error) {
          console.error(`[${timestamp()}] Error procesando mensaje:`, error.message);
        }
      }
    });
  } catch (error) {
    console.error(`[${timestamp()}] Error crítico en consumer JSON:`, error.message);

    try {
      await consumer.disconnect();
    } catch (_) {}

    stream.end();
    process.exit(1);
  }
}

runConsumerJson();