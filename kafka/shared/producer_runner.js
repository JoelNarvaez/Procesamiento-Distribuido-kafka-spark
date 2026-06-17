const { createKafkaClient } = require("./kafka_client");
const { generarEvento } = require("./event_generator");
require("dotenv").config();

const PRODUCER_DELAY_MS = Number(process.env.PRODUCER_DELAY_MS || 10);

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runProducer({ clientId, topic, tipoEvento }) {
  const kafka = createKafkaClient(clientId);
  const producer = kafka.producer();

  let mensajesEnviados = 0;
  let corriendo = true;

  const detener = (sig) => {
    if (!corriendo) return;
    console.log(`\n[${clientId}] Señal ${sig} recibida. Deteniendo productor...`);
    corriendo = false;
  };
  process.on("SIGINT", () => detener("SIGINT"));
  process.on("SIGTERM", () => detener("SIGTERM"));

  try {
    await producer.connect();

    console.log("=========================================");
    console.log(` Producer iniciado: ${clientId}`);
    console.log(` Tópico destino: ${topic}`);
    console.log(` Tipo de evento: ${tipoEvento}`);
    console.log(" Modo: generación continua");
    console.log("=========================================");

    while (corriendo) {
      const evento = generarEvento(tipoEvento);

      await producer.send({
        topic,
        acks: -1,
        timeout: 30000,
        messages: [
          {
            key: evento.servidor,
            value: JSON.stringify(evento)
          }
        ]
      });

      mensajesEnviados++;

      if (mensajesEnviados % 1000 === 0) {
        console.log(`Mensajes enviados por ${clientId}: ${mensajesEnviados}`);
      }

      if (PRODUCER_DELAY_MS > 0) {
        await delay(PRODUCER_DELAY_MS);
      }
    }
  } catch (error) {
    console.error(`Error en ${clientId}:`, error.message);
    process.exitCode = 1;
  } finally {
    await producer.disconnect();
    console.log("=========================================");
    console.log(` ${clientId} -> TOTAL DE EVENTOS PRODUCIDOS: ${mensajesEnviados}`);
    console.log("=========================================");
  }
}

module.exports = {
  runProducer
};