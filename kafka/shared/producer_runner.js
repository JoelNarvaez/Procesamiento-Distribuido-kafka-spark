const { createKafkaClient } = require("./kafka_client");
const { generarEvento } = require("./event_generator");
require("dotenv").config();

const TOTAL_MESSAGES = Number(process.env.TOTAL_MESSAGES || 1000);
const PRODUCER_DELAY_MS = Number(process.env.PRODUCER_DELAY_MS || 10);

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runProducer({ clientId, topic, tipoEvento }) {
  const kafka = createKafkaClient(clientId);
  const producer = kafka.producer();

  let mensajesEnviados = 0;

  try {
    await producer.connect();

    console.log("=========================================");
    console.log(` Producer iniciado: ${clientId}`);
    console.log(` Tópico destino: ${topic}`);
    console.log(` Tipo de evento: ${tipoEvento}`);
    console.log(` Total de mensajes: ${TOTAL_MESSAGES}`);
    console.log("=========================================");

    for (let i = 1; i <= TOTAL_MESSAGES; i++) {
      const evento = generarEvento(tipoEvento);

      await producer.send({
        topic,
        messages: [
          {
            key: evento.servidor,
            value: JSON.stringify(evento)
          }
        ]
      });

      mensajesEnviados++;

      if (i % 1000 === 0 || i === TOTAL_MESSAGES) {
        console.log(`Mensajes enviados por ${clientId}: ${i}/${TOTAL_MESSAGES}`);
      }

      if (PRODUCER_DELAY_MS > 0) {
        await delay(PRODUCER_DELAY_MS);
      }
    }

    console.log("=========================================");
    console.log(` Producer finalizado: ${clientId}`);
    console.log(` Mensajes enviados: ${mensajesEnviados}`);
    console.log("=========================================");
  } catch (error) {
    console.error(`Error en ${clientId}:`, error.message);
    process.exitCode = 1;
  } finally {
    await producer.disconnect();
  }
}

module.exports = {
  runProducer
};