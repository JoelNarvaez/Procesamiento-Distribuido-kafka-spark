const { runProducer } = require("../shared/producer_runner");

runProducer({
  clientId: "producer-red",
  topic: "metricas_red",
  tipoEvento: "network"
});