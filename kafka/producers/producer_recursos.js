const { runProducer } = require("../shared/producer_runner");

runProducer({
  clientId: "producer-recursos",
  topic: "metricas_recursos",
  tipoEvento: "resource"
});