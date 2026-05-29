const { runProducer } = require("../shared/producer_runner");

runProducer({
  clientId: "producer-errores",
  topic: "logs_errores",
  tipoEvento: "error"
});