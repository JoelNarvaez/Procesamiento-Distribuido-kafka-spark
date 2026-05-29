const { runProducer } = require("../shared/producer_runner");

runProducer({
  clientId: "producer-seguridad",
  topic: "logs_seguridad",
  tipoEvento: "security"
});