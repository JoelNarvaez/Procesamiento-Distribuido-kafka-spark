const { runProducer } = require("../shared/producer_runner");

runProducer({
  clientId: "producer-http",
  topic: "logs_http",
  tipoEvento: "request"
});