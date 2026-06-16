const { Kafka } = require("kafkajs");
require("dotenv").config();

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",").map((broker) => broker.trim())
  : [
      "100.124.245.95:9092",
      "100.126.190.35:9092",
      "100.87.252.100:9092"
    ];

function createKafkaClient(clientId) {
  return new Kafka({
    clientId,
    brokers,

    connectionTimeout: 10000,
    requestTimeout: 30000,

    retry: {
      initialRetryTime: 1000,
      retries: 20
    }
  });
}

module.exports = {
  createKafkaClient,
  brokers
};