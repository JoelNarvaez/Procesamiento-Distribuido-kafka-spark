const { Kafka } = require("kafkajs");
require("dotenv").config();

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",").map((broker) => broker.trim())
  : [
      "192.168.1.65:9092",
      "192.168.1.66:9092",
      "192.168.1.67:9092"
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