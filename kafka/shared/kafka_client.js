const { Kafka } = require("kafkajs");
require("dotenv").config();

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : [
      "192.168.1.65:9092",
      "192.168.1.66:9092",
      "192.168.1.67:9092"
    ];

function createKafkaClient(clientId) {
  return new Kafka({
    clientId,
    brokers,
    retry: {
      initialRetryTime: 300,
      retries: 8
    }
  });
}

module.exports = {
  createKafkaClient,
  brokers
};