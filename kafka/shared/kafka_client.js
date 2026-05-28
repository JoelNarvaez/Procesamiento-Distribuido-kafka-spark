const { Kafka } = require("kafkajs");
require("dotenv").config();

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(",")
  : [
      "192.168.10.101:9092",
      "192.168.10.102:9092",
      "192.168.10.103:9092"
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