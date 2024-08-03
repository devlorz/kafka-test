const { Kafka } = require("kafkajs");
const { Order } = require("./schema");

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092", "localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "message-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "message-order", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);
