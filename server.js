const express = require("express");
const { Kafka } = require("kafkajs");
const WebSocket = require("ws");
const cors = require("cors");

const app = express();

app.use(express.json());
app.use(cors());

const server = app.listen(3000, () => {
  console.log("Server running on http://localhost:3000");
});

const kafka = new Kafka({
  clientId: "web-gateway",
  brokers: ["localhost:9092"]
});

const producer = kafka.producer();

(async () => {
  await producer.connect();
  console.log("Kafka producer connected");
})();

app.post("/publish", async (req, res) => {
  const event = {
    ip: req.ip,
    type: "BUTTON_CLICK",
    timestamp: Date.now(),
  };

  await producer.send({
    topic: "button-events",
    messages: [{ value: JSON.stringify(event) }],
  });

  res.json({ status: "event published" });
});

