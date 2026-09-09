import { Kafka } from "kafkajs";
import { TruckArrivalEvent } from "../shared/types";

const kafka = new Kafka({
  clientId: "logitrack-gateway",
  brokers: ["localhost:9092"]
});

const producer = kafka.producer();

let connected = false;

export async function publishTruckArrival(
  event: TruckArrivalEvent
): Promise<void> {
  if (!connected) {
    await producer.connect();
    connected = true;
    console.log("Gateway conectado ao Kafka.");
  }

  await producer.send({
    topic: "truck-arrivals",
    messages: [
      {
        key: event.truckId,
        value: JSON.stringify(event)
      }
    ]
  });

  console.log(
    `[GATEWAY] Evento ${event.eventId} publicado no Kafka.`
  );
}