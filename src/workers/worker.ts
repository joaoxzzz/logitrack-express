import "dotenv/config";
import { Kafka } from "kafkajs";
import { Pool } from "pg";
import { LamportClock } from "../services/lamport-clock";
import { TruckArrivalEvent } from "../shared/types";

const workerId = process.env.WORKER_ID || "worker-local";
const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL não foi definida no arquivo .env.");
}

const pool = new Pool({
  connectionString: databaseUrl
});

const kafka = new Kafka({
  clientId: workerId,
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",")
});

const consumer = kafka.consumer({
  groupId: "logitrack-workers"
});

const clock = new LamportClock();

async function processTruckArrival(
  event: TruckArrivalEvent,
  logicalClock: number
): Promise<void> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const duplicateEvent = await client.query(
      "SELECT id FROM audit_events WHERE event_id = $1",
      [event.eventId]
    );

    if (duplicateEvent.rowCount && duplicateEvent.rowCount > 0) {
      await client.query("ROLLBACK");
      console.log(`[${workerId}] Evento ${event.eventId} já foi processado.`);
      return;
    }

    const truckResult = await client.query(
      `INSERT INTO trucks (plate, carrier, status)
       VALUES ($1, $2, $3)
       ON CONFLICT (plate)
       DO UPDATE SET carrier = EXCLUDED.carrier
       RETURNING id`,
      [event.plate, event.carrier || "Não informada", "ARRIVED"]
    );

    const dockResult = await client.query(
      "SELECT id, status FROM docks WHERE code = $1 FOR UPDATE",
      [event.requestedDock]
    );

    if (dockResult.rowCount === 0) {
      throw new Error(`Doca ${event.requestedDock} não encontrada.`);
    }

    const dock = dockResult.rows[0];

    if (dock.status !== "AVAILABLE") {
      throw new Error(
        `Doca ${event.requestedDock} não está disponível. Status: ${dock.status}`
      );
    }

    await client.query(
      `INSERT INTO dock_schedules (truck_id, dock_id, status, logical_clock)
       VALUES ($1, $2, $3, $4)`,
      [truckResult.rows[0].id, dock.id, "SCHEDULED", logicalClock]
    );

    await client.query(
      "UPDATE docks SET status = $1 WHERE id = $2",
      ["OCCUPIED", dock.id]
    );

    await client.query(
      `INSERT INTO audit_events
       (event_id, worker_id, event_type, logical_clock, payload)
       VALUES ($1, $2, $3, $4, $5::jsonb)`,
      [
        event.eventId,
        workerId,
        "TRUCK_ARRIVAL_PROCESSED",
        logicalClock,
        JSON.stringify(event)
      ]
    );

    await client.query("COMMIT");

    console.log(
      `[${workerId}] Caminhão ${event.plate} agendado na ${event.requestedDock}.`
    );
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function startWorker(): Promise<void> {
  await pool.query("SELECT 1");
  console.log(`[${workerId}] Conectado ao PostgreSQL.`);

  await consumer.connect();

  await consumer.subscribe({
    topic: "truck-arrivals",
    fromBeginning: false
  });

  console.log(`[${workerId}] Conectado ao Kafka.`);
  console.log(`[${workerId}] Aguardando eventos de chegada...`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const event = JSON.parse(
        message.value.toString()
      ) as TruckArrivalEvent;

      const updatedClock = clock.update(event.lamportClock);

      console.log("\n--------------------------------------");
      console.log(`[${workerId}] Evento recebido: ${event.eventId}`);
      console.log(`[${workerId}] Caminhão: ${event.plate}`);
      console.log(`[${workerId}] Doca solicitada: ${event.requestedDock}`);
      console.log(
        `[${workerId}] Lamport recebido: ${event.lamportClock}`
      );
      console.log(
        `[${workerId}] Lamport atualizado: ${updatedClock}`
      );

      await processTruckArrival(event, updatedClock);

      console.log("--------------------------------------");
    }
  });
}

startWorker().catch(async (error) => {
  console.error(`[${workerId}] Erro fatal:`, error);
  await pool.end();
  process.exit(1);
});