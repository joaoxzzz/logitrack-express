import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { publishTruckArrival } from "./kafka-producer";
import { v4 as uuidv4 } from "uuid";
import { TruckArrivalEvent } from "../shared/types";

const protoPath = path.join(
  __dirname,
  "../../proto/logitrack.proto"
);

const packageDefinition = protoLoader.loadSync(protoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const loadedProto = grpc.loadPackageDefinition(
  packageDefinition
) as any;

const logitrack = loadedProto.logitrack;

let gatewayLamportClock = 0;

async function registerTruckArrival(
  call: grpc.ServerUnaryCall<any, any>,
  callback: grpc.sendUnaryData<any>
): Promise<void> {
  try {
    gatewayLamportClock += 1;

    const request = call.request;

    const event: TruckArrivalEvent = {
      eventId: uuidv4(),
      eventType: "TRUCK_ARRIVAL_REQUESTED",
      truckId: request.truck_id,
      plate: request.plate,
      carrier: request.carrier,
      latitude: request.latitude,
      longitude: request.longitude,
      requestedDock: request.requested_dock,
      lamportClock: gatewayLamportClock,
      traceId: uuidv4(),
      createdAt: new Date().toISOString()
    };

    await publishTruckArrival(event);

    callback(null, {
      event_id: event.eventId,
      status: "RECEIVED",
      message: "Solicitação recebida e publicada no Kafka."
    });
  } catch (error) {
    console.error("Erro ao publicar no Kafka:", error);

    callback({
      code: grpc.status.INTERNAL,
      message: "Não foi possível publicar a solicitação."
    });
  }
}

const server = new grpc.Server();

server.addService(logitrack.LogiTrackGateway.service, {
  RegisterTruckArrival: registerTruckArrival
});

server.bindAsync(
  "0.0.0.0:50051",
  grpc.ServerCredentials.createInsecure(),
  (error, port) => {
    if (error) {
      console.error("Erro ao iniciar o Gateway:", error);
      return;
    }

    console.log(`Gateway gRPC iniciado na porta ${port}.`);
  }
);