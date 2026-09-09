import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { randomUUID } from "crypto";

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

const client = new logitrack.LogiTrackGateway(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

client.RegisterTruckArrival(
  {
    truck_id: randomUUID(),
    plate: "ABC1D23",
    carrier: "LogiTrack Transportes",
    latitude: -20.8689,
    longitude: -41.1267,
    requested_dock: "DOCA-01"
  },
  (error: Error | null, response: any) => {
    if (error) {
      console.error("Erro ao chamar o Gateway:", error.message);
      return;
    }

    console.log("Resposta do Gateway:");
    console.log(response);
  }
);