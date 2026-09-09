export interface TruckArrivalEvent {
  eventId: string;
  eventType: "TRUCK_ARRIVAL_REQUESTED";
  truckId: string;
  plate: string;
  carrier: string;
  latitude: number;
  longitude: number;
  requestedDock: string;
  lamportClock: number;
  traceId: string;
  createdAt: string;
}