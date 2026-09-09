import { LamportClock } from "./lamport-clock";

const worker1Clock = new LamportClock();
const worker2Clock = new LamportClock();

console.log("=== TESTE DO RELÓGIO DE LAMPORT ===");

console.log("\nWorker 1 realiza um evento local:");
const sentClock = worker1Clock.tick();
console.log(`Worker 1: ${sentClock}`);

console.log("\nWorker 1 envia uma mensagem ao Worker 2:");
console.log(`Carimbo enviado: ${sentClock}`);

console.log("\nWorker 2 recebe a mensagem:");
const receivedClock = worker2Clock.update(sentClock);
console.log(`Worker 2: ${receivedClock}`);

console.log("\nWorker 2 realiza outro evento local:");
const finalClock = worker2Clock.tick();
console.log(`Worker 2: ${finalClock}`);

console.log("\nResultado final:");
console.log(`Relógio do Worker 1: ${worker1Clock.getValue()}`);
console.log(`Relógio do Worker 2: ${worker2Clock.getValue()}`);