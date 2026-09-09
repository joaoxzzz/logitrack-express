export class LamportClock {
  private value: number;

  constructor(initialValue = 0) {
    this.value = initialValue;
  }

  public tick(): number {
    this.value += 1;
    return this.value;
  }

  public update(receivedClock: number): number {
    this.value = Math.max(this.value, receivedClock) + 1;
    return this.value;
  }

  public getValue(): number {
    return this.value;
  }
}