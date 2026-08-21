export class GracefulQuitGate {
  private stopping = false
  private ready = false

  intercept(shutdown: () => Promise<void>, exit: () => void): boolean {
    if (this.ready) return false
    if (this.stopping) return true
    this.stopping = true
    void shutdown().finally(() => {
      this.ready = true
      exit()
    })
    return true
  }
}
