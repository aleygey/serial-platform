import type { InputObservation, PortSnapshot, TimelineEvent } from '../shared/contracts'

interface PendingInput {
  port: string
  epoch: string
  generation: number
  sentAt: number
  txSeq?: number
}

/** Match a local write to the first subsequent live RX, even if it precedes the ACK. */
export class InputTraceTracker {
  private readonly pending = new Map<string, PendingInput>()

  start(id: string, port: PortSnapshot, sentAt: number): void {
    if (this.pending.size >= 32) this.cancel(this.pending.keys().next().value!)
    this.pending.set(id, { port: port.config.port, epoch: port.daemon_epoch, generation: port.generation, sentAt })
  }
  cancel(id: string): void { this.pending.delete(id) }
  clear(port?: string): void {
    if (port === undefined) this.pending.clear()
    else for (const [id, trace] of this.pending) if (trace.port === port) this.cancel(id)
  }

  observe(event: TimelineEvent): InputObservation | undefined {
    if (!this.pending.size || event.replay) return
    const receivedAt = performance.timeOrigin + performance.now()
    const ids: string[] = []
    for (const [id, pending] of this.pending) {
      if (receivedAt - pending.sentAt > 30_000) { this.cancel(id); continue }
      if (pending.port !== event.port) continue
      if (event.daemon_epoch !== pending.epoch || event.generation !== pending.generation ||
          event.kind === 'gap' || event.kind === 'serial_closed') { this.cancel(id); continue }
      if (event.direction === 'tx' && event.operation_id === id) pending.txSeq = event.seq
      else if (event.direction === 'rx' && (event.text || event.rxBytes?.length) && pending.txSeq !== undefined && event.seq > pending.txSeq) {
        ids.push(id)
        this.cancel(id)
      }
    }
    return ids.length ? { ids, receivedAt } : undefined
  }
}
