import type { HumanCommandSubmission, InputObservation, TimelineEvent } from '../../shared/contracts'

export interface InputSample {
  id: string
  index: number
  kind: '空回车' | '命令'
  keyAt: number
  handledAt: number
  sentAt?: number
  acceptedAt?: number
  receivedAt?: number
  frameAt?: number
  promptAt?: number
  sharedRx?: boolean
  status: 'sending' | HumanCommandSubmission['status']
}

export class InputDiagnostics {
  private rows: InputSample[] = []
  private listeners = new Set<() => void>()
  private nextIndex = 0
  private frozen?: InputSample[]
  private observed?: { epoch: string; seq: number }
  private promptWatch?: { ids: string[]; epoch: string; generation: number; receivedAt: number; tail: string }
  private frames = new Set<number>()
  subscribe = (listener: () => void) => { this.listeners.add(listener); return () => { this.listeners.delete(listener) } }
  snapshot = () => this.frozen ?? this.rows
  get paused() { return this.frozen !== undefined }

  togglePause(): void {
    this.frozen = this.paused ? undefined : [...this.rows]
    this.notify()
  }
  begin(command: string, keyTime?: number): string | undefined {
    if (this.paused) return
    const now = performance.now()
    const id = crypto.randomUUID()
    this.rows = [{ id, index: ++this.nextIndex, kind: command ? '命令' : '空回车',
      keyAt: performance.timeOrigin + (keyTime !== undefined && keyTime > 0 && keyTime <= now ? keyTime : now),
      handledAt: performance.timeOrigin + now, status: 'sending' } satisfies InputSample, ...this.rows].slice(0, 8)
    this.notify()
    return id
  }
  finish(id: string, result: HumanCommandSubmission): void {
    this.update([id], { status: result.status, ...(result.status === 'accepted' ? result.timing : undefined) })
  }

  /** Called after the terminal DOM commit. Only inspect newly delivered port events. */
  committed(events: TimelineEvent[], prompts: (string | null | undefined)[], textFor: (key: string) => string | undefined): void {
    const last = events.at(-1)
    if (!last) return
    const previous = this.observed
    this.observed = { epoch: last.daemon_epoch, seq: last.seq }
    if (!this.rows.length) return
    let first = events.length
    while (first > 0) {
      const event = events[first - 1]
      if (event.daemon_epoch !== last.daemon_epoch || (previous?.epoch === event.daemon_epoch && event.seq <= previous.seq)) break
      first--
    }
    const observations: InputObservation[] = []
    const matched: string[] = []
    for (let i = first; i < events.length; i++) {
      const event = events[i]
      const existing = this.promptWatch
      if (existing && (event.replay || event.direction === 'tx' || event.kind === 'gap' || event.kind === 'serial_closed' ||
          existing.epoch !== event.daemon_epoch || existing.generation !== event.generation)) this.promptWatch = undefined
      if (event.replay || event.direction !== 'rx') continue
      if (event.inputObservation) {
        const { ids, receivedAt } = event.inputObservation
        observations.push(event.inputObservation)
        this.promptWatch = prompts.some(Boolean) ? { ids, receivedAt, epoch: event.daemon_epoch, generation: event.generation, tail: '' } : undefined
      }
      const watch = this.promptWatch
      if (!watch) continue
      if (performance.timeOrigin + performance.now() - watch.receivedAt > 30_000) { this.promptWatch = undefined; continue }
      const text = watch.tail + (textFor(event.daemon_epoch + ':' + event.seq) ?? '')
      if (prompts.some(prompt => prompt && text.includes(prompt))) {
        matched.push(...watch.ids)
        this.promptWatch = undefined
      } else {
        const overlap = Math.max(0, ...prompts.map(prompt => (prompt?.length ?? 0) - 1))
        watch.tail = overlap ? text.slice(-overlap) : ''
      }
    }
    if (!observations.length && !matched.length) return
    const frame = requestAnimationFrame(() => {
      this.frames.delete(frame)
      const frameAt = performance.timeOrigin + performance.now()
      for (const { ids, receivedAt } of observations) this.update(ids, { receivedAt, frameAt, sharedRx: ids.length > 1 })
      if (matched.length) this.update(matched, { promptAt: frameAt })
    })
    this.frames.add(frame)
  }

  dispose(): void {
    for (const frame of this.frames) cancelAnimationFrame(frame)
    this.frames.clear()
  }
  private update(ids: string[], fields: Partial<InputSample>): void {
    if (!this.rows.some(row => ids.includes(row.id))) return
    this.rows = this.rows.map(row => ids.includes(row.id) ? { ...row, ...fields } : row)
    if (!this.paused) this.notify()
  }
  private notify(): void { this.listeners.forEach(listener => listener()) }
}
