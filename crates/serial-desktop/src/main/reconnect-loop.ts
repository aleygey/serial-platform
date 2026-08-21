export interface ReconnectLoopOptions {
  minDelayMs?: number
  maxDelayMs?: number
}

export class ReconnectLoop {
  private timer?: ReturnType<typeof setTimeout>
  private generation = 0
  private runningGeneration?: number
  private failures = 0
  private readonly minDelayMs: number
  private readonly maxDelayMs: number

  constructor(
    private readonly attempt: () => Promise<void>,
    private readonly onFailure: (error: unknown) => void,
    options: ReconnectLoopOptions = {}
  ) {
    this.minDelayMs = options.minDelayMs ?? 1_500
    this.maxDelayMs = options.maxDelayMs ?? 10_000
  }

  schedule(): void {
    const generation = this.generation
    if (this.timer || this.runningGeneration === generation) return
    const delay = Math.min(this.maxDelayMs, this.minDelayMs * 2 ** Math.min(this.failures, 10))
    this.timer = setTimeout(() => {
      this.timer = undefined
      void this.run(generation)
    }, delay)
  }

  cancel(): void {
    this.generation += 1
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
    this.failures = 0
  }

  private async run(generation: number): Promise<void> {
    if (generation !== this.generation) return
    this.runningGeneration = generation
    let retry = false
    try {
      await this.attempt()
      if (generation === this.generation) this.failures = 0
    } catch (error) {
      if (generation === this.generation) {
        this.failures += 1
        retry = true
        this.onFailure(error)
      }
    } finally {
      if (this.runningGeneration === generation) this.runningGeneration = undefined
      if (retry && generation === this.generation) this.schedule()
    }
  }
}
