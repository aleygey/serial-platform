import { afterEach, describe, expect, it, vi } from 'vitest'
import { stopManagedChild, type ManagedChild } from './managed-child'

describe('managed child shutdown', () => {
  afterEach(() => vi.useRealTimers())

  it('closes stdin first and only kills after the grace period', async () => {
    vi.useFakeTimers()
    const calls: string[] = []
    const child: ManagedChild = {
      stdin: { end: () => calls.push('stdin.end') },
      exitCode: null,
      once: vi.fn(),
      kill: () => { calls.push('kill'); return true }
    }

    const stopping = stopManagedChild(child, 3_000)
    expect(calls).toEqual(['stdin.end'])
    await vi.advanceTimersByTimeAsync(2_999)
    expect(calls).toEqual(['stdin.end'])
    await vi.advanceTimersByTimeAsync(1)
    await stopping
    expect(calls).toEqual(['stdin.end', 'kill'])
  })
})
