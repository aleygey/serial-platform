import { afterEach, describe, expect, it, vi } from 'vitest'
import { ReconnectLoop } from './reconnect-loop'

describe('persistent reconnect loop', () => {
  afterEach(() => vi.useRealTimers())

  it('keeps one bounded retry timer until an attempt succeeds', async () => {
    vi.useFakeTimers()
    const attempt = vi.fn()
      .mockRejectedValueOnce(new Error('offline-1'))
      .mockRejectedValueOnce(new Error('offline-2'))
      .mockResolvedValue(undefined)
    const failures = vi.fn()
    const loop = new ReconnectLoop(attempt, failures, { minDelayMs: 100, maxDelayMs: 250 })

    loop.schedule()
    loop.schedule()
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(100)
    expect(attempt).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(200)
    expect(attempt).toHaveBeenCalledTimes(2)
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(249)
    expect(attempt).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(1)
    expect(attempt).toHaveBeenCalledTimes(3)
    expect(failures).toHaveBeenCalledTimes(2)
    expect(vi.getTimerCount()).toBe(0)

    loop.schedule()
    expect(vi.getTimerCount()).toBe(1)
    loop.cancel()
    expect(vi.getTimerCount()).toBe(0)
  })
})
