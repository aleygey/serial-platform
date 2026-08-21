import { describe, expect, it, vi } from 'vitest'
import { GracefulQuitGate } from './graceful-quit'

describe('graceful quit gate', () => {
  it('waits once, avoids recursive shutdown, then permits the final exit', async () => {
    let finish!: () => void
    const shutdown = vi.fn(() => new Promise<void>((resolve) => { finish = resolve }))
    const exit = vi.fn()
    const gate = new GracefulQuitGate()

    expect(gate.intercept(shutdown, exit)).toBe(true)
    expect(gate.intercept(shutdown, exit)).toBe(true)
    expect(shutdown).toHaveBeenCalledTimes(1)
    finish()
    await Promise.resolve()
    expect(exit).toHaveBeenCalledTimes(1)
    expect(gate.intercept(shutdown, exit)).toBe(false)
  })
})
