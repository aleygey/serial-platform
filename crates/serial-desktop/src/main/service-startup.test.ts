import { describe, expect, it, vi } from 'vitest'
import { startAndVerifyService } from './service-startup'

describe('owned service startup', () => {
  it('stops the child when health verification fails', async () => {
    const service = { start: vi.fn(async () => undefined), markReady: vi.fn(), stop: vi.fn(async () => undefined) }
    await expect(startAndVerifyService(service, 'http://127.0.0.1:3210', async () => {
      throw new Error('health timeout')
    })).rejects.toThrow('health timeout')
    expect(service.stop).toHaveBeenCalledOnce()
    expect(service.markReady).not.toHaveBeenCalled()
  })
})
