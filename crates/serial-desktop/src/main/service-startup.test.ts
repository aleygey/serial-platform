import { describe, expect, it, vi } from 'vitest'
import {
  recoverConcurrentWinner,
  startAndVerifyService,
  waitForOwnedServiceIdentity
} from './service-startup'

describe('owned service startup', () => {
  it('stops the child when health verification fails', async () => {
    const service = { start: vi.fn(async () => undefined), markReady: vi.fn(), stop: vi.fn(async () => undefined) }
    await expect(startAndVerifyService(service, 'http://127.0.0.1:3210', async () => {
      throw new Error('health timeout')
    })).rejects.toThrow('health timeout')
    expect(service.stop).toHaveBeenCalledOnce()
    expect(service.markReady).not.toHaveBeenCalled()
  })

  it('reports the child startup error instead of a generic health timeout', async () => {
    const service = {
      start: vi.fn(async () => undefined),
      markReady: vi.fn(),
      stop: vi.fn(async () => undefined),
      startupFailure: vi.fn(() => new Error('seriald config: unknown profile'))
    }
    await expect(startAndVerifyService(service, 'http://127.0.0.1:3210', async () => {
      throw new Error('health timeout')
    })).rejects.toThrow('seriald config: unknown profile')
    expect(service.stop).toHaveBeenCalledOnce()
    expect(service.markReady).not.toHaveBeenCalled()
  })

  it('rediscovers the winner when an App and another launcher race for one data root', async () => {
    const winner = { endpoint: 'http://127.0.0.1:3210' }
    const discover = vi.fn()
      .mockRejectedValueOnce(new Error('marker not published'))
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce(winner)
    const pause = vi.fn(async () => undefined)

    await expect(recoverConcurrentWinner(
      new Error('seriald data root is already owned by another process'),
      discover,
      4,
      pause
    )).resolves.toBe(winner)
    expect(discover).toHaveBeenCalledTimes(3)
    expect(pause).toHaveBeenCalledTimes(2)
  })

  it('does not hide an unrelated startup failure behind discovery retries', async () => {
    const error = new Error('seriald config is invalid')
    const discover = vi.fn(async () => ({ endpoint: 'http://127.0.0.1:3210' }))
    await expect(recoverConcurrentWinner(error, discover)).rejects.toBe(error)
    expect(discover).not.toHaveBeenCalled()
  })

  it('rejects a foreign healthy listener when the local child then loses the bind', async () => {
    const bindFailure = new Error('seriald 启动失败：Address already in use')
    let failure: Error | undefined
    let foreignHealthReachable = false
    const service = {
      start: vi.fn(async () => undefined),
      markReady: vi.fn(),
      stop: vi.fn(async () => undefined),
      startupFailure: vi.fn(() => failure),
      startingPid: vi.fn(() => 41)
    }
    const discover = vi.fn()
      .mockResolvedValueOnce(undefined)
      .mockImplementationOnce(async () => {
        failure = bindFailure
        return undefined
      })
    const verifyHealth = vi.fn(async () => foreignHealthReachable)
    const pause = vi.fn(async () => {
      foreignHealthReachable = true
    })

    await expect(startAndVerifyService(
      service,
      'http://127.0.0.1:3210',
      async () => {
        await waitForOwnedServiceIdentity(
          service,
          discover,
          verifyHealth,
          pause
        )
      }
    )).rejects.toBe(bindFailure)
    expect(verifyHealth).not.toHaveBeenCalled()
    expect(foreignHealthReachable).toBe(true)
    expect(service.markReady).not.toHaveBeenCalled()
    expect(service.stop).toHaveBeenCalledOnce()
  })

  it('waits for a live child recovering a large journal and gives the next stage its own deadline', async () => {
    const owned = { endpoint: 'http://127.0.0.1:3210', pid: 41, serverId: 'server', daemonEpoch: 'epoch', protocolVersion: 8 }
    let elapsed = 0
    const service = { start: vi.fn(), stop: vi.fn(), markReady: vi.fn(), startingPid: () => 41,
      startupStage: () => elapsed < 300_000 ? 'recovering serial journals' : 'starting HTTP service' }
    await expect(waitForOwnedServiceIdentity(service, async () => elapsed < 310_000 ? undefined : owned,
      async () => true, async ms => { elapsed += ms }, () => elapsed)).resolves.toBe(owned)
    expect(service.stop).not.toHaveBeenCalled()
  })

  it('stops waiting when the user closes the app during recovery', async () => {
    let pid: number | undefined = 41
    const service = { start: vi.fn(), stop: vi.fn(), markReady: vi.fn(), startingPid: () => pid,
      startupStage: () => 'recovering serial journals' }
    await expect(waitForOwnedServiceIdentity(service, async () => undefined, async () => false,
      async () => { pid = undefined })).rejects.toThrow('已取消')
  })

  it('accepts only the matching child marker and health identity', async () => {
    const service = {
      start: vi.fn(async () => undefined),
      markReady: vi.fn(),
      stop: vi.fn(async () => undefined),
      startupFailure: vi.fn(() => undefined),
      startingPid: vi.fn(() => 41)
    }
    const foreign = {
      endpoint: 'http://127.0.0.1:3210',
      serverId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      daemonEpoch: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      protocolVersion: 5,
      pid: 99
    }
    const owned = {
      ...foreign,
      serverId: 'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
      daemonEpoch: 'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
      pid: 41
    }
    const discover = vi.fn()
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce(foreign)
      .mockResolvedValueOnce(owned)
    const verifyHealth = vi.fn(async (candidate) => candidate === owned)

    await expect(startAndVerifyService(
      service,
      owned.endpoint,
      async () => {
        await waitForOwnedServiceIdentity(
          service,
          discover,
          verifyHealth,
          async () => undefined
        )
      }
    )).resolves.toBeUndefined()
    expect(verifyHealth).toHaveBeenCalledTimes(1)
    expect(verifyHealth).toHaveBeenCalledWith(owned)
    expect(service.markReady).toHaveBeenCalledOnce()
  })
})
