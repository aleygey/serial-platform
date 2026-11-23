import { setTimeout as delay } from 'node:timers/promises'
import type { DiscoveredSeriald } from './service-command'

export interface StartingService {
  start(endpoint: string): Promise<void>
  markReady(): void
  stop(): Promise<void>
  startupFailure?(): Error | undefined
  startupStage?(): string | undefined
  startingPid?(): number | undefined
}

export async function startAndVerifyService(
  service: StartingService,
  endpoint: string,
  waitUntilReachable: () => Promise<void>
): Promise<void> {
  await service.start(endpoint)
  try {
    await waitUntilReachable()
    const startupFailure = service.startupFailure?.()
    if (startupFailure) throw startupFailure
    service.markReady()
  } catch (error) {
    const startupFailure = service.startupFailure?.()
    await service.stop()
    throw startupFailure ?? error
  }
}

export async function waitForOwnedServiceIdentity(
  service: StartingService,
  discover: () => Promise<DiscoveredSeriald | undefined>,
  verifyHealth: (discovered: DiscoveredSeriald) => Promise<boolean>,
  pause: (ms: number) => Promise<unknown> = delay,
  now: () => number = Date.now
): Promise<DiscoveredSeriald> {
  const pid = service.startingPid?.()
  let stage: string | undefined
  let since = now()
  let interval = 150
  for (;;) {
    throwStartupFailure(service)
    if (!pid || service.startingPid?.() !== pid) throw new Error('本地后端启动已取消')
    const discovered = await discover()
    throwStartupFailure(service)
    if (discovered?.pid === pid && await verifyHealth(discovered)) {
      throwStartupFailure(service)
      if (service.startingPid?.() !== pid) throw new Error('本地后端启动已取消')
      return discovered
    }
    const current = service.startupStage?.()
    if (stage !== current) {
      stage = current
      since = now()
    }
    // Journal and automation recovery depend on the size of data on disk.
    // A dead child is detected above even if it exited while recovering.
    if (stage !== 'recovering serial journals' && stage !== 'loading automation state' && now() - since >= 90_000) {
      throw new Error('本地后端未能发布匹配当前 App 子进程的服务身份')
    }
    await pause(interval)
    interval = Math.min(1000, interval + 150)
  }
}

export async function recoverConcurrentWinner<T>(
  startupError: unknown,
  discover: () => Promise<T | undefined>,
  attempts = 120,
  pause: (ms: number) => Promise<unknown> = delay
): Promise<T> {
  if (!errorMessage(startupError).includes('already owned by another process')) {
    throw startupError
  }
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const winner = await discover()
      if (winner) return winner
    } catch {
      // The winner can own the root before its verified endpoint is published.
    }
    await pause(Math.min(1000, 150 * (attempt + 1)))
  }
  throw startupError
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function throwStartupFailure(service: StartingService): void {
  const failure = service.startupFailure?.()
  if (failure) throw failure
}
