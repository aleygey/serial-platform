import { setTimeout as delay } from 'node:timers/promises'
import type { DiscoveredSeriald } from './service-command'

export interface StartingService {
  start(endpoint: string): Promise<void>
  markReady(): void
  stop(): Promise<void>
  startupFailure?(): Error | undefined
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
  attempts = 40,
  pause: () => Promise<unknown> = () => delay(150)
): Promise<DiscoveredSeriald> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    throwStartupFailure(service)
    const discovered = await discover()
    throwStartupFailure(service)
    const childPid = service.startingPid?.()
    if (
      discovered
      && childPid !== undefined
      && discovered.pid === childPid
      && await verifyHealth(discovered)
    ) {
      throwStartupFailure(service)
      return discovered
    }
    await pause()
  }
  throw new Error('本地后端启动后未能发布匹配当前 App 子进程的服务身份')
}

export async function recoverConcurrentWinner<T>(
  startupError: unknown,
  discover: () => Promise<T | undefined>,
  attempts = 30,
  pause: () => Promise<unknown> = () => delay(100)
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
    await pause()
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
