import { networkInterfaces } from 'node:os'

export function buildSpawnArgs(prefix: string[], endpoint: string): string[] {
  const { hostname, port } = new URL(endpoint)
  return [...prefix, '--bind', (hostname === 'localhost' ? '127.0.0.1' : hostname) + ':' + (port || '80')]
}

export function canTryLocalStartup(endpoint: string, error: unknown, interfaces = networkInterfaces()): boolean {
  const cause = error instanceof TypeError ? error.cause as NodeJS.ErrnoException | undefined : undefined
  if (cause?.code !== 'ECONNREFUSED' && cause?.code !== 'ETIMEDOUT' && !(error instanceof Error && error.name === 'TimeoutError')) return false
  const { hostname, protocol } = new URL(endpoint)
  const address = hostname.replace(/^\[|\]$/g, '')
  return protocol === 'http:' && (address === 'localhost' || address === '::1' || /^127(?:\.\d+){3}$/.test(address) ||
    Object.values(interfaces).some(entries => entries?.some(entry => entry.address === address)))
}

export function buildDiscoverArgs(): string[] {
  return ['discover', '--json']
}

export interface LocalEndpointSelection {
  endpoint: string
  discovered: boolean
}

export interface DiscoveredSeriald {
  endpoint: string
  serverId: string
  daemonEpoch: string
  protocolVersion: number
  pid: number
}

export async function selectLocalEndpoint(
  preferred: string,
  reachable: (endpoint: string, expected?: DiscoveredSeriald) => Promise<boolean>,
  discover: () => Promise<DiscoveredSeriald | undefined>
): Promise<LocalEndpointSelection | undefined> {
  if (await reachable(preferred)) return { endpoint: preferred, discovered: false }
  const active = await discover()
  if (!active || !(await reachable(active.endpoint, active))) return undefined
  return { endpoint: active.endpoint, discovered: active.endpoint !== preferred }
}
