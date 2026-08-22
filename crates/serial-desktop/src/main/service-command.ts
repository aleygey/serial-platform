export function buildSpawnArgs(prefix: string[], endpoint: string): string[] {
  return [...prefix, '--bind', new URL(endpoint).host]
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
