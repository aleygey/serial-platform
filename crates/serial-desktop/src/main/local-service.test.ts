import { describe, expect, it } from 'vitest'
import { parseDiscoveredEndpoint } from './local-service'
import { buildDiscoverArgs, buildSpawnArgs, canTryLocalStartup, selectLocalEndpoint } from './service-command'

describe('local service command', () => {
  const discovered = {
    endpoint: 'http://127.0.0.1:3210',
    serverId: '11111111-1111-4111-8111-111111111111',
    daemonEpoch: '22222222-2222-4222-8222-222222222222',
    protocolVersion: 9,
    pid: 42
  }

  it('starts both serial and seriald as an EOF-managed service', () => {
    expect(buildSpawnArgs(['serve', '--managed'], 'http://127.0.0.1:3210')).toEqual([
      'serve', '--managed', '--bind', '127.0.0.1:3210'
    ])
  })

  it('handles localhost and this machine’s saved LAN endpoint while the local service is recovering', () => {
    expect(buildSpawnArgs(['serve'], 'http://localhost:3210')).toEqual(['serve', '--bind', '127.0.0.1:3210'])
    const timeout = new DOMException('health timed out', 'TimeoutError')
    const interfaces = { lan: [{ address: '192.168.10.24', netmask: '255.255.255.0', family: 'IPv4' as const, mac: '', internal: false, cidr: '192.168.10.24/24' }] }
    expect(canTryLocalStartup('http://192.168.10.24:3210', timeout, interfaces)).toBe(true)
    expect(canTryLocalStartup('http://192.168.10.25:3210', timeout, interfaces)).toBe(false)
    expect(canTryLocalStartup('http://localhost:3210', new Error('wrong protocol'), interfaces)).toBe(false)
  })

  it('uses one discovery command for either packaged daemon launcher', () => {
    expect(buildDiscoverArgs()).toEqual(['discover', '--json'])
    expect(parseDiscoveredEndpoint('')).toBeUndefined()
    expect(parseDiscoveredEndpoint(JSON.stringify({
      schema_version: 1,
      endpoint: 'http://127.0.0.1:4321',
      address: '127.0.0.1:4321',
      server_id: discovered.serverId,
      daemon_epoch: discovered.daemonEpoch,
      protocol_version: 9,
      pid: 42
    }))).toEqual({ ...discovered, endpoint: 'http://127.0.0.1:4321' })
    expect(() => parseDiscoveredEndpoint(JSON.stringify({
      schema_version: 1,
      endpoint: 'http://127.0.0.1:4321',
      address: '127.0.0.1:4321',
      server_id: discovered.serverId,
      daemon_epoch: discovered.daemonEpoch,
      protocol_version: 5,
      pid: 42
    }))).toThrow('不兼容')
  })

  it('reuses a serial-first endpoint instead of the different App preference', async () => {
    const reachable = async (endpoint: string): Promise<boolean> => endpoint === 'http://127.0.0.1:3210'
    const selected = await selectLocalEndpoint(
      'http://127.0.0.1:4321',
      reachable,
      async () => discovered
    )
    expect(selected).toEqual({ endpoint: 'http://127.0.0.1:3210', discovered: true })
  })

  it('keeps an App-first custom endpoint without running discovery', async () => {
    let discoveries = 0
    const selected = await selectLocalEndpoint(
      'http://127.0.0.1:4321',
      async () => true,
      async () => { discoveries += 1; return discovered }
    )
    expect(selected).toEqual({ endpoint: 'http://127.0.0.1:4321', discovered: false })
    expect(discoveries).toBe(0)
  })

  it('passes the complete discovery identity to endpoint health verification', async () => {
    let expectedIdentity: typeof discovered | undefined
    const selected = await selectLocalEndpoint(
      'http://127.0.0.1:4321',
      async (_endpoint, expected) => {
        expectedIdentity = expected
        return expected !== undefined
      },
      async () => discovered
    )
    expect(expectedIdentity).toEqual(discovered)
    expect(selected).toEqual({ endpoint: discovered.endpoint, discovered: true })
  })
})
