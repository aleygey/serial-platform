import { afterEach, describe, expect, it, vi } from 'vitest'
import type { SerialConfigurationDraft, TransportProfile } from '../shared/contracts'
import {
  assertCompatibleProtocol,
  ConfigurationConflictError,
  configuredPortFromDraft,
  contentAddressedTransportProfile,
  parseSerialdHealth,
  SerialClient,
  serialdIdentityMatches,
  stageTransportCatalog
} from './serial-client'

afterEach(() => vi.unstubAllGlobals())

describe('component protocol gate', () => {
  it('accepts v6 and rejects an older backend before opening the live socket', () => {
    expect(() => assertCompatibleProtocol(6)).not.toThrow()
    expect(() => assertCompatibleProtocol(5)).toThrow(/App 需要 v6，后端提供 v5/)
  })

  it('requires an ok v6 health response with stable UUID identities', () => {
    const identity = parseSerialdHealth({
      status: 'ok',
      server_id: '11111111-1111-4111-8111-111111111111',
      daemon_epoch: '22222222-2222-4222-8222-222222222222',
      protocol_version: 6,
      uptime_ms: 10
    })
    expect(identity).toEqual({
      serverId: '11111111-1111-4111-8111-111111111111',
      daemonEpoch: '22222222-2222-4222-8222-222222222222',
      protocolVersion: 6
    })
    expect(serialdIdentityMatches(identity, identity)).toBe(true)
    expect(serialdIdentityMatches(identity, { ...identity, daemonEpoch: '33333333-3333-4333-8333-333333333333' }))
      .toBe(false)
    expect(() => parseSerialdHealth({ ...identity, status: 'ok', protocol_version: 6 }))
      .toThrow('服务身份')
    expect(() => parseSerialdHealth({
      status: 'ok', server_id: identity.serverId, daemon_epoch: identity.daemonEpoch,
      protocol_version: 5
    })).toThrow(/App 需要 v6/)
  })
})

const profile: TransportProfile = {
  name: 'human-name', baud_rate: 115200, data_bits: 'eight', parity: 'none',
  stop_bits: 'one', flow_control: 'none', dtr: false, rts: false, auto_open: true
}

describe('transport profile staging', () => {
  it('uses port plus content hash and never the mutable source name as identity', () => {
    const first = contentAddressedTransportProfile('COM 6', profile)
    const renamed = contentAddressedTransportProfile('COM 6', { ...profile, name: 'renamed' })
    const changed = contentAddressedTransportProfile('COM 6', { ...profile, baud_rate: 9600 })

    expect(first.name).toMatch(/^desktop-COM-6-[a-f0-9]{10}$/)
    expect(renamed.name).toBe(first.name)
    expect(changed.name).not.toBe(first.name)
  })

  it('keeps at most one unbound candidate across repeated failed switch attempts', () => {
    const first = stageTransportCatalog('COM6', profile, [], new Set())
    const second = stageTransportCatalog(
      'COM6',
      { ...profile, baud_rate: 9600 },
      first.profiles,
      new Set()
    )

    expect(first.profiles.filter((item) => item.name.startsWith('desktop-COM6-'))).toHaveLength(1)
    expect(second.profiles.filter((item) => item.name.startsWith('desktop-COM6-'))).toHaveLength(1)
    expect(second.selected.name).not.toBe(first.selected.name)
  })
})

describe('port model binding', () => {
  const draft: SerialConfigurationDraft = {
    port: 'COM6', enabled: true, transportProfile: profile,
    modelProfile: 'TL-AS7230 Shell', modelFamily: 'TL-AS7230'
  }

  it('preserves the concrete model when a serial-only save omits modelName', () => {
    expect(configuredPortFromDraft(draft, 'desktop-COM6-hash', {
      port: 'COM6', enabled: true, transport_profile: 'old-uart',
      model_profile: 'TL-AS7230 Shell', model_family: 'TL-AS7230', model_name: 'TL-AS7230-W 1.0'
    })).toMatchObject({
      model_profile: 'TL-AS7230 Shell', model_family: 'TL-AS7230', model_name: 'TL-AS7230-W 1.0'
    })
  })

  it('keeps identity when only the behavior profile changes', () => {
    const existing = {
      port: 'COM6', enabled: true, transport_profile: 'old-uart',
      model_profile: 'TL-AS7230 Shell', model_family: 'TL-AS7230', model_name: 'TL-AS7230-W 1.0'
    }
    expect(configuredPortFromDraft({ ...draft, modelProfile: 'Generic Shell' }, 'uart', existing)).toMatchObject({
      model_profile: 'Generic Shell', model_family: 'TL-AS7230', model_name: 'TL-AS7230-W 1.0'
    })
  })

  it('clears the concrete model when its family changes or it is explicitly unbound', () => {
    const existing = {
      port: 'COM6', enabled: true, transport_profile: 'old-uart',
      model_profile: 'TL-AS7230 Shell', model_family: 'TL-AS7230', model_name: 'TL-AS7230-W 1.0'
    }
    expect(configuredPortFromDraft({ ...draft, modelFamily: 'TL-AS7250' }, 'uart', existing).model_name).toBeNull()
    expect(configuredPortFromDraft({ ...draft, modelName: null }, 'uart', existing).model_name).toBeNull()
  })
})

describe('configuration revision safety', () => {
  it('retries parallel reads until status and every catalog share one revision', async () => {
    let round = 0
    const fetchMock = vi.fn(async (input: string | URL | Request): Promise<Response> => {
      const path = new URL(String(input)).pathname
      if (path === '/api/v1/ports') return jsonResponse([])
      if (path === '/api/v1/status') {
        round += 1
        return jsonResponse(statusResponse(round))
      }
      const revision = path === '/api/v1/config/model-profiles' && round === 1 ? 2 : round
      if (path === '/api/v1/config/model-families') {
        return jsonResponse({ families: [], config_revision: revision })
      }
      return jsonResponse({ profiles: [], config_revision: revision })
    })
    vi.stubGlobal('fetch', fetchMock)

    const data = await new SerialClient('http://127.0.0.1:3210').refresh()
    expect(data.status.config_revision).toBe(2)
    expect(fetchMock.mock.calls.filter(([input]) => String(input).endsWith('/api/v1/status'))).toHaveLength(2)
  })

  it('bounds retries when the backend never yields one configuration revision', async () => {
    let round = 0
    const fetchMock = vi.fn(async (input: string | URL | Request): Promise<Response> => {
      const path = new URL(String(input)).pathname
      if (path === '/api/v1/ports') return jsonResponse([])
      if (path === '/api/v1/status') {
        round += 1
        return jsonResponse(statusResponse(round))
      }
      if (path === '/api/v1/config/model-families') {
        return jsonResponse({ families: [], config_revision: round + 1 })
      }
      return jsonResponse({ profiles: [], config_revision: round + 1 })
    })
    vi.stubGlobal('fetch', fetchMock)

    await expect(new SerialClient('http://127.0.0.1:3210').refresh()).rejects.toThrow('无法获取一致快照')
    expect(fetchMock.mock.calls.filter(([input]) => String(input).endsWith('/api/v1/status'))).toHaveLength(3)
  })

  it('makes no write when the UI revision is older than the consistent backend snapshot', async () => {
    const fetchMock = vi.fn(async (
      input: string | URL | Request,
      init?: RequestInit
    ): Promise<Response> => {
      const path = new URL(String(input)).pathname
      if (path === '/api/v1/ports') return jsonResponse([])
      if (path === '/api/v1/status') return jsonResponse(statusResponse(8))
      if (path === '/api/v1/config/model-families') {
        return jsonResponse({ families: [], config_revision: 8 })
      }
      return jsonResponse({ profiles: [], config_revision: 8 })
    })
    vi.stubGlobal('fetch', fetchMock)
    const client = new SerialClient('http://127.0.0.1:3210')

    await expect(client.saveSerialConfiguration(serialDraft(), 7))
      .rejects.toBeInstanceOf(ConfigurationConflictError)

    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(0)
    expect(client.data().status.config_revision).toBe(8)
  })

  it('refreshes and reports a configuration conflict when transport staging loses its revision', async () => {
    let revision = 7
    const putPaths: string[] = []
    const fetchMock = vi.fn(async (
      input: string | URL | Request,
      init?: RequestInit
    ): Promise<Response> => {
      const path = new URL(String(input)).pathname
      if (init?.method === 'PUT') {
        putPaths.push(path)
        revision = 8
        return new Response('{"code":"revision_conflict"}', { status: 409 })
      }
      if (path === '/api/v1/ports') return jsonResponse([])
      if (path === '/api/v1/status') return jsonResponse(statusResponse(revision))
      if (path === '/api/v1/config/model-families') {
        return jsonResponse({ families: [], config_revision: revision })
      }
      return jsonResponse({ profiles: [], config_revision: revision })
    })
    vi.stubGlobal('fetch', fetchMock)
    const client = new SerialClient('http://127.0.0.1:3210')

    await expect(client.saveSerialConfiguration(serialDraft(), 7))
      .rejects.toBeInstanceOf(ConfigurationConflictError)

    expect(putPaths).toEqual(['/api/v1/config/transport-profiles'])
    expect(client.data().status.config_revision).toBe(8)
  })

  it('refreshes and reports a configuration conflict when the port switch loses its revision', async () => {
    let revision = 7
    const selected = contentAddressedTransportProfile('COM6', profile)
    const putPaths: string[] = []
    const fetchMock = vi.fn(async (
      input: string | URL | Request,
      init?: RequestInit
    ): Promise<Response> => {
      const path = new URL(String(input)).pathname
      if (init?.method === 'PUT') {
        putPaths.push(path)
        revision = 8
        return new Response('{"code":"revision_conflict"}', { status: 409 })
      }
      if (path === '/api/v1/ports') return jsonResponse([])
      if (path === '/api/v1/status') return jsonResponse(statusResponse(revision))
      if (path === '/api/v1/config/transport-profiles') {
        return jsonResponse({ profiles: [selected], config_revision: revision })
      }
      if (path === '/api/v1/config/model-families') {
        return jsonResponse({ families: [], config_revision: revision })
      }
      return jsonResponse({ profiles: [], config_revision: revision })
    })
    vi.stubGlobal('fetch', fetchMock)
    const client = new SerialClient('http://127.0.0.1:3210')

    await expect(client.saveSerialConfiguration(serialDraft(), 7))
      .rejects.toBeInstanceOf(ConfigurationConflictError)

    expect(putPaths).toEqual(['/api/v1/config/ports'])
    expect(client.data().status.config_revision).toBe(8)
  })

  for (const catalog of ['profiles', 'families'] as const) {
    it(`uses the loaded revision for model ${catalog} and refreshes after a lost update`, async () => {
      const putPath = catalog === 'profiles'
        ? '/api/v1/config/model-profiles'
        : '/api/v1/config/model-families'
      let putBody: Record<string, unknown> | undefined
      const fetchMock = vi.fn(async (
        input: string | URL | Request,
        init?: RequestInit
      ): Promise<Response> => {
        const path = new URL(String(input)).pathname
        if (path === putPath && init?.method === 'PUT') {
          putBody = JSON.parse(String(init.body)) as Record<string, unknown>
          return new Response('{"code":"revision_conflict"}', { status: 409 })
        }
        if (path === '/api/v1/ports') return jsonResponse([])
        if (path === '/api/v1/status') return jsonResponse(statusResponse(8))
        if (path === '/api/v1/config/model-families') {
          return jsonResponse({ families: [], config_revision: 8 })
        }
        return jsonResponse({ profiles: [], config_revision: 8 })
      })
      vi.stubGlobal('fetch', fetchMock)
      const client = new SerialClient('http://127.0.0.1:3210')

      const save = catalog === 'profiles'
        ? client.saveModelProfiles([], 7)
        : client.saveModelFamilies([], 7)
      await expect(save).rejects.toBeInstanceOf(ConfigurationConflictError)
      await expect(save).rejects.toThrow('App 已刷新到最新内容')
      expect(putBody?.expected_revision).toBe(7)
      expect(fetchMock.mock.calls[0]?.[0].toString()).toContain(putPath)
      expect(client.data().status.config_revision).toBe(8)
    })
  }
})

function serialDraft(): SerialConfigurationDraft {
  return {
    port: 'COM6',
    enabled: true,
    transportProfile: profile,
    modelProfile: null,
    modelFamily: null,
    modelName: null
  }
}

function statusResponse(configRevision: number): Record<string, unknown> {
  return {
    server_id: '11111111-1111-4111-8111-111111111111',
    daemon_epoch: '22222222-2222-4222-8222-222222222222',
    protocol_version: 6,
    config_revision: configRevision,
    ports: []
  }
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'content-type': 'application/json' }
  })
}
