import { once } from 'node:events'
import type { AddressInfo } from 'node:net'
import { WebSocketServer, type RawData } from 'ws'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type {
  PendingRunStartApproval,
  PortSnapshot,
  SerialConfigurationDraft,
  TimelineEvent,
  TransportProfile
} from '../shared/contracts'
import { decodeFrame, encodeControl, type WireControl } from './protocol'
import {
  assertCompatibleProtocol,
  buildHumanCommandMessage,
  buildRunStartDecisionMessage,
  ConfigurationConflictError,
  configuredPortFromDraft,
  contentAddressedTransportProfile,
  HumanCommandOutcomeUncertainError,
  parseSerialdHealth,
  SerialCommandError,
  SerialClient,
  serialdIdentityMatches,
  stageTransportCatalog
} from './serial-client'

const serialBackends: SerialTestBackend[] = []

afterEach(async () => {
  vi.unstubAllGlobals()
  await Promise.all(serialBackends.splice(0).map((backend) => backend.close()))
})

describe('component protocol gate', () => {
  it('accepts v8 and rejects an older backend before opening the live socket', () => {
    expect(() => assertCompatibleProtocol(8)).not.toThrow()
    expect(() => assertCompatibleProtocol(7)).toThrow(/App 需要 v8，后端提供 v7/)
  })

  it('requires an ok v8 health response with stable UUID identities', () => {
    const identity = parseSerialdHealth({
      status: 'ok',
      server_id: '11111111-1111-4111-8111-111111111111',
      daemon_epoch: '22222222-2222-4222-8222-222222222222',
      protocol_version: 8,
      uptime_ms: 10
    })
    expect(identity).toEqual({
      serverId: '11111111-1111-4111-8111-111111111111',
      daemonEpoch: '22222222-2222-4222-8222-222222222222',
      protocolVersion: 8
    })
    expect(serialdIdentityMatches(identity, identity)).toBe(true)
    expect(serialdIdentityMatches(identity, { ...identity, daemonEpoch: '33333333-3333-4333-8333-333333333333' }))
      .toBe(false)
    expect(() => parseSerialdHealth({ ...identity, status: 'ok', protocol_version: 8 }))
      .toThrow('服务身份')
    expect(() => parseSerialdHealth({
      status: 'ok', server_id: identity.serverId, daemon_epoch: identity.daemonEpoch,
      protocol_version: 6
    })).toThrow(/App 需要 v8/)
  })
})

describe('v8 Human control messages', () => {
  it('sends Enter as one atomic Human command without queue or lease fields', () => {
    const message = buildHumanCommandMessage({
      requestId: 'request', operationId: 'operation', port: 'COM6', expectedGeneration: 4,
      data: 'dmVyc2lvbg0='
    })
    expect(message).toEqual({
      type: 'send_human_command', request_id: 'request', port: 'COM6', expected_generation: 4,
      data: 'dmVyc2lvbg0=', operation_id: 'operation', description: null
    })
    expect(message.mode).toBeUndefined()
    expect(message.control_id).toBeUndefined()
    expect(message.fence).toBeUndefined()
  })

  it('binds a decision to the visible approval ID', () => {
    expect(buildRunStartDecisionMessage('decision', 'COM6', 'approval', 'deny')).toEqual({
      type: 'decide_run_start', request_id: 'decision', port: 'COM6', approval_id: 'approval', decision: 'deny'
    })
  })

  it('sends Human Ctrl-D/C exactly without EOL or input-history metadata, and sends bare Enter', async () => {
    const backend = await serialBackend({ serverId: FIRST_SERVER_ID, daemonEpoch: FIRST_DAEMON_EPOCH, headSeq: 0, events: [], humanCommandOutcome: 'accepted' })
    const client = new SerialClient(backend.endpoint)
    await client.start()
    await client.sendSignal('COM6', 'ctrl_d')
    await client.sendSignal('COM6', 'ctrl_c')
    await client.sendCommand('COM6', '')
    await client.sendCommand('COM6', 'version')
    expect(backend.humanCommands.map((item) => [...Buffer.from(String(item.data), 'base64')])).toEqual([[4], [3], [13], [...Buffer.from('version\r')]])
    expect(backend.humanCommands[0].input).toBeUndefined()
    expect(backend.humanCommands[1].input).toBeUndefined()
    expect(backend.humanCommands[2].input).toEqual({ command: '' })
    expect(backend.humanCommands[3].input).toEqual({ command: 'version' })
    await client.stop()
  })

  it('marks a TX-followed-by-disconnect result as uncertain and never resends it after reconnect', async () => {
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 0,
      events: [],
      humanCommandOutcome: 'disconnect_after_tx'
    })
    const client = new SerialClient(backend.endpoint)
    await client.start()
    const received = once(client, 'timeline')
    const reconnected = once(client, 'connected')

    await expect(client.sendCommand('COM6', 'version'))
      .rejects.toBeInstanceOf(HumanCommandOutcomeUncertainError)
    const [event] = await received as [TimelineEvent]
    expect(event).toMatchObject({
      kind: 'tx',
      direction: 'tx',
      metadata: { human_command: true }
    })

    backend.state.humanCommandOutcome = 'accepted'
    await expect(withTestTimeout(reconnected, 3_000, 'client did not reconnect')).resolves.toBeDefined()
    expect(backend.humanCommands).toHaveLength(1)
    await client.stop()
  })

  it('keeps an explicit daemon Error as a definite rejection', async () => {
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 0,
      events: [],
      humanCommandOutcome: 'rejected'
    })
    const client = new SerialClient(backend.endpoint)
    await client.start()

    const error = await client.sendCommand('COM6', 'version').then(
      () => undefined,
      (reason: unknown) => reason
    )
    expect(error).toBeInstanceOf(SerialCommandError)
    expect(error).toMatchObject({ code: 'bad_request' })
    expect(backend.humanCommands).toHaveLength(1)
    await client.stop()
  })

  it('maps daemon write_outcome_uncertain only for a pending Human command', async () => {
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 0,
      events: [],
      humanCommandOutcome: 'uncertain_error'
    })
    const client = new SerialClient(backend.endpoint)
    await client.start()

    const error = await client.sendCommand('COM6', 'version').then(
      () => undefined,
      (reason: unknown) => reason
    )
    expect(error).toBeInstanceOf(HumanCommandOutcomeUncertainError)
    expect(error).toHaveProperty('message', expect.stringContaining('勿直接重发'))
    expect(backend.humanCommands).toHaveLength(1)
    await client.stop()
  })

  it('does not reinterpret write_outcome_uncertain for a non-Human pending RPC', async () => {
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 0,
      events: [],
      failNext: 'hello',
      failNextErrorCode: 'write_outcome_uncertain'
    })
    const client = new SerialClient(backend.endpoint)

    const error = await client.start().then(
      () => undefined,
      (reason: unknown) => reason
    )
    expect(error).toBeInstanceOf(SerialCommandError)
    expect(error).not.toBeInstanceOf(HumanCommandOutcomeUncertainError)
    expect(error).toMatchObject({ code: 'write_outcome_uncertain' })
    await client.stop()
  })
})

describe('authoritative WebSocket reconnect', () => {
  it('clears old-epoch history and attaches from zero when the restarted daemon head is zero', async () => {
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 100,
      events: [timelineEvent(FIRST_DAEMON_EPOCH, 100, 'rx')]
    })
    const client = new SerialClient(backend.endpoint)
    await client.start()
    expect(client.data().events.COM6?.map((event) => event.daemon_epoch)).toEqual([FIRST_DAEMON_EPOCH])

    backend.state = {
      serverId: FIRST_SERVER_ID,
      daemonEpoch: SECOND_DAEMON_EPOCH,
      headSeq: 0,
      events: []
    }
    const reconnected = once(client, 'connected')
    backend.disconnect()
    await expect(withTestTimeout(reconnected, 3_000, 'client did not reconnect')).resolves.toBeDefined()

    expect(backend.attachRequests.at(-1)?.subscriptions).toEqual([{
      port: 'COM6',
      cursor: { epoch: SECOND_DAEMON_EPOCH, after_seq: 0 },
      tail_events: 1000
    }])
    expect(client.data().status.daemon_epoch).toBe(SECOND_DAEMON_EPOCH)
    expect(client.data().events.COM6 ?? []).toEqual([])
    await client.stop()
  })

  it('does not reuse a high old seq in a new epoch, so early approval timeline remains reachable', async () => {
    const initialApproval = runStartApproval(FIRST_DAEMON_EPOCH)
    const backend = await serialBackend({
      serverId: FIRST_SERVER_ID,
      daemonEpoch: FIRST_DAEMON_EPOCH,
      headSeq: 100,
      events: [
        timelineEvent(FIRST_DAEMON_EPOCH, 40, 'run_start_requested', { approval: initialApproval }),
        timelineEvent(FIRST_DAEMON_EPOCH, 100, 'rx')
      ]
    })
    const client = new SerialClient(backend.endpoint)
    const bootstrap = await client.start()
    expect(bootstrap.actor?.id).toBe(HUMAN_ACTOR.id)
    expect(bootstrap.status.ports[0]?.pending_run_start?.id).toBe(initialApproval.id)
    const approval = runStartApproval(SECOND_DAEMON_EPOCH)
    backend.state = {
      serverId: SECOND_SERVER_ID,
      daemonEpoch: SECOND_DAEMON_EPOCH,
      headSeq: 150,
      events: [
        timelineEvent(SECOND_DAEMON_EPOCH, 40, 'run_start_requested', { approval }),
        timelineEvent(SECOND_DAEMON_EPOCH, 150, 'rx')
      ]
    }
    const received: TimelineEvent[] = []
    client.on('timeline', (event: TimelineEvent) => received.push(event))
    const reconnected = once(client, 'connected')
    backend.disconnect()
    await expect(withTestTimeout(reconnected, 3_000, 'client did not reconnect')).resolves.toBeDefined()

    expect(backend.attachRequests.at(-1)?.subscriptions[0]?.cursor).toEqual({
      epoch: SECOND_DAEMON_EPOCH,
      after_seq: 0
    })
    expect(client.data().events.COM6?.map((event) => [event.daemon_epoch, event.seq])).toEqual([
      [SECOND_DAEMON_EPOCH, 40],
      [SECOND_DAEMON_EPOCH, 150]
    ])
    expect(received.some((event) => event.kind === 'run_start_requested' && event.seq === 40)).toBe(true)
    expect(client.data().status.ports[0]?.pending_run_start?.id).toBe(approval.id)
    expect(client.data().actor?.id).toBe(HUMAN_ACTOR.id)
    await client.stop()
  })

  for (const phase of ['hello', 'attach'] as const) {
    it(`rejects a ${phase} error, never reports connected, and succeeds on an authoritative retry`, async () => {
      const backend = await serialBackend({
        serverId: FIRST_SERVER_ID,
        daemonEpoch: FIRST_DAEMON_EPOCH,
        headSeq: 0,
        events: [],
        failNext: phase
      })
      const client = new SerialClient(backend.endpoint)
      const connected = vi.fn()
      client.on('connected', connected)

      await expect(client.start()).rejects.toThrow(new RegExp(`${phase} rejected`, 'i'))
      expect(connected).not.toHaveBeenCalled()
      expect(client.data().actor).toBeUndefined()

      await client.start()
      expect(connected).toHaveBeenCalledTimes(1)
      expect(client.data().actor?.id).toBe(HUMAN_ACTOR.id)
      expect(backend.attachRequests.at(-1)?.subscriptions[0]?.cursor).toEqual({
        epoch: FIRST_DAEMON_EPOCH,
        after_seq: 0
      })
      await client.stop()
    })
  }
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
    protocol_version: 8,
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

const FIRST_SERVER_ID = '11111111-1111-4111-8111-111111111111'
const SECOND_SERVER_ID = '99999999-9999-4999-8999-999999999999'
const FIRST_DAEMON_EPOCH = '22222222-2222-4222-8222-222222222222'
const SECOND_DAEMON_EPOCH = '33333333-3333-4333-8333-333333333333'
const HUMAN_ACTOR = { id: 'desktop-human', label: 'desktop', kind: 'human' as const }

interface SerialDaemonState {
  serverId: string
  daemonEpoch: string
  headSeq: number
  events: TimelineEvent[]
  failNext?: 'hello' | 'attach'
  failNextErrorCode?: string
  humanCommandOutcome?: 'accepted' | 'rejected' | 'uncertain_error' | 'disconnect_after_tx'
}

interface AttachRequest {
  subscriptions: Array<{
    port: string
    cursor: { epoch: string; after_seq: number }
    tail_events: number
  }>
}

interface SerialTestBackend {
  endpoint: string
  state: SerialDaemonState
  attachRequests: AttachRequest[]
  humanCommands: WireControl[]
  disconnect(): void
  close(): Promise<void>
}

async function serialBackend(initial: SerialDaemonState): Promise<SerialTestBackend> {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(server, 'listening')
  const address = server.address() as AddressInfo
  const backend: SerialTestBackend = {
    endpoint: `http://127.0.0.1:${address.port}`,
    state: initial,
    attachRequests: [],
    humanCommands: [],
    disconnect(): void {
      for (const socket of server.clients) socket.terminate()
    },
    async close(): Promise<void> {
      for (const socket of server.clients) socket.terminate()
      if (server.address() === null) return
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve())
      })
    }
  }
  serialBackends.push(backend)
  server.on('connection', (socket) => {
    socket.on('message', (raw: RawData) => {
      const decoded = decodeFrame(rawDataBuffer(raw))
      if (decoded.kind !== 'control') return
      const message = decoded.message
      if (message.type === 'hello') {
        if (backend.state.failNext === 'hello') {
          backend.state.failNext = undefined
          const code = backend.state.failNextErrorCode
          backend.state.failNextErrorCode = undefined
          sendControl(socket, commandError(message, 'hello rejected', code))
          return
        }
        sendControl(socket, {
          type: 'welcome',
          server_id: backend.state.serverId,
          daemon_epoch: backend.state.daemonEpoch,
          protocol_version: 8,
          actor: HUMAN_ACTOR
        })
        sendControl(socket, {
          type: 'result',
          request_id: message.request_id,
          result: { type: 'hello_accepted', actor: HUMAN_ACTOR }
        })
        return
      }
      if (message.type === 'send_human_command') {
        backend.humanCommands.push(structuredClone(message))
        if (backend.state.humanCommandOutcome === 'uncertain_error') {
          sendControl(socket, commandError(
            message,
            'the daemon cannot prove whether the write completed',
            'write_outcome_uncertain'
          ))
          return
        }
        if (backend.state.humanCommandOutcome === 'rejected') {
          sendControl(socket, commandError(message, 'human command rejected'))
          return
        }
        if (backend.state.humanCommandOutcome === 'disconnect_after_tx') {
          const event: TimelineEvent = {
            ...timelineEvent(backend.state.daemonEpoch, backend.state.headSeq + 1, 'tx'),
            direction: 'tx',
            operation_id: String(message.operation_id),
            text: Buffer.from(String(message.data), 'base64').toString('utf8'),
            metadata: { human_command: true, cooperative: false }
          }
          backend.state.headSeq = event.seq
          backend.state.events.push(event)
          socket.send(encodeControl({ type: 'timeline', event, replay: false }), () => socket.terminate())
          return
        }
        sendControl(socket, {
          type: 'result',
          request_id: message.request_id,
          result: { type: 'human_command_accepted', event_seq: backend.state.headSeq + 1, mode: 'owned' }
        })
        return
      }
      if (message.type !== 'attach') return
      const request = structuredClone(message) as unknown as AttachRequest
      backend.attachRequests.push(request)
      if (backend.state.failNext === 'attach') {
        backend.state.failNext = undefined
        sendControl(socket, commandError(message, 'attach rejected'))
        return
      }
      const invalid = request.subscriptions.find((subscription) => (
        subscription.cursor.epoch !== backend.state.daemonEpoch
        || subscription.cursor.after_seq > backend.state.headSeq
      ))
      if (invalid) {
        sendControl(socket, commandError(message, 'attach cursor is not authoritative'))
        return
      }
      for (const subscription of request.subscriptions) {
        sendControl(socket, { type: 'snapshot', port: portSnapshot(backend.state) })
        for (const event of backend.state.events) {
          if (event.port !== subscription.port || event.seq <= subscription.cursor.after_seq) continue
          sendControl(socket, { type: 'timeline', event, replay: true })
        }
        sendControl(socket, {
          type: 'ready',
          port: subscription.port,
          head_seq: backend.state.headSeq
        })
      }
      sendControl(socket, {
        type: 'result',
        request_id: message.request_id,
        result: {
          type: 'attached',
          ports: request.subscriptions.map((subscription) => subscription.port)
        }
      })
    })
  })
  vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request): Promise<Response> => {
    const url = new URL(String(input))
    if (url.pathname === '/api/v1/ports') return jsonResponse([])
    if (url.pathname === '/api/v1/history/commands') return jsonResponse({ server_id: backend.state.serverId, revision: 0, entries: [] })
    if (url.pathname === '/api/v1/status') return jsonResponse(statusFor(backend.state))
    if (url.pathname.endsWith('/events')) return jsonResponse({ events: backend.state.events })
    if (url.pathname === '/api/v1/config/model-families') {
      return jsonResponse({ families: [], config_revision: 1 })
    }
    if (url.pathname.startsWith('/api/v1/config/')) {
      return jsonResponse({ profiles: [], config_revision: 1 })
    }
    throw new Error(`unexpected HTTP request ${url.pathname}`)
  }))
  return backend
}

function statusFor(state: SerialDaemonState): Record<string, unknown> {
  return {
    server_id: state.serverId,
    daemon_epoch: state.daemonEpoch,
    protocol_version: 8,
    config_revision: 1,
    ports: [portSnapshot(state)]
  }
}

function portSnapshot(state: SerialDaemonState): PortSnapshot {
  const approval = state.events
    .find((event) => event.kind === 'run_start_requested')
    ?.metadata.approval as PendingRunStartApproval | undefined
  return {
    config: {
      port: 'COM6',
      enabled: true,
      transport_profile: null,
      model_profile: null,
      model_family: null,
      model_name: null
    },
    daemon_epoch: state.daemonEpoch,
    head_seq: state.headSeq,
    generation: 1,
    endpoint_present: true,
    session_state: 'online',
    state_reason: null,
    control: approval ? {
      id: approval.expected_control_id,
      owner: HUMAN_ACTOR,
      epoch: state.daemonEpoch,
      generation: 1,
      fence: approval.expected_fence,
      issued_wall_time_ns: 1,
      expires_wall_time_ns: Number.MAX_SAFE_INTEGER
    } : null,
    pending_run_start: approval ?? null,
    run_context: null,
    effective_shell_prompt: null,
    effective_uboot_prompt: null,
    effective_write_eol: '\r',
    effective_transport: null
  }
}

function timelineEvent(
  daemonEpoch: string,
  seq: number,
  kind: string,
  metadata: Record<string, unknown> = {}
): TimelineEvent {
  return {
    port: 'COM6',
    daemon_epoch: daemonEpoch,
    seq,
    generation: 1,
    wall_time_ns: seq,
    kind,
    direction: kind === 'rx' ? 'rx' : 'none',
    actor: null,
    run_id: null,
    operation_id: null,
    stream_offset_start: null,
    stream_offset_end: null,
    text: kind === 'rx' ? `event-${seq}` : '',
    metadata,
    durable: true
  }
}

function runStartApproval(daemonEpoch: string): PendingRunStartApproval {
  return {
    id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    port: 'COM6',
    requester: { id: 'agent', label: 'agent', kind: 'agent' },
    required_approver: HUMAN_ACTOR,
    label: 'restart approval',
    metadata: {},
    control_ttl_ms: 60_000,
    daemon_epoch: daemonEpoch,
    generation: 1,
    expected_control_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    expected_fence: 4,
    requested_wall_time_ns: 1,
    expires_wall_time_ns: Number.MAX_SAFE_INTEGER
  }
}

function commandError(message: WireControl, detail: string, code = 'bad_request'): WireControl {
  return {
    type: 'error',
    request_id: message.request_id,
    code,
    message: detail,
    retryable: false
  }
}

function sendControl(socket: import('ws').WebSocket, message: WireControl): void {
  socket.send(encodeControl(message))
}

function rawDataBuffer(raw: RawData): Buffer {
  return Array.isArray(raw) ? Buffer.concat(raw) : Buffer.from(raw as ArrayBuffer)
}

async function withTestTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(message)), ms)
      })
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}
