import { EventEmitter } from 'node:events'
import { createHash, randomUUID } from 'node:crypto'
import WebSocket from 'ws'
import type {
  DesktopSnapshot,
  ModelProfile,
  PortDescriptor,
  SerialConfigurationDraft,
  PortSnapshot,
  TimelineEvent,
  TransportProfile
} from '../shared/contracts'
import { decodeFrame, encodeControl, normalizeTimelineEvent, type WireControl } from './protocol'
import { ReconnectLoop } from './reconnect-loop'

interface StatusResponse {
  server_id: string
  daemon_epoch: string
  protocol_version: number
  config_revision: number
  ports: PortSnapshot[]
}

interface ProfileList<T> {
  profiles: T[]
  config_revision: number
}

interface ConfigurePortsResponse {
  ports: PortSnapshot[]
  config_revision: number
}

interface EventQueryResponse {
  events: Record<string, unknown>[]
}

interface Lease {
  id: string
  fence: number
  owner: { id: string }
}

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  port?: string
}

export interface ServerData {
  status: StatusResponse
  availablePorts: PortDescriptor[]
  transportProfiles: TransportProfile[]
  modelProfiles: ModelProfile[]
  events: Record<string, TimelineEvent[]>
}

export class SerialClient extends EventEmitter {
  readonly endpoint: string
  private status?: StatusResponse
  private availablePorts: PortDescriptor[] = []
  private transportProfiles: TransportProfile[] = []
  private modelProfiles: ModelProfile[] = []
  private readonly events = new Map<string, TimelineEvent[]>()
  private socket?: WebSocket
  private renewTimer?: NodeJS.Timeout
  private stopped = false
  private connectionVersion = 0
  private actorId?: string
  private readonly leases = new Map<string, Lease>()
  private readonly pending = new Map<string, PendingRequest>()
  private readonly acquiring = new Map<string, Promise<Lease>>()
  private readonly reconnectLoop: ReconnectLoop

  constructor(endpoint: string) {
    super()
    const parsed = new URL(endpoint)
    if (!['http:', 'https:'].includes(parsed.protocol)) throw new Error('后端地址必须使用 http 或 https')
    this.endpoint = endpoint.replace(/\/$/, '')
    this.reconnectLoop = new ReconnectLoop(
      () => this.reconnectAttempt(),
      (error) => this.emit('error', error)
    )
  }

  async healthReachable(): Promise<boolean> {
    try {
      await this.request('/api/v1/health', { timeout: 1_500 })
      return true
    } catch {
      return false
    }
  }

  async start(): Promise<ServerData> {
    this.stopped = false
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    const data = await this.refresh(true)
    await this.openSocket(this.connectionVersion)
    return data
  }

  async refresh(loadHistory = false): Promise<ServerData> {
    const [status, availablePorts, transport, models] = await Promise.all([
      this.get<StatusResponse>('/api/v1/status'),
      this.get<PortDescriptor[]>('/api/v1/ports'),
      this.get<ProfileList<TransportProfile>>('/api/v1/config/transport-profiles'),
      this.get<ProfileList<ModelProfile>>('/api/v1/config/model-profiles')
    ])
    this.status = status
    this.availablePorts = availablePorts
    this.transportProfiles = transport.profiles
    this.modelProfiles = models.profiles
    if (loadHistory) {
      await Promise.all(status.ports.map((configured) => this.loadHistory(configured)))
    }
    return this.data()
  }

  data(): ServerData {
    if (!this.status) throw new Error('尚未连接后端')
    return {
      status: this.status,
      availablePorts: [...this.availablePorts],
      transportProfiles: structuredClone(this.transportProfiles),
      modelProfiles: structuredClone(this.modelProfiles),
      events: Object.fromEntries([...this.events].map(([port, items]) => [port, [...items]]))
    }
  }

  async setPortOpen(port: string, open: boolean): Promise<void> {
    const status = await this.get<StatusResponse>('/api/v1/status')
    const ports = status.ports.map((configured) =>
      configured.config.port === port ? { ...configured.config, enabled: open } : configured.config
    )
    await this.put('/api/v1/config/ports', {
      ports,
      source: 'human:desktop',
      expected_revision: status.config_revision
    })
    await this.refresh()
    await this.reconnectSocket()
  }

  async saveSerialConfiguration(draft: SerialConfigurationDraft): Promise<void> {
    const [status, catalog] = await Promise.all([
      this.get<StatusResponse>('/api/v1/status'),
      this.get<ProfileList<TransportProfile>>('/api/v1/config/transport-profiles')
    ])
    const boundBefore = new Set(status.ports.map((configured) => configured.config.transport_profile).filter(Boolean))
    const stage = stageTransportCatalog(draft.port, draft.transportProfile, catalog.profiles, boundBefore)
    const staged = sameProfileCatalog(stage.profiles, catalog.profiles)
      ? { config_revision: status.config_revision }
      : await this.put<ProfileList<TransportProfile>>('/api/v1/config/transport-profiles', {
          profiles: stage.profiles,
          expected_revision: status.config_revision
        })
    const next = {
      port: draft.port,
      enabled: draft.enabled,
      transport_profile: stage.selected.name,
      model_profile: draft.modelProfile ?? null
    }
    const ports = status.ports.some((configured) => configured.config.port === draft.port)
      ? status.ports.map((configured) => (configured.config.port === draft.port ? next : configured.config))
      : [...status.ports.map((configured) => configured.config), next]
    const switched = await this.put<ConfigurePortsResponse>('/api/v1/config/ports', {
      ports,
      source: 'human:desktop',
      expected_revision: staged.config_revision
    })
    const bound = new Set(switched.ports.map((configured) => configured.config.transport_profile).filter(Boolean))
    const prefix = transportCandidatePrefix(draft.port)
    const cleaned = stage.profiles.filter(
      (profile) => !profile.name.startsWith(prefix) || bound.has(profile.name)
    )
    if (cleaned.length !== stage.profiles.length) {
      try {
        await this.put('/api/v1/config/transport-profiles', {
          profiles: cleaned,
          expected_revision: switched.config_revision
        })
      } catch {}
    }
    await this.refresh()
    await this.reconnectSocket()
  }

  async saveModelProfiles(profiles: ModelProfile[]): Promise<void> {
    const status = await this.get<StatusResponse>('/api/v1/status')
    await this.put('/api/v1/config/model-profiles', {
      profiles,
      expected_revision: status.config_revision
    })
    await this.refresh()
  }

  async sendCommand(port: string, command: string): Promise<void> {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) throw new Error('实时连接尚未建立')
    const lease = await this.acquire(port)
    const configured = this.status?.ports.find((item) => item.config.port === port)
    const eol = configured?.effective_write_eol ?? '\r'
    await this.control({
      type: 'write',
      request_id: randomUUID(),
      port,
      control_id: lease.id,
      fence: lease.fence,
      data: Buffer.from(`${command}${eol}`).toString('base64'),
      operation_id: randomUUID(),
      expected_run_id: null,
      pacing: null,
      description: null,
      command_sequence: null,
      command_capture_matchers: [],
      sequence_precondition: null,
      cooperative: false
    })
  }

  async stop(): Promise<void> {
    this.stopped = true
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    this.stopRenewingLeases()
    const socket = this.socket
    this.socket = undefined
    if (socket?.readyState === WebSocket.OPEN) {
      for (const [port, lease] of this.leases) {
        socket.send(
          encodeControl({
            type: 'release_control',
            request_id: randomUUID(),
            port,
            control_id: lease.id,
            fence: lease.fence
          })
        )
      }
      socket.close()
    }
    this.rejectPending(new Error('实时连接已关闭'))
    this.leases.clear()
    this.acquiring.clear()
  }

  private async loadHistory(configured: PortSnapshot): Promise<void> {
    const after = Math.max(0, configured.head_seq - 4_000)
    const query = new URLSearchParams({
      epoch: configured.daemon_epoch,
      after_seq: String(after),
      through_seq: String(configured.head_seq),
      limit_events: '4000',
      limit_bytes: String(2 * 1024 * 1024)
    })
    const response = await this.get<EventQueryResponse>(
      `/api/v1/ports/${encodeURIComponent(configured.config.port)}/events?${query}`
    )
    const history = response.events.map(normalizeTimelineEvent).sort((a, b) => a.seq - b.seq)
    this.events.set(configured.config.port, history)
  }

  private async openSocket(version = this.connectionVersion): Promise<void> {
    if (!this.status || this.stopped) return
    const socket = new WebSocket(this.endpoint.replace(/^http/, 'ws') + '/api/v1/ws')
    socket.binaryType = 'nodebuffer'
    this.socket = socket
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('实时连接超时')), 5_000)
        socket.once('open', () => {
          clearTimeout(timer)
          resolve()
        })
        socket.once('error', (error) => {
          clearTimeout(timer)
          reject(error)
        })
      })
    } catch (error) {
      if (this.socket === socket) this.socket = undefined
      socket.removeAllListeners()
      socket.on('error', () => undefined)
      socket.terminate()
      throw error
    }
    if (this.stopped || version !== this.connectionVersion || this.socket !== socket) {
      if (this.socket === socket) this.socket = undefined
      socket.close()
      return
    }
    socket.on('message', (data) => this.onSocketMessage(Buffer.from(data as Buffer)))
    socket.on('close', () => this.onSocketClose(socket))
    socket.on('error', (error) => this.emit('error', error))
    socket.send(
      encodeControl({
        type: 'hello',
        request_id: randomUUID(),
        protocol_version: 4,
        client_name: 'serial-platform-desktop',
        actor_kind: 'human'
      })
    )
    socket.send(
      encodeControl({
        type: 'attach',
        request_id: randomUUID(),
        subscriptions: this.status.ports.map((configured) => ({
          port: configured.config.port,
          cursor: {
            epoch: configured.daemon_epoch,
            after_seq: this.events.get(configured.config.port)?.at(-1)?.seq ?? 0
          },
          tail_events: 1000
        }))
      })
    )
    this.stopRenewingLeases()
    this.renewTimer = setInterval(() => void this.renewLeases(), 10_000)
    this.emit('connected')
  }

  private onSocketMessage(data: Buffer): void {
    try {
      const frame = decodeFrame(data)
      if (frame.kind === 'timeline') {
        this.appendEvent(frame.event)
        return
      }
      this.handleControl(frame.message)
    } catch (error) {
      this.emit('error', error)
    }
  }

  private handleControl(message: WireControl): void {
    if (message.type === 'welcome') {
      const actor = message.actor as { id?: string } | undefined
      this.actorId = actor?.id
      return
    }
    if (message.type === 'snapshot') {
      const configured = message.port as PortSnapshot
      if (this.status) {
        const index = this.status.ports.findIndex((item) => item.config.port === configured.config.port)
        if (index >= 0) this.status.ports[index] = configured
      }
      this.emit('snapshot', configured)
      return
    }
    if (message.type === 'timeline') {
      const event = normalizeTimelineEvent({
        ...(message.event as Record<string, unknown>),
        replay: message.replay
      })
      this.observeControlTimeline(event)
      this.appendEvent(event)
      return
    }
    if (message.type === 'result') {
      const requestId = String(message.request_id ?? '')
      const result = message.result as Record<string, unknown>
      if (result?.type === 'control_granted') {
        const lease = result.lease as Lease
        const port = this.pending.get(requestId)?.port
        if (port) this.leases.set(port, lease)
      }
      if (result?.type === 'control_queued') return
      const pending = this.pending.get(requestId)
      this.pending.delete(requestId)
      pending?.resolve(result)
      return
    }
    if (message.type === 'error') {
      const requestId = message.request_id ? String(message.request_id) : undefined
      const error = new Error(String(message.message ?? '后端拒绝请求'))
      if (requestId) {
        const pending = this.pending.get(requestId)
        this.pending.delete(requestId)
        pending?.reject(error)
      } else {
        this.emit('error', error)
      }
      return
    }
    if (message.type === 'gap' || message.type === 'lagged') {
      this.emit('notice', `${String(message.port)} 的实时记录存在缺口`)
    }
  }

  private appendEvent(event: TimelineEvent): void {
    const items = this.events.get(event.port) ?? []
    const last = items.at(-1)
    if (last?.daemon_epoch === event.daemon_epoch && last.seq >= event.seq) return
    items.push(event)
    if (items.length > 12_000) items.splice(0, items.length - 12_000)
    this.events.set(event.port, items)
    this.emit('timeline', event)
  }

  private observeControlTimeline(event: TimelineEvent): void {
    if (event.kind === 'control_granted') {
      const lease = event.metadata.lease as Lease | undefined
      if (lease && lease.owner?.id === this.actorId) {
        this.leases.set(event.port, lease)
        for (const [requestId, pending] of this.pending) {
          if (pending.port !== event.port) continue
          this.pending.delete(requestId)
          pending.resolve({ type: 'control_granted', lease })
        }
      }
    }
    if (['control_released', 'control_revoked', 'control_expired'].includes(event.kind)) {
      if (event.actor?.id === this.actorId) this.leases.delete(event.port)
    }
  }

  private acquire(port: string): Promise<Lease> {
    const current = this.leases.get(port)
    if (current) return Promise.resolve(current)
    const existing = this.acquiring.get(port)
    if (existing) return existing
    const request = this.control({
      type: 'acquire_control',
      request_id: randomUUID(),
      port,
      mode: 'queue',
      ttl_ms: 30_000
    }, port).then((result) => {
      const lease = (result as { lease: Lease }).lease
      this.leases.set(port, lease)
      return lease
    }).finally(() => this.acquiring.delete(port))
    this.acquiring.set(port, request)
    return request
  }

  private control(message: WireControl, port?: string): Promise<unknown> {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('实时连接尚未建立'))
    }
    const requestId = String(message.request_id)
    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { resolve, reject, port })
      this.socket?.send(encodeControl(message), (error) => {
        if (!error) return
        this.pending.delete(requestId)
        reject(error)
      })
    })
  }

  private async renewLeases(): Promise<void> {
    for (const [port, lease] of this.leases) {
      try {
        const result = (await this.control({
          type: 'renew_control',
          request_id: randomUUID(),
          port,
          control_id: lease.id,
          fence: lease.fence,
          ttl_ms: 30_000
        })) as { lease?: Lease }
        if (result.lease) this.leases.set(port, result.lease)
      } catch {
        this.leases.delete(port)
      }
    }
  }

  private async reconnectSocket(): Promise<void> {
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    const socket = this.socket
    this.socket = undefined
    if (socket) socket.close()
    this.stopRenewingLeases()
    this.rejectPending(new Error('配置已更新，正在重建实时连接'))
    this.leases.clear()
    try {
      await this.openSocket(this.connectionVersion)
    } catch (error) {
      if (!this.stopped) this.reconnectLoop.schedule()
      throw error
    }
  }

  private onSocketClose(socket: WebSocket): void {
    if (this.stopped || this.socket !== socket) return
    this.socket = undefined
    this.stopRenewingLeases()
    this.rejectPending(new Error('实时连接中断'))
    this.leases.clear()
    this.emit('disconnected')
    this.reconnectLoop.schedule()
  }

  private async reconnectAttempt(): Promise<void> {
    const version = this.connectionVersion
    await this.refresh()
    if (this.stopped || version !== this.connectionVersion) return
    await this.openSocket(version)
  }

  private stopRenewingLeases(): void {
    if (this.renewTimer) clearInterval(this.renewTimer)
    this.renewTimer = undefined
  }

  private rejectPending(error: Error): void {
    for (const pending of this.pending.values()) pending.reject(error)
    this.pending.clear()
  }

  private async get<T>(path: string): Promise<T> {
    return this.request(path) as Promise<T>
  }

  private async put<T = unknown>(path: string, body: unknown): Promise<T> {
    return this.request(path, { method: 'PUT', body }) as Promise<T>
  }

  private async request(
    path: string,
    options: { method?: string; body?: unknown; timeout?: number } = {}
  ): Promise<unknown> {
    const response = await fetch(`${this.endpoint}${path}`, {
      method: options.method ?? 'GET',
      headers: options.body ? { 'content-type': 'application/json' } : undefined,
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: AbortSignal.timeout(options.timeout ?? 8_000)
    })
    if (!response.ok) {
      const detail = (await response.text()).trim()
      throw new Error(`后端返回 ${response.status}${detail ? `：${detail}` : ''}`)
    }
    return response.json()
  }
}

export function contentAddressedTransportProfile(port: string, profile: TransportProfile): TransportProfile {
  const settings = transportSettings(profile)
  const hash = createHash('sha256').update(JSON.stringify(settings)).digest('hex').slice(0, 10)
  return { ...profile, name: `${transportCandidatePrefix(port)}${hash}` }
}

export function stageTransportCatalog(
  port: string,
  profile: TransportProfile,
  catalog: TransportProfile[],
  bound: Set<string | null | undefined>
): { selected: TransportProfile; profiles: TransportProfile[] } {
  const prefix = transportCandidatePrefix(port)
  const retained = catalog.filter((item) => !item.name.startsWith(prefix) || bound.has(item.name))
  const candidate = contentAddressedTransportProfile(port, profile)
  const selected = retained.find((item) => sameTransportSettings(item, candidate)) ?? candidate
  const profiles = retained.some((item) => item.name === selected.name) ? retained : [...retained, selected]
  return { selected, profiles }
}

function transportCandidatePrefix(port: string): string {
  const safePort = port.replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'port'
  return `desktop-${safePort}-`
}

function sameTransportSettings(left: TransportProfile, right: TransportProfile): boolean {
  return JSON.stringify(transportSettings(left)) === JSON.stringify(transportSettings(right))
}

function sameProfileCatalog(left: TransportProfile[], right: TransportProfile[]): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function transportSettings(profile: TransportProfile): Omit<TransportProfile, 'name'> {
  const { name: _name, ...settings } = profile
  return settings
}
