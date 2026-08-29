import { EventEmitter } from 'node:events'
import { createHash, randomUUID } from 'node:crypto'
import WebSocket from 'ws'
import { HUMAN_COMMAND_UNCERTAIN_MESSAGE } from '../shared/contracts'
import type {
  Actor,
  ModelFamily,
  ModelProfile,
  PendingRunStartApproval,
  PortDescriptor,
  RunStartDecision,
  SerialConfigurationDraft,
  PortSnapshot,
  TimelineEvent,
  TransportProfile
} from '../shared/contracts'
import {
  decodeFrame,
  encodeControl,
  normalizeTimelineEvent,
  SERIAL_PROTOCOL_VERSION,
  type WireControl
} from './protocol'
import { ReconnectLoop } from './reconnect-loop'
import { isApprovalActionable } from '../shared/run-start'

interface StatusResponse {
  server_id: string
  daemon_epoch: string
  protocol_version: number
  config_revision: number
  ports: PortSnapshot[]
}

export interface SerialdHealthIdentity {
  serverId: string
  daemonEpoch: string
  protocolVersion: number
}

interface ProfileList<T> {
  profiles: T[]
  config_revision: number
}

interface FamilyList {
  families: ModelFamily[]
  config_revision: number
}

interface ConfigurationSnapshot {
  status: StatusResponse
  transport: ProfileList<TransportProfile>
  profiles: ProfileList<ModelProfile>
  families: FamilyList
}

const CONFIGURATION_SNAPSHOT_ATTEMPTS = 3
const SOCKET_HANDSHAKE_TIMEOUT_MS = 5_000

interface ConfigurePortsResponse {
  ports: PortSnapshot[]
  config_revision: number
}

interface EventQueryResponse {
  events: Record<string, unknown>[]
}

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  outcomeUncertainOnTransportLoss: boolean
  outcomeUncertainErrorCodes: readonly string[]
}

interface PendingWelcome {
  socket: WebSocket
  expectedServerId: string
  expectedDaemonEpoch: string
  resolve: (actor: Actor) => void
  reject: (reason: Error) => void
}

interface HumanCommandAccepted {
  type: 'human_command_accepted'
  event_seq: number
  mode: 'owned' | 'cooperative'
  interfered_run_id?: string
  context_revision?: number
}

export interface ServerData {
  status: StatusResponse
  actor?: Actor
  availablePorts: PortDescriptor[]
  transportProfiles: TransportProfile[]
  modelProfiles: ModelProfile[]
  modelFamilies: ModelFamily[]
  events: Record<string, TimelineEvent[]>
}

class SerialHttpError extends Error {
  constructor(readonly status: number, detail: string) {
    super(`后端返回 ${status}${detail ? `：${detail}` : ''}`)
  }
}

export class ConfigurationConflictError extends Error {}

export class SerialCommandError extends Error {
  constructor(
    message: string,
    readonly code?: string,
    readonly retryable = false
  ) {
    super(message)
  }
}

export class HumanCommandOutcomeUncertainError extends Error {
  constructor(detail?: string) {
    super(detail ? `${HUMAN_COMMAND_UNCERTAIN_MESSAGE}（${detail}）` : HUMAN_COMMAND_UNCERTAIN_MESSAGE)
    this.name = 'HumanCommandOutcomeUncertainError'
  }
}

export class SerialClient extends EventEmitter {
  readonly endpoint: string
  private status?: StatusResponse
  private availablePorts: PortDescriptor[] = []
  private transportProfiles: TransportProfile[] = []
  private modelProfiles: ModelProfile[] = []
  private modelFamilies: ModelFamily[] = []
  private readonly events = new Map<string, TimelineEvent[]>()
  private socket?: WebSocket
  private readySocket?: WebSocket
  private stopped = false
  private connectionVersion = 0
  private actor?: Actor
  private readonly pending = new Map<string, PendingRequest>()
  private pendingWelcome?: PendingWelcome
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
      await this.healthIdentity()
      return true
    } catch {
      return false
    }
  }

  async healthIdentity(): Promise<SerialdHealthIdentity> {
    return parseSerialdHealth(
      await this.request('/api/v1/health', { timeout: 1_500 })
    )
  }

  async start(): Promise<ServerData> {
    this.stopped = false
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    await this.refresh(true)
    await this.openSocket(this.connectionVersion)
    return this.data()
  }

  async refresh(loadHistory = false): Promise<ServerData> {
    const [availablePorts, configuration] = await Promise.all([
      this.get<PortDescriptor[]>('/api/v1/ports'),
      this.loadConsistentConfiguration()
    ])
    const { status, transport, profiles, families } = configuration
    assertCompatibleProtocol(status.protocol_version)
    this.reconcileEventEpochs(status)
    this.status = status
    this.availablePorts = availablePorts
    this.transportProfiles = transport.profiles
    this.modelProfiles = profiles.profiles
    this.modelFamilies = families.families
    if (loadHistory) {
      await Promise.all(status.ports.map((configured) => this.loadHistory(configured)))
    }
    return this.data()
  }

  data(): ServerData {
    if (!this.status) throw new Error('尚未连接后端')
    return {
      status: this.status,
      actor: this.actor ? structuredClone(this.actor) : undefined,
      availablePorts: [...this.availablePorts],
      transportProfiles: structuredClone(this.transportProfiles),
      modelProfiles: structuredClone(this.modelProfiles),
      modelFamilies: structuredClone(this.modelFamilies),
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

  async saveSerialConfiguration(
    draft: SerialConfigurationDraft,
    expectedRevision: number
  ): Promise<void> {
    const { status, transport: catalog } = await this.loadConsistentConfiguration()
    if (status.config_revision !== expectedRevision) {
      await this.raiseConfigurationConflict()
    }
    const boundBefore = new Set(status.ports.map((configured) => configured.config.transport_profile).filter(Boolean))
    const stage = stageTransportCatalog(draft.port, draft.transportProfile, catalog.profiles, boundBefore)
    const existing = status.ports.find((configured) => configured.config.port === draft.port)?.config
    const next = configuredPortFromDraft(draft, stage.selected.name, existing)
    const ports = status.ports.some((configured) => configured.config.port === draft.port)
      ? status.ports.map((configured) => (configured.config.port === draft.port ? next : configured.config))
      : [...status.ports.map((configured) => configured.config), next]
    let switched: ConfigurePortsResponse
    try {
      const staged = sameProfileCatalog(stage.profiles, catalog.profiles)
        ? { config_revision: status.config_revision }
        : await this.put<ProfileList<TransportProfile>>('/api/v1/config/transport-profiles', {
            profiles: stage.profiles,
            expected_revision: status.config_revision
          })
      switched = await this.put<ConfigurePortsResponse>('/api/v1/config/ports', {
        ports,
        source: 'human:desktop',
        expected_revision: staged.config_revision
      })
    } catch (error) {
      return this.recoverConfigurationConflict(error)
    }
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

  async saveModelProfiles(profiles: ModelProfile[], expectedRevision: number): Promise<void> {
    try {
      await this.put('/api/v1/config/model-profiles', {
        profiles,
        expected_revision: expectedRevision
      })
    } catch (error) {
      await this.recoverConfigurationConflict(error)
    }
    await this.refresh()
  }

  async saveModelFamilies(families: ModelFamily[], expectedRevision: number): Promise<void> {
    try {
      await this.put('/api/v1/config/model-families', {
        families,
        expected_revision: expectedRevision
      })
    } catch (error) {
      await this.recoverConfigurationConflict(error)
    }
    await this.refresh()
  }

  async sendCommand(port: string, command: string): Promise<void> {
    const socket = this.readySocket
    if (!socket || this.socket !== socket || socket.readyState !== WebSocket.OPEN) {
      throw new Error('实时连接尚未建立')
    }
    const configured = this.status?.ports.find((item) => item.config.port === port)
    if (!configured) throw new Error(`未找到串口 ${port}`)
    const eol = configured?.effective_write_eol ?? '\r'
    const result = await this.control(buildHumanCommandMessage({
      requestId: randomUUID(),
      operationId: randomUUID(),
      port,
      expectedGeneration: configured.generation,
      data: Buffer.from(`${command}${eol}`).toString('base64')
    }), socket, {
      outcomeUncertainOnTransportLoss: true,
      outcomeUncertainErrorCodes: ['write_outcome_uncertain']
    }) as HumanCommandAccepted
    if (result?.type !== 'human_command_accepted') {
      throw new HumanCommandOutcomeUncertainError('后端返回了未知的人工命令结果')
    }
    if (result.mode === 'cooperative') {
      this.emit('notice', '人工命令已介入 Agent Run；Agent 必须先读取最新串口输出后才能继续。')
    }
  }

  async decideRunStart(port: string, approvalId: string, decision: RunStartDecision): Promise<void> {
    const configured = this.status?.ports.find((item) => item.config.port === port)
    const approval = configured?.pending_run_start
    if (!configured || !approval || approval.id !== approvalId || !isApprovalActionable(configured, approval, this.actor)) {
      throw new Error('该 Run 启动审批已失效，请等待最新状态')
    }
    const result = await this.control(buildRunStartDecisionMessage(
      randomUUID(),
      port,
      approvalId,
      decision
    )) as { type?: string; approval_id?: string }
    if (![
      'run_start_granted',
      'run_start_denied',
      'run_start_timed_out',
      'run_start_cancelled'
    ].includes(String(result?.type))) throw new Error('后端返回了未知的 Run 启动审批结果')
    if (configured.pending_run_start?.id === approvalId) configured.pending_run_start = null
    this.emit('snapshot', configured)
  }

  async stop(): Promise<void> {
    this.stopped = true
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    const socket = this.socket
    this.socket = undefined
    this.readySocket = undefined
    if (socket?.readyState === WebSocket.OPEN) {
      socket.close()
    } else if (socket && socket.readyState !== WebSocket.CLOSED) {
      socket.terminate()
    }
    const error = new Error('实时连接已关闭')
    if (socket) this.rejectSocketHandshake(socket, error)
    this.rejectPending(error)
    this.actor = undefined
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
    if (history.some((event) => (
      event.port !== configured.config.port
      || event.daemon_epoch !== configured.daemon_epoch
      || event.seq > configured.head_seq
    ))) {
      throw new Error(`串口 ${configured.config.port} 的历史响应跨越了后端周期或查询上界`)
    }
    this.events.set(configured.config.port, history)
  }

  private async openSocket(version = this.connectionVersion): Promise<void> {
    if (!this.status || this.stopped) return
    const expectedStatus = this.status
    const socket = new WebSocket(this.endpoint.replace(/^http/, 'ws') + '/api/v1/ws')
    socket.binaryType = 'nodebuffer'
    this.socket = socket
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => finish(new Error('实时连接超时')), SOCKET_HANDSHAKE_TIMEOUT_MS)
        const onOpen = (): void => finish()
        const onError = (error: Error): void => finish(error)
        const onClose = (): void => finish(new Error('实时连接在握手前关闭'))
        const finish = (error?: Error): void => {
          clearTimeout(timer)
          socket.off('open', onOpen)
          socket.off('error', onError)
          socket.off('close', onClose)
          if (error) reject(error)
          else resolve()
        }
        socket.once('open', onOpen)
        socket.once('error', onError)
        socket.once('close', onClose)
      })

      if (this.stopped || version !== this.connectionVersion || this.socket !== socket) {
        throw new Error('实时连接握手已取消')
      }
      socket.on('message', (data) => this.onSocketMessage(socket, Buffer.from(data as Buffer)))
      socket.on('close', () => this.onSocketClose(socket))
      socket.on('error', (error) => {
        this.emit('error', error)
        if (this.readySocket !== socket) {
          const failure = asError(error)
          this.rejectSocketHandshake(socket, failure)
          this.rejectPending(failure)
        }
      })

      const helloRequestId = randomUUID()
      const welcome = this.waitForWelcome(socket, expectedStatus)
      const helloResult = this.control({
        type: 'hello',
        request_id: helloRequestId,
        protocol_version: SERIAL_PROTOCOL_VERSION,
        client_name: 'serial-platform-desktop',
        actor_kind: 'human'
      }, socket)
      const [actor, accepted] = await withTimeout(
        Promise.all([welcome, helloResult]),
        SOCKET_HANDSHAKE_TIMEOUT_MS,
        '后端未确认 Hello'
      )
      const acceptedActor = actorValue((accepted as Record<string, unknown>)?.actor)
      if ((accepted as Record<string, unknown>)?.type !== 'hello_accepted' || acceptedActor?.id !== actor.id) {
        throw new Error('后端返回了与 Welcome 不一致的 Hello 结果')
      }
      if (this.status !== expectedStatus) throw new Error('后端状态在实时握手期间发生变化')

      const subscriptions = expectedStatus.ports.map((configured) => ({
        port: configured.config.port,
        cursor: this.authoritativeCursor(configured),
        tail_events: 1000
      }))
      const attachResult = await withTimeout(this.control({
        type: 'attach',
        request_id: randomUUID(),
        subscriptions
      }, socket), SOCKET_HANDSHAKE_TIMEOUT_MS, '后端未确认 Attach') as Record<string, unknown>
      const attached = Array.isArray(attachResult?.ports)
        ? attachResult.ports.map(String).sort()
        : undefined
      const expectedPorts = subscriptions.map((subscription) => subscription.port).sort()
      if (attachResult?.type !== 'attached' || !attached || !sameStrings(attached, expectedPorts)) {
        throw new Error('后端未确认完整的权威端口订阅')
      }
      if (
        this.stopped
        || version !== this.connectionVersion
        || this.socket !== socket
        || this.status !== expectedStatus
      ) {
        throw new Error('实时连接握手已取消')
      }
      this.actor = actor
      this.readySocket = socket
      this.emit('snapshot')
      this.emit('connected')
    } catch (error) {
      const failure = asError(error)
      if (this.socket === socket) this.socket = undefined
      if (this.readySocket === socket) this.readySocket = undefined
      this.actor = undefined
      this.rejectSocketHandshake(socket, failure)
      this.rejectPending(failure)
      socket.removeAllListeners()
      socket.on('error', () => undefined)
      if (socket.readyState !== WebSocket.CLOSED) socket.terminate()
      throw failure
    }
  }

  private onSocketMessage(socket: WebSocket, data: Buffer): void {
    if (this.socket !== socket) return
    try {
      const frame = decodeFrame(data)
      if (frame.kind === 'timeline') {
        this.appendEvent(frame.event)
        return
      }
      this.handleControl(socket, frame.message)
    } catch (error) {
      const failure = asError(error)
      this.emit('error', failure)
      if (this.readySocket === socket) socket.terminate()
      else {
        this.rejectSocketHandshake(socket, failure)
        this.rejectPending(failure)
        socket.terminate()
      }
    }
  }

  private handleControl(socket: WebSocket, message: WireControl): void {
    if (message.type === 'welcome') {
      const pending = this.pendingWelcome
      if (!pending || pending.socket !== socket) throw new Error('后端发送了非预期的 Welcome')
      const actor = actorValue(message.actor)
      if (
        message.protocol_version !== SERIAL_PROTOCOL_VERSION
        || message.server_id !== pending.expectedServerId
        || message.daemon_epoch !== pending.expectedDaemonEpoch
        || actor?.kind !== 'human'
      ) {
        throw new Error('WebSocket Welcome 与已验证的后端身份不一致')
      }
      this.pendingWelcome = undefined
      pending.resolve(actor)
      return
    }
    if (message.type === 'snapshot') {
      const configured = message.port as PortSnapshot
      this.assertSnapshotIdentity(configured)
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
      this.assertTimelineIdentity(event)
      this.observeTimelineState(event)
      this.appendEvent(event)
      return
    }
    if (message.type === 'result') {
      const requestId = String(message.request_id ?? '')
      const result = message.result as Record<string, unknown>
      const pending = this.pending.get(requestId)
      this.pending.delete(requestId)
      pending?.resolve(result)
      return
    }
    if (message.type === 'error') {
      const requestId = message.request_id ? String(message.request_id) : undefined
      const code = message.code ? String(message.code) : undefined
      const error = new SerialCommandError(
        String(message.message ?? '后端拒绝请求'),
        code,
        Boolean(message.retryable)
      )
      if (requestId) {
        const pending = this.pending.get(requestId)
        this.pending.delete(requestId)
        if (pending) {
          pending.reject(code && pending.outcomeUncertainErrorCodes.includes(code)
            ? new HumanCommandOutcomeUncertainError(error.message)
            : error)
        }
        else throw error
      } else {
        throw error
      }
      return
    }
    if (message.type === 'gap' || message.type === 'lagged') {
      this.emit('notice', `${String(message.port)} 的实时记录存在缺口`)
    }
  }

  private appendEvent(event: TimelineEvent): void {
    this.assertTimelineIdentity(event)
    let items = this.events.get(event.port) ?? []
    if (items.some((item) => item.daemon_epoch !== event.daemon_epoch)) {
      items = []
    }
    const last = items.at(-1)
    if (last && last.seq >= event.seq) return
    items.push(event)
    if (items.length > 12_000) items.splice(0, items.length - 12_000)
    this.events.set(event.port, items)
    this.emit('timeline', event)
  }

  private observeTimelineState(event: TimelineEvent): void {
    const configured = this.status?.ports.find((item) => item.config.port === event.port)
    if (!configured) return
    if (event.kind === 'run_start_requested') {
      const approval = pendingRunStartApproval(event.metadata.approval)
      if (approval) {
        configured.pending_run_start = approval
        this.emit('snapshot', configured)
      }
      return
    }
    if ([
      'run_start_approved',
      'run_start_denied',
      'run_start_timed_out',
      'run_start_cancelled'
    ].includes(event.kind)) {
      const approvalId = typeof event.metadata.approval_id === 'string'
        ? event.metadata.approval_id
        : pendingRunStartApproval(event.metadata.approval)?.id
      if (!approvalId || configured.pending_run_start?.id === approvalId) {
        configured.pending_run_start = null
        this.emit('snapshot', configured)
      }
    }
  }

  private reconcileEventEpochs(next: StatusResponse): void {
    for (const configured of next.ports) {
      if (configured.daemon_epoch !== next.daemon_epoch) {
        throw new Error(`串口 ${configured.config.port} 的 snapshot 与后端周期不一致`)
      }
    }
    const previous = this.status
    if (
      previous
      && (previous.server_id !== next.server_id || previous.daemon_epoch !== next.daemon_epoch)
    ) {
      this.events.clear()
    }
    const configuredPorts = new Set(next.ports.map((configured) => configured.config.port))
    for (const port of this.events.keys()) {
      if (!configuredPorts.has(port)) this.events.delete(port)
    }
    for (const configured of next.ports) {
      const items = this.events.get(configured.config.port)
      if (items?.some((event) => event.daemon_epoch !== configured.daemon_epoch)) {
        this.events.delete(configured.config.port)
      }
    }
  }

  private authoritativeCursor(configured: PortSnapshot): { epoch: string; after_seq: number } {
    let items = this.events.get(configured.config.port)
    if (items?.some((event) => event.daemon_epoch !== configured.daemon_epoch)) {
      this.events.delete(configured.config.port)
      items = undefined
    }
    const seq = items?.at(-1)?.seq
    return {
      epoch: configured.daemon_epoch,
      after_seq: typeof seq === 'number' && Number.isSafeInteger(seq) && seq >= 0 ? seq : 0
    }
  }

  private assertSnapshotIdentity(configured: PortSnapshot): void {
    if (!configured?.config?.port || !this.status) throw new Error('后端发送了无效的端口 snapshot')
    if (configured.daemon_epoch !== this.status.daemon_epoch) {
      throw new Error(`串口 ${configured.config.port} 的 snapshot 来自另一后端周期`)
    }
  }

  private assertTimelineIdentity(event: TimelineEvent): void {
    if (!this.status || event.daemon_epoch !== this.status.daemon_epoch) {
      throw new Error(`串口 ${event.port} 的 timeline 来自另一后端周期`)
    }
    const configured = this.status.ports.find((item) => item.config.port === event.port)
    if (configured && event.daemon_epoch !== configured.daemon_epoch) {
      throw new Error(`串口 ${event.port} 的 timeline 与权威 snapshot 周期不一致`)
    }
  }

  private waitForWelcome(socket: WebSocket, expected: StatusResponse): Promise<Actor> {
    if (this.pendingWelcome) throw new Error('已有未完成的 WebSocket Welcome')
    return new Promise((resolve, reject) => {
      this.pendingWelcome = {
        socket,
        expectedServerId: expected.server_id,
        expectedDaemonEpoch: expected.daemon_epoch,
        resolve,
        reject
      }
    })
  }

  private rejectSocketHandshake(socket: WebSocket, error: Error): void {
    if (this.pendingWelcome?.socket !== socket) return
    const pending = this.pendingWelcome
    this.pendingWelcome = undefined
    pending.reject(error)
  }

  private control(
    message: WireControl,
    socket = this.socket,
    options: {
      outcomeUncertainOnTransportLoss?: boolean
      outcomeUncertainErrorCodes?: readonly string[]
    } = {}
  ): Promise<unknown> {
    if (!socket || this.socket !== socket || socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('实时连接尚未建立'))
    }
    const requestId = String(message.request_id)
    return new Promise((resolve, reject) => {
      const outcomeUncertainOnTransportLoss = Boolean(options.outcomeUncertainOnTransportLoss)
      this.pending.set(requestId, {
        resolve,
        reject,
        outcomeUncertainOnTransportLoss,
        outcomeUncertainErrorCodes: options.outcomeUncertainErrorCodes ?? []
      })
      socket.send(encodeControl(message), (error) => {
        if (!error) return
        this.pending.delete(requestId)
        reject(outcomeUncertainOnTransportLoss
          ? new HumanCommandOutcomeUncertainError(asError(error).message)
          : error)
      })
    })
  }

  private async reconnectSocket(): Promise<void> {
    this.connectionVersion += 1
    this.reconnectLoop.cancel()
    const socket = this.socket
    this.socket = undefined
    this.readySocket = undefined
    if (socket) socket.close()
    const error = new Error('配置已更新，正在重建实时连接')
    if (socket) this.rejectSocketHandshake(socket, error)
    this.rejectPending(error)
    this.actor = undefined
    try {
      await this.openSocket(this.connectionVersion)
    } catch (error) {
      if (!this.stopped) this.reconnectLoop.schedule()
      throw error
    }
  }

  private onSocketClose(socket: WebSocket): void {
    if (this.stopped || this.socket !== socket) return
    const wasReady = this.readySocket === socket
    this.socket = undefined
    if (wasReady) this.readySocket = undefined
    const error = new Error('实时连接中断')
    this.rejectSocketHandshake(socket, error)
    this.rejectPending(error)
    this.actor = undefined
    if (wasReady) {
      this.emit('disconnected')
      this.emit('snapshot')
    }
    this.reconnectLoop.schedule()
  }

  private async reconnectAttempt(): Promise<void> {
    const version = this.connectionVersion
    await this.refresh()
    if (this.stopped || version !== this.connectionVersion) return
    await this.openSocket(version)
  }

  private rejectPending(error: Error): void {
    for (const pending of this.pending.values()) {
      pending.reject(pending.outcomeUncertainOnTransportLoss
        ? new HumanCommandOutcomeUncertainError(error.message)
        : error)
    }
    this.pending.clear()
  }

  private async loadConsistentConfiguration(): Promise<ConfigurationSnapshot> {
    for (let attempt = 0; attempt < CONFIGURATION_SNAPSHOT_ATTEMPTS; attempt += 1) {
      const [status, transport, profiles, families] = await Promise.all([
        this.get<StatusResponse>('/api/v1/status'),
        this.get<ProfileList<TransportProfile>>('/api/v1/config/transport-profiles'),
        this.get<ProfileList<ModelProfile>>('/api/v1/config/model-profiles'),
        this.get<FamilyList>('/api/v1/config/model-families')
      ])
      const revision = status.config_revision
      if (
        transport.config_revision === revision
        && profiles.config_revision === revision
        && families.config_revision === revision
      ) {
        return { status, transport, profiles, families }
      }
    }
    throw new Error('后端配置正在持续变化，无法获取一致快照，请重试')
  }

  private async recoverConfigurationConflict(error: unknown): Promise<never> {
    if (!(error instanceof SerialHttpError) || error.status !== 409) throw error
    return this.raiseConfigurationConflict()
  }

  private async raiseConfigurationConflict(): Promise<never> {
    try {
      await this.refresh()
    } catch (refreshError) {
      const detail = refreshError instanceof Error ? refreshError.message : String(refreshError)
      throw new ConfigurationConflictError(`配置已被其他操作更新，自动刷新失败：${detail}`)
    }
    throw new ConfigurationConflictError('配置已被其他操作更新，App 已刷新到最新内容，请重新确认后保存')
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
      throw new SerialHttpError(response.status, detail)
    }
    return response.json()
  }
}

function pendingRunStartApproval(value: unknown): PendingRunStartApproval | undefined {
  if (!value || typeof value !== 'object') return undefined
  const approval = value as Record<string, unknown>
  const requester = actorValue(approval.requester)
  const requiredApprover = actorValue(approval.required_approver)
  if (
    typeof approval.id !== 'string'
    || typeof approval.port !== 'string'
    || typeof approval.label !== 'string'
    || typeof approval.daemon_epoch !== 'string'
    || typeof approval.expected_control_id !== 'string'
    || !requester
    || !requiredApprover
  ) return undefined
  return {
    id: approval.id,
    port: approval.port,
    requester,
    required_approver: requiredApprover,
    label: approval.label,
    metadata: approval.metadata && typeof approval.metadata === 'object'
      ? approval.metadata as Record<string, unknown>
      : {},
    control_ttl_ms: finiteNumber(approval.control_ttl_ms),
    daemon_epoch: approval.daemon_epoch,
    generation: finiteNumber(approval.generation),
    expected_control_id: approval.expected_control_id,
    expected_fence: finiteNumber(approval.expected_fence),
    requested_wall_time_ns: finiteNumber(approval.requested_wall_time_ns),
    expires_wall_time_ns: finiteNumber(approval.expires_wall_time_ns)
  }
}

function actorValue(value: unknown): Actor | undefined {
  if (!value || typeof value !== 'object') return undefined
  const actor = value as Record<string, unknown>
  if (
    typeof actor.id !== 'string'
    || typeof actor.label !== 'string'
    || !['human', 'agent', 'script', 'system'].includes(String(actor.kind))
  ) return undefined
  return { id: actor.id, label: actor.label, kind: actor.kind as Actor['kind'] }
}

function finiteNumber(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs)
      })
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function sameStrings(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

export function buildHumanCommandMessage(value: {
  requestId: string
  operationId: string
  port: string
  expectedGeneration: number
  data: string
}): WireControl {
  return {
    type: 'send_human_command',
    request_id: value.requestId,
    port: value.port,
    expected_generation: value.expectedGeneration,
    data: value.data,
    operation_id: value.operationId,
    description: null
  }
}

export function buildRunStartDecisionMessage(
  requestId: string,
  port: string,
  approvalId: string,
  decision: RunStartDecision
): WireControl {
  return {
    type: 'decide_run_start',
    request_id: requestId,
    port,
    approval_id: approvalId,
    decision
  }
}

export function assertCompatibleProtocol(actual: number): void {
  if (actual !== SERIAL_PROTOCOL_VERSION) {
    throw new Error(
      `组件协议不兼容：App 需要 v${SERIAL_PROTOCOL_VERSION}，后端提供 v${actual}。请使用同一版本的 serial 与 App。`
    )
  }
}

export function parseSerialdHealth(value: unknown): SerialdHealthIdentity {
  if (!value || typeof value !== 'object') throw new Error('后端健康响应格式无效')
  const health = value as Record<string, unknown>
  if (health.status !== 'ok') throw new Error(`后端健康状态无效：${String(health.status)}`)
  if (!validUuid(health.server_id) || !validUuid(health.daemon_epoch)) {
    throw new Error('后端健康响应缺少有效的服务身份')
  }
  if (typeof health.protocol_version !== 'number') {
    throw new Error('后端健康响应缺少组件协议版本')
  }
  assertCompatibleProtocol(health.protocol_version)
  return {
    serverId: health.server_id,
    daemonEpoch: health.daemon_epoch,
    protocolVersion: health.protocol_version
  }
}

export function serialdIdentityMatches(
  actual: SerialdHealthIdentity,
  expected: SerialdHealthIdentity
): boolean {
  return actual.protocolVersion === expected.protocolVersion
    && actual.serverId === expected.serverId
    && actual.daemonEpoch === expected.daemonEpoch
}

function validUuid(value: unknown): value is string {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
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

export function configuredPortFromDraft(
  draft: SerialConfigurationDraft,
  transportProfile: string,
  existing?: PortSnapshot['config']
): PortSnapshot['config'] {
  const modelProfile = draft.modelProfile ?? null
  const modelFamily = draft.modelFamily ?? null
  const modelName = modelFamily === null
    ? null
    : draft.modelName !== undefined
      ? draft.modelName
      : existing?.model_family === modelFamily
        ? existing.model_name ?? null
        : null
  return {
    port: draft.port,
    enabled: draft.enabled,
    transport_profile: transportProfile,
    model_profile: modelProfile,
    model_family: modelFamily,
    model_name: modelName
  }
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
