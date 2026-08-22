import { setTimeout as delay } from 'node:timers/promises'
import type {
  DesktopEvent,
  DesktopPreferences,
  DesktopSnapshot,
  ModelProfile,
  SerialConfigurationDraft,
  TimelineEvent
} from '../shared/contracts'
import { createQaSnapshot } from '../shared/qa-fixture'
import { createOfflineSnapshot } from '../shared/offline-snapshot'
import { LocalService } from './local-service'
import { SerialClient, serialdIdentityMatches, type ServerData } from './serial-client'
import { selectLocalEndpoint, type DiscoveredSeriald } from './service-command'
import { SettingsStore } from './settings'
import {
  recoverConcurrentWinner,
  startAndVerifyService,
  waitForOwnedServiceIdentity
} from './service-startup'

export class DesktopCoordinator {
  private readonly emit: (event: DesktopEvent) => void
  private readonly settings = new SettingsStore()
  private readonly service: LocalService
  private preferences?: DesktopPreferences
  private client?: SerialClient
  private connection: DesktopSnapshot['connection'] = 'offline'
  private connectionMessage = '尚未连接'
  private qaMode: boolean
  private qaTheme?: 'dark' | 'light'

  constructor(emit: (event: DesktopEvent) => void, qaMode = false, qaTheme?: 'dark' | 'light') {
    this.emit = emit
    this.qaMode = qaMode
    this.qaTheme = qaTheme
    this.service = new LocalService((service) => this.emit({ type: 'service', service }))
  }

  async bootstrap(): Promise<DesktopSnapshot> {
    this.preferences = await this.settings.load()
    if (this.qaTheme) this.preferences = { ...this.preferences, theme: this.qaTheme }
    if (this.qaMode) return createQaSnapshot(this.preferences)
    return this.connectOrOffline(this.preferences.autoStartLocal)
  }

  async refresh(): Promise<DesktopSnapshot> {
    if (this.qaMode) return createQaSnapshot(this.preferences ?? (await this.settings.load()))
    if (!this.client) {
      const snapshot = await this.connectOrOffline(false)
      this.emit({ type: 'snapshot', snapshot })
      return snapshot
    }
    const data = await this.client.refresh()
    const snapshot = this.toSnapshot(data)
    this.emit({ type: 'snapshot', snapshot })
    return snapshot
  }

  async sendCommand(port: string, command: string): Promise<void> {
    if (this.qaMode) return
    const value = command.replace(/[\r\n]+$/, '')
    if (!value) return
    await this.requireClient().sendCommand(port, value)
  }

  async setPortOpen(port: string, open: boolean): Promise<void> {
    if (this.qaMode) return
    await this.requireClient().setPortOpen(port, open)
    await this.publishSnapshot()
  }

  async saveSerialConfiguration(draft: SerialConfigurationDraft): Promise<void> {
    if (this.qaMode) return
    await this.requireClient().saveSerialConfiguration(draft)
    await this.publishSnapshot()
  }

  async saveModelProfiles(profiles: ModelProfile[]): Promise<void> {
    if (this.qaMode) return
    await this.requireClient().saveModelProfiles(profiles)
    await this.publishSnapshot()
  }

  async savePreferences(preferences: DesktopPreferences): Promise<void> {
    const endpointChanged = this.preferences?.endpoint !== preferences.endpoint
    this.preferences = preferences
    await this.settings.save(preferences)
    if (this.qaMode) return
    if (endpointChanged) {
      await this.client?.stop()
      this.client = undefined
      if (this.service.state().owned) await this.service.stop()
      const snapshot = await this.connectOrOffline(preferences.autoStartLocal)
      this.emit({ type: 'snapshot', snapshot })
    } else {
      await this.publishSnapshot()
    }
  }

  async startLocalService(): Promise<void> {
    if (this.qaMode) return
    const preferences = this.preferences ?? (await this.settings.load())
    const existing = await this.findExistingLocalClient(preferences.endpoint, true)
    if (existing) {
      this.emit({ type: 'notice', message: '已连接该地址上的现有服务，App 不会接管它的进程' })
    } else {
      await this.startServiceOrRecoverWinner(preferences.endpoint)
    }
    if (!this.client) {
      const snapshot = await this.connectOrOffline(false)
      this.emit({ type: 'snapshot', snapshot })
      if (snapshot.connection !== 'connected') throw new Error(snapshot.connectionMessage)
    }
  }

  async stopLocalService(): Promise<void> {
    if (this.qaMode) return
    if (!this.service.state().owned) {
      this.emit({ type: 'notice', message: '当前服务不是由 App 启动，未执行停止' })
      return
    }
    await this.client?.stop()
    this.client = undefined
    await this.service.stop()
    this.setConnection('offline', '本地服务已停止')
  }

  async shutdown(): Promise<void> {
    await this.client?.stop()
    if (this.service.state().owned) await this.service.stop()
  }

  private async connect(startLocal: boolean): Promise<DesktopSnapshot> {
    const preferences = this.preferences ?? (await this.settings.load())
    this.preferences = preferences
    this.setConnection('starting', '正在连接 Serial Platform…')
    let client: SerialClient | undefined
    try {
      client = await this.findExistingLocalClient(preferences.endpoint, startLocal)
      if (!client) {
        if (!startLocal) throw new Error(`无法连接后端 ${preferences.endpoint}`)
        client = await this.startServiceOrRecoverWinner(preferences.endpoint)
      }
      this.bindClient(client)
      this.client = client
      const data = await client.start()
      this.setConnection('connected', '实时连接已建立')
      return this.toSnapshot(data)
    } catch (error) {
      await client?.stop()
      this.client = undefined
      this.setConnection('offline', errorMessage(error))
      throw error
    }
  }

  private async connectOrOffline(startLocal: boolean): Promise<DesktopSnapshot> {
    try {
      return await this.connect(startLocal)
    } catch (error) {
      const message = errorMessage(error)
      if (this.connection !== 'offline' || this.connectionMessage !== message) {
        this.setConnection('offline', message)
      }
      return createOfflineSnapshot(this.preferences!, this.service.state(), message)
    }
  }

  private bindClient(client: SerialClient): void {
    client.on('timeline', (event: TimelineEvent) => this.emit({ type: 'timeline', event }))
    client.on('snapshot', () => void this.publishSnapshot())
    client.on('connected', () => this.setConnection('connected', '实时连接已建立'))
    client.on('disconnected', () => this.setConnection('reconnecting', '实时连接中断，正在重连…'))
    client.on('notice', (message: string) => this.emit({ type: 'notice', message }))
    client.on('error', (error: unknown) => this.emit({ type: 'error', message: errorMessage(error) }))
  }

  private async findExistingLocalClient(
    preferred: string,
    discoverActive: boolean
  ): Promise<SerialClient | undefined> {
    const clients = new Map<string, SerialClient>()
    let preferredError: unknown
    const reachable = async (endpoint: string, expected?: DiscoveredSeriald): Promise<boolean> => {
      try {
        const client = clients.get(endpoint) ?? new SerialClient(endpoint)
        clients.set(endpoint, client)
        const identity = await client.healthIdentity()
        if (expected && !serialdIdentityMatches(identity, expected)) {
          throw new Error(`发现记录与 ${endpoint} 的后端身份不一致`)
        }
        return true
      } catch (error) {
        if (endpoint !== preferred) throw error
        preferredError = error
        return false
      }
    }
    const selection = discoverActive
      ? await selectLocalEndpoint(preferred, reachable, () => this.service.discoverEndpoint())
      : (await reachable(preferred) ? { endpoint: preferred, discovered: false } : undefined)
    if (!selection) {
      if (preferredError) throw preferredError
      return undefined
    }
    if (selection.discovered) {
      this.preferences = { ...(this.preferences ?? (await this.settings.load())), endpoint: selection.endpoint }
      await this.settings.save(this.preferences)
      this.emit({
        type: 'notice',
        message: `发现当前数据目录的 seriald，已改用 ${selection.endpoint}`
      })
    }
    return clients.get(selection.endpoint)
  }

  private async startServiceOrRecoverWinner(preferred: string): Promise<SerialClient> {
    const probe = new SerialClient(preferred)
    try {
      await startAndVerifyService(this.service, preferred, () => this.waitUntilOwnedService(probe))
      return probe
    } catch (error) {
      const winner = await recoverConcurrentWinner(error, () => (
        this.findDiscoveredLocalClient(preferred)
      ))
      this.emit({
        type: 'notice',
        message: `另一启动器已先启动 seriald，App 已连接 ${winner.endpoint}`
      })
      return winner
    }
  }

  private async findDiscoveredLocalClient(preferred: string): Promise<SerialClient | undefined> {
    const discovered = await this.service.discoverEndpoint()
    if (!discovered) return undefined
    const client = new SerialClient(discovered.endpoint)
    const identity = await client.healthIdentity()
    if (!serialdIdentityMatches(identity, discovered)) {
      throw new Error(`发现记录与 ${discovered.endpoint} 的后端身份不一致`)
    }
    if (discovered.endpoint !== preferred) {
      this.preferences = {
        ...(this.preferences ?? (await this.settings.load())),
        endpoint: discovered.endpoint
      }
      await this.settings.save(this.preferences)
    }
    return client
  }

  private async waitUntilOwnedService(probe: SerialClient): Promise<void> {
    await waitForOwnedServiceIdentity(
      this.service,
      () => this.service.discoverEndpoint(),
      async (discovered) => {
        if (discovered.endpoint !== probe.endpoint) return false
        try {
          return serialdIdentityMatches(await probe.healthIdentity(), discovered)
        } catch {
          return false
        }
      },
      40,
      () => delay(150)
    )
  }

  private setConnection(state: DesktopSnapshot['connection'], message: string): void {
    this.connection = state
    this.connectionMessage = message
    this.emit({ type: 'connection', state, message })
  }

  private async publishSnapshot(): Promise<void> {
    if (!this.client) return
    const snapshot = this.toSnapshot(this.client.data())
    this.emit({ type: 'snapshot', snapshot })
  }

  private toSnapshot(data: ServerData): DesktopSnapshot {
    return {
      connection: this.connection,
      connectionMessage: this.connectionMessage,
      serverId: data.status.server_id,
      daemonEpoch: data.status.daemon_epoch,
      configRevision: data.status.config_revision,
      configuredPorts: data.status.ports,
      availablePorts: data.availablePorts,
      transportProfiles: data.transportProfiles,
      modelProfiles: data.modelProfiles,
      events: data.events,
      preferences: this.preferences!,
      service: this.service.state()
    }
  }

  private requireClient(): SerialClient {
    if (!this.client) throw new Error('尚未连接 Serial Platform 后端')
    return this.client
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
