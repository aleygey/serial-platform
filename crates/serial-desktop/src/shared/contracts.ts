export type ThemePreference = 'system' | 'dark' | 'light'
export type ConnectionState = 'starting' | 'connected' | 'reconnecting' | 'offline'
export type SessionState = 'disabled' | 'waiting_for_port' | 'opening' | 'online' | 'backoff' | 'stopping'

export interface TransportProfile {
  name: string
  baud_rate: number
  data_bits: 'five' | 'six' | 'seven' | 'eight'
  parity: 'none' | 'odd' | 'even'
  stop_bits: 'one' | 'two'
  flow_control: 'none' | 'software' | 'hardware'
  dtr: boolean
  rts: boolean
  auto_open: boolean
}

export interface ModelProfile {
  name: string
  model_names: string[]
  shell_prompt?: string | null
  uboot_prompt?: string | null
  write_eol?: string | null
  echo?: 'on' | 'off' | 'auto' | null
  write_chunk_size?: number | null
  write_chunk_delay_ms?: number | null
}

export interface PortConfig {
  port: string
  transport_profile?: string | null
  model_profile?: string | null
  model_name?: string | null
  enabled: boolean
}

export interface PortSnapshot {
  config: PortConfig
  daemon_epoch: string
  head_seq: number
  generation: number
  endpoint_present: boolean
  session_state: SessionState
  state_reason?: string | null
  effective_shell_prompt?: string | null
  effective_uboot_prompt?: string | null
  effective_write_eol?: string | null
  effective_transport?: Omit<TransportProfile, 'name'> | null
}

export interface PortDescriptor {
  name: string
  port_type: string
  manufacturer?: string | null
  product?: string | null
  serial_number?: string | null
}

export interface Actor {
  id: string
  label: string
  kind: 'human' | 'agent' | 'script' | 'system'
}

export interface TimelineEvent {
  port: string
  daemon_epoch: string
  seq: number
  generation: number
  wall_time_ns: number
  kind: string
  direction: 'rx' | 'tx' | 'none'
  actor?: Actor | null
  run_id?: string | null
  operation_id?: string | null
  text: string
  metadata: Record<string, unknown>
  durable: boolean
  replay?: boolean
}

export interface DesktopPreferences {
  endpoint: string
  autoStartLocal: boolean
  theme: ThemePreference
  selectedPort?: string
}

export interface ServiceState {
  owned: boolean
  pid?: number
  status: 'stopped' | 'starting' | 'running' | 'exited'
  program?: string
}

export interface DesktopSnapshot {
  connection: ConnectionState
  connectionMessage: string
  serverId?: string
  daemonEpoch?: string
  configRevision: number
  configuredPorts: PortSnapshot[]
  availablePorts: PortDescriptor[]
  transportProfiles: TransportProfile[]
  modelProfiles: ModelProfile[]
  events: Record<string, TimelineEvent[]>
  preferences: DesktopPreferences
  service: ServiceState
}

export interface SerialConfigurationDraft {
  port: string
  enabled: boolean
  transportProfile: TransportProfile
  modelProfile?: string | null
  modelName?: string | null
}

export interface DesktopBridge {
  bootstrap(): Promise<DesktopSnapshot>
  refresh(): Promise<DesktopSnapshot>
  sendCommand(port: string, command: string): Promise<void>
  setPortOpen(port: string, open: boolean): Promise<void>
  saveSerialConfiguration(draft: SerialConfigurationDraft): Promise<void>
  saveModelProfiles(profiles: ModelProfile[]): Promise<void>
  savePreferences(preferences: DesktopPreferences): Promise<void>
  startLocalService(): Promise<void>
  stopLocalService(): Promise<void>
  onEvent(listener: (event: DesktopEvent) => void): () => void
}

export type DesktopEvent =
  | { type: 'snapshot'; snapshot: DesktopSnapshot }
  | { type: 'timeline'; event: TimelineEvent }
  | { type: 'connection'; state: ConnectionState; message: string }
  | { type: 'service'; service: ServiceState }
  | { type: 'notice'; message: string }
  | { type: 'error'; message: string }

declare global {
  interface Window {
    serial: DesktopBridge
  }
}
