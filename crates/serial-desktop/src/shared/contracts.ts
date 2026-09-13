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
  shell_prompt?: string | null
  uboot_prompt?: string | null
  write_eol?: string | null
  echo?: 'on' | 'off' | 'auto' | null
  write_chunk_size?: number | null
  write_chunk_delay_ms?: number | null
}

export interface ModelFamily {
  name: string
  model_names: string[]
}

export interface PortConfig {
  port: string
  transport_profile?: string | null
  model_profile?: string | null
  model_family?: string | null
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
  control?: ControlLease | null
  pending_run_start?: PendingRunStartApproval | null
  run_context?: RunContextState | null
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

export interface ControlLease {
  id: string
  owner: Actor
  epoch: string
  generation: number
  fence: number
  issued_wall_time_ns: number
  expires_wall_time_ns: number
}

export interface PendingRunStartApproval {
  id: string
  port: string
  requester: Actor
  required_approver: Actor
  label: string
  metadata: Record<string, unknown>
  control_ttl_ms: number
  daemon_epoch: string
  generation: number
  expected_control_id: string
  expected_fence: number
  requested_wall_time_ns: number
  expires_wall_time_ns: number
}

export interface RunContextState {
  run_id: string
  revision: number
  last_human_command_seq?: number | null
  acknowledged_revision: number
  acknowledged_through_seq?: number | null
}

export type RunStartDecision = 'approve' | 'deny'

export const HUMAN_COMMAND_UNCERTAIN_MESSAGE = '人工命令的物理写入结果不确定。请先查看串口时间线，勿直接重发。'

export type HumanCommandSubmission =
  | { status: 'accepted' }
  | { status: 'rejected'; message: string }
  | { status: 'uncertain'; message: string }

export interface HumanCommandHistory {
  server_id: string
  revision: number
  entries: { id: string; command: string; port: string; wall_time_ns: number; revision: number; uses: number }[]
  next_before_revision?: number | null
  warning?: string | null
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
  stream_offset_start?: number | null
  stream_offset_end?: number | null
  text: string
  metadata: Record<string, unknown>
  durable: boolean
  replay?: boolean
}

export interface MacroParameter {
  type: 'string' | 'integer' | 'boolean'
  default?: string | number | boolean
  minimum?: number
  maximum?: number
  description?: string
}

export interface MacroSummary {
  id: string
  name: string
  description: string
  language_version: number
  revision: number
  parameters: Record<string, MacroParameter>
  shared: boolean
  applies_to?: { model_family: string; model_names: string[] } | null
}

export interface MacroDefinition extends MacroSummary {
  script: string
  updated_at_ns: number
}

export interface MacroListQuery {
  id?: string
  query?: string
  include_drafts?: boolean
  offset?: number
  limit?: number
}

export interface MacroListResponse {
  catalog_revision: number
  macros: MacroSummary[]
  definition?: MacroDefinition | null
  total: number
  next_offset?: number | null
}

export interface MacroSaveRequest {
  id: string
  name: string
  description: string
  parameters: Record<string, MacroParameter>
  script: string
  expected_revision?: number
  shared?: boolean
  applies_to?: MacroSummary['applies_to']
}

export interface MacroRunRequest {
  macro_id?: string
  revision?: number
  script?: string
  description?: string
  args: Record<string, string | number | boolean>
  timeout_seconds: number
}

export interface MacroExecution {
  id: string
  port: string
  daemon_epoch: string
  generation: number
  owner: Actor
  run_id?: string | null
  macro_id?: string | null
  revision?: number | null
  description: string
  status: 'running' | 'stopping' | 'succeeded' | 'timed_out' | 'cancelled' | 'interrupted_by_user' | 'failed'
  started_at_ns: number
  completed_at_ns?: number | null
  line: number
  column: number
  writes: number
  input_verified_writes?: number
  send_only_writes?: number
  bytes_written: number
  first_seq: number
  through_seq: number
  message?: string | null
  outcome_uncertain: boolean
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
  actor?: Actor
  configRevision: number
  configuredPorts: PortSnapshot[]
  availablePorts: PortDescriptor[]
  transportProfiles: TransportProfile[]
  modelProfiles: ModelProfile[]
  modelFamilies: ModelFamily[]
  events: Record<string, TimelineEvent[]>
  humanHistory?: HumanCommandHistory
  preferences: DesktopPreferences
  service: ServiceState
}

export interface SerialConfigurationDraft {
  port: string
  enabled: boolean
  transportProfile: TransportProfile
  modelProfile?: string | null
  modelFamily?: string | null
  modelName?: string | null
}

export interface DesktopBridge {
  bootstrap(): Promise<DesktopSnapshot>
  refresh(): Promise<DesktopSnapshot>
  sendCommand(port: string, command: string): Promise<HumanCommandSubmission>
  sendSignal(port: string, signal: 'ctrl_c' | 'ctrl_d'): Promise<HumanCommandSubmission>
  queryHumanHistory(query: string, contains?: boolean): Promise<HumanCommandHistory>
  listMacros(query: MacroListQuery): Promise<MacroListResponse>
  saveMacro(definition: MacroSaveRequest): Promise<{ catalog_revision: number; definition: MacroDefinition }>
  runMacro(port: string, spec: MacroRunRequest): Promise<MacroExecution>
  cancelMacro(port: string, executionId: string): Promise<MacroExecution>
  decideRunStart(port: string, approvalId: string, decision: RunStartDecision): Promise<void>
  setPortOpen(port: string, open: boolean): Promise<void>
  saveSerialConfiguration(draft: SerialConfigurationDraft, expectedRevision: number): Promise<void>
  saveModelProfiles(profiles: ModelProfile[], expectedRevision: number): Promise<void>
  saveModelFamilies(families: ModelFamily[], expectedRevision: number): Promise<void>
  savePreferences(preferences: DesktopPreferences): Promise<void>
  startLocalService(): Promise<void>
  stopLocalService(): Promise<void>
  onEvent(listener: (event: DesktopEvent) => void): () => void
}

export type DesktopEvent =
  | { type: 'snapshot'; snapshot: DesktopSnapshot }
  | { type: 'timeline'; event: TimelineEvent }
  | { type: 'macro'; execution: MacroExecution }
  | { type: 'connection'; state: ConnectionState; message: string }
  | { type: 'service'; service: ServiceState }
  | { type: 'notice'; message: string }
  | { type: 'error'; message: string }

declare global {
  interface Window {
    serial: DesktopBridge
  }
}
