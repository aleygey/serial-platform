import type { DesktopBridge, DesktopEvent, DesktopPreferences, MacroDefinition, MacroExecution, TimelineEvent } from '../shared/contracts'
import { previewMacro, previewRunEvents } from './preview-run'
import { createQaSnapshot } from '../shared/qa-fixture'
import { paletteId } from '../shared/appearance'
import { setupChanges, type SetupRequest, type SetupPlan } from '../shared/first-setup'

export function installQaBridge(): boolean {
  const runtimeWindow = window as unknown as { serial?: DesktopBridge; location: Location }
  if (runtimeWindow.serial) return true
  const params = new URLSearchParams(runtimeWindow.location.search)
  const demo = document.documentElement.hasAttribute('data-serial-preview') || params.get('preview') === '1'
  if (!demo && params.get('qa') !== '1') return false
  if (demo && params.get('focus') === 'agent') document.documentElement.dataset.serialFocus = 'agent'
  const theme = params.get('theme')
  let saved: Pick<Partial<DesktopPreferences>, 'palette' | 'theme' | 'firstUse'> = {}
  if (demo) {
    try { saved = JSON.parse(window.localStorage.getItem('serial-preview-appearance') ?? '{}') ?? {} } catch { /* Storage may be disabled in an embedded preview. */ }
  }
  const preferences: DesktopPreferences = {
    endpoint: 'http://127.0.0.1:3210', autoStartLocal: true,
    theme: theme === 'light' || theme === 'dark' ? theme : saved.theme === 'dark' || saved.theme === 'system' ? saved.theme : demo ? 'light' : 'system',
    palette: paletteId(saved.palette), selectedPort: demo ? 'COM7' : 'COM6',
    firstUse: saved.firstUse ?? (document.documentElement.hasAttribute('data-serial-preview') && document.documentElement.dataset.serialFocus !== 'agent' ? 'new' : 'dismissed')
  }
  let snapshot = createQaSnapshot(preferences)
  const listeners = new Set<(event: DesktopEvent) => void>()
  const publish = (): void => listeners.forEach((listener) => listener({ type: 'snapshot', snapshot }))
  const human = { id: 'preview:human', label: '本机', kind: 'human' as const }
  snapshot.actor = human
  const sensor = snapshot.configuredPorts.find(item => item.config.port === 'COM7')!
  const recording = demo ? previewRunEvents(snapshot) : []
  if (demo) {
    const initial = recording.slice(0, -3)
    snapshot.events.COM7 = initial
    sensor.head_seq = initial.at(-1)!.seq
    const router = snapshot.configuredPorts.find(item => item.config.port === 'COM6')!
    const base = snapshot.events.COM6.at(-1)!
    const tx = { ...base, kind: 'tx', direction: 'tx' as const, actor: sensor.control!.owner, operation_id: 'aux-probe', seq: base.seq + 1,
      text: 'aux status\r', metadata: { command_description: '查询扩展模块' } }
    const rx = { ...base, seq: tx.seq + 1, text: 'aux status\n' }
    const timeout = { ...base, kind: 'command_capture_completed', direction: 'none' as const, seq: rx.seq + 1, text: '', operation_id: tx.operation_id,
      metadata: { capture: { daemon_epoch: router.daemon_epoch, generation: router.generation, operation_id: tx.operation_id,
        tx_event_seq: tx.seq, evidence_from_seq: tx.seq, evidence_through_seq: rx.seq, completion: 'timeout', confidence: 'partial' } } }
    snapshot.events.COM6 = [...snapshot.events.COM6, tx, rx, timeout]
    router.head_seq = timeout.seq
  }
  let playback: number | undefined
  let macroTimer: number | undefined
  let execution: MacroExecution | undefined
  const macros = new Map<string, MacroDefinition>(demo ? [[previewMacro.id, previewMacro]] : [])
  const tasks = new Map<string, { task: SetupRequest; readyAt: number }>()
  const checks = new Map<string, SetupPlan>()
  const appliedTasks: SetupRequest[] = []
  function applyPreviewTask(task: SetupRequest, expectedRevision?: number): MacroDefinition {
    const changes = setupChanges(task, snapshot.configuredPorts.map(p => p.config), snapshot.modelProfiles)
    if (!changes.alreadyBound && expectedRevision !== undefined && expectedRevision !== snapshot.configRevision) throw new Error('串口配置已改变，请重新确认。')
    const previous = macros.get(changes.macro.id)
    if (previous && previous.script !== changes.macro.script) throw new Error('宏已被编辑，请重新生成任务。')
    const definition = previous ?? { ...changes.macro, language_version: 1, revision: 1, shared: true, updated_at_ns: Date.now() * 1_000_000 }
    macros.set(definition.id, definition)
    checks.set(definition.id, task.plan!)
    snapshot = { ...snapshot, modelProfiles: changes.profiles, configRevision: snapshot.configRevision + (changes.alreadyBound ? 0 : 1),
      configuredPorts: snapshot.configuredPorts.map(p => p.config.port === task.brief.port ? { ...p, config: changes.ports.find(c => c.port === task.brief.port)!, effective_shell_prompt: task.plan!.shell_prompt } : p) }
    if (!appliedTasks.some(t => t.id === task.id)) appliedTasks.push(task)
    return definition
  }
  if (demo) {
    try {
      const stored: SetupRequest[] = JSON.parse(window.localStorage.getItem('serial-preview-setups') ?? '[]')
      for (const task of stored) if (task.plan) applyPreviewTask(task)
    } catch { /* Embedded previews may have no storage. */ }
  }
  const echoedPrompt = (port: string) => port === 'COM7' ? 'sensor> ' : port === 'COM6' ? 'root@router:~# ' : 'uart> '
  const hidden = new Map<string, string[]>()
  let visibilityRevision = 0
  const online = (port: string) => snapshot.connection === 'connected' && snapshot.configuredPorts.some(item => item.config.port === port && item.session_state === 'online')
  function append(port: string, input: Pick<TimelineEvent, 'kind' | 'text' | 'direction'> & Partial<TimelineEvent>) {
    const current = snapshot.configuredPorts.find(item => item.config.port === port)!
    const seq = current.head_seq + 1
    const entry: TimelineEvent = { durable: true, metadata: {}, daemon_epoch: current.daemon_epoch, generation: current.generation,
      ...input, port, seq, wall_time_ns: Date.now() * 1_000_000 }
    snapshot = { ...snapshot, events: { ...snapshot.events, [port]: [...(snapshot.events[port] ?? []), entry] },
      configuredPorts: snapshot.configuredPorts.map(item => item === current ? { ...current, head_seq: seq } : item) }
    return seq
  }
  function replaySampleRun() {
    window.clearTimeout(playback)
    const humanMacro = execution?.port === 'COM7' && execution.status === 'running'
    if (humanMacro) {
      listeners.forEach(listener => listener({ type: 'notice', message: 'COM7 正在回放宏，结束后可回放示例 Run。' }))
      return
    }
    hidden.set('COM7', [])
    visibilityRevision++
    const approver = { id:'preview:operator', label:'值班工作台', kind:'human' as const }
    const lease = { ...sensor.control!, owner:approver }
    const now = Date.now() * 1e6
    const request = { id:'sample-handoff', port:'COM7', requester:sensor.active_run!.owner, required_approver:approver,
      label:sensor.active_run!.label, metadata:{}, control_ttl_ms:30_000, daemon_epoch:sensor.daemon_epoch, generation:sensor.generation,
      expected_control_id:lease.id, expected_fence:lease.fence, requested_wall_time_ns:now, expires_wall_time_ns:now + 30e9 }
    snapshot = { ...snapshot, events: { ...snapshot.events, COM7: recording.slice(0, 1) },
      configuredPorts: snapshot.configuredPorts.map(item => item.config.port === 'COM7'
        ? { ...item, config: { ...item.config, enabled: true }, session_state: 'online', head_seq: 1, active_run: null, control:lease, pending_run_start:request } : item) }
    publish()
    const mapped = new Map([[1, 1]])
    let index = 1
    const advance = () => {
      if (!online('COM7')) return
      const entry = recording[index++]
      let metadata = entry.metadata
      if (entry.kind === 'command_capture_completed') {
        const c = metadata.capture as Record<string, number>
        metadata = { ...metadata, capture: { ...c, tx_event_seq: mapped.get(c.tx_event_seq), evidence_from_seq: mapped.get(c.evidence_from_seq), evidence_through_seq: mapped.get(c.evidence_through_seq) } }
      }
      if (entry.kind === 'run_started' || entry.kind === 'run_ended') {
        const active = entry.kind === 'run_started'
        snapshot = { ...snapshot, configuredPorts: snapshot.configuredPorts.map(item => item.config.port === 'COM7'
          ? { ...item, active_run: active ? sensor.active_run : null, control: active ? sensor.control : null, pending_run_start:null } : item) }
      }
      mapped.set(entry.seq, append('COM7', { ...entry, metadata }))
      publish()
      if (index < recording.length) playback = window.setTimeout(advance, entry.kind === 'run_started' ? 1650 : 540)
    }
    playback = window.setTimeout(advance, 1300)
  }
  const emitMacro = (value: MacroExecution) => {
    execution = value
    listeners.forEach(listener => listener({ type: 'macro', execution: value }))
  }
  const bridge: DesktopBridge = {
    createSetup: async brief => {
      const task: SetupRequest = { id: crypto.randomUUID(), brief, plan: null, agent: null }
      tasks.set(task.id, { task, readyAt: Date.now() + 1800 })
      return task
    },
    readSetup: async id => {
      const entry = tasks.get(id)
      if (!entry) throw new Error('示例任务已结束，请重新生成任务。')
      const { task, readyAt } = entry
      if (!task.plan && Date.now() >= readyAt) entry.task = {
        ...task, agent: '示例 Agent', plan: { shell_prompt: task.brief.prompt_hint || echoedPrompt(task.brief.port),
          command: task.brief.check === 'heartbeat' ? '' : 'version', expected_reply: task.brief.check === 'heartbeat' ? '' : 'firmware' }
      }
      return entry.task
    },
    applySetup: async (id, revision) => {
      const task = await bridge.readSetup(id)
      const definition = applyPreviewTask(task, revision)
      if (demo) {
        try { window.localStorage.setItem('serial-preview-setups', JSON.stringify(appliedTasks)) } catch { /* Optional preview storage. */ }
      }
      publish()
      return definition
    },
    ...(demo && { replaySampleRun }),
    agentHistory: async port => ({ server_id: 'qa-server', runs: [], visibility: { revision: visibilityRevision, hidden: hidden.get(port) ?? [] }, truncated: false, indexing: false }),
    clearAgentHistory: async (port, ids) => {
      const ended = (snapshot.events[port] ?? []).filter(e => ['run_ended', 'run_aborted'].includes(e.kind) && e.run_id).map(e => e.run_id!)
      const next = [...new Set([...(hidden.get(port) ?? []), ...ended.filter(id => !ids || ids.includes(id))])]
      hidden.set(port, next)
      return { revision: ++visibilityRevision, hidden: next }
    },
    agentRunEvents: async record => ({ events: (snapshot.events[record.port] ?? []).filter(e => e.daemon_epoch === record.epoch && e.seq >= record.run.start_seq && e.seq <= record.through_seq), limited: false }),
    commandEvidence: async target => (snapshot.events[target.port] ?? []).filter(e => e.daemon_epoch === target.epoch && e.seq >= target.firstSeq && e.seq <= target.throughSeq),
    bootstrap: async () => snapshot,
    refresh: async () => { publish(); return snapshot },
    sendCommand: async (port, command) => {
      if (demo) await new Promise(resolve => window.setTimeout(resolve, 240))
      if (!online(port)) return { status: 'rejected', message: '模拟串口未打开' }
      append(port, { kind: 'tx', direction: 'tx', text: command + '\r', actor: human })
      const output = command.trim() === 'version' ? 'firmware  2.4.1\n' : ''
      append(port, { kind: 'rx', direction: 'rx', text: command + '\n' + output + (port === 'COM7' ? 'sensor> ' : 'root@router:~# ') })
      const history = snapshot.humanHistory!
      if (command.trim()) snapshot = { ...snapshot, humanHistory: { ...history, revision: history.revision + 1, entries: [
        { id: 'sample-' + Date.now(), command, port, wall_time_ns: Date.now() * 1_000_000, revision: history.revision + 1, uses: 1 }, ...history.entries.filter(item => item.command !== command),
      ] } }
      publish()
      return { status: 'accepted' }
    },
    sendSignal: async (port, signal) => {
      if (!online(port)) return { status: 'rejected', message: '模拟串口未打开' }
      append(port, { kind: 'tx', direction: 'tx', text: signal === 'ctrl_c' ? '\x03' : '\x04', actor: human })
      publish()
      return { status: 'accepted' }
    },
    queryHumanHistory: async (query, contains) => ({ ...snapshot.humanHistory!, entries: snapshot.humanHistory!.entries.filter((entry) => contains ? entry.command.includes(query) : entry.command.startsWith(query)) }),
    listMacros: async query => {
      const all = [...macros.values()].filter(m => (query.include_drafts || m.shared) && (!query.query || (m.id + m.name + m.description).includes(query.query)))
      return { catalog_revision: macros.size, macros: all, total: all.length, definition: query.id ? macros.get(query.id) : undefined }
    },
    saveMacro: async request => {
      const definition = { ...request, language_version: 1, revision: (request.expected_revision ?? 0) + 1, shared: request.shared ?? false, updated_at_ns: Date.now() * 1_000_000 }
      macros.set(definition.id, definition)
      return { catalog_revision: macros.size, definition }
    },
    runMacro: async (port, spec) => {
      const definition = spec.macro_id && macros.get(spec.macro_id)
      const check = definition ? checks.get(definition.id) : undefined
      if (!definition || (!check && definition.script !== previewMacro.script) || definition.revision !== spec.revision) throw new Error('浏览器支持示例宏和向导生成的检查。自定义脚本可在桌面端运行。')
      const target = snapshot.configuredPorts.find(item => item.config.port === port)!
      if (!online(port) || target.control || execution?.status === 'running') throw new Error('请选择空闲的模拟串口，或等待当前执行结束。')
      const now = Date.now() * 1_000_000
      const started: MacroExecution = { id: 'preview-' + Date.now(), port, daemon_epoch: target.daemon_epoch, generation: target.generation,
        description: definition.name, status: 'running', owner: human, started_at_ns: now, first_seq: target.head_seq + 1, through_seq: target.head_seq,
        macro_id: definition.id, revision: definition.revision, line: 1, column: 1, writes: 0, bytes_written: 0, outcome_uncertain: false }
      emitMacro(started)
      const commands = check ? [check.command] : ['temp read', 'humidity read']
      let matched = true
      let writes = 0
      let bytes = 0
      macroTimer = window.setInterval(() => {
        if (!online(port)) {
          window.clearInterval(macroTimer)
          emitMacro({ ...execution!, status: 'failed', message: '模拟串口已关闭' })
          return
        }
        if (writes === commands.length) { window.clearInterval(macroTimer); emitMacro({ ...execution!, status: matched ? 'succeeded' : 'timed_out', completed_at_ns: Date.now() * 1_000_000 }); return }
        const command = commands[writes++]
        bytes += command.length + 1
        append(port, { kind: 'tx', direction: 'tx', text: command + '\r', actor: human, operation_id: started.id + '-' + writes, metadata: { macro_execution_id: started.id } })
        const reply = check ? (command === 'version' ? 'firmware 2.4.1\n' : '') + echoedPrompt(port) : writes === 1 ? '23.6 °C\n' : '48.2 %RH\n'
        if (check) matched = reply.includes(check.shell_prompt) && reply.includes(check.expected_reply)
        const seq = append(port, { kind: 'rx', direction: 'rx', text: command + '\n' + reply })
        publish()
        emitMacro({ ...started, line: writes, writes, input_verified_writes: writes, through_seq: seq, bytes_written: bytes })
      }, 700)
      return started
    },
    cancelMacro: async (port, id) => {
      if (!execution || execution.port !== port || execution.id !== id) throw new Error('没有相应的模拟宏')
      window.clearInterval(macroTimer)
      emitMacro({ ...execution, status: 'cancelled' })
      return execution
    },
    decideRunStart: async () => undefined,
    setPortOpen: async (port, open) => {
      const run = snapshot.configuredPorts.find(item => item.config.port === port)?.active_run
      if (!open && run) {
        if (port === 'COM7') window.clearInterval(playback)
        append(port, { kind: 'run_aborted', direction: 'none', text: '', run_id: run.id, metadata: { run } })
      }
      snapshot = {
        ...snapshot,
        configuredPorts: snapshot.configuredPorts.map((item) => item.config.port === port
          ? { ...item, config: { ...item.config, enabled: open }, session_state: open ? 'online' : 'disabled', ...(!open && { control: null, active_run: null }) }
          : item)
      }
      publish()
    },
    setPortModel: async (port, family, name, revision) => {
      if (revision !== snapshot.configRevision) throw new Error('配置已更新，请重新选择')
      snapshot = { ...snapshot, configRevision: revision + 1, configuredPorts: snapshot.configuredPorts.map(item =>
        item.config.port === port ? { ...item, config: { ...item.config, model_family: family, model_name: name } } : item) }
      publish()
    },
    saveSerialConfiguration: async draft => {
      if (!Number.isInteger(draft.transportProfile.baud_rate) || draft.transportProfile.baud_rate < 1) throw new Error('请输入有效的波特率')
      const name = 'port:' + draft.port
      const profile = { ...draft.transportProfile, name }
      const prior = snapshot.configuredPorts.find(item => item.config.port === draft.port)
      const configured = { daemon_epoch: snapshot.daemonEpoch ?? 'qa-epoch', generation: 1, endpoint_present: true, head_seq: 0, ...prior,
        config: { port: draft.port, enabled: draft.enabled, model_profile: draft.modelProfile, model_name: draft.modelName, model_family: draft.modelFamily, transport_profile: name },
        session_state: draft.enabled ? 'online' as const : 'disabled' as const, effective_transport: profile,
        ...(!draft.enabled && { control: null, active_run: null }) }
      snapshot = { ...snapshot, configRevision: snapshot.configRevision + 1,
        transportProfiles: [...snapshot.transportProfiles.filter(p => p.name !== name), profile],
        configuredPorts: prior ? snapshot.configuredPorts.map(p => p === prior ? configured : p) : [...snapshot.configuredPorts, configured] }
      publish()
    },
    saveModelProfiles: async modelProfiles => { snapshot = { ...snapshot, modelProfiles, configRevision: snapshot.configRevision + 1 }; publish() },
    saveModelFamilies: async modelFamilies => { snapshot = { ...snapshot, modelFamilies, configRevision: snapshot.configRevision + 1 }; publish() },
    savePreferences: async (patch) => {
      const next = { ...snapshot.preferences, ...patch }
      snapshot = { ...snapshot, preferences: next }
      if (demo) {
        try { window.localStorage.setItem('serial-preview-appearance', JSON.stringify({theme:next.theme, palette:next.palette, firstUse:next.firstUse})) } catch { /* Optional preview storage. */ }
      }
      publish()
    },
    startLocalService: async () => {
      snapshot = {
        ...snapshot,
        connection: 'connected',
        connectionMessage: '实时连接已建立',
        service: { owned: true, status: 'running', pid: 4208, program: 'seriald' }
      }
      publish()
    },
    stopLocalService: async () => {
      snapshot = {
        ...snapshot,
        connection: 'offline',
        connectionMessage: '本地服务已停止',
        service: { owned: false, status: 'stopped' }
      }
      publish()
    },
    onEvent(listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    }
  }
  if (demo && (document.documentElement.hasAttribute('data-serial-preview') || document.documentElement.dataset.serialFocus === 'agent')) replaySampleRun()
  Object.defineProperty(runtimeWindow, 'serial', { value: bridge, configurable: false })
  return true
}
