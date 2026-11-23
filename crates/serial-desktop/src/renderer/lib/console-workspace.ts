import type { Actor, PortSnapshot, TimelineEvent } from '../../shared/contracts'
import type { AgentCommand, AgentHistoryItem } from './history'

export interface PaneLayout { ports: string[]; active?: string }

export function selectPane(layout: PaneLayout, port: string, beside = false): PaneLayout {
  if (layout.ports.includes(port)) return layout.active === port ? layout : { ...layout, active: port }
  const ports = beside || !layout.active ? [...layout.ports, port]
    : layout.ports.map((current) => current === layout.active ? port : current)
  return { ports, active: port }
}

export function insertPane(layout: PaneLayout, port: string, slot: number): PaneLayout {
  const from = layout.ports.indexOf(port)
  const ports = layout.ports.filter(current => current !== port)
  const index = from >= 0 && from < slot ? slot - 1 : slot
  ports.splice(Math.max(0, Math.min(ports.length, index)), 0, port)
  return { ports, active: port }
}

export function dockedWidths(ports: string[], current: Record<string, number>): Record<string, number> {
  const widths = Object.values(current)
  const average = widths.length ? widths.reduce((sum, width) => sum + width, 0) / widths.length : 1
  const weights = ports.map(port => Object.hasOwn(current, port) ? current[port] : average)
  const total = weights.reduce((sum, weight) => sum + weight, 0)
  return Object.fromEntries(ports.map((port, index) => [port, 100 * weights[index] / total]))
}

export function reorderPane(layout: PaneLayout, port: string, target: string): PaneLayout {
  const from = layout.ports.indexOf(port)
  const to = layout.ports.indexOf(target)
  if (from < 0 || to < 0 || from === to) return layout
  const ports = [...layout.ports]
  ports[from] = target
  ports[to] = port
  return { ...layout, ports }
}

export function closePane(layout: PaneLayout, port: string): PaneLayout {
  const index = layout.ports.indexOf(port)
  if (index < 0) return layout
  const ports = layout.ports.filter((current) => current !== port)
  return { ports, active: port === layout.active ? ports[Math.min(index, ports.length - 1)] : layout.active }
}

export type StateTone = 'idle' | 'working' | 'waiting' | 'done' | 'failed'
export interface ExecutionState { label: string; tone: StateTone }

export function commandState(command: AgentCommand, awaitingCapture: boolean): ExecutionState {
  if (command.partial) return { label: '部分写入', tone: 'failed' }
  const capture = command.capture
  if (!capture) return awaitingCapture && command.expectsCapture !== false
    ? { label: '等待回执', tone: 'working' } : { label: '已发送', tone: 'idle' }
  switch (capture.completion) {
    case 'timeout': return { label: '已超时', tone: 'failed' }
    case 'disconnected': return { label: '连接中断', tone: 'failed' }
    case 'run_aborted': return { label: '已中止', tone: 'failed' }
  }
  if (['partial', 'incomplete', 'unreliable', 'interfered', 'low'].includes(capture.confidence))
    return { label: '需核对输出', tone: 'waiting' }
  if (['prompt', 'regex', 'literal'].includes(capture.completion)) return { label: '回显已匹配', tone: 'done' }
  if (capture.completion === 'quiet') return { label: '输出已静默', tone: 'done' }
  if (capture.completion === 'signal') return { label: '信号已记录', tone: 'done' }
  return { label: '已记录回执', tone: 'idle' }
}

export function portActivity(port: PortSnapshot | undefined, connected: boolean, actor?: Actor) {
  const online = connected && port?.session_state === 'online'
  const control = port?.control
  const owner = online && control?.epoch === port.daemon_epoch && control.generation === port.generation ? control.owner : undefined
  const agentOwned = owner?.kind === 'agent'
  const run = agentOwned && port?.active_run?.status === 'active' && port.active_run.owner.id === owner.id ? port.active_run : undefined
  const cooperative = !!run
  const local = !!owner && owner.kind === 'human' && owner.id === actor?.id
  const canWrite = !!online && (!owner || local || cooperative)
  const waiting = !!run && !!port?.run_context && port.run_context.revision > port.run_context.acknowledged_revision
  const pending = port?.pending_run_start
  const requested = online && pending?.daemon_epoch === port.daemon_epoch && pending.generation === port.generation
  const phase = !online ? 'offline' : agentOwned ? 'agent' : requested ? 'approval' : local ? 'local' : owner ? 'occupied' : 'idle'
  const label = { offline: '串口离线', agent: '外部 Agent 占用', approval: '等待接管授权', local: '本机控制', occupied: '串口已占用', idle: '串口空闲' }[phase]
  const tone: StateTone = agentOwned ? 'working' : phase === 'approval' || phase === 'occupied' ? 'waiting' : 'idle'
  const detail = agentOwned ? [owner.label, waiting ? '等待读取人工输入' : run?.label].filter(Boolean).join(' · ')
    : requested ? pending.requester.label : owner?.label
  return { phase, label, tone, detail, agentOwned, run, canWrite, cooperative }
}

export function lastRunOutcome(events: TimelineEvent[], port?: PortSnapshot, history: AgentHistoryItem[] = []): (ExecutionState & { runId?: string }) | undefined {
  if (!port) return
  for (let i = events.length - 1; i >= 0; i--) {
    const event = events[i]
    if (event.port !== port.config.port || event.daemon_epoch !== port.daemon_epoch || event.generation !== port.generation) continue
    switch (event.kind) {
      case 'run_started': case 'run_start_requested': return
      case 'run_ended': case 'run_aborted': {
        const run = history.find(item => item.kind === 'run' && item.id === 'run:' + event.run_id)
        const metadata = event.metadata.run as { label?: unknown } | undefined
        const name = typeof metadata?.label === 'string' && metadata.label.trim()
          ? metadata.label.replace(/[\u0000-\u001f\u007f]+/g, ' ').trim() : run?.kind === 'run' ? run.label : 'Agent 任务'
        const completed = event.kind === 'run_ended'
        return { runId: event.run_id ?? undefined, label: name + (completed ? '已完成' : '已中止'), tone: completed ? 'done' : 'failed' }
      }
      case 'run_start_denied': return { label: '接管请求被拒绝', tone: 'failed' }
      case 'run_start_cancelled': return { label: '接管请求已取消', tone: 'idle' }
      case 'run_start_timed_out': return { label: '接管请求已超时', tone: 'failed' }
    }
  }
}
