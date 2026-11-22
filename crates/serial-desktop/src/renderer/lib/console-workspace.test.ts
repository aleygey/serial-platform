import { describe, expect, it } from 'vitest'
import { previewRunEvents } from '../preview-run'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { closePane, dockedWidths, insertPane, commandState, lastRunOutcome, portActivity, selectPane } from './console-workspace'
import { buildAgentHistory, displayCommand } from './history'

describe('port console', () => {
  it('focuses an existing pane, replaces only the active pane and closes to a neighbour', () => {
    let layout = selectPane({ ports: [] }, 'A')
    layout = selectPane(layout, 'B', true)
    layout = selectPane(layout, 'C', true)
    layout = selectPane(layout, 'B', true)
    expect(layout).toEqual({ ports: ['A', 'B', 'C'], active: 'B' })
    layout = selectPane(layout, 'D')
    expect(closePane(layout, 'D')).toEqual({ ports: ['A', 'C'], active: 'C' })
    expect(closePane(layout, 'C')).toEqual({ ports: ['A', 'D'], active: 'D' })
  })

  it('places a port on either side, moves open panes without duplicating them, and can close the last view', () => {
    const layout = { ports: ['A', 'B'], active: 'B' }
    expect(insertPane(layout, 'C', 0).ports).toEqual(['C', 'A', 'B'])
    expect(insertPane(layout, 'C', 1).ports).toEqual(['A', 'C', 'B'])
    expect(insertPane(layout, 'C', 2).ports).toEqual(['A', 'B', 'C'])
    expect(insertPane(layout, 'A', 2)).toEqual({ ports: ['B', 'A'], active: 'A' })
    expect(insertPane(layout, 'A', 1).ports).toEqual(layout.ports)
    expect(closePane({ ports: ['A'], active: 'A' }, 'A')).toEqual({ ports: [], active: undefined })
  })

  it('keeps manually resized proportions when docking and reordering', () => {
    expect(dockedWidths(['A'], {})).toEqual({ A: 100 })
    const split = dockedWidths(['B', 'C', 'A'], { A: 60, B: 40 })
    expect(split.A).toBe(40)
    expect(split.B).toBeCloseTo(26.67, 2)
    expect(split.C).toBeCloseTo(33.33, 2)
    expect(dockedWidths(['B', 'A'], { A: 60, B: 40 })).toEqual({ B: 40, A: 60 })
  })

  it('allows cooperation only with the Agent holding the active Run and disables writes when disconnected', () => {
    const port = createQaSnapshot().configuredPorts[1]
    expect(portActivity(port, true)).toMatchObject({ label: '外部 Agent 占用', canWrite: true, cooperative: true, agentOwned: true })
    expect(portActivity(port, false)).toMatchObject({ canWrite: false, cooperative: false })
    expect(portActivity({ ...port, active_run: null }, true)).toMatchObject({ label: '外部 Agent 占用', canWrite: false })
    const human = { kind: 'human' as const, id: 'other-human', label: '另一工作台' }
    const busy = { ...port, active_run: null, control: { ...port.control!, owner: human } }
    expect(portActivity(busy, true)).toMatchObject({ label: '串口已占用', canWrite: false })
    expect(portActivity(busy, true, human)).toMatchObject({ canWrite: true, label: '本机控制' })
  })

  it('distinguishes an unconfirmed send, a matching reply, an interrupted capture and a timeout', () => {
    const groups = buildAgentHistory(createQaSnapshot().events.COM7).filter((item) => item.kind === 'command')
    const first = groups[0].commands[0]
    const last = groups[1].commands[0]
    expect(commandState(last, true).label).toBe('等待回执')
    expect(commandState(last, false).label).toBe('已发送')
    expect(commandState(first, false).label).toBe('回显已匹配')
    expect(commandState({ ...first, capture: { ...first.capture!, completion: 'timeout' } }, false).tone).toBe('failed')
    expect(commandState({ ...first, capture: { ...first.capture!, confidence: 'interfered' } }, false).tone).toBe('waiting')
  })

  it('uses the declared sequence length while only one step is visible, and keeps macro completion separate from write receipts', () => {
    const events = previewRunEvents(createQaSnapshot())
    const seqStart = events.find(event => event.metadata.command_sequence_step_index === 0)!
    const partial = buildAgentHistory([seqStart]).find(item => item.kind === 'command')!
    expect(partial).toMatchObject({ actionKind: 'sequence', stepCount: 2 })
    expect(partial.commands).toHaveLength(1)
    const actions = buildAgentHistory(events).filter(item => item.kind === 'command')
    const macro = actions.find(action => action.actionKind === 'macro')!
    expect(macro.macro?.status).toBe('succeeded')
    expect(macro.stepCount).toBeUndefined()
    expect(macro.commands.map(command => commandState(command, true).label)).toEqual(['已发送', '已发送'])
    const checkpoint = events.find(event => event.metadata.macro_phase === 'started')!
    expect(buildAgentHistory([checkpoint])[0]).toMatchObject({ actionKind: 'macro', commands: [], macro: { status: 'running' } })
  })

  it('retains trigger fires with one operation ID, physical BREAK and printable signal names', () => {
    const tx = createQaSnapshot().events.COM7[2]
    const events = [
      { ...tx, seq: 3, text: 'a', metadata: { trigger_id: 'trigger' } },
      { ...tx, seq: 4, text: 'b', metadata: { trigger_id: 'trigger' } },
      { ...tx, seq: 5, kind: 'trigger_completed', direction: 'none' as const, metadata: { trigger_id: 'trigger', trigger: { status: 'matched' } } },
      { ...tx, seq: 6, kind: 'break', direction: 'none' as const, metadata: { duration_ms: 150 } },
    ]
    const [trigger, signal] = buildAgentHistory(events).filter(item => item.kind === 'command')
    expect(trigger.triggerStatus).toBe('matched')
    expect(trigger.commands.map(cmd => cmd.text)).toEqual(['a', 'b'])
    expect(signal.commands[0].text).toBe('BREAK 150 ms')
    expect(displayCommand('\x03')).toBe('Ctrl+C')
    expect(displayCommand('constructor')).toBe('constructor')
  })

  it('keeps the orb idle for a historical Run, stale leases and pending handoffs', () => {
    const port = createQaSnapshot().configuredPorts[1]
    for (const control of [null, { ...port.control!, generation: port.generation + 1 }, { ...port.control!, epoch: 'earlier' }]) {
      expect(portActivity({ ...port, control }, true)).toMatchObject({ agentOwned: false, cooperative: false, phase: 'idle' })
    }
    expect(portActivity(port, false)).toMatchObject({ agentOwned: false, phase: 'offline' })
    const human = { id: 'local', kind: 'human' as const, label: '本机' }
    const pending = { requester: port.control!.owner, daemon_epoch: port.daemon_epoch, generation: port.generation } as NonNullable<typeof port.pending_run_start>
    expect(portActivity({ ...port, control: { ...port.control!, owner: human }, pending_run_start: pending }, true, human))
      .toMatchObject({ agentOwned: false, phase: 'approval', canWrite: true })
  })

  it('names the task from its own lifecycle and clears the result when a new task starts', () => {
    const snapshot = createQaSnapshot()
    const port = snapshot.configuredPorts[1]
    const events = previewRunEvents(snapshot)
    const end = events.at(-1)!
    const history = buildAgentHistory(events)
    const completed = { runId: end.run_id, label: '读取传感器状态已完成', tone: 'done' }
    expect(lastRunOutcome(events, port, history)).toEqual(completed)
    expect(lastRunOutcome([{ ...end, metadata: {} }], port, history)).toEqual(completed)
    expect(lastRunOutcome([{ ...end, kind: 'run_aborted', metadata: { run: { label: '校准探头' } } }], port, history)?.label).toBe('校准探头已中止')
    expect(lastRunOutcome([{ ...end, port: 'COM6' }, { ...end, generation: port.generation - 1 }, { ...end, daemon_epoch: 'other' }], port, history)).toBeUndefined()
    expect(lastRunOutcome([...events, { ...end, kind: 'run_started', run_id: 'next' }], port, history)).toBeUndefined()
  })
})
