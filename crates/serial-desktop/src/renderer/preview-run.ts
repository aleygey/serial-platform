import type { DesktopSnapshot, MacroDefinition, MacroExecution, TimelineEvent } from '../shared/contracts'

export const previewMacro: MacroDefinition = {
  id: 'sample_sensor', name: '采样温湿度', description: '依次读取温度和湿度', revision: 1, shared: true,
  script: 'cmd("temp read");\ncmd("humidity read");\n', language_version: 1,
  updated_at_ns: 0, parameters: {},
}

export function previewRunEvents(snapshot: DesktopSnapshot): TimelineEvent[] {
  const port = snapshot.configuredPorts.find(item => item.config.port === 'COM7')!
  const run = port.active_run!
  const actor = run.owner
  const events = snapshot.events.COM7.slice(0, 5)
  const event = (kind: string, direction: TimelineEvent['direction'], text: string, metadata: TimelineEvent['metadata'] = {}, operation_id?: string) => {
    const seq = events.length + 1
    events.push({ kind, direction, text, metadata, operation_id, port: 'COM7', daemon_epoch: port.daemon_epoch, generation: port.generation,
      run_id: run.id, seq, wall_time_ns: events[0].wall_time_ns + seq * 400_000_000, durable: true, actor: direction === 'rx' ? undefined : actor })
    return seq
  }
  function command(line: string, output: string, metadata: TimelineEvent['metadata'], capture = true) {
    const operation = 'sample-' + events.length
    const tx = event('tx', 'tx', line + '\r', metadata, operation)
    const rx = event('rx', 'rx', line + '\n' + output + '\nsensor> ')
    if (capture) event('command_capture_completed', 'none', '', {
      capture: { operation_id: operation, daemon_epoch: port.daemon_epoch, generation: port.generation,
        tx_event_seq: tx, evidence_from_seq: tx, evidence_through_seq: rx, completion: 'prompt', confidence: 'high' },
    }, operation)
  }
  const sequence = { command_sequence_id: 'bus-check', command_sequence_description: '检查采样总线', command_sequence_step_count: 2,
    command_capture_matchers: [{ kind: 'contains', value: 'sensor> ' }] }
  command('bus status', 'I2C0     online\nclock    400 kHz', { ...sequence, command_sequence_step_index: 0, command_description: '读取采样总线状态' })
  command('sensor id', 'address  0x44\ndevice   SHT31-D', { ...sequence, command_sequence_step_index: 1, command_description: '读取传感器标识' })
  const macro: MacroExecution = { id: 'sample-macro', port: 'COM7', daemon_epoch: port.daemon_epoch, generation: port.generation,
    owner: actor, run_id: run.id, macro_id: previewMacro.id, revision: 1, description: previewMacro.name, status: 'running',
    line: 1, column: 1, writes: 0, bytes_written: 0, started_at_ns: events.at(-1)!.wall_time_ns, first_seq: events.length + 1, through_seq: events.length, outcome_uncertain: false }
  event('checkpoint', 'none', '', { macro_phase: 'started', macro_execution: macro })
  const write = { macro_execution_id: macro.id, macro_description: macro.description }
  command('temp read', 'temperature    23.6 °C', write, false)
  command('humidity read', 'humidity       48.2 %RH', write, false)
  event('checkpoint', 'none', '', { macro_phase: 'completed', macro_execution: { ...macro, status: 'succeeded', writes: 2, line: 2, through_seq: events.length } })
  command('sensor read', 'Sampling channel 0…\nstatus   ready', { command_description: '确认传感器状态', command_capture_matchers: [{ kind: 'contains', value: 'sensor> ' }] })
  event('run_ended', 'none', '', { run: { ...run, status: 'completed', end_seq: events.length + 1 } })
  return events
}
