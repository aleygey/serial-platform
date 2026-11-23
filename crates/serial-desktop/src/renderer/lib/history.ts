import type { TimelineEvent, AgentHistoryResponse, MacroExecution } from '../../shared/contracts'

export interface AgentCommand {
  id: string
  daemonEpoch: string
  generation: number
  firstSeq: number
  operationId?: string
  stepIndex?: number
  text: string
  description?: string
  captureMatchers?: Array<{
    kind: 'contains' | 'regex' | 'shell_prompt' | 'uboot_prompt'
    value: string
  }>
  capture?: CommandCaptureEvidence
  expectsCapture?: boolean
  partial?: boolean
}

export interface CommandCaptureEvidence {
  daemonEpoch: string
  generation: number
  operationId: string
  txEventSeq: number
  evidenceFromSeq: number
  evidenceThroughSeq: number
  rxStreamOffsetStart?: number
  rxStreamOffsetEnd?: number
  completion: string
  confidence: string
}

export type AgentHistoryItem =
  | {
      kind: 'run'
      id: string
      firstSeq: number
      label: string
      status: 'running' | 'completed' | 'aborted'
      startedAt?: number
      startSeq?: number
    }
  | {
      kind: 'command'
      id: string
      firstSeq: number
      description: string
      actionKind?: 'single' | 'sequence' | 'macro' | 'signal' | 'trigger'
      triggerStatus?: string
      stepCount?: number
      macro?: MacroExecution
      runId?: string
      commands: AgentCommand[]
    }

export interface MatchRange {
  daemonEpoch: string
  generation: number
  fromSeq: number
  throughSeq: number
  evidenceFromSeq: number
  evidenceThroughSeq: number
  fromStreamOffset?: number
  throughStreamOffset?: number
  inferred: boolean
  hasVisibleOutput: boolean
}

export function buildAgentHistory(events: TimelineEvent[]): AgentHistoryItem[] {
  const items: AgentHistoryItem[] = []
  const runs = new Map<string, Extract<AgentHistoryItem, { kind: 'run' }>>()
  const commandGroups = new Map<string, Extract<AgentHistoryItem, { kind: 'command' }>>()
  const captures = new Map<string, CommandCaptureEvidence>()
  for (const event of events) {
    const capture = commandCapture(event)
    if (capture) captures.set(`${capture.daemonEpoch}:${capture.generation}:${capture.operationId}`, capture)
  }
  for (const event of [...events].sort((a, b) => a.seq - b.seq)) {
    if (['run_started', 'run_ended', 'run_aborted'].includes(event.kind) && event.run_id) {
      const metadata = event.metadata.run as { label?: unknown; start_seq?: number } | undefined
      const existing = runs.get(event.run_id)
      const status = event.kind === 'run_started' ? 'running' : event.kind === 'run_ended' ? 'completed' : 'aborted'
      if (existing) {
        existing.status = status
        if (typeof metadata?.label === 'string' && metadata.label.trim()) existing.label = cleanInline(metadata.label)
      } else {
        const run: Extract<AgentHistoryItem, { kind: 'run' }> = {
          kind: 'run',
          id: `run:${event.run_id}`,
          firstSeq: event.seq,
          startSeq: event.kind === 'run_started' ? event.seq : metadata?.start_seq,
          startedAt: event.kind === 'run_started' ? event.wall_time_ns : undefined,
          label: cleanInline(typeof metadata?.label === 'string' ? metadata.label : 'Agent 任务'),
          status
        }
        runs.set(event.run_id, run)
        items.push(run)
      }
      continue
    }
    const macro = event.metadata.macro_execution as MacroExecution | undefined
    if (event.kind === 'checkpoint' && macro?.owner?.kind === 'agent' && macro.daemon_epoch === event.daemon_epoch && macro.generation === event.generation && macro.id) {
      const runId = event.run_id ?? macro.run_id ?? undefined
      const id = `macro:${event.daemon_epoch}:${runId ?? ''}:${macro.id}`
      const group = commandGroups.get(id)
      if (group) group.macro = macro
      else {
        const item: Extract<AgentHistoryItem, { kind: 'command' }> = {
          kind: 'command', id, firstSeq: event.seq, runId, actionKind: 'macro',
          description: cleanInline(macro.description || macro.macro_id || 'Macro'), macro, commands: [],
        }
        commandGroups.set(id, item)
        items.push(item)
      }
      continue
    }
    const commandDescription = cleanInline(stringMetadata(event, 'command_description') ?? '') || undefined
    const triggerId = stringMetadata(event, 'trigger_id')
    if (triggerId && event.actor?.kind === 'agent') {
      const id = `trigger:${event.daemon_epoch}:${event.run_id ?? ''}:${triggerId}`
      let action = commandGroups.get(id)
      if (!action) {
        action = { kind: 'command', id, firstSeq: event.seq, actionKind: 'trigger', runId: event.run_id ?? undefined, description: '串口触发器', commands: [] }
        commandGroups.set(id, action)
        items.push(action)
      }
      if (event.kind === 'tx') {
        // A trigger reuses its operation ID across fires. Each confirmed physical write is a separate step.
        action.commands.push({ id: 'fire:' + event.seq, daemonEpoch: event.daemon_epoch, generation: event.generation,
          firstSeq: event.seq, text: event.text, description: commandDescription, expectsCapture: false, partial: event.metadata.partial === true })
      } else {
        const info = event.metadata.trigger as { status?: unknown } | undefined
        if (typeof info?.status === 'string') action.triggerStatus = info.status
      }
      continue
    }
    if (event.kind === 'break' && event.actor?.kind === 'agent') {
      const duration = numberMetadata(event, 'duration_ms')
      const operation = event.operation_id ?? 'event:' + event.seq
      items.push({ kind: 'command', id: `break:${event.daemon_epoch}:${operation}`, firstSeq: event.seq, actionKind: 'signal',
        description: '发送 BREAK', runId: event.run_id ?? undefined, commands: [{ id: operation, daemonEpoch: event.daemon_epoch,
          generation: event.generation, firstSeq: event.seq, operationId: event.operation_id ?? undefined, expectsCapture: false,
          description: commandDescription, text: duration === undefined ? 'BREAK' : `BREAK ${duration} ms` }] })
      continue
    }
    if (event.kind !== 'tx' || event.direction !== 'tx' || event.actor?.kind !== 'agent') continue
    const macroExecutionId = stringMetadata(event, 'macro_execution_id')
    const description = (macroExecutionId
      ? `Macro · ${stringMetadata(event, 'macro_description') ?? stringMetadata(event, 'macro_id') ?? 'inline'}`
      : undefined)
      ?? stringMetadata(event, 'command_sequence_description')
      ?? stringMetadata(event, 'command_description')
      ?? (event.run_id ? controlSignal(event.text) ?? '串口写入' : undefined)
    if (!description) continue
    const sequenceId = stringMetadata(event, 'command_sequence_id')
    const operationId = event.operation_id ?? undefined
    const groupKey = macroExecutionId
      ? `macro:${event.daemon_epoch}:${event.run_id ?? ''}:${macroExecutionId}`
      : sequenceId
      ? `sequence:${event.daemon_epoch}:${event.run_id ?? ''}:${sequenceId}`
      : operationId
        ? `operation:${event.daemon_epoch}:${event.run_id ?? ''}:${operationId}`
        : `event:${event.daemon_epoch}:${event.seq}`
    let group = commandGroups.get(groupKey)
    if (!group) {
      group = {
        kind: 'command',
        id: groupKey,
        firstSeq: event.seq,
        description: cleanInline(description),
        actionKind: macroExecutionId ? 'macro' : sequenceId ? 'sequence' : controlSignal(event.text) ? 'signal' : 'single',
        stepCount: sequenceId ? integerValue(event.metadata.command_sequence_step_count) : undefined,
        runId: event.run_id ?? undefined,
        commands: []
      }
      commandGroups.set(groupKey, group)
      items.push(group)
    }
    const stepIndex = numberMetadata(event, 'command_sequence_step_index')
    const commandKey = macroExecutionId || stepIndex === undefined ? operationId ?? `event:${event.seq}` : `step:${stepIndex}`
    const existing = group.commands.find((command) => command.id === commandKey)
    if (existing) {
      existing.partial ||= event.metadata.partial === true
      existing.text += event.text
      existing.description ??= commandDescription
    } else {
      group.commands.push({
        id: commandKey,
        daemonEpoch: event.daemon_epoch,
        generation: event.generation,
        firstSeq: event.seq,
        operationId,
        expectsCapture: !macroExecutionId && (!!sequenceId || !!stringMetadata(event, 'command_description') || !!captureMatchers(event)?.length),
        stepIndex,
        partial: event.metadata.partial === true,
        text: event.text,
        description: commandDescription,
        captureMatchers: captureMatchers(event),
        capture: operationId ? captures.get(`${event.daemon_epoch}:${event.generation}:${operationId}`) : undefined
      })
    }
    group.commands.sort((a, b) => (a.stepIndex ?? Number.MAX_SAFE_INTEGER) - (b.stepIndex ?? Number.MAX_SAFE_INTEGER) || a.firstSeq - b.firstSeq)
  }
  return items.sort((a, b) => a.firstSeq - b.firstSeq)
}

export function mergeAgentHistory(events: TimelineEvent[], persisted?: AgentHistoryResponse, activeRun?: AgentHistoryResponse['runs'][number]['run']): AgentHistoryItem[] {
  const unique = new Map<string, TimelineEvent>()
  for (const event of events) {
    if (event.kind === 'rx' && event.direction === 'rx' && !(event.actor?.kind === 'agent' && event.metadata.trigger_id)) continue
    unique.set(`${event.daemon_epoch}:${event.seq}`, event)
  }
  const hidden = new Set(persisted?.visibility.hidden ?? [])
  const items = buildAgentHistory([...unique.values()].filter((event) => !event.run_id || !hidden.has(event.run_id)))
  const runs = items.filter((item): item is Extract<AgentHistoryItem, { kind: 'run' }> => item.kind === 'run')
  if (activeRun && activeRun.status === 'active' && !runs.some(row => row.id === 'run:' + activeRun.id)) {
    runs.push({ kind: 'run', id: 'run:' + activeRun.id, firstSeq: activeRun.start_seq, startSeq: activeRun.start_seq, label: cleanInline(activeRun.label), status: 'running' })
  }
  const known = new Set(runs.map((run) => run.id))
  for (const record of persisted?.runs ?? []) {
    const existing = runs.find((run) => run.id === `run:${record.run.id}`)
    if (existing) {
      existing.startSeq ??= record.run.start_seq
      existing.startedAt ??= record.started_wall_time_ns
    }
    if (existing && record.run.label) existing.label = cleanInline(record.run.label)
    if (existing?.status === 'running' && record.run.status !== 'active') existing.status = record.run.status
    if (!known.has(`run:${record.run.id}`) && !hidden.has(record.run.id)) runs.push({
      kind: 'run', id: `run:${record.run.id}`, firstSeq: record.run.start_seq, startSeq: record.run.start_seq, startedAt: record.started_wall_time_ns,
      label: cleanInline(record.run.label), status: record.run.status === 'active' ? 'running' : record.run.status
    })
  }
  const rank = new Map(persisted?.runs.map((run, index) => [`run:${run.run.id}`, index]))
  runs.sort((a, b) => (rank.get(a.id) ?? Number.MAX_SAFE_INTEGER) - (rank.get(b.id) ?? Number.MAX_SAFE_INTEGER) || a.firstSeq - b.firstSeq)
  const result: AgentHistoryItem[] = []
  for (const run of runs) {
    result.push(run, ...items.filter((item) => item.kind === 'command' && `run:${item.runId}` === run.id))
  }
  result.push(...items.filter((item) => item.kind === 'command' && !runs.some((run) => run.id === `run:${item.runId}`)))
  return result
}

export function locateCommandOutput(
  events: TimelineEvent[],
  command: AgentCommand
): MatchRange | undefined {
  const capture = command.capture
  if (
    capture
    && capture.daemonEpoch === command.daemonEpoch
    && capture.generation === command.generation
    && capture.txEventSeq >= command.firstSeq
    && capture.evidenceThroughSeq >= capture.evidenceFromSeq
  ) {
    const visible = events.filter((event) => (
      event.direction === 'rx'
      && event.daemon_epoch === capture.daemonEpoch
      && event.generation === capture.generation
      && event.seq >= capture.evidenceFromSeq
      && event.seq <= capture.evidenceThroughSeq
    ))
    return {
      daemonEpoch: capture.daemonEpoch,
      generation: capture.generation,
      fromSeq: visible[0]?.seq ?? capture.evidenceFromSeq,
      throughSeq: visible.at(-1)?.seq ?? capture.evidenceThroughSeq,
      evidenceFromSeq: capture.evidenceFromSeq,
      evidenceThroughSeq: capture.evidenceThroughSeq,
      fromStreamOffset: capture.rxStreamOffsetStart,
      throughStreamOffset: capture.rxStreamOffsetEnd,
      inferred: false,
      hasVisibleOutput: visible.length > 0
    }
  }
  const target = command.text.replace(/[\r\n]+$/g, '')
  if (!target) return undefined
  const ordered = events
    .filter((event) => event.daemon_epoch === command.daemonEpoch && event.generation === command.generation)
    .sort((left, right) => left.seq - right.seq)
  const boundary = ordered.find((event) => (
    event.seq > command.firstSeq
    && (
      (event.direction === 'tx' && (command.operationId ? event.operation_id !== command.operationId : true))
      || isCommandBoundary(event.kind)
    )
  ))?.seq
  const candidates = ordered.filter((event) => (
    event.direction === 'rx'
    && event.seq >= command.firstSeq
    && (boundary === undefined || event.seq < boundary)
  ))
  const combined = candidates.map((event) => event.text).join('')
  if (!command.captureMatchers?.length) return undefined
  let matcherStart = -1
  let matcherEnd = Number.POSITIVE_INFINITY
  for (const matcher of command.captureMatchers) {
    let start = -1
    let end = -1
    if (matcher.kind === 'regex') {
      try {
        const found = new RegExp(matcher.value).exec(combined)
        if (found) {
          start = found.index
          end = found.index + found[0].length
        }
      } catch {
        continue
      }
    } else if (matcher.kind === 'shell_prompt' || matcher.kind === 'uboot_prompt') {
      const found = findCompletedPrompt(combined, matcher.value)
      if (found) ({ start, end } = found)
    } else {
      start = combined.indexOf(matcher.value)
      end = start < 0 ? -1 : start + matcher.value.length
    }
    if (end >= 0 && end < matcherEnd) {
      matcherStart = start
      matcherEnd = end
    }
  }
  if (!Number.isFinite(matcherEnd)) return undefined
  const commandOffset = combined.indexOf(target)
  const startOffset = commandOffset >= 0 && commandOffset <= matcherStart ? commandOffset : 0
  return {
    daemonEpoch: command.daemonEpoch,
    generation: command.generation,
    fromSeq: sequenceAtOffset(candidates, startOffset),
    throughSeq: sequenceAtOffset(candidates, Math.max(startOffset, matcherEnd - 1)),
    evidenceFromSeq: command.firstSeq,
    evidenceThroughSeq: sequenceAtOffset(candidates, Math.max(startOffset, matcherEnd - 1)),
    inferred: true,
    hasVisibleOutput: true
  }
}

/**
 * A persisted profile prompt is a command boundary only at the end of a
 * terminal line (or the current document). In particular, `root# show` is an
 * echoed command prefix, not the prompt that completed that command. A target
 * may reset prompt styling before its newline, so accept only SGR resets in
 * that narrow suffix.
 */
function findCompletedPrompt(text: string, prompt: string): { start: number; end: number } | undefined {
  if (!prompt) return undefined
  let from = 0
  while (from <= text.length - prompt.length) {
    const start = text.indexOf(prompt, from)
    if (start < 0) return undefined
    const end = start + prompt.length
    const suffix = text.slice(end)
    if (/^(?:(?:\u001b\[(?:0)?m))*(?:\r\n|\r|\n|$)/u.test(suffix)) return { start, end }
    from = start + Math.max(1, prompt.length)
  }
  return undefined
}

function commandCapture(event: TimelineEvent): CommandCaptureEvidence | undefined {
  if (event.kind !== 'command_capture_completed') return undefined
  const value = event.metadata.capture
  if (!value || typeof value !== 'object') return undefined
  const capture = value as Record<string, unknown>
  const operationId = stringValue(capture.operation_id)
  const daemonEpoch = stringValue(capture.daemon_epoch)
  const generation = integerValue(capture.generation)
  const txEventSeq = integerValue(capture.tx_event_seq)
  const evidenceFromSeq = integerValue(capture.evidence_from_seq)
  const evidenceThroughSeq = integerValue(capture.evidence_through_seq)
  if (
    !operationId || !daemonEpoch || generation === undefined || txEventSeq === undefined
    || evidenceFromSeq === undefined || evidenceThroughSeq === undefined
  ) return undefined
  return {
    daemonEpoch,
    generation,
    operationId,
    txEventSeq,
    evidenceFromSeq,
    evidenceThroughSeq,
    rxStreamOffsetStart: integerValue(capture.rx_stream_offset_start),
    rxStreamOffsetEnd: integerValue(capture.rx_stream_offset_end),
    completion: stringValue(capture.completion) ?? 'unknown',
    confidence: stringValue(capture.confidence) ?? 'unknown'
  }
}

function captureMatchers(event: TimelineEvent): AgentCommand['captureMatchers'] {
  const values = event.metadata.command_capture_matchers
  if (!Array.isArray(values)) return undefined
  const matchers = values.filter((value): value is NonNullable<AgentCommand['captureMatchers']>[number] => {
    if (!value || typeof value !== 'object') return false
    const matcher = value as Record<string, unknown>
    return typeof matcher.value === 'string'
      && Boolean(matcher.value)
      && ['contains', 'regex', 'shell_prompt', 'uboot_prompt'].includes(String(matcher.kind))
  })
  return matchers.length ? matchers : undefined
}

function sequenceAtOffset(events: TimelineEvent[], target: number): number {
  let offset = 0
  for (const event of events) {
    offset += event.text.length
    if (target < offset) return event.seq
  }
  return events.at(-1)?.seq ?? 0
}

function controlSignal(text: string): string | undefined {
  switch (text) {
    case '\x03': return 'Ctrl+C'
    case '\x04': return 'Ctrl+D'
    case '\x1a': return 'Ctrl+Z'
  }
}

export function displayCommand(text: string): string {
  return controlSignal(text) ?? text.replace(/\r/g, '').replace(/\n+$/g, '')
}

function cleanInline(value: string): string {
  return value.replace(/[\u0000-\u001f\u007f]+/g, ' ').trim()
}

function stringMetadata(event: TimelineEvent, key: string): string | undefined {
  const value = event.metadata[key]
  return typeof value === 'string' && value.trim() ? value : undefined
}

function numberMetadata(event: TimelineEvent, key: string): number | undefined {
  const value = event.metadata[key]
  return typeof value === 'number' && Number.isInteger(value) ? value : undefined
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value ? value : undefined
}

function integerValue(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined
}

function isCommandBoundary(kind: string): boolean {
  return [
    'serial_opening', 'serial_opened', 'serial_open_failed', 'serial_closed',
    'port_reconfigured', 'port_removed', 'trigger_started', 'break', 'gap', 'logging_degraded'
  ].includes(kind)
}
