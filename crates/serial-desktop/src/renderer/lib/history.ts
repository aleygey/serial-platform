import type { TimelineEvent } from '../../shared/contracts'

export interface AgentCommand {
  id: string
  firstSeq: number
  operationId?: string
  stepIndex?: number
  text: string
  captureMatchers?: Array<{
    kind: 'contains' | 'regex' | 'shell_prompt' | 'uboot_prompt'
    value: string
  }>
}

export type AgentHistoryItem =
  | {
      kind: 'run'
      id: string
      firstSeq: number
      label: string
      status: 'running' | 'completed' | 'aborted'
    }
  | {
      kind: 'command'
      id: string
      firstSeq: number
      description: string
      runId?: string
      commands: AgentCommand[]
    }

export interface MatchRange {
  fromSeq: number
  throughSeq: number
}

export function buildAgentHistory(events: TimelineEvent[]): AgentHistoryItem[] {
  const items: AgentHistoryItem[] = []
  const runs = new Map<string, Extract<AgentHistoryItem, { kind: 'run' }>>()
  const commandGroups = new Map<string, Extract<AgentHistoryItem, { kind: 'command' }>>()
  for (const event of [...events].sort((a, b) => a.seq - b.seq)) {
    if (['run_started', 'run_ended', 'run_aborted'].includes(event.kind) && event.run_id) {
      const metadata = event.metadata.run as { label?: unknown } | undefined
      const existing = runs.get(event.run_id)
      const status = event.kind === 'run_started' ? 'running' : event.kind === 'run_ended' ? 'completed' : 'aborted'
      if (existing) {
        existing.status = status
      } else {
        const run: Extract<AgentHistoryItem, { kind: 'run' }> = {
          kind: 'run',
          id: `run:${event.run_id}`,
          firstSeq: event.seq,
          label: cleanInline(typeof metadata?.label === 'string' ? metadata.label : 'Agent 任务'),
          status
        }
        runs.set(event.run_id, run)
        items.push(run)
      }
      continue
    }
    if (event.kind !== 'tx' || event.direction !== 'tx' || event.actor?.kind !== 'agent') continue
    const description = stringMetadata(event, 'command_sequence_description')
      ?? stringMetadata(event, 'command_description')
    if (!description) continue
    const sequenceId = stringMetadata(event, 'command_sequence_id')
    const operationId = event.operation_id ?? undefined
    const groupKey = sequenceId
      ? `sequence:${sequenceId}`
      : operationId
        ? `operation:${operationId}`
        : `event:${event.seq}`
    let group = commandGroups.get(groupKey)
    if (!group) {
      group = {
        kind: 'command',
        id: groupKey,
        firstSeq: event.seq,
        description: cleanInline(description),
        runId: event.run_id ?? undefined,
        commands: []
      }
      commandGroups.set(groupKey, group)
      items.push(group)
    }
    const stepIndex = numberMetadata(event, 'command_sequence_step_index')
    const commandKey = stepIndex === undefined ? operationId ?? `event:${event.seq}` : `step:${stepIndex}`
    const existing = group.commands.find((command) => command.id === commandKey)
    if (existing) {
      existing.text += event.text
    } else {
      group.commands.push({
        id: commandKey,
        firstSeq: event.seq,
        operationId,
        stepIndex,
        text: event.text,
        captureMatchers: captureMatchers(event)
      })
    }
    group.commands.sort((a, b) => (a.stepIndex ?? Number.MAX_SAFE_INTEGER) - (b.stepIndex ?? Number.MAX_SAFE_INTEGER) || a.firstSeq - b.firstSeq)
  }
  return items.sort((a, b) => a.firstSeq - b.firstSeq)
}

export function locateCommandOutput(
  events: TimelineEvent[],
  command: AgentCommand
): MatchRange | undefined {
  const target = command.text.replace(/[\r\n]+$/g, '')
  if (!target) return undefined
  const candidates = events.filter((event) => event.direction === 'rx' && event.seq >= command.firstSeq)
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
    fromSeq: sequenceAtOffset(candidates, startOffset),
    throughSeq: sequenceAtOffset(candidates, Math.max(startOffset, matcherEnd - 1))
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

export function displayCommand(text: string): string {
  return text.replace(/\r/g, '').replace(/\n+$/g, '')
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
