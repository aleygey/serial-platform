import type { TimelineEvent } from '../../shared/contracts'
import { commandState, type StateTone } from './console-workspace'
import type { AgentCommand } from './history'
import { sanitizeTerminalText, type TerminalDocumentChunk } from './terminal-find'

export interface EchoStyle { length: number; tone: StateTone }
type EchoChunk = Pick<TerminalDocumentChunk, 'key' | 'text'>

// Only color an exact echoed line immediately following its TX. A later occurrence may be device output.
export function commandEchoStyles(events: TimelineEvent[], chunks: readonly EchoChunk[], commands: AgentCommand[], current?: AgentCommand) {
  const styles = new Map<string, EchoStyle>()
  if (!commands.length) return styles
  const writes = new Map(commands.map(command => [command.daemonEpoch + ':' + command.firstSeq, command]))
  const textByKey = new Map(chunks.map(chunk => [chunk.key, chunk.text]))
  let pending: { rest: string; tone: StateTone; generation: number; epoch: string; pieces: [string, number][] } | undefined
  let previous: number | undefined
  for (const event of events) {
    const key = event.daemon_epoch + ':' + event.seq
    if (pending && (event.daemon_epoch !== pending.epoch || event.generation !== pending.generation || event.seq !== (previous ?? 0) + 1)) pending = undefined
    previous = event.seq
    if (event.direction === 'tx') {
      const cmd = writes.get(key)
      const line = cmd && sanitizeTerminalText(cmd.text).replace(/\n+$/, '')
      pending = cmd && line && !line.includes('\n') ? { rest: line, tone: commandState(cmd, cmd === current).tone,
        epoch: cmd.daemonEpoch, generation: cmd.generation, pieces: [] } : undefined
      continue
    }
    if (['gap', 'serial_closed', 'break', 'run_aborted', 'run_ended'].includes(event.kind)) { pending = undefined; continue }
    if (!pending || event.direction !== 'rx') continue
    const text = textByKey.get(key)
    if (!text) continue
    const length = Math.min(pending.rest.length, text.length)
    if (!text.startsWith(pending.rest.slice(0, length))) { pending = undefined; continue }
    if (length) pending.pieces.push([key, length])
    pending.rest = pending.rest.slice(length)
    if (text.length === length) continue
    if (!pending.rest && text[length] === '\n') {
      for (const [id, end] of pending.pieces) styles.set(id, { length: end, tone: pending.tone })
    }
    pending = undefined
  }
  return styles
}
