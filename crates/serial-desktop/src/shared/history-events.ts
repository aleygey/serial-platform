import type { TimelineEvent } from './contracts'

export function macroGroupKey(event: TimelineEvent, runId: string | null | undefined, execution: string): string {
  return `macro:${event.daemon_epoch}:${runId ?? ''}:${execution}`
}

/** Keep one representative write even if the macro's starting checkpoint has aged out. */
export function repeatedMacroWrite(event: TimelineEvent, macroWrites: Set<string>): boolean {
  const execution = event.metadata.macro_execution_id
  if (event.kind === 'tx' && typeof execution === 'string' && execution) {
    const key = macroGroupKey(event, event.run_id, execution)
    if (macroWrites.has(key)) return true
    macroWrites.add(key)
  }
  return false
}
