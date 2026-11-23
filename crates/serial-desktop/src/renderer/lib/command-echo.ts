import type { TimelineEvent } from '../../shared/contracts'
import { commandState, type StateTone } from './console-workspace'
import type { AgentCommand } from './history'
import { indexAtOrAfterSeq, sanitizeTerminalText } from './terminal-find'

export interface EchoStyle { length: number; tone: StateTone }

// Only color an exact echoed line immediately following its TX. A later occurrence may be device output.
export function commandEchoStyles(events: TimelineEvent[], textFor: (key: string) => string | undefined, commands: AgentCommand[], current?: AgentCommand) {
  const styles = new Map<string, EchoStyle>()
  if (!events.length) return styles
  const epoch = events[0].daemon_epoch
  const singleEpoch = epoch === events.at(-1)!.daemon_epoch
  for (const command of commands) {
    if (singleEpoch && command.daemonEpoch !== epoch) continue
    const index = singleEpoch ? indexAtOrAfterSeq(events, command.firstSeq) :
      events.findIndex(event => event.seq === command.firstSeq && event.daemon_epoch === command.daemonEpoch)
    const tx = events[index]
    if (!tx || tx.seq !== command.firstSeq || tx.direction !== 'tx' || tx.generation !== command.generation) continue
    let rest = sanitizeTerminalText(command.text).replace(/\n+$/, '')
    if (!rest || rest.includes('\n')) continue
    const pieces: [string, number][] = []
    for (let i = index + 1; i < events.length; i++) {
      const event = events[i]
      if (event.seq !== events[i - 1].seq + 1 || event.daemon_epoch !== tx.daemon_epoch || event.generation !== tx.generation ||
          event.direction === 'tx' || ['gap', 'serial_closed', 'break', 'run_aborted', 'run_ended'].includes(event.kind)) break
      if (event.direction !== 'rx') continue
      const key = event.daemon_epoch + ':' + event.seq
      const text = textFor(key)
      if (!text) continue
      const length = Math.min(rest.length, text.length)
      if (!text.startsWith(rest.slice(0, length))) break
      if (length) pieces.push([key, length])
      rest = rest.slice(length)
      if (text.length === length) continue
      if (!rest && text[length] === '\n') {
        const tone = commandState(command, command === current).tone
        for (const [id, end] of pieces) styles.set(id, { length: end, tone })
      }
      break
    }
  }
  return styles
}
