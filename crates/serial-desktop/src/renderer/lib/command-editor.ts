export interface CommandBuffer {
  value: string
  start: number
  end: number
}

export interface HistoryCursor {
  saved: CommandBuffer
  entries: string[]
  index: number
}

export function commandBuffer(value = ''): CommandBuffer {
  return { value, start: value.length, end: value.length }
}

/** The input contains confirmed Human LINE history only, newest first. */
export function humanHistoryCandidates(history: readonly string[], query: string, contains = false, limit = 50): string[] {
  const seen = new Set<string>()
  const matches: string[] = []
  for (const command of history) {
    if (!command || /[\r\n\u0000-\u0008\u000b-\u001f\u007f]/.test(command) || seen.has(command)) continue
    seen.add(command)
    if (contains ? !command.includes(query) : !command.startsWith(query)) continue
    matches.push(command)
    if (matches.length >= limit) break
  }
  return matches
}

export function suggestedSuffix(buffer: CommandBuffer, history: readonly string[]): string {
  if (!buffer.value || buffer.start !== buffer.end || buffer.end !== buffer.value.length) return ''
  // A repeated exact match is not useful ghost text; prefer the next longer match.
  return humanHistoryCandidates(history, buffer.value).find((item) => item.length > buffer.value.length)?.slice(buffer.value.length) ?? ''
}

export function stepHistory(
  buffer: CommandBuffer,
  cursor: HistoryCursor | undefined,
  history: readonly string[],
  direction: 'older' | 'newer'
): { buffer: CommandBuffer; cursor?: HistoryCursor } {
  if (!cursor) {
    if (direction === 'newer') return { buffer }
    const entries = humanHistoryCandidates(history, '', false, 1000)
    if (!entries.length) return { buffer }
    return { buffer: commandBuffer(entries[0]), cursor: { saved: { ...buffer }, entries, index: 0 } }
  }
  const index = cursor.index + (direction === 'older' ? 1 : -1)
  if (index < 0) return { buffer: cursor.saved }
  const bounded = Math.min(index, cursor.entries.length - 1)
  return { buffer: commandBuffer(cursor.entries[bounded]), cursor: { ...cursor, index: bounded } }
}

export function acceptsSuggestion(key: string, buffer: CommandBuffer, suffix: string, composing: boolean, modifiers: boolean): boolean {
  return !composing && !modifiers && (key === 'ArrowRight' || key === 'End')
    && !!suffix && buffer.start === buffer.end && buffer.end === buffer.value.length
}
