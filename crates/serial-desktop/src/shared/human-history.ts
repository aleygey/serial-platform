import type { HumanCommandHistory } from './contracts'

export const HISTORY_WINDOW = 1000

/** A confirmed command moves to the front, retaining its identity on reuse. */
export function applyHistoryEntry(previous: HumanCommandHistory | undefined, page: HumanCommandHistory): HumanCommandHistory | undefined {
  const [entry] = page.entries
  if (!previous || previous.server_id !== page.server_id || page.revision !== previous.revision + 1 ||
      page.entries.length !== 1 || entry.revision !== page.revision) return
  const entries = [entry, ...previous.entries.filter(item => item.id !== entry.id)]
  const hasOlder = previous.next_before_revision != null || entries.length > HISTORY_WINDOW
  if (entries.length > HISTORY_WINDOW) entries.length = HISTORY_WINDOW
  return { ...page, entries, next_before_revision: hasOlder ? entries.at(-1)!.revision : null }
}
