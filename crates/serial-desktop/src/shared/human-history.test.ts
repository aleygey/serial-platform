import { expect, it } from 'vitest'
import type { HumanCommandHistory } from './contracts'
import { applyHistoryEntry, HISTORY_WINDOW } from './human-history'

const entry = (revision: number, id = String(revision)) => ({ revision, id, command: 'read ' + id, port: 'COM1', wall_time_ns: 1, uses: 1 })
const page = (revision: number, entries = [entry(revision)]): HumanCommandHistory => ({ server_id: 'lab', revision, entries })

it('moves repeated commands to the front and preserves the cursor for an authoritative history page', () => {
  const previous = page(2, [entry(2), entry(1)])
  const latest = { ...entry(3, '1'), uses: 2 }
  expect(applyHistoryEntry(previous, { ...page(3, [latest]), next_before_revision: 3 })).toEqual({
    ...page(3, [latest, entry(2)]), next_before_revision: null
  })
  const full = page(HISTORY_WINDOW, Array.from({ length: HISTORY_WINDOW }, (_, i) => entry(HISTORY_WINDOW - i)))
  const extended = applyHistoryEntry(full, page(HISTORY_WINDOW + 1))!
  expect(extended.entries).toHaveLength(HISTORY_WINDOW)
  expect(extended.next_before_revision).toBe(2)
  expect(applyHistoryEntry(extended, page(HISTORY_WINDOW + 2, [entry(HISTORY_WINDOW + 2, '2')]))?.next_before_revision).toBe(3)
})

it('requires a known base and the immediately following confirmed revision', () => {
  const previous = page(1)
  expect(applyHistoryEntry(undefined, page(2))).toBeUndefined()
  expect(applyHistoryEntry(previous, { ...page(2), server_id: 'other' })).toBeUndefined()
  expect(applyHistoryEntry(previous, page(3))).toBeUndefined()
  expect(applyHistoryEntry(previous, page(2, []))).toBeUndefined()
  expect(applyHistoryEntry(previous, page(2, [entry(1)]))).toBeUndefined()
})
