import { expect, it, vi } from 'vitest'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { buildAgentHistory } from './history'
import { commandEchoStyles } from './command-echo'

it('colors just the exact echoed line across RX fragments and leaves device output untouched', () => {
  const events = createQaSnapshot().events.COM7
  const cmd = buildAgentHistory(events).filter(row => row.kind === 'command')[0].commands[0]
  const tx = events[2]
  const rx = events[3]
  const text = new Map([['qa-epoch:4', 'ver'], ['qa-epoch:5', 'sion\nversion X\n']])
  const textFor = vi.fn((key: string) => text.get(key))
  const stream = [tx, rx, { ...rx, seq: 5 }]
  const later = Array.from({ length: 12000 }, (_, i) => ({ ...rx, seq: 6 + i }))
  const styles = commandEchoStyles([...stream, ...later], textFor, [cmd])
  expect([...styles.values()]).toEqual([{ length: 3, tone: 'done' }, { length: 4, tone: 'done' }])
  expect(textFor).toHaveBeenCalledTimes(2)
  expect(commandEchoStyles(stream, () => 'versionX\n', [cmd]).size).toBe(0)
  expect(commandEchoStyles([tx, { ...rx, seq: 5 }], () => 'version\n', [cmd]).size).toBe(0)
  expect(commandEchoStyles([tx, { ...rx, generation: 9 }], () => 'version\n', [cmd]).size).toBe(0)
})
