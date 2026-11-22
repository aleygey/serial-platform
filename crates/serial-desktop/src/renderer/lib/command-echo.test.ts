import { expect, it } from 'vitest'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { buildAgentHistory } from './history'
import { commandEchoStyles } from './command-echo'

it('colors just the exact echoed line across RX fragments and leaves device output untouched', () => {
  const events = createQaSnapshot().events.COM7
  const cmd = buildAgentHistory(events).filter(row => row.kind === 'command')[0].commands[0]
  const tx = events[2]
  const rx = events[3]
  const chunks = [{ key: 'qa-epoch:4', text: 'ver' }, { key: 'qa-epoch:5', text: 'sion\nversion X\n' }]
  const stream = [tx, rx, { ...rx, seq: 5 }]
  const styles = commandEchoStyles(stream, chunks, [cmd])
  expect([...styles.values()]).toEqual([{ length: 3, tone: 'done' }, { length: 4, tone: 'done' }])
  expect(commandEchoStyles(stream, [{ ...chunks[0], text: 'versionX\n' }, chunks[1]], [cmd]).size).toBe(0)
  expect(commandEchoStyles([tx, { ...rx, seq: 5 }], [{ key: 'qa-epoch:5', text: 'version\n' }], [cmd]).size).toBe(0)
  expect(commandEchoStyles([tx, { ...rx, generation: 9 }], [{ key: 'qa-epoch:4', text: 'version\n' }], [cmd]).size).toBe(0)
})
