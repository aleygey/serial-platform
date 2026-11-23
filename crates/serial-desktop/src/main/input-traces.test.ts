import { expect, it } from 'vitest'
import { createQaSnapshot } from '../shared/qa-fixture'
import { InputTraceTracker } from './input-traces'

it('matches first live RX to actual TX on the same connection and reports shared packets', () => {
  const tracker = new InputTraceTracker()
  const snapshot = createQaSnapshot()
  const port = snapshot.configuredPorts[0]
  const rx = { ...snapshot.events.COM6[0], seq: 5 }
  const tx = { ...rx, seq: 4, direction: 'tx' as const, operation_id: 'first' }
  const now = performance.timeOrigin + performance.now()
  for (const id of ['first', 'second']) tracker.start(id, port, now)
  expect(tracker.observe(rx)).toBeUndefined()
  tracker.observe(tx)
  tracker.observe({ ...tx, operation_id: 'second' })
  expect(tracker.observe({ ...rx, replay: true })).toBeUndefined()
  expect(tracker.observe({ ...rx, port: 'COM7' })).toBeUndefined()
  expect(tracker.observe(rx)?.ids).toEqual(['first', 'second'])
  expect(tracker.observe(rx)).toBeUndefined()

  for (const discontinuity of [{ generation: 100 }, { daemon_epoch: 'restarted' }, { kind: 'gap' }, { kind: 'serial_closed' }]) {
    tracker.start('first', port, now)
    tracker.observe(tx)
    tracker.observe({ ...rx, ...discontinuity })
    expect(tracker.observe(rx)).toBeUndefined()
  }
})
