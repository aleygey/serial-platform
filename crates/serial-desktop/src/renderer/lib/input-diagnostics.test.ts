import { afterEach, expect, it, vi } from 'vitest'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { InputDiagnostics } from './input-diagnostics'

afterEach(() => vi.unstubAllGlobals())

it('times the first RX and a later split prompt independently, including when the ACK arrives last', () => {
  const frames: FrameRequestCallback[] = []
  vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => frames.push(callback))
  const recorder = new InputDiagnostics()
  const base = createQaSnapshot().events.COM6[0]
  const id = recorder.begin('')!
  const sentAt = performance.timeOrigin + performance.now()
  const events = [{ ...base, seq: 1, text: 'log\nro', inputObservation: { ids: [id], receivedAt: sentAt } }]
  const textFor = (key: string) => events.find(e => key === e.daemon_epoch + ':' + e.seq)?.text
  recorder.committed(events, ['root# '], textFor)
  frames.shift()!(0)
  expect(recorder.snapshot()[0].frameAt).toBeDefined()
  expect(recorder.snapshot()[0].promptAt).toBeUndefined()
  const second = { ...base, seq: 2, text: 'ot# ' }
  recorder.committed([...events, second], ['root# '], key => key.endsWith(':2') ? second.text : textFor(key))
  frames.shift()!(0)
  recorder.finish(id, { status: 'accepted', timing: { sentAt, acceptedAt: performance.timeOrigin + performance.now() } })
  expect(recorder.snapshot()[0]).toMatchObject({ id, status: 'accepted', kind: '空回车', promptAt: expect.any(Number) })
  recorder.togglePause()
  const frozen = recorder.snapshot()
  expect(recorder.begin('version')).toBeUndefined()
  expect(recorder.snapshot()).toBe(frozen)
  recorder.togglePause()
  for (let i = 0; i < 10; i++) recorder.begin('private command')
  expect(recorder.snapshot()).toHaveLength(8)
  expect(JSON.stringify(recorder.snapshot())).not.toContain('private command')
})
