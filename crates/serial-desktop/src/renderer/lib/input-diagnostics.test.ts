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
  const deliveredAt = performance.timeOrigin + performance.now()
  const events = [{ ...base, seq: 1, text: 'log\nro', inputObservation: { ids: [id], receivedAt: sentAt, deliveredAt } }]
  const textFor = (key: string) => events.find(e => key === e.daemon_epoch + ':' + e.seq)?.text
  recorder.committed(events, ['root# '], textFor)
  frames.shift()!(0)
  const sample = recorder.snapshot()[0]
  expect(sample.deliveredAt).toBe(deliveredAt)
  expect(sample.committedAt!).toBeGreaterThanOrEqual(deliveredAt)
  expect(sample.frameAt!).toBeGreaterThanOrEqual(sample.committedAt!)
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

it('shares a frame for text edits, bounds the sample window, and keeps a frozen reading stable', () => {
  const frames: FrameRequestCallback[] = []
  vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => frames.push(callback))
  const recorder = new InputDiagnostics()
  for (let i = 0; i < 20; i++) recorder.textInput(performance.now())
  expect(frames).toHaveLength(1)
  frames.shift()!(0)
  const values = recorder.typingSnapshot()
  expect(values).toHaveLength(16)
  expect(values.every(value => value >= 0)).toBe(true)
  recorder.textInput(performance.now())
  recorder.togglePause()
  frames.shift()!(0)
  recorder.textInput(performance.now())
  expect(frames).toHaveLength(0)
  expect(recorder.typingSnapshot()).toBe(values)
  recorder.togglePause()
  expect(recorder.typingSnapshot()).not.toBe(values)
})
