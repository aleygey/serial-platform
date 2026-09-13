import { describe, expect, it } from 'vitest'
import { decodeFrame, encodeControl, SERIAL_PROTOCOL_VERSION } from './protocol'

describe('serial wire envelope', () => {
  it('uses the current shared protocol generation', () => {
    expect(SERIAL_PROTOCOL_VERSION).toBe(9)
  })

  it('encodes control JSON behind the protocol header', () => {
    const frame = encodeControl({ type: 'ping', request_id: 'request' })
    expect(frame[0]).toBe(1)
    expect(frame.readUInt32BE(1)).toBe(frame.length - 5)
    expect(JSON.parse(frame.subarray(5).toString())).toEqual({ type: 'ping', request_id: 'request' })
  })

  it('decodes raw RX payload without executing terminal control sequences', () => {
    const header = Buffer.from(JSON.stringify({
      port: 'COM6', daemon_epoch: 'epoch', seq: 7, generation: 1,
      wall_time_ns: 0, kind: 'rx', direction: 'rx', metadata: {}, durable: true,
      stream_offset_start: 12, stream_offset_end: 18
    }))
    const frame = Buffer.alloc(5 + header.length + 6)
    frame[0] = 2
    frame.writeUInt32BE(header.length, 1)
    header.copy(frame, 5)
    Buffer.from('ok\u001b[2J').copy(frame, 5 + header.length)

    const decoded = decodeFrame(frame)
    expect(decoded.kind === 'timeline' && decoded.event.text).toBe('ok\u001b[2J')
    expect(decoded.kind === 'timeline' && decoded.event.stream_offset_start).toBe(12)
    expect(decoded.kind === 'timeline' && decoded.event.stream_offset_end).toBe(18)
  })

  it('encodes Human commands without a control lease or queue semantics', () => {
    const frame = encodeControl({
      type: 'send_human_command', request_id: 'request', port: 'COM6', expected_generation: 3,
      data: 'dmVyc2lvbg0=', operation_id: 'operation', description: null
    })
    const header = JSON.parse(frame.subarray(5).toString())
    expect(header.port).toBe('COM6')
    expect(header.expected_generation).toBe(3)
    expect(header.control_id).toBeUndefined()
    expect(header.mode).toBeUndefined()
  })
})
