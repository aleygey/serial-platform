import { describe, expect, it } from 'vitest'
import { decodeFrame, encodeControl } from './protocol'

describe('serial wire envelope', () => {
  it('encodes control JSON behind the protocol header', () => {
    const frame = encodeControl({ type: 'ping', request_id: 'request' })
    expect(frame[0]).toBe(1)
    expect(frame.readUInt32BE(1)).toBe(frame.length - 5)
    expect(JSON.parse(frame.subarray(5).toString())).toEqual({ type: 'ping', request_id: 'request' })
  })

  it('decodes raw RX payload without executing terminal control sequences', () => {
    const header = Buffer.from(JSON.stringify({
      port: 'COM6', daemon_epoch: 'epoch', seq: 7, generation: 1,
      wall_time_ns: 0, kind: 'rx', direction: 'rx', metadata: {}, durable: true
    }))
    const frame = Buffer.alloc(5 + header.length + 6)
    frame[0] = 2
    frame.writeUInt32BE(header.length, 1)
    header.copy(frame, 5)
    Buffer.from('ok\u001b[2J').copy(frame, 5 + header.length)

    const decoded = decodeFrame(frame)
    expect(decoded.kind === 'timeline' && decoded.event.text).toBe('ok\u001b[2J')
  })

  it('keeps the v4 capture matcher array explicit on Human writes', () => {
    const frame = encodeControl({
      type: 'write', request_id: 'request', port: 'COM6', control_id: 'control', fence: 1,
      data: 'dmVyc2lvbg0=', command_capture_matchers: []
    })
    const header = JSON.parse(frame.subarray(5).toString())
    expect(header.port).toBe('COM6')
    expect(header.command_capture_matchers).toEqual([])
  })
})
