import type { TimelineEvent } from '../shared/contracts'

const CONTROL = 0x01
const RX = 0x02
const TX = 0x03

export const SERIAL_PROTOCOL_VERSION = 5

export interface WireControl {
  type: string
  [key: string]: unknown
}

export type DecodedFrame =
  | { kind: 'control'; message: WireControl }
  | { kind: 'timeline'; event: TimelineEvent }

export function encodeControl(message: WireControl): Buffer {
  const header = Buffer.from(JSON.stringify(message))
  const envelope = Buffer.allocUnsafe(5 + header.length)
  envelope[0] = CONTROL
  envelope.writeUInt32BE(header.length, 1)
  header.copy(envelope, 5)
  return envelope
}

export function decodeFrame(data: Buffer): DecodedFrame {
  if (data.length < 5) throw new Error('WebSocket 帧不完整')
  const tag = data[0]
  const headerLength = data.readUInt32BE(1)
  const payloadStart = 5 + headerLength
  if (payloadStart > data.length) throw new Error('WebSocket 帧头长度无效')
  const header = JSON.parse(data.subarray(5, payloadStart).toString('utf8')) as Record<string, unknown>
  if (tag === CONTROL) return { kind: 'control', message: header as WireControl }
  if (tag !== RX && tag !== TX) throw new Error(`未知 WebSocket 帧类型 ${tag}`)
  return {
    kind: 'timeline',
    event: normalizeTimelineEvent({
      ...header,
      text: data.subarray(payloadStart).toString('utf8'),
      direction: tag === RX ? 'rx' : 'tx'
    })
  }
}

export function normalizeTimelineEvent(raw: Record<string, unknown>): TimelineEvent {
  const encoded = typeof raw.data === 'string' ? raw.data : undefined
  return {
    port: String(raw.port ?? ''),
    daemon_epoch: String(raw.daemon_epoch ?? ''),
    seq: Number(raw.seq ?? 0),
    generation: Number(raw.generation ?? 0),
    wall_time_ns: Number(raw.wall_time_ns ?? 0),
    kind: String(raw.kind ?? 'gap'),
    direction: (raw.direction as TimelineEvent['direction']) ?? 'none',
    actor: (raw.actor as TimelineEvent['actor']) ?? null,
    run_id: raw.run_id ? String(raw.run_id) : null,
    operation_id: raw.operation_id ? String(raw.operation_id) : null,
    text:
      typeof raw.text === 'string'
        ? raw.text
        : encoded
          ? Buffer.from(encoded, 'base64').toString('utf8')
          : '',
    metadata: (raw.metadata as Record<string, unknown>) ?? {},
    durable: Boolean(raw.durable),
    replay: Boolean(raw.replay)
  }
}
