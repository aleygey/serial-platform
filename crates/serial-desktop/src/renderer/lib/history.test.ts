import { describe, expect, it } from 'vitest'
import type { TimelineEvent } from '../../shared/contracts'
import { buildAgentHistory, locateCommandOutput } from './history'

function event(seq: number, direction: 'rx' | 'tx', text: string): TimelineEvent {
  return {
    port: 'COM6', daemon_epoch: 'epoch', seq, generation: 1, wall_time_ns: 0,
    kind: direction, direction, text, metadata: {}, durable: true
  }
}

describe('Agent history', () => {
  it('keeps old-to-new order and groups command sequence steps', () => {
    const second = event(12, 'tx', 'password\r')
    second.actor = { id: 'agent', label: 'Agent', kind: 'agent' }
    second.metadata = {
      command_description: '输入密码', command_sequence_description: '登录设备',
      command_sequence_id: 'login', command_sequence_step_index: 1
    }
    const first = event(10, 'tx', 'admin\r')
    first.actor = second.actor
    first.metadata = { ...second.metadata, command_description: '输入账号', command_sequence_step_index: 0 }
    const later = event(20, 'tx', 'version\r')
    later.actor = second.actor
    later.operation_id = 'version'
    later.metadata = { command_description: '读取版本' }

    const history = buildAgentHistory([later, second, first])

    expect(history.map((item) => item.firstSeq)).toEqual([10, 20])
    expect(history[0].kind === 'command' && history[0].commands.map((item) => item.text)).toEqual([
      'admin\r', 'password\r'
    ])
  })

  it('uses the persisted prompt matcher and otherwise returns no synthetic range', () => {
    const command = {
      id: 'version', daemonEpoch: 'epoch', generation: 1, firstSeq: 5, text: 'version\r',
      captureMatchers: [{ kind: 'shell_prompt' as const, value: 'root# ' }]
    }
    const events = [event(6, 'rx', 'ver'), event(7, 'rx', 'sion\n1.0.0\n'), event(8, 'rx', 'root# ')]

    expect(locateCommandOutput(events, command)).toEqual({
      daemonEpoch: 'epoch', generation: 1,
      fromSeq: 6, throughSeq: 8, evidenceFromSeq: 5, evidenceThroughSeq: 8,
      inferred: true, hasVisibleOutput: true
    })
    expect(locateCommandOutput(events, { ...command, captureMatchers: undefined })).toBeUndefined()
  })

  it('does not finish a legacy capture at the prompt prefix of an echoed command', () => {
    const command = {
      id: 'version', daemonEpoch: 'epoch', generation: 1, firstSeq: 5, text: 'show version\r',
      captureMatchers: [{ kind: 'shell_prompt' as const, value: 'root# ' }]
    }
    const echoed = event(6, 'rx', 'root# show version\r\n')
    const output = event(7, 'rx', 'Version 1.2.3\r\n')
    const completed = event(8, 'rx', 'root# \u001b[0m')

    expect(locateCommandOutput([echoed, output, completed], command)).toEqual({
      daemonEpoch: 'epoch', generation: 1,
      fromSeq: 6, throughSeq: 8, evidenceFromSeq: 5, evidenceThroughSeq: 8,
      inferred: true, hasVisibleOutput: true
    })
    expect(locateCommandOutput([echoed, output], command)).toBeUndefined()
  })

  it('prefers the durable command capture matcher over profile prompts', () => {
    const events = [event(6, 'rx', 'working\n'), event(7, 'rx', 'DONE\n'), event(8, 'rx', 'root# ')]
    const command = {
      id: 'wait', daemonEpoch: 'epoch', generation: 1, firstSeq: 5, text: 'wait\r',
      captureMatchers: [
        { kind: 'contains' as const, value: 'root# ' },
        { kind: 'contains' as const, value: 'DONE' }
      ]
    }

    expect(locateCommandOutput(events, command)).toEqual({
      daemonEpoch: 'epoch', generation: 1,
      fromSeq: 6, throughSeq: 7, evidenceFromSeq: 5, evidenceThroughSeq: 7,
      inferred: true, hasVisibleOutput: true
    })
  })

  it('does not let a legacy matcher drift into a later command or daemon generation', () => {
    const command = {
      id: 'first', daemonEpoch: 'epoch', generation: 1, firstSeq: 5, operationId: 'first', text: 'first\r',
      captureMatchers: [{ kind: 'contains' as const, value: 'DONE' }]
    }
    const next = event(7, 'tx', 'next\r')
    next.operation_id = 'next'
    const wrongEpoch = { ...event(6, 'rx', 'DONE\n'), daemon_epoch: 'other' }

    expect(locateCommandOutput([event(6, 'rx', 'working\n'), next, event(8, 'rx', 'DONE\n'), wrongEpoch], command))
      .toBeUndefined()
  })

  it('uses the durable capture boundary without re-running a repeated matcher', () => {
    const tx = event(5, 'tx', 'status\r')
    tx.actor = { id: 'agent', label: 'Agent', kind: 'agent' }
    tx.operation_id = 'operation'
    tx.metadata = {
      command_description: '读取状态',
      command_capture_matchers: [{ kind: 'contains', value: 'root# ' }]
    }
    const capture = event(9, 'rx', '')
    capture.kind = 'command_capture_completed'
    capture.direction = 'none'
    capture.operation_id = 'operation'
    capture.metadata = {
      capture: {
        daemon_epoch: 'epoch', generation: 1, run_id: 'run', operation_id: 'operation',
        tx_event_seq: 5, evidence_from_seq: 5, evidence_through_seq: 8,
        completion: 'quiet', confidence: 'high', record_event_seq: 9,
        rx_stream_offset_start: 120, rx_stream_offset_end: 188
      }
    }
    const events = [tx, event(6, 'rx', 'working\n'), event(7, 'rx', 'root# old\n'), event(8, 'rx', 'done\n'), capture, event(10, 'rx', 'root# ')]
    const history = buildAgentHistory(events)
    const command = history.find((item) => item.kind === 'command')

    expect(command?.kind === 'command' && locateCommandOutput(events, command.commands[0])).toEqual({
      daemonEpoch: 'epoch',
      generation: 1,
      fromSeq: 6,
      throughSeq: 8,
      evidenceFromSeq: 5,
      evidenceThroughSeq: 8,
      fromStreamOffset: 120,
      throughStreamOffset: 188,
      inferred: false,
      hasVisibleOutput: true
    })
  })
})
