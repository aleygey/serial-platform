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
      id: 'version', firstSeq: 5, text: 'version\r',
      captureMatchers: [{ kind: 'shell_prompt' as const, value: 'root# ' }]
    }
    const events = [event(6, 'rx', 'ver'), event(7, 'rx', 'sion\n1.0.0\n'), event(8, 'rx', 'root# ')]

    expect(locateCommandOutput(events, command)).toEqual({ fromSeq: 6, throughSeq: 8 })
    expect(locateCommandOutput(events, { ...command, captureMatchers: undefined })).toBeUndefined()
  })

  it('prefers the durable command capture matcher over profile prompts', () => {
    const events = [event(6, 'rx', 'working\n'), event(7, 'rx', 'DONE\n'), event(8, 'rx', 'root# ')]
    const command = {
      id: 'wait', firstSeq: 5, text: 'wait\r',
      captureMatchers: [
        { kind: 'contains' as const, value: 'root# ' },
        { kind: 'contains' as const, value: 'DONE' }
      ]
    }

    expect(locateCommandOutput(events, command)).toEqual({ fromSeq: 6, throughSeq: 7 })
  })
})
