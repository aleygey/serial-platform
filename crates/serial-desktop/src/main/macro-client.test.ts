import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ControlLease, MacroExecution } from '../shared/contracts'
import { HumanCommandOutcomeUncertainError, SerialClient } from './serial-client'
import type { WireControl } from './protocol'

afterEach(() => vi.useRealTimers())

const actor = { id: 'human:app', label: 'App', kind: 'human' as const }
const lease: ControlLease = { id: 'lease', owner: actor, epoch: 'epoch', generation: 2, fence: 3, issued_wall_time_ns: 0, expires_wall_time_ns: 0 }
const running: MacroExecution = { id: 'execution', port: 'COM6', daemon_epoch: 'epoch', generation: 2, owner: actor, description: 'macro', status: 'running', started_at_ns: 0, line: 1, column: 1, writes: 0, bytes_written: 0, first_seq: 0, through_seq: 0, outcome_uncertain: false }

function clientFixture(owned = false) {
  const client = new SerialClient('http://127.0.0.1:3210')
  const socket = { readyState: 3 }
  const status = { ports: [{ config: { port: 'COM6' }, session_state: 'online', control: owned ? lease : null }] }
  Object.assign(client, { socket, readySocket: socket, actor, status })
  let execution = { ...running }
  const control = vi.fn(async (message: WireControl) => {
    if (message.type === 'acquire_control' || message.type === 'renew_control') return { type: message.type === 'acquire_control' ? 'control_granted' : 'control_renewed', lease }
    if (message.type === 'macro_start') return { type: 'macro_started', execution }
    if (message.type === 'macro_status') return { type: 'macro_status', execution }
    if (message.type === 'macro_cancel') { execution = { ...execution, status: 'cancelled' }; return { type: 'macro_cancelled', execution } }
    return { type: 'control_released' }
  })
  Object.assign(client, { control })
  return { client, control, status, finish: (status: MacroExecution['status']) => { execution = { ...execution, status } } }
}

describe('Human macro execution ownership', () => {
  it('retains the temporarily acquired lease when Human input interrupts the macro', async () => {
    vi.useFakeTimers()
    const { client, control, finish } = clientFixture()
    await client.runMacro('COM6', { macro_id: 'boot', revision: 1, args: {}, timeout_seconds: 30 })
    finish('interrupted_by_user')
    await vi.advanceTimersByTimeAsync(250)
    expect(control.mock.calls.some(([message]) => message.type === 'release_control')).toBe(false)
    await client.stop()
  })

  it('recovers an uncertain start only by its stable execution id, without replay', async () => {
    vi.useFakeTimers()
    const { client } = clientFixture()
    let operationId = ''
    const control = vi.fn(async (message: WireControl) => {
      if (message.type === 'acquire_control') return { type: 'control_granted', lease }
      if (message.type === 'macro_start') {
        operationId = String(message.operation_id)
        throw new HumanCommandOutcomeUncertainError('lost acknowledgement')
      }
      if (message.type === 'macro_status') return { type: 'macro_status', execution: { ...running, id: operationId, status: 'cancelled' } }
      return { type: 'control_released' }
    })
    Object.assign(client, { control })
    const result = await client.runMacro('COM6', { macro_id: 'boot', revision: 1, args: {}, timeout_seconds: 30 })
    expect(result.id).toBe(operationId)
    expect(control.mock.calls.filter(([message]) => message.type === 'macro_start')).toHaveLength(1)
    expect(control.mock.calls.find(([message]) => message.type === 'macro_status')?.[0]).toMatchObject({ execution_id: operationId })
    await client.stop()
  })

  it('acquires without takeover, pins revision, renews while running and stops before releasing', async () => {
    vi.useFakeTimers()
    const { client, control } = clientFixture()
    await client.runMacro('COM6', { macro_id: 'boot', revision: 4, args: {}, timeout_seconds: 30 })
    expect(control.mock.calls[0][0]).toMatchObject({ type: 'acquire_control', mode: 'queue' })
    expect(control.mock.calls[1][0]).toMatchObject({ type: 'macro_start', control_id: 'lease', fence: 3, daemon_epoch: 'epoch', generation: 2, expected_run_id: null, spec: { macro_id: 'boot', revision: 4 } })
    await expect(client.runMacro('COM6', { script: 'cmd("reboot");', args: {}, timeout_seconds: 30 })).rejects.toThrow('已有宏')
    await vi.advanceTimersByTimeAsync(3_000)
    expect(control.mock.calls.some(([message]) => message.type === 'renew_control')).toBe(true)
    await client.cancelMacro('COM6', 'execution')
    expect(control.mock.calls.some(([message]) => message.type === 'release_control')).toBe(false)
    await vi.advanceTimersByTimeAsync(250)
    expect(control.mock.calls.at(-1)?.[0]).toMatchObject({ type: 'release_control', control_id: 'lease' })
    await client.stop()
  })

  it('retains a pre-existing Human lease and refuses another owner without making a request', async () => {
    vi.useFakeTimers()
    const { client, control, finish, status } = clientFixture(true)
    await client.runMacro('COM6', { macro_id: 'boot', revision: 1, args: {}, timeout_seconds: 30 })
    finish('interrupted_by_user')
    await vi.advanceTimersByTimeAsync(250)
    expect(control.mock.calls.map(([message]) => message.type)).toEqual(['macro_start', 'macro_status'])
    status.ports[0].control = { ...lease, owner: { ...actor, id: 'agent' } }
    const before = control.mock.calls.length
    await expect(client.runMacro('COM6', { script: 'cmd("x");', args: {}, timeout_seconds: 30 })).rejects.toThrow('占用')
    expect(control).toHaveBeenCalledTimes(before)
    await client.stop()
  })
})
