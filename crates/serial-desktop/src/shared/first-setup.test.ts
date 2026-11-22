import { describe, expect, it } from 'vitest'
import type { ModelProfile, PortConfig } from './contracts'
import { setupChanges, type SetupRequest } from './first-setup'

const ports: PortConfig[] = ['COM3', 'COM4'].map(port => ({
  port, enabled: true, model_profile: 'shared', transport_profile: '8N1', model_family: 'Probe', model_name: 'A',
}))
const profiles: ModelProfile[] = [{ name: 'shared', shell_prompt: '$ ', uboot_prompt: '=> ', echo: 'off' }]
const heartbeat: SetupRequest = {
  id: '132caa6a-80e7-4fb0-a8a4-20420d4d0320', agent: 'Test',
  brief: { port: 'COM3', prompt_hint: '', check: 'heartbeat', intent: '' },
  plan: { shell_prompt: 'probe> ', command: '', expected_reply: '' },
}
describe('first-use configuration', () => {
  it('preserves the other port and shared profile and makes a retry idempotent', () => {
    const changes = setupChanges(heartbeat, ports, profiles)
    expect(changes.profile).toMatchObject({ shell_prompt: 'probe> ', uboot_prompt: '=> ', echo: 'off' })
    expect(changes.profiles[0]).toEqual(profiles[0])
    expect(changes.ports[1]).toEqual(ports[1])
    expect(changes.ports[0]).toEqual({ ...ports[0], model_profile: changes.profile.name })
    expect(changes.macro.applies_to).toEqual({ model_family: 'Probe', model_names: ['A'] })
    expect(changes.macro.script.split('\n')).toEqual(['let ready = watch(prompt("shell"));', 'cmd("", "send_only");', 'expect(ready, 3000);'])
    const retry = setupChanges(heartbeat, changes.ports, changes.profiles)
    expect(retry).toMatchObject({ alreadyBound: true, profiles: changes.profiles, macro: changes.macro })
  })
  it('watches a literal reply before sending a status check and retains prompt whitespace', () => {
    const plan = { shell_prompt: '$ ', command: 'get "device"', expected_reply: 'state="ready"' }
    const changes = setupChanges({ ...heartbeat, brief: { ...heartbeat.brief, check: 'status' }, plan }, ports, profiles)
    expect(changes.alreadyBound).toBe(true)
    expect(changes.macro.script.split('\n')).toEqual([
      'let ready = watch(prompt("shell"));', 'let reply = watch(' + JSON.stringify(plan.expected_reply) + ');',
      'cmd(' + JSON.stringify(plan.command) + ', "send_only");', 'expect(reply, 3000);', 'expect(ready, 3000);',
    ])
  })
  it('fits UTF-8 catalog limits and keeps an existing raw-input profile intact', () => {
    const port = '/dev/' + '设备'.repeat(64)
    const snapshot = [{ ...ports[0], port }]
    const raw: ModelProfile[] = [{ ...profiles[0], shell_prompt: 'probe> ', write_eol: '' }]
    const task: SetupRequest = { ...heartbeat, brief: { ...heartbeat.brief, port, check: 'status', intent: '读取设备版本\n'.repeat(80) },
      plan: { shell_prompt: 'probe> ', command: 'version', expected_reply: 'firmware' } }
    const changes = setupChanges(task, snapshot, raw)
    expect(changes.profile.write_eol).toBe('\r')
    expect(changes.profiles[0].write_eol).toBe('')
    expect(Buffer.byteLength(changes.profile.name)).toBeLessThanOrEqual(64)
    expect(Buffer.byteLength(changes.macro.name)).toBeLessThanOrEqual(128)
    expect(Buffer.byteLength(changes.macro.description)).toBeLessThanOrEqual(256)
    expect(changes.macro.description).not.toContain('\n')
  })
})
