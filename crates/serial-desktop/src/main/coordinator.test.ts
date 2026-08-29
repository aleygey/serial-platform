import { describe, expect, it, vi } from 'vitest'

vi.mock('./settings', () => ({
  SettingsStore: class {
    async load(): Promise<object> {
      return { endpoint: 'not a valid endpoint', autoStartLocal: false, theme: 'dark' }
    }

    async save(): Promise<void> {}
  }
}))

vi.mock('./local-service', () => ({
  LocalService: class {
    state(): object {
      return { owned: false, status: 'stopped' }
    }

    async stop(): Promise<void> {}
  }
}))

vi.mock('./serial-client', () => ({
  ConfigurationConflictError: class extends Error {},
  HumanCommandOutcomeUncertainError: class extends Error {},
  serialdIdentityMatches: vi.fn(() => true),
  SerialClient: class {
    constructor() {
      throw new Error('Invalid URL')
    }
  }
}))

import { DesktopCoordinator } from './coordinator'
import {
  ConfigurationConflictError,
  HumanCommandOutcomeUncertainError,
  type ServerData
} from './serial-client'
import type { SerialConfigurationDraft } from '../shared/contracts'

describe('DesktopCoordinator offline bootstrap', () => {
  it('returns a configurable offline snapshot when an invalid endpoint cannot auto-start', async () => {
    const emit = vi.fn()
    const coordinator = new DesktopCoordinator(emit)
    const snapshot = await coordinator.bootstrap()

    expect(snapshot.connection).toBe('offline')
    expect(snapshot.connectionMessage).toBe('Invalid URL')
    expect(snapshot.preferences).toEqual({ endpoint: 'not a valid endpoint', autoStartLocal: false, theme: 'dark' })
    expect(snapshot.service).toEqual({ owned: false, status: 'stopped' })
    expect(snapshot.configuredPorts).toEqual([])
    expect(emit).toHaveBeenCalledWith({ type: 'connection', state: 'offline', message: 'Invalid URL' })
  })

  it('publishes the refreshed snapshot when a serial save detects a configuration conflict', async () => {
    const emit = vi.fn()
    const coordinator = new DesktopCoordinator(emit)
    const conflict = new ConfigurationConflictError('configuration conflict')
    const client = {
      saveSerialConfiguration: vi.fn().mockRejectedValue(conflict),
      data: vi.fn(() => serverData(8))
    }
    Object.assign(coordinator, {
      client,
      preferences: { endpoint: 'http://127.0.0.1:3210', autoStartLocal: false, theme: 'dark' }
    })
    const draft: SerialConfigurationDraft = {
      port: 'COM6',
      enabled: true,
      transportProfile: {
        name: '115200-8N1',
        baud_rate: 115200,
        data_bits: 'eight',
        parity: 'none',
        stop_bits: 'one',
        flow_control: 'none',
        dtr: false,
        rts: false,
        auto_open: true
      }
    }

    await expect(coordinator.saveSerialConfiguration(draft, 7)).rejects.toBe(conflict)

    expect(client.saveSerialConfiguration).toHaveBeenCalledWith(draft, 7)
    expect(emit).toHaveBeenCalledWith({
      type: 'snapshot',
      snapshot: expect.objectContaining({ configRevision: 8 })
    })
  })

  it('returns structured Human command outcomes without turning a definite rejection into uncertainty', async () => {
    const coordinator = new DesktopCoordinator(vi.fn())
    const sendCommand = vi.fn()
    Object.assign(coordinator, { client: { sendCommand } })

    sendCommand.mockRejectedValueOnce(new Error('daemon rejected the command'))
    await expect(coordinator.sendCommand('COM6', 'version')).resolves.toEqual({
      status: 'rejected',
      message: 'daemon rejected the command'
    })

    sendCommand.mockRejectedValueOnce(new HumanCommandOutcomeUncertainError('socket closed'))
    await expect(coordinator.sendCommand('COM6', 'version')).resolves.toEqual({
      status: 'uncertain',
      message: 'socket closed'
    })
  })
})

function serverData(configRevision: number): ServerData {
  return {
    status: {
      server_id: '11111111-1111-4111-8111-111111111111',
      daemon_epoch: '22222222-2222-4222-8222-222222222222',
      protocol_version: 7,
      config_revision: configRevision,
      ports: []
    },
    availablePorts: [],
    transportProfiles: [],
    modelProfiles: [],
    modelFamilies: [],
    events: {}
  }
}
