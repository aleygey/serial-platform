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
  serialdIdentityMatches: vi.fn(() => true),
  SerialClient: class {
    constructor() {
      throw new Error('Invalid URL')
    }
  }
}))

import { DesktopCoordinator } from './coordinator'

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
})
