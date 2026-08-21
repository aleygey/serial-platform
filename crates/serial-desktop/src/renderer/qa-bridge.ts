import type { DesktopBridge, DesktopEvent, DesktopPreferences } from '../shared/contracts'
import { createQaSnapshot } from '../shared/qa-fixture'

export function installQaBridge(): boolean {
  const runtimeWindow = window as unknown as { serial?: DesktopBridge; location: Location }
  if (runtimeWindow.serial) return true
  const params = new URLSearchParams(runtimeWindow.location.search)
  if (params.get('qa') !== '1') return false
  const theme = params.get('theme')
  const preferences: DesktopPreferences = {
    endpoint: 'http://127.0.0.1:3210', autoStartLocal: true,
    theme: theme === 'light' || theme === 'dark' ? theme : 'system', selectedPort: 'COM6'
  }
  let snapshot = createQaSnapshot(preferences)
  const listeners = new Set<(event: DesktopEvent) => void>()
  const publish = (): void => listeners.forEach((listener) => listener({ type: 'snapshot', snapshot }))
  const bridge: DesktopBridge = {
    bootstrap: async () => snapshot,
    refresh: async () => snapshot,
    sendCommand: async () => undefined,
    setPortOpen: async (port, open) => {
      snapshot = {
        ...snapshot,
        configuredPorts: snapshot.configuredPorts.map((item) => item.config.port === port
          ? { ...item, config: { ...item.config, enabled: open }, session_state: open ? 'online' : 'disabled' }
          : item)
      }
      publish()
    },
    saveSerialConfiguration: async () => undefined,
    saveModelProfiles: async () => undefined,
    savePreferences: async (next) => {
      snapshot = { ...snapshot, preferences: next }
      publish()
    },
    startLocalService: async () => {
      snapshot = {
        ...snapshot,
        connection: 'connected',
        connectionMessage: '实时连接已建立',
        service: { owned: true, status: 'running', pid: 4208, program: 'seriald' }
      }
      publish()
    },
    stopLocalService: async () => {
      snapshot = {
        ...snapshot,
        connection: 'offline',
        connectionMessage: '本地服务已停止',
        service: { owned: false, status: 'stopped' }
      }
      publish()
    },
    onEvent(listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    }
  }
  Object.defineProperty(runtimeWindow, 'serial', { value: bridge, configurable: false })
  return true
}
