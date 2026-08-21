import type { DesktopPreferences, DesktopSnapshot, ServiceState } from './contracts'

export function createOfflineSnapshot(
  preferences: DesktopPreferences,
  service: ServiceState,
  message: string
): DesktopSnapshot {
  return {
    connection: 'offline',
    connectionMessage: message,
    configRevision: 0,
    configuredPorts: [],
    availablePorts: [],
    transportProfiles: [],
    modelProfiles: [],
    events: {},
    preferences,
    service
  }
}
