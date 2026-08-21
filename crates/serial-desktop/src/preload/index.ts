import { contextBridge, ipcRenderer } from 'electron'
import type {
  DesktopBridge,
  DesktopEvent,
  DesktopPreferences,
  ModelProfile,
  SerialConfigurationDraft
} from '../shared/contracts'

const bridge: DesktopBridge = {
  bootstrap: () => ipcRenderer.invoke('serial:bootstrap'),
  refresh: () => ipcRenderer.invoke('serial:refresh'),
  sendCommand: (port, command) => ipcRenderer.invoke('serial:send-command', port, command),
  setPortOpen: (port, open) => ipcRenderer.invoke('serial:set-port-open', port, open),
  saveSerialConfiguration: (draft: SerialConfigurationDraft) =>
    ipcRenderer.invoke('serial:save-serial-configuration', draft),
  saveModelProfiles: (profiles: ModelProfile[]) =>
    ipcRenderer.invoke('serial:save-model-profiles', profiles),
  savePreferences: (preferences: DesktopPreferences) =>
    ipcRenderer.invoke('serial:save-preferences', preferences),
  startLocalService: () => ipcRenderer.invoke('serial:start-local-service'),
  stopLocalService: () => ipcRenderer.invoke('serial:stop-local-service'),
  onEvent(listener) {
    const handler = (_event: Electron.IpcRendererEvent, payload: DesktopEvent): void => listener(payload)
    ipcRenderer.on('serial:event', handler)
    return () => ipcRenderer.removeListener('serial:event', handler)
  }
}

contextBridge.exposeInMainWorld('serial', bridge)
