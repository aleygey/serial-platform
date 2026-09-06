import { contextBridge, ipcRenderer } from 'electron'
import type {
  DesktopBridge,
  DesktopEvent,
  DesktopPreferences,
  ModelFamily,
  ModelProfile,
  SerialConfigurationDraft
} from '../shared/contracts'

const bridge: DesktopBridge = {
  bootstrap: () => ipcRenderer.invoke('serial:bootstrap'),
  refresh: () => ipcRenderer.invoke('serial:refresh'),
  sendCommand: (port, command) => ipcRenderer.invoke('serial:send-command', port, command),
  sendSignal: (port, signal) => ipcRenderer.invoke('serial:send-signal', port, signal),
  queryHumanHistory: (query, contains) => ipcRenderer.invoke('serial:human-history', query, contains),
  listMacros: (query) => ipcRenderer.invoke('serial:macro-list', query),
  saveMacro: (definition) => ipcRenderer.invoke('serial:macro-save', definition),
  runMacro: (port, spec) => ipcRenderer.invoke('serial:macro-run', port, spec),
  cancelMacro: (port, executionId) => ipcRenderer.invoke('serial:macro-cancel', port, executionId),
  decideRunStart: (port, approvalId, decision) =>
    ipcRenderer.invoke('serial:decide-run-start', port, approvalId, decision),
  setPortOpen: (port, open) => ipcRenderer.invoke('serial:set-port-open', port, open),
  saveSerialConfiguration: (draft: SerialConfigurationDraft, expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-serial-configuration', draft, expectedRevision),
  saveModelProfiles: (profiles: ModelProfile[], expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-model-profiles', profiles, expectedRevision),
  saveModelFamilies: (families: ModelFamily[], expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-model-families', families, expectedRevision),
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
