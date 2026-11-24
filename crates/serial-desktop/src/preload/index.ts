import { contextBridge, ipcRenderer } from 'electron'
import { version } from '../../package.json'
import type {
  DesktopBridge,
  DesktopEvent,
  DesktopPreferences,
  DesktopUpdates,
  DesktopWindowBridge,
  ModelFamily,
  ModelProfile,
  SerialConfigurationDraft
} from '../shared/contracts'

const bridge: DesktopBridge = {
  createSetup: brief => ipcRenderer.invoke('serial:setup-create', brief),
  readSetup: id => ipcRenderer.invoke('serial:setup-read', id),
  applySetup: (id, revision) => ipcRenderer.invoke('serial:setup-apply', id, revision),
  bootstrap: () => ipcRenderer.invoke('serial:bootstrap'),
  refresh: () => ipcRenderer.invoke('serial:refresh'),
  sendCommand: (port, command, traceId) => ipcRenderer.invoke('serial:send-command', port, command, traceId),
  sendSignal: (port, signal) => ipcRenderer.invoke('serial:send-signal', port, signal),
  queryHumanHistory: (query, contains, limit) => ipcRenderer.invoke('serial:human-history', query, contains, limit),
  agentHistory: (port) => ipcRenderer.invoke('serial:agent-history', port),
  clearAgentHistory: (port, ids) => ipcRenderer.invoke('serial:clear-agent-history', port, ids),
  agentRunEvents: (record) => ipcRenderer.invoke('serial:agent-run-events', record),
  commandEvidence: (target) => ipcRenderer.invoke('serial:command-evidence', target),
  listMacros: (query) => ipcRenderer.invoke('serial:macro-list', query),
  saveMacro: (definition) => ipcRenderer.invoke('serial:macro-save', definition),
  runMacro: (port, spec) => ipcRenderer.invoke('serial:macro-run', port, spec),
  cancelMacro: (port, executionId) => ipcRenderer.invoke('serial:macro-cancel', port, executionId),
  decideRunStart: (port, approvalId, decision) =>
    ipcRenderer.invoke('serial:decide-run-start', port, approvalId, decision),
  setPortOpen: (port, open) => ipcRenderer.invoke('serial:set-port-open', port, open),
  setPortModel: (port, family, name, expectedRevision) =>
    ipcRenderer.invoke('serial:set-port-model', port, family, name, expectedRevision),
  saveSerialConfiguration: (draft: SerialConfigurationDraft, expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-serial-configuration', draft, expectedRevision),
  saveModelProfiles: (profiles: ModelProfile[], expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-model-profiles', profiles, expectedRevision),
  saveModelFamilies: (families: ModelFamily[], expectedRevision: number) =>
    ipcRenderer.invoke('serial:save-model-families', families, expectedRevision),
  savePreferences: (preferences: Partial<DesktopPreferences>) =>
    ipcRenderer.invoke('serial:save-preferences', preferences),
  startLocalService: () => ipcRenderer.invoke('serial:start-local-service'),
  stopLocalService: () => ipcRenderer.invoke('serial:stop-local-service'),
  onEvent(listener) {
    const handler = (_event: Electron.IpcRendererEvent, payload: DesktopEvent): void => {
      const events = payload.type === 'timeline-batch' ? payload.events : payload.type === 'timeline' ? [payload.event] : []
      for (const event of events) {
        if (event.inputObservation) event.inputObservation.deliveredAt = performance.timeOrigin + performance.now()
      }
      listener(payload)
    }
    ipcRenderer.on('serial:event', handler)
    return () => ipcRenderer.removeListener('serial:event', handler)
  }
}

contextBridge.exposeInMainWorld('serial', bridge)

const desktopWindow: DesktopWindowBridge = {
  act: action => ipcRenderer.send('window:action', action),
  isFullScreen: () => ipcRenderer.invoke('window:is-fullscreen'),
  onFullScreenChange(listener) {
    const handler = (_event: Electron.IpcRendererEvent, fullScreen: boolean) => listener(fullScreen)
    ipcRenderer.on('window:fullscreen', handler)
    return () => ipcRenderer.removeListener('window:fullscreen', handler)
  }
}
contextBridge.exposeInMainWorld('desktopWindow', desktopWindow)

const updates: DesktopUpdates = { installedVersion: version, check: () => ipcRenderer.invoke('desktop:check-release') }
contextBridge.exposeInMainWorld('desktopUpdates', updates)
