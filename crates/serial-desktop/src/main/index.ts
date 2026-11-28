import { writeSync } from 'node:fs'
import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { app, BrowserWindow, dialog, ipcMain, Menu, nativeTheme, net, shell } from 'electron'
import { DesktopCoordinator } from './coordinator'
import type { DesktopPreferences, MacroListQuery, MacroRunRequest, MacroSaveRequest, ModelFamily, ModelProfile, RunStartDecision, SerialConfigurationDraft } from '../shared/contracts'
import { HELP, startupAction } from './startup'
import { GracefulQuitGate } from './graceful-quit'
import { installWindowControls, titleBarOptions } from './window-controls'
import { createReleaseChecker, type CheckedRelease } from './release-check'
import { installRelease, installTarget } from './release-install'

const qaMode = process.argv.includes('--qa-screenshot')
const qaThemeArgument = process.argv.find((argument) => argument.startsWith('--qa-theme='))?.split('=')[1]
const qaTheme = qaThemeArgument === 'light' ? 'light' : 'dark'
const startup = startupAction(process.argv.slice(1))
let window: BrowserWindow | undefined
let coordinator: DesktopCoordinator | undefined
const quitGate = new GracefulQuitGate()

function send(channel: string, payload: unknown): void {
  const target = window
  if (target && !target.isDestroyed()) target.webContents.send(channel, payload)
}

function createWindow(): void {
  window = new BrowserWindow({
    width: 1480,
    height: 920,
    minWidth: 1080,
    minHeight: 700,
    show: false,
    backgroundColor: nativeTheme.shouldUseDarkColors ? '#0a0d12' : '#f4f6f8',
    ...titleBarOptions,
    webPreferences: {
      preload: join(__dirname, '../preload/index.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true
    }
  })
  window.once('ready-to-show', () => window?.show())
  installWindowControls(window)
  window.webContents.setWindowOpenHandler(({ url }) => {
    void shell.openExternal(url)
    return { action: 'deny' }
  })
  if (process.env.ELECTRON_RENDERER_URL) {
    void window.loadURL(process.env.ELECTRON_RENDERER_URL)
  } else {
    void window.loadFile(join(__dirname, '../renderer/index.html'))
  }
  if (qaMode) {
    window.webContents.once('did-finish-load', () => {
      setTimeout(async () => {
        const qaDirectory = join(process.cwd(), 'qa')
        await mkdir(qaDirectory, { recursive: true })
        await writeFile(join(qaDirectory, `serial-platform-desktop-${qaTheme}.png`), (await window!.capturePage()).toPNG())
        app.quit()
      }, 1_800)
    })
  }
}

function installIpc(): void {
  const request = (url: string, init: RequestInit) => net.fetch(url, init)
  const checkRelease = createReleaseChecker(app.getVersion(), process.platform, process.arch, request)
  const target = app.isPackaged && installTarget(process.platform, process.execPath, process.env)
  let offered: CheckedRelease | null = null
  let installing = false
  ipcMain.handle('desktop:check-release', async () => {
    offered = await checkRelease()
    return offered && { version: offered.version, installable: !!target && offered.installable }
  })
  ipcMain.handle('desktop:install-release', async (_event, version: string) => {
    if (installing) throw new Error('更新正在进行')
    if (!target || !offered?.asset || offered.version !== version) throw new Error('请重新检查更新')
    installing = true
    try {
      await installRelease(offered.asset, target, request)
      app.quit()
    } catch (error) {
      installing = false
      throw error
    }
  })
  ipcMain.handle('serial:setup-create', (_, brief: import('../shared/first-setup').SetupBrief) => coordinator!.createSetup(brief))
  ipcMain.handle('serial:setup-read', (_, id: string) => coordinator!.readSetup(id))
  ipcMain.handle('serial:setup-apply', (_, id: string, revision: number) => coordinator!.applySetup(id, revision))
  ipcMain.handle('serial:bootstrap', () => coordinator!.bootstrap())
  ipcMain.handle('serial:refresh', () => coordinator!.refresh())
  ipcMain.handle('serial:send-command', (_event, port: string, command: string, traceId?: string) =>
    coordinator!.sendCommand(port, command, traceId))
  ipcMain.handle('serial:send-signal', (_event, port: string, signal: 'ctrl_c' | 'ctrl_d') => coordinator!.sendSignal(port, signal))
  ipcMain.handle('serial:human-history', (_event, query: string, contains: boolean, limit?: number) => coordinator!.queryHumanHistory(query, contains, limit))
  ipcMain.handle('serial:agent-history', (_event, port: string) => coordinator!.agentHistory(port))
  ipcMain.handle('serial:clear-agent-history', (_event, port: string, ids?: string[]) => coordinator!.clearAgentHistory(port, ids))
  ipcMain.handle('serial:agent-run-events', (_event, record: import('../shared/contracts').AgentRunRecord) => coordinator!.agentRunEvents(record))
  ipcMain.handle('serial:command-evidence', (_event, target: Parameters<import('../shared/contracts').DesktopBridge['commandEvidence']>[0]) => coordinator!.commandEvidence(target))
  ipcMain.handle('serial:macro-list', (_event, query: MacroListQuery) => coordinator!.listMacros(query))
  ipcMain.handle('serial:macro-save', (_event, definition: MacroSaveRequest) => coordinator!.saveMacro(definition))
  ipcMain.handle('serial:macro-run', (_event, port: string, spec: MacroRunRequest) => coordinator!.runMacro(port, spec))
  ipcMain.handle('serial:macro-cancel', (_event, port: string, executionId: string) => coordinator!.cancelMacro(port, executionId))
  ipcMain.handle('serial:decide-run-start', (
    _event,
    port: string,
    approvalId: string,
    decision: RunStartDecision
  ) => coordinator!.decideRunStart(port, approvalId, decision))
  ipcMain.handle('serial:set-port-open', (_event, port: string, open: boolean) =>
    coordinator!.setPortOpen(port, open))
  ipcMain.handle('serial:set-port-model', (_event, port: string, family: string | null, name: string | null, expectedRevision: number) =>
    coordinator!.setPortModel(port, family, name, expectedRevision))
  ipcMain.handle('serial:save-serial-configuration', (
    _event,
    draft: SerialConfigurationDraft,
    expectedRevision: number
  ) => coordinator!.saveSerialConfiguration(draft, expectedRevision))
  ipcMain.handle('serial:save-model-profiles', (_event, profiles: ModelProfile[], expectedRevision: number) =>
    coordinator!.saveModelProfiles(profiles, expectedRevision))
  ipcMain.handle('serial:save-model-families', (_event, families: ModelFamily[], expectedRevision: number) =>
    coordinator!.saveModelFamilies(families, expectedRevision))
  ipcMain.handle('serial:save-preferences', (_event, preferences: Partial<DesktopPreferences>) =>
    coordinator!.savePreferences(preferences))
  ipcMain.handle('serial:start-local-service', () => coordinator!.startLocalService())
  ipcMain.handle('serial:stop-local-service', () => coordinator!.stopLocalService())
}

function installMenu(): void {
  const menu = Menu.buildFromTemplate([
    ...(process.platform === 'darwin' ? [{ role: 'appMenu' as const }] : []),
    {
      label: '编辑',
      submenu: [
        { role: 'undo' }, { role: 'redo' }, { type: 'separator' },
        { role: 'cut' }, { role: 'copy' }, { role: 'paste' }, { role: 'selectAll' }
      ]
    },
    {
      label: '视图',
      submenu: [
        { role: 'reload' }, { role: 'toggleDevTools' }, { type: 'separator' },
        { role: 'resetZoom' }, { role: 'zoomIn' }, { role: 'zoomOut' }, { role: 'togglefullscreen' }
      ]
    },
    { role: 'windowMenu' }
  ])
  Menu.setApplicationMenu(menu)
}

if (startup === 'help') {
  writeSync(process.stdout.fd, HELP)
  process.exit(0)
} else if (startup === 'version') {
  writeSync(process.stdout.fd, `serial-desktop ${app.getVersion()}\n`)
  process.exit(0)
} else if (!app.requestSingleInstanceLock()) {
  app.quit()
} else {
  app.on('second-instance', () => {
    if (window?.isMinimized()) window.restore()
    window?.focus()
  })
  app.whenReady().then(() => {
    coordinator = new DesktopCoordinator((event) => send('serial:event', event), qaMode, qaTheme)
    installIpc()
    installMenu()
    createWindow()
    if (process.env.SERIAL_PLATFORM_UPDATE_ERROR) {
      dialog.showErrorBox('更新失败，已恢复原版本', process.env.SERIAL_PLATFORM_UPDATE_ERROR)
      delete process.env.SERIAL_PLATFORM_UPDATE_ERROR
    }
  })
  app.on('before-quit', (event) => {
    const intercepted = quitGate.intercept(
      () => coordinator?.shutdown() ?? Promise.resolve(),
      () => app.exit(0)
    )
    if (intercepted) event.preventDefault()
  })
  app.on('window-all-closed', () => app.quit())
}
