import { writeSync } from 'node:fs'
import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { app, BrowserWindow, ipcMain, Menu, nativeTheme, shell } from 'electron'
import { DesktopCoordinator } from './coordinator'
import type { DesktopPreferences, ModelProfile, SerialConfigurationDraft } from '../shared/contracts'
import { HELP, startupAction } from './startup'
import { GracefulQuitGate } from './graceful-quit'

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
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'hidden',
    trafficLightPosition: { x: 18, y: 18 },
    webPreferences: {
      preload: join(__dirname, '../preload/index.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true
    }
  })
  window.once('ready-to-show', () => window?.show())
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
  ipcMain.handle('serial:bootstrap', () => coordinator!.bootstrap())
  ipcMain.handle('serial:refresh', () => coordinator!.refresh())
  ipcMain.handle('serial:send-command', (_event, port: string, command: string) =>
    coordinator!.sendCommand(port, command))
  ipcMain.handle('serial:set-port-open', (_event, port: string, open: boolean) =>
    coordinator!.setPortOpen(port, open))
  ipcMain.handle('serial:save-serial-configuration', (_event, draft: SerialConfigurationDraft) =>
    coordinator!.saveSerialConfiguration(draft))
  ipcMain.handle('serial:save-model-profiles', (_event, profiles: ModelProfile[]) =>
    coordinator!.saveModelProfiles(profiles))
  ipcMain.handle('serial:save-preferences', (_event, preferences: DesktopPreferences) =>
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
