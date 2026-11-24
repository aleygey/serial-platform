import type { BrowserWindow, BrowserWindowConstructorOptions } from 'electron'
import type { WindowAction } from '../shared/contracts'

export const titleBarOptions: BrowserWindowConstructorOptions = process.platform === 'darwin'
  ? { titleBarStyle: 'hidden' }
  : { frame: false, autoHideMenuBar: true }

export function installWindowControls(target: BrowserWindow): void {
  if (process.platform === 'darwin') target.setWindowButtonVisibility(false)
  const contents = target.webContents
  const inFullScreen = () => target.isFullScreen() || (process.platform === 'darwin' && target.isSimpleFullScreen())
  let lastState = inFullScreen()
  const notifyState = () => {
    const state = inFullScreen()
    if (state !== lastState) contents.send('window:fullscreen', state)
    lastState = state
  }
  contents.ipc.handle('window:is-fullscreen', inFullScreen)
  contents.ipc.on('window:action', (event, action: WindowAction) => {
    if (event.senderFrame !== contents.mainFrame) return
    switch (action) {
      case 'minimize':
        if (process.platform === 'darwin' && target.isSimpleFullScreen()) target.setSimpleFullScreen(false)
        target.minimize()
        break
      case 'toggle-fullscreen':
        if (process.platform === 'darwin' && !target.isFullScreen()) {
          // Keep the console in its current macOS Space.
          target.setSimpleFullScreen(!target.isSimpleFullScreen())
          notifyState()
        } else target.setFullScreen(!target.isFullScreen())
        break
      case 'close': target.close(); break
    }
  })
  target.on('enter-full-screen', notifyState)
  target.on('leave-full-screen', notifyState)
  if (process.platform === 'darwin') target.on('resize', notifyState)
}
