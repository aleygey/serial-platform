import { ArrowDownToLine, Fullscreen, Minimize, Minus, X } from 'lucide-react'
import { useEffect, useState, type ReactNode } from 'react'
import type { ReleaseUpdates } from '../lib/use-release-updates'

export function WindowTitleBar({ children, updates }: { children?: ReactNode; updates?: ReleaseUpdates }) {
  const bridge = typeof window !== 'undefined' ? window.desktopWindow : undefined
  const [fullScreen, setFullScreen] = useState(false)
  useEffect(() => {
    if (!bridge) return
    let useInitial = true
    const unsubscribe = bridge.onFullScreenChange(value => {
      useInitial = false
      setFullScreen(value)
    })
    void bridge.isFullScreen().then(value => { if (useInitial) setFullScreen(value) })
    return () => { useInitial = false; unsubscribe() }
  }, [bridge])
  const screenLabel = fullScreen ? '退出全屏' : '进入全屏'
  const ScreenIcon = fullScreen ? Minimize : Fullscreen
  return <header className="window-bar">
    {children}
    <div className="window-drag-region" aria-hidden="true" />
    {updates?.available && <button type="button" className="window-update" disabled={!updates.available.installable || updates.busy}
      onClick={updates.install} title={updates.status || '安装更新并重启'} aria-label={'更新并重启至 ' + updates.available.version}>
      <ArrowDownToLine size={14} aria-hidden />{updates.phase === 'installing' ? '正在更新…' : updates.phase === 'failed' ? '重试更新' : '更新至 ' + updates.available.version}</button>}
    {bridge && <div className="window-controls" role="group" aria-label="窗口控制">
      <button type="button" aria-label="最小化窗口" title="最小化窗口" onClick={() => bridge.act('minimize')}><Minus aria-hidden size={16} /></button>
      <button type="button" aria-label={screenLabel} title={screenLabel} onClick={() => bridge.act('toggle-fullscreen')}><ScreenIcon aria-hidden size={15} /></button>
      <button type="button" className="window-close" aria-label="关闭窗口" title="关闭窗口" onClick={() => bridge.act('close')}><X aria-hidden size={17} /></button>
    </div>}
  </header>
}
