import { ArrowDownToLine, Fullscreen, Minimize, Minus, X } from 'lucide-react'
import { useEffect, useState, type ReactNode } from 'react'
import type { AvailableRelease } from '../../shared/contracts'

export function WindowTitleBar({ children, available }: { children?: ReactNode; available?: AvailableRelease | null }) {
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
    {available && <a className="window-update" href={available.url} target="_blank" rel="noreferrer"
      aria-label={`${available.portable ? '下载' : '查看'}新版 ${available.version}`}><ArrowDownToLine size={14} aria-hidden />新版 {available.version}</a>}
    {bridge && <div className="window-controls" role="group" aria-label="窗口控制">
      <button type="button" aria-label="最小化窗口" title="最小化窗口" onClick={() => bridge.act('minimize')}><Minus aria-hidden size={16} /></button>
      <button type="button" aria-label={screenLabel} title={screenLabel} onClick={() => bridge.act('toggle-fullscreen')}><ScreenIcon aria-hidden size={15} /></button>
      <button type="button" className="window-close" aria-label="关闭窗口" title="关闭窗口" onClick={() => bridge.act('close')}><X aria-hidden size={17} /></button>
    </div>}
  </header>
}
