import { useCallback, useEffect, useRef, useState } from 'react'
import type { AvailableRelease } from '../../shared/contracts'

export function useReleaseUpdates(automatic: boolean | undefined) {
  const bridge = typeof window !== 'undefined' ? window.desktopUpdates : undefined
  const [phase, setPhase] = useState<'idle' | 'checking' | 'checked' | 'offline' | 'installing' | 'failed'>('idle')
  const [available, setAvailable] = useState<AvailableRelease | null>(null)
  const [error, setError] = useState('')
  const pending = useRef(false)
  const check = useCallback(async () => {
    if (!bridge || pending.current) return
    pending.current = true
    setPhase('checking')
    try {
      setAvailable(await bridge.check())
      setPhase('checked')
    } catch {
      setPhase('offline')
    } finally {
      pending.current = false
    }
  }, [bridge])

  const install = useCallback(async () => {
    if (!bridge || !available?.installable || pending.current) return
    pending.current = true
    setError('')
    setPhase('installing')
    try {
      await bridge.install(available.version)
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason))
      setPhase('failed')
      pending.current = false
    }
  }, [bridge, available])

  useEffect(() => {
    if (!bridge || !automatic) return
    const startup = window.setTimeout(check, 8_000)
    const daily = window.setInterval(check, 24 * 60 * 60 * 1000)
    return () => { window.clearTimeout(startup); window.clearInterval(daily) }
  }, [automatic, bridge, check])

  const status = phase === 'installing' ? '正在下载并校验，完成后将自动重启…'
    : phase === 'failed' ? '更新失败，可重试。' + error
    : phase === 'offline' ? '暂时无法检查更新，请稍后重试'
    : phase === 'checking' ? '正在检查…'
    : phase === 'checked' && !available ? '已是最新正式版'
    : available && !available.installable ? '此安装形式暂无可自动安装的更新包' : ''
  return bridge ? { installed: bridge.installedVersion, available, busy: phase === 'checking' || phase === 'installing', phase, status, check, install } : undefined
}

export type ReleaseUpdates = NonNullable<ReturnType<typeof useReleaseUpdates>>
