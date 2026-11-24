import { useCallback, useEffect, useRef, useState } from 'react'
import type { AvailableRelease } from '../../shared/contracts'

export function useReleaseUpdates(automatic: boolean | undefined) {
  const bridge = typeof window !== 'undefined' ? window.desktopUpdates : undefined
  const [phase, setPhase] = useState<'idle' | 'checking' | 'checked' | 'offline'>('idle')
  const [available, setAvailable] = useState<AvailableRelease | null>(null)
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

  useEffect(() => {
    if (!bridge || !automatic) return
    const startup = window.setTimeout(check, 8_000)
    const daily = window.setInterval(check, 24 * 60 * 60 * 1000)
    return () => { window.clearTimeout(startup); window.clearInterval(daily) }
  }, [automatic, bridge, check])

  return bridge ? { installed: bridge.installedVersion, available, phase, check } : undefined
}

export type ReleaseUpdates = NonNullable<ReturnType<typeof useReleaseUpdates>>
