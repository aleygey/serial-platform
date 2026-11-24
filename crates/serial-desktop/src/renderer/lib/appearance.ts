import { useSyncExternalStore } from 'react'
import type { ThemePreference } from '../../shared/contracts'
import { palettes, paletteId, interfaceFontSize, terminalFontSize } from '../../shared/appearance'

const query = '(prefers-color-scheme: dark)'
function watchSystemMode(notify: () => void) {
  const media = window.matchMedia(query)
  media.addEventListener('change', notify)
  return () => media.removeEventListener('change', notify)
}
export function useAppearanceMode(preference: ThemePreference) {
  const dark = useSyncExternalStore(watchSystemMode, () => window.matchMedia(query).matches, () => false)
  return preference === 'system' ? (dark ? 'dark' : 'light') : preference
}

const pixelRatio = () => window.devicePixelRatio || 1
function watchPixelRatio(notify: () => void) {
  const resolution = () => window.matchMedia(`(resolution: ${pixelRatio()}dppx)`)
  let media = resolution()
  function changed() {
    media.removeEventListener('change', changed)
    media = resolution()
    media.addEventListener('change', changed)
    notify()
  }
  media.addEventListener('change', changed)
  return () => media.removeEventListener('change', changed)
}
export function useDevicePixelRatio() {
  return useSyncExternalStore(watchPixelRatio, pixelRatio, () => 1)
}

export function applyAppearance(mode: 'dark' | 'light', value: unknown, fontSize: number | undefined, uiSize: number | undefined, density: number) {
  const root = document.documentElement
  const palette = paletteId(value)
  const size = terminalFontSize(fontSize)
  root.style.setProperty('--terminal-font-size', size + 'px')
  // Keep each new row on the same physical pixel grid while following live output.
  root.style.setProperty('--terminal-line-height', Math.round(size * 1.72 * density) / density + 'px')
  root.style.fontSize = interfaceFontSize(uiSize) / 16 * 100 + '%'
  root.dataset.theme = mode
  root.dataset.palette = palette
  for (const [token, color] of Object.entries(palettes[palette][mode])) root.style.setProperty('--work-' + token, color)
}
