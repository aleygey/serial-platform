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
export function applyAppearance(mode: 'dark' | 'light', value: unknown, fontSize?: number, uiSize?: number) {
  const root = document.documentElement
  const palette = paletteId(value)
  root.style.setProperty('--terminal-font-size', terminalFontSize(fontSize) + 'px')
  root.style.fontSize = interfaceFontSize(uiSize) / 16 * 100 + '%'
  root.dataset.theme = mode
  root.dataset.palette = palette
  for (const [token, color] of Object.entries(palettes[palette][mode])) root.style.setProperty('--work-' + token, color)
}
