export const palettes = {
  harbor: {
    name: '海湾蓝',
    light: { accent: '#156680', rail: '#f4f8fa', panel: '#ffffff', terminal: '#fcfdfe', ink: '#1c303b', output: '#273e4b' },
    dark:  { accent: '#86cce5', rail: '#182630', panel: '#1d2d37', terminal: '#111b24', ink: '#e6f0f5', output: '#c9dbe5' },
  },
  phosphor: {
    name: '磷光绿',
    light: { accent: '#246541', rail: '#eff5ef', panel: '#fcfefb', terminal: '#f5faf3', ink: '#203525', output: '#284c30' },
    dark:  { accent: '#82e29b', rail: '#101a13', panel: '#14231a', terminal: '#070e09', ink: '#dcf4e0', output: '#a4e4ad' },
  },
  amber: {
    name: '琥珀金',
    light: { accent: '#845309', rail: '#f7f3e9', panel: '#fffdf7', terminal: '#fffcf3', ink: '#3a3020', output: '#504329' },
    dark:  { accent: '#f0bd67', rail: '#251f16', panel: '#30271c', terminal: '#19150f', ink: '#f4e7ce', output: '#e4c58c' },
  },
  oxide: {
    name: '赤陶红',
    light: { accent: '#994535', rail: '#f8f1ed', panel: '#fffcfa', terminal: '#fffaf7', ink: '#402c27', output: '#513c36' },
    dark:  { accent: '#efa28d', rail: '#291f1c', panel: '#352723', terminal: '#1a1412', ink: '#f5e7df', output: '#e7c8ba' },
  },
  graphite: {
    name: '石墨灰',
    light: { accent: '#495661', rail: '#f0f2f3', panel: '#ffffff', terminal: '#f9fafb', ink: '#252c30', output: '#343c41' },
    dark:  { accent: '#c1cbd1', rail: '#1b2023', panel: '#242a2d', terminal: '#111416', ink: '#edf1f3', output: '#d2d8dc' },
  },
} as const

export type PaletteId = keyof typeof palettes
export const paletteId = (value: unknown): PaletteId =>
  typeof value === 'string' && Object.hasOwn(palettes, value) ? value as PaletteId : 'harbor'

export const terminalFontSizes = Array.from({ length: 19 }, (_, index) => 10 + index)

export function terminalFontSize(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value)
    ? Math.max(10, Math.min(28, Math.round(value))) : 13
}
