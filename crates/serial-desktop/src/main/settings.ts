import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { app } from 'electron'
import type { DesktopPreferences } from '../shared/contracts'

const defaults: DesktopPreferences = {
  endpoint: 'http://127.0.0.1:3210',
  autoStartLocal: true,
  theme: 'system'
}

export class SettingsStore {
  readonly path: string

  constructor(path = join(app.getPath('userData'), 'desktop.json')) {
    this.path = path
  }

  async load(): Promise<DesktopPreferences> {
    try {
      const parsed = JSON.parse(await readFile(this.path, 'utf8')) as Partial<DesktopPreferences>
      return { ...defaults, ...parsed }
    } catch {
      return { ...defaults }
    }
  }

  async save(preferences: DesktopPreferences): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true })
    const temporary = `${this.path}.tmp`
    await writeFile(temporary, `${JSON.stringify(preferences, null, 2)}\n`, { mode: 0o600 })
    await rename(temporary, this.path)
  }
}
