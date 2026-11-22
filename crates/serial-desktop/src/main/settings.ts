import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { app } from 'electron'
import type { DesktopPreferences } from '../shared/contracts'

const defaults: DesktopPreferences = {
  endpoint: 'http://127.0.0.1:3210',
  autoStartLocal: true,
  theme: 'system',
  palette: 'harbor',
  firstUse: 'new'
}

export class SettingsStore {
  readonly path: string
  private pendingWrite: Promise<void> = Promise.resolve()

  constructor(path = join(app.getPath('userData'), 'desktop.json')) {
    this.path = path
  }

  async load(): Promise<DesktopPreferences> {
    try {
      const parsed = JSON.parse(await readFile(this.path, 'utf8')) as Partial<DesktopPreferences>
      return { ...defaults, firstUse: 'dismissed', ...parsed }
    } catch {
      return { ...defaults }
    }
  }

  save(preferences: DesktopPreferences): Promise<void> {
    const write = this.pendingWrite.then(async () => {
      await mkdir(dirname(this.path), { recursive: true })
      const temporary = `${this.path}.tmp`
      await writeFile(temporary, `${JSON.stringify(preferences, null, 2)}\n`, { mode: 0o600 })
      await rename(temporary, this.path)
    })
    this.pendingWrite = write.catch(() => {})
    return write
  }
}
