import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'

vi.mock('electron', () => ({ app: { getPath: () => tmpdir() } }))

import { SettingsStore } from './settings'

describe('desktop preferences', () => {
  it('defaults auto-start on and persists an explicit change', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'serial-desktop-settings-'))
    try {
      const store = new SettingsStore(join(directory, 'desktop.json'))
      const defaults = await store.load()
      expect(defaults.autoStartLocal).toBe(true)
      expect(defaults.firstUse).toBe('new')

      await store.save({ ...defaults, autoStartLocal: false, firstUse: 'complete' })
      expect(await store.load()).toMatchObject({ autoStartLocal: false, firstUse: 'complete' })
      await writeFile(store.path, JSON.stringify({ theme: 'dark' }))
      expect(await store.load()).toMatchObject({ theme: 'dark', firstUse: 'dismissed' })
    } finally {
      await rm(directory, { recursive: true, force: true })
    }
  })
})
