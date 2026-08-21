import { mkdtemp, rm } from 'node:fs/promises'
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

      await store.save({ ...defaults, autoStartLocal: false })
      expect((await store.load()).autoStartLocal).toBe(false)
    } finally {
      await rm(directory, { recursive: true, force: true })
    }
  })
})
