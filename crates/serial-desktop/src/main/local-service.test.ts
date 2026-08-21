import { describe, expect, it } from 'vitest'
import { buildSpawnArgs } from './service-command'

describe('local service command', () => {
  it('starts both serial and seriald as an EOF-managed service', () => {
    expect(buildSpawnArgs(['serve', '--managed'], 'http://127.0.0.1:3210')).toEqual([
      'serve', '--managed', '--bind', '127.0.0.1:3210'
    ])
  })
})
