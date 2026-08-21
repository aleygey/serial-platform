import { describe, expect, it } from 'vitest'
import { startupAction } from './startup'

describe('desktop startup action', () => {
  it('handles smoke-test flags before creating a window or daemon', () => {
    expect(startupAction([])).toBe('gui')
    expect(startupAction(['--help'])).toBe('help')
    expect(startupAction(['--version'])).toBe('version')
  })
})
