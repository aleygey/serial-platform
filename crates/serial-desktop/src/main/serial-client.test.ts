import { describe, expect, it } from 'vitest'
import type { TransportProfile } from '../shared/contracts'
import { contentAddressedTransportProfile, stageTransportCatalog } from './serial-client'

const profile: TransportProfile = {
  name: 'human-name', baud_rate: 115200, data_bits: 'eight', parity: 'none',
  stop_bits: 'one', flow_control: 'none', dtr: false, rts: false, auto_open: true
}

describe('transport profile staging', () => {
  it('uses port plus content hash and never the mutable source name as identity', () => {
    const first = contentAddressedTransportProfile('COM 6', profile)
    const renamed = contentAddressedTransportProfile('COM 6', { ...profile, name: 'renamed' })
    const changed = contentAddressedTransportProfile('COM 6', { ...profile, baud_rate: 9600 })

    expect(first.name).toMatch(/^desktop-COM-6-[a-f0-9]{10}$/)
    expect(renamed.name).toBe(first.name)
    expect(changed.name).not.toBe(first.name)
  })

  it('keeps at most one unbound candidate across repeated failed switch attempts', () => {
    const first = stageTransportCatalog('COM6', profile, [], new Set())
    const second = stageTransportCatalog(
      'COM6',
      { ...profile, baud_rate: 9600 },
      first.profiles,
      new Set()
    )

    expect(first.profiles.filter((item) => item.name.startsWith('desktop-COM6-'))).toHaveLength(1)
    expect(second.profiles.filter((item) => item.name.startsWith('desktop-COM6-'))).toHaveLength(1)
    expect(second.selected.name).not.toBe(first.selected.name)
  })
})
