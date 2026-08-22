import { describe, expect, it } from 'vitest'
import type { SerialConfigurationDraft, TransportProfile } from '../shared/contracts'
import {
  assertCompatibleProtocol,
  configuredPortFromDraft,
  contentAddressedTransportProfile,
  parseSerialdHealth,
  serialdIdentityMatches,
  stageTransportCatalog
} from './serial-client'

describe('component protocol gate', () => {
  it('accepts v5 and rejects an older backend before opening the live socket', () => {
    expect(() => assertCompatibleProtocol(5)).not.toThrow()
    expect(() => assertCompatibleProtocol(4)).toThrow(/App 需要 v5，后端提供 v4/)
  })

  it('requires an ok v5 health response with stable UUID identities', () => {
    const identity = parseSerialdHealth({
      status: 'ok',
      server_id: '11111111-1111-4111-8111-111111111111',
      daemon_epoch: '22222222-2222-4222-8222-222222222222',
      protocol_version: 5,
      uptime_ms: 10
    })
    expect(identity).toEqual({
      serverId: '11111111-1111-4111-8111-111111111111',
      daemonEpoch: '22222222-2222-4222-8222-222222222222',
      protocolVersion: 5
    })
    expect(serialdIdentityMatches(identity, identity)).toBe(true)
    expect(serialdIdentityMatches(identity, { ...identity, daemonEpoch: '33333333-3333-4333-8333-333333333333' }))
      .toBe(false)
    expect(() => parseSerialdHealth({ ...identity, status: 'ok', protocol_version: 5 }))
      .toThrow('服务身份')
    expect(() => parseSerialdHealth({
      status: 'ok', server_id: identity.serverId, daemon_epoch: identity.daemonEpoch,
      protocol_version: 4
    })).toThrow(/App 需要 v5/)
  })
})

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

describe('port model binding', () => {
  const draft: SerialConfigurationDraft = {
    port: 'COM6', enabled: true, transportProfile: profile, modelProfile: 'TL-AS7230 Family'
  }

  it('preserves the concrete model when a serial-only save omits modelName', () => {
    expect(configuredPortFromDraft(draft, 'desktop-COM6-hash', {
      port: 'COM6', enabled: true, transport_profile: 'old-uart',
      model_profile: 'TL-AS7230 Family', model_name: 'TL-AS7230-W 1.0'
    })).toMatchObject({
      model_profile: 'TL-AS7230 Family', model_name: 'TL-AS7230-W 1.0'
    })
  })

  it('clears the concrete model when its profile is changed or explicitly unbound', () => {
    const existing = {
      port: 'COM6', enabled: true, transport_profile: 'old-uart',
      model_profile: 'TL-AS7230 Family', model_name: 'TL-AS7230-W 1.0'
    }
    expect(configuredPortFromDraft({ ...draft, modelProfile: 'Other Family' }, 'uart', existing).model_name).toBeNull()
    expect(configuredPortFromDraft({ ...draft, modelName: null }, 'uart', existing).model_name).toBeNull()
  })
})
