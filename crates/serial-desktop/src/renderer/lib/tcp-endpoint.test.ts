import { expect, it } from 'vitest'
import { tcpPortAddress } from './tcp-endpoint'

it('accepts IP literals and preserves explicit ports including 80', () => {
  expect(tcpPortAddress('192.168.1.252:10001')).toBe('192.168.1.252:10001')
  expect(tcpPortAddress('tcp://[0:0:0:0:0:0:0:1]:80')).toBe('[::1]:80')
  for (const input of ['COM4', '/dev/ttyUSB0', '192.168.1.252', '127.0.0.1:0', '127.0.0.1:65536', '012.0.0.1:12', '300.1.1.1:123', '[not-ip]:1']) {
    expect(tcpPortAddress(input)).toBeUndefined()
  }
})
