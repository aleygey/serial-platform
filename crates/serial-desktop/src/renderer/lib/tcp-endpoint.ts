/** Canonical IP literal accepted by seriald, including bracketed IPv6. */
export function tcpPortAddress(input: string): string | undefined {
  const match = /^(?:tcp:\/\/)?(\[[\da-f:.]+\]|\d+(?:\.\d+){3}):(\d{1,5})$/i.exec(input.trim())
  if (!match) return
  const [, host, portText] = match
  const port = Number(portText)
  if (port < 1 || port > 65535) return
  if (!host.startsWith('[') && !host.split('.').every(octet => String(Number(octet)) === octet && Number(octet) <= 255)) return
  try { return new URL('http://' + host).hostname + ':' + port } catch { return }
}
