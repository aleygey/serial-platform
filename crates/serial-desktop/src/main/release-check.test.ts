import { describe, expect, it, vi } from 'vitest'
import { createReleaseChecker } from './release-check'

function release(version: string, overrides = {}) {
  return Response.json({ tag_name: 'v' + version, draft: false, prerelease: false,
    assets: [{ name: `serial-platform-v${version}-windows-x86_64.exe`, state: 'uploaded', size: 1024, digest: 'sha256:' + 'a'.repeat(64), browser_download_url: 'https://example.org/other' }],
    ...overrides })
}

describe('desktop release checks', () => {
  it('offers the official portable EXE for a newer stable Windows version and compares numeric components', async () => {
    const request = vi.fn(async () => release('0.10.0'))
    const check = createReleaseChecker('0.9.10', 'win32', 'x64', request)
    const [first, second] = await Promise.all([check(), check()])
    expect(first).toMatchObject({ version: '0.10.0', installable: true, asset: { kind: 'windows', sha256: 'a'.repeat(64), size: 1024,
      url: 'https://github.com/aleygey/serial-platform/releases/download/v0.10.0/serial-platform-v0.10.0-windows-x86_64.exe' } })
    expect(second).toEqual(first)
    expect(request).toHaveBeenCalledTimes(1)
    expect(request).toHaveBeenCalledWith('https://api.github.com/repos/aleygey/serial-platform/releases/latest', expect.objectContaining({ credentials: 'omit' }))
  })

  it('keeps the current version and requires a matching verified asset to offer installation', async () => {
    for (const version of ['0.9.10', '0.9.9']) {
      expect(await createReleaseChecker('0.9.10', 'win32', 'x64', async () => release(version))()).toBeNull()
    }
    for (const [platform, assets] of [['darwin', undefined], ['win32', []]] as const) {
      expect(await createReleaseChecker('0.9.6', platform, 'x64', async () => release('1.0.0', assets ? { assets } : {}))())
        .toEqual({ version: '1.0.0', installable: false })
    }
  })

  it.each([
    ['darwin', 'arm64', 'macos-aarch64', '.zip', 'Serial Platform.app'],
    ['darwin', 'x64', 'macos-x86_64', '.zip', 'Serial Platform.app'],
    ['linux', 'x64', 'linux-x86_64-ubuntu20.04', '.tar.gz', 'Serial Platform.AppImage']
  ])('selects the published %s %s bundle', async (platform, arch, suffix, extension, member) => {
    const root = 'serial-platform-v1.0.0-' + suffix
    const file = { name: root + extension, state: 'uploaded', size: 8, digest: 'sha256:' + '0'.repeat(64) }
    const latest = (digest: string) => createReleaseChecker('0.9.7', platform, arch, async () => release('1.0.0', { assets: [{ ...file, digest }] }))()
    expect(await latest(file.digest)).toMatchObject({ installable: true, asset: { member: root + '/' + member } })
    expect(await latest('')).toEqual({ version: '1.0.0', installable: false })
  })

  it('allows retry after a network failure and skips draft or prerelease metadata', async () => {
    const request = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValueOnce(new Response(null, {status: 429}))
      .mockResolvedValueOnce(release('0.9.7', {draft: true})).mockResolvedValueOnce(release('0.9.7', {prerelease: true}))
      .mockResolvedValueOnce(release('0.9.7-rc.1')).mockResolvedValueOnce(release('0.9.7'))
    const check = createReleaseChecker('0.9.6', 'win32', 'x64', request)
    for (let i = 0; i < 5; i++) await expect(check()).rejects.toThrow()
    await expect(check()).resolves.toMatchObject({ version: '0.9.7' })
  })
})
