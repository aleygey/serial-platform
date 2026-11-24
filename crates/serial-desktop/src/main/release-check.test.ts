import { describe, expect, it, vi } from 'vitest'
import { createReleaseChecker } from './release-check'

function release(version: string, overrides = {}) {
  return Response.json({ tag_name: 'v' + version, draft: false, prerelease: false,
    assets: [{ name: `serial-platform-v${version}-windows-x86_64.exe`, state: 'uploaded', browser_download_url: 'https://example.org/other' }],
    ...overrides })
}

describe('desktop release checks', () => {
  it('offers the official portable EXE for a newer stable Windows version and compares numeric components', async () => {
    const request = vi.fn(async () => release('0.10.0'))
    const check = createReleaseChecker('0.9.10', 'win32', 'x64', request)
    const [first, second] = await Promise.all([check(), check()])
    expect(first).toEqual({ version: '0.10.0', portable: true,
      url: 'https://github.com/aleygey/serial-platform/releases/download/v0.10.0/serial-platform-v0.10.0-windows-x86_64.exe' })
    expect(second).toEqual(first)
    expect(request).toHaveBeenCalledTimes(1)
    expect(request).toHaveBeenCalledWith('https://api.github.com/repos/aleygey/serial-platform/releases/latest', expect.objectContaining({ credentials: 'omit' }))
  })

  it('keeps the current version and falls back to release notes for other platforms or missing assets', async () => {
    for (const version of ['0.9.10', '0.9.9']) {
      expect(await createReleaseChecker('0.9.10', 'win32', 'x64', async () => release(version))()).toBeNull()
    }
    for (const [platform, assets] of [['darwin', undefined], ['win32', []]] as const) {
      expect(await createReleaseChecker('0.9.6', platform, 'x64', async () => release('1.0.0', assets ? { assets } : {}))())
        .toEqual({ version: '1.0.0', portable: false, url: 'https://github.com/aleygey/serial-platform/releases/tag/v1.0.0' })
    }
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
