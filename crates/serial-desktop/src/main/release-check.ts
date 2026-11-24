import type { AvailableRelease } from '../shared/contracts'

const repository = 'aleygey/serial-platform'
const stableVersion = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/

export function createReleaseChecker(installed: string, platform: string, arch: string, request: (url: string, init: RequestInit) => Promise<Response>) {
  let pending: Promise<AvailableRelease | null> | undefined

  async function latest(): Promise<AvailableRelease | null> {
    const response = await request(`https://api.github.com/repos/${repository}/releases/latest`, {
      headers: { Accept: 'application/vnd.github+json', 'User-Agent': 'Serial-Platform/' + installed },
      credentials: 'omit', cache: 'no-store', signal: AbortSignal.timeout(8_000)
    })
    if (!response.ok) throw new Error('GitHub HTTP ' + response.status)
    const release = await response.json()
    const tagName: unknown = release?.tag_name
    const version = typeof tagName === 'string' && tagName.startsWith('v') ? tagName.slice(1) : ''
    if (release?.draft !== false || release.prerelease !== false || !stableVersion.test(version) || !stableVersion.test(installed)) {
      throw new Error('Invalid stable release')
    }
    const current = installed.split('.').map(Number)
    const next = version.split('.').map(Number)
    const firstDifference = next.findIndex((part, i) => part !== current[i])
    if (firstDifference < 0 || next[firstDifference] < current[firstDifference]) return null

    const tag = 'v' + version
    const name = `serial-platform-${tag}-windows-x86_64.exe`
    const portable = platform === 'win32' && arch === 'x64' && Array.isArray(release.assets)
      && release.assets.some((asset: { name?: string; state?: string } | null) => asset?.name === name && asset.state === 'uploaded')
    const base = `https://github.com/${repository}/releases`
    return { version, portable, url: portable ? `${base}/download/${tag}/${name}` : `${base}/tag/${tag}` }
  }

  return () => pending ??= latest().finally(() => { pending = undefined })
}
