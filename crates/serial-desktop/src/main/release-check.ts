import type { AvailableRelease } from '../shared/contracts'

const repository = 'aleygey/serial-platform'
const stableVersion = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/
const maxBytes = 512 * 1024 * 1024

export type ReleaseAsset = {
  kind: 'windows' | 'linux' | 'mac'
  url: string
  sha256: string
  size: number
  member?: string
}
export interface CheckedRelease extends AvailableRelease { asset?: ReleaseAsset }
export type FetchRelease = (url: string, init: RequestInit) => Promise<Response>

function packageFor(tag: string, platform: string, arch: string) {
  const base = 'serial-platform-' + tag + '-'
  if (platform === 'win32' && arch === 'x64') return { kind: 'windows' as const, name: base + 'windows-x86_64.exe' }
  if (platform === 'linux' && arch === 'x64') {
    const root = base + 'linux-x86_64-ubuntu20.04'
    return { kind: 'linux' as const, name: root + '.tar.gz', member: root + '/Serial Platform.AppImage' }
  }
  if (platform === 'darwin' && (arch === 'x64' || arch === 'arm64')) {
    const root = base + 'macos-' + (arch === 'arm64' ? 'aarch64' : 'x86_64')
    return { kind: 'mac' as const, name: root + '.zip', member: root + '/Serial Platform.app' }
  }
}

export function createReleaseChecker(installed: string, platform: string, arch: string, request: FetchRelease) {
  let pending: Promise<CheckedRelease | null> | undefined
  async function latest(): Promise<CheckedRelease | null> {
    const response = await request(`https://api.github.com/repos/${repository}/releases/latest`, {
      headers: { Accept: 'application/vnd.github+json', 'User-Agent': 'Serial-Platform/' + installed },
      credentials: 'omit', cache: 'no-store', signal: AbortSignal.timeout(8_000)
    })
    if (!response.ok) throw new Error('GitHub HTTP ' + response.status)
    const release = await response.json()
    const tag = release?.tag_name
    const version = typeof tag === 'string' && tag.startsWith('v') ? tag.slice(1) : ''
    if (release?.draft !== false || release.prerelease !== false || !stableVersion.test(version) || !stableVersion.test(installed)) {
      throw new Error('Invalid stable release')
    }
    const current = installed.split('.').map(Number)
    const next = version.split('.').map(Number)
    const firstDifference = next.findIndex((part, i) => part !== current[i])
    if (firstDifference < 0 || next[firstDifference] < current[firstDifference]) return null

    const pkg = packageFor(tag, platform, arch)
    const file = pkg && Array.isArray(release.assets) && release.assets.find(
      (item: { name?: string; state?: string } | null) => item?.name === pkg.name && item.state === 'uploaded')
    if (!pkg || !file || typeof file.digest !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(file.digest)
      || !Number.isSafeInteger(file.size) || file.size < 1 || file.size > maxBytes) return { version, installable: false }
    return { version, installable: true, asset: { kind: pkg.kind, member: pkg.member,
      url: `https://github.com/${repository}/releases/download/${tag}/${pkg.name}`, sha256: file.digest.slice(7), size: file.size } }
  }
  return () => pending ??= latest().finally(() => { pending = undefined })
}
