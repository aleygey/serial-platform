import { execFile, spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { constants } from 'node:fs'
import { access, chmod, lstat, mkdtemp, open, realpath, rename, rm } from 'node:fs/promises'
import { basename, dirname, isAbsolute, join } from 'node:path'
import { promisify } from 'node:util'
import type { FetchRelease, ReleaseAsset } from './release-check'

const run = promisify(execFile)

export function installTarget(platform: string, exe: string, env: NodeJS.ProcessEnv): string | undefined {
  if (platform === 'win32') return env.PORTABLE_EXECUTABLE_FILE
  if (platform === 'linux') return env.APPIMAGE
  if (platform === 'darwin') {
    const contents = dirname(dirname(exe))
    const bundle = dirname(contents)
    if (basename(contents) === 'Contents' && basename(dirname(exe)) === 'MacOS' && bundle.endsWith('.app')) return bundle
  }
}

export async function downloadAsset(asset: ReleaseAsset, path: string, request: FetchRelease) {
  const response = await request(asset.url, { credentials: 'omit', cache: 'no-store', signal: AbortSignal.timeout(5 * 60_000) })
  if (!response.ok || !response.body) throw new Error('下载更新失败（HTTP ' + response.status + '）')
  const file = await open(path, 'wx', 0o600)
  const reader = response.body.getReader()
  const hash = createHash('sha256')
  let size = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      size += value.length
      if (size > asset.size) throw new Error('更新文件大小超出预期')
      hash.update(value)
      await file.writeFile(value)
    }
    if (size !== asset.size || hash.digest('hex') !== asset.sha256) throw new Error('更新文件 SHA-256 校验失败，请重试')
    await file.sync()
  } finally {
    try { await reader.cancel() } finally { reader.releaseLock(); await file.close() }
  }
}

// The portable launcher holds its EXE until Electron and its managed service have exited.
// File.Replace swaps on the same volume and retains a backup until the new launcher starts.
const windowsHandoff = `
$ErrorActionPreference = 'Stop'
$stage = $env:SERIAL_UPDATE_STAGE
$target = $env:SERIAL_UPDATE_TARGET
$next = [IO.Path]::Combine($stage, 'next')
$backup = [IO.Path]::Combine($stage, 'previous')
$failed = [IO.Path]::Combine($stage, 'failed')
$owner = Get-Process -Id ([int]$env:SERIAL_UPDATE_PID) -ErrorAction SilentlyContinue
if ($owner -and !$owner.WaitForExit(90000)) { exit 1 }
$replaced = $false
try {
  $timeout = [Diagnostics.Stopwatch]::StartNew()
  while (!$replaced) {
    try { [IO.File]::Replace($next, $target, $backup); $replaced = $true }
    catch [IO.IOException] {
      if ($timeout.Elapsed.TotalSeconds -ge 30) { throw }
      Start-Sleep -Milliseconds 150
    }
  }
  $launcher = Start-Process -FilePath $target -WorkingDirectory ([IO.Path]::GetDirectoryName($target)) -PassThru
  if ($launcher.WaitForExit(1200) -and $launcher.ExitCode -ne 0) { throw ('Launcher exited with code ' + $launcher.ExitCode) }
} catch {
  $env:SERIAL_PLATFORM_UPDATE_ERROR = $_.Exception.Message
  if ($replaced) { [IO.File]::Replace($backup, $target, $failed) }
  Start-Process -FilePath $target -WorkingDirectory ([IO.Path]::GetDirectoryName($target))
}
foreach ($path in @($next, $backup, $failed)) { [IO.File]::Delete($path) }
[IO.Directory]::Delete($stage)
`

const unixHandoff = `
stage=$SERIAL_UPDATE_STAGE
target=$SERIAL_UPDATE_TARGET
pid=$SERIAL_UPDATE_PID
for_wait=0
while kill -0 "$pid" 2>/dev/null; do
  [ "$for_wait" -lt 900 ] || exit 1
  sleep 0.1
  for_wait=$((for_wait+1))
done
launch() {
  if [ "$SERIAL_UPDATE_KIND" = mac ]; then /usr/bin/open -n "$target"
  else
    "$target" </dev/null >/dev/null 2>&1 &
    launcher=$!
    sleep 1
    kill -0 "$launcher" 2>/dev/null || wait "$launcher"
  fi
}
if mv "$target" "$stage/previous"; then
  if mv "$stage/next" "$target" && launch; then
    rm -rf "$stage"
    exit 0
  fi
  if [ -e "$target" ]; then mv "$target" "$stage/failed" || exit 1; fi
  mv "$stage/previous" "$target" || exit 1
fi
export SERIAL_PLATFORM_UPDATE_ERROR='无法替换或启动新版应用。'
launch && rm -rf "$stage"
`

export async function handoffUpdate(stage: string, target: string, asset: ReleaseAsset, pid = process.pid) {
  const windows = asset.kind === 'windows'
  const command = windows ? join(process.env.SystemRoot!, 'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe') : '/bin/sh'
  const args = windows ? ['-NoProfile', '-NonInteractive', '-Command', windowsHandoff] : ['-c', unixHandoff]
  const child = spawn(command, args, {
    // On Windows, DETACHED_PROCESS can make console PowerShell exit before running its command. A child
    // outlives its parent without DETACHED_PROCESS. On Unix give the helper its own session.
    cwd: dirname(target), detached: !windows, windowsHide: true, stdio: 'ignore',
    env: { ...process.env, SERIAL_UPDATE_STAGE: stage, SERIAL_UPDATE_TARGET: target, SERIAL_UPDATE_KIND: asset.kind, SERIAL_UPDATE_PID: String(pid) }
  })
  await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve) })
  child.unref()
}

export async function installRelease(asset: ReleaseAsset, entry: string, request: FetchRelease, handoff = handoffUpdate) {
  if (!isAbsolute(entry)) throw new Error('找不到可更新的应用路径')
  const target = await realpath(entry)
  const stage = await mkdtemp(join(dirname(target), '.serial-update-'))
  try {
    const next = join(stage, 'next')
    if (asset.kind === 'windows') {
      await downloadAsset(asset, next, request)
    } else {
      const archive = join(stage, 'download')
      await downloadAsset(asset, archive, request)
      const member = asset.member!
      if (asset.kind === 'mac') {
        await run('/usr/bin/unzip', ['-q', archive, member + '/*', '-d', stage])
      } else {
        await run('/bin/tar', ['-xzf', archive, '-C', stage, member])
      }
      const item = join(stage, member)
      const stat = await lstat(item)
      if (asset.kind === 'mac' ? !stat.isDirectory() : !stat.isFile()) throw new Error('更新包中缺少应用')
      if (asset.kind === 'linux') await chmod(item, 0o755)
      await access(asset.kind === 'mac' ? join(item, 'Contents', 'MacOS', 'Serial Platform') : item, constants.X_OK)
      await rename(item, next)
      await rm(archive)
    }
    await handoff(stage, target, asset)
  } catch (error) {
    await rm(stage, { recursive: true, force: true })
    throw error
  }
}
