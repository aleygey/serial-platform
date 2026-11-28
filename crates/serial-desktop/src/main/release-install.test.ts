import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { chmod, copyFile, mkdir, mkdtemp, readFile, readdir, realpath, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReleaseAsset } from './release-check'
import { handoffUpdate, installRelease } from './release-install'

const bytes = Buffer.from('new executable fixture')
const asset: ReleaseAsset = { kind: 'windows', url: 'https://example.invalid/release.exe',
  size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') }
let dir: string
let target: string
beforeEach(async () => {
  dir = await realpath(await mkdtemp(join(tmpdir(), 'serial update 中文-')))
  target = join(dir, 'Serial Platform.exe')
  await writeFile(target, 'old')
})
afterEach(async () => { vi.unstubAllEnvs(); await rm(dir, { recursive: true, force: true, maxRetries: 5 }) })

describe('desktop update installation', () => {
  it('stages a verified EXE beside the installed portable launcher before handoff', async () => {
    const handoff = vi.fn(async (stage: string, path: string) => {
      expect(path).toBe(target)
      expect(await readFile(join(stage, 'next'))).toEqual(bytes)
      expect(await readFile(target, 'utf8')).toBe('old')
    })
    await installRelease(asset, target, async () => new Response(bytes), handoff)
    expect(handoff).toHaveBeenCalledTimes(1)
  })

  it.each([Buffer.alloc(bytes.length), bytes.subarray(1), Buffer.concat([bytes, bytes])])('rejects a corrupt or incomplete update', async body => {
    const handoff = vi.fn()
    await expect(installRelease(asset, target, async () => new Response(body), handoff)).rejects.toThrow()
    expect(handoff).not.toHaveBeenCalled()
    expect(await readFile(target, 'utf8')).toBe('old')
    expect(await readdir(dir)).toEqual(['Serial Platform.exe'])
  })

  it.runIf(process.platform === 'linux')('extracts the release AppImage, replaces the original path and launches it', async () => {
    const root = 'serial-platform-v1.0.0-linux-x86_64-ubuntu20.04'
    const member = root + '/Serial Platform.AppImage'
    await mkdir(join(dir, root))
    await writeFile(join(dir, member), '#!/bin/sh\nprintf started > "$(dirname "$0")/started"\n')
    await chmod(join(dir, member), 0o755)
    const archive = execFileSync('tar', ['-czf', '-', '-C', dir, member])
    const linux: ReleaseAsset = { kind: 'linux', url: asset.url, member, size: archive.length, sha256: createHash('sha256').update(archive).digest('hex') }
    await installRelease(linux, target, async () => new Response(archive), (stage, entry, pkg) => handoffUpdate(stage, entry, pkg, 2147483647))
    await vi.waitFor(async () => expect(await readFile(join(dir, 'started'), 'utf8')).toBe('started'))
  })

  it.runIf(process.platform === 'win32')('replaces the launcher and starts the new executable from its original path', async () => {
    const marker = join(dir, 'started.json')
    const preload = join(dir, 'mark.cjs')
    await writeFile(preload, "require('fs').writeFileSync(" + JSON.stringify(marker) + ", JSON.stringify(process.execPath)); process.exit(0)")
    vi.stubEnv('NODE_OPTIONS', '--require "' + preload.replaceAll('\\', '/') + '"')
    const stage = await mkdtemp(join(dir, '.serial-update-'))
    await copyFile(process.execPath, join(stage, 'next'))
    // Use a PID that has already exited so the test process can observe the handoff.
    await handoffUpdate(stage, target, asset, 2147483647)
    await vi.waitFor(async () => expect(JSON.parse(await readFile(marker, 'utf8'))).toBe(target), { timeout: 15000 })
  }, 20000)
})
