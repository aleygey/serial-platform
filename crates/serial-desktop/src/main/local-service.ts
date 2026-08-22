import { access } from 'node:fs/promises'
import { constants } from 'node:fs'
import { dirname, join } from 'node:path'
import { execFile, spawn, type ChildProcess } from 'node:child_process'
import { app } from 'electron'
import type { ServiceState } from '../shared/contracts'
import { SERIAL_PROTOCOL_VERSION } from './protocol'
import {
  buildDiscoverArgs,
  buildSpawnArgs,
  type DiscoveredSeriald
} from './service-command'
import { stopManagedChild } from './managed-child'

interface Program {
  path: string
  args: string[]
}

export class LocalService {
  private child?: ChildProcess
  private current: ServiceState = { owned: false, status: 'stopped' }
  private stderrTail = ''
  private startupFailureValue?: Error
  private readonly onState: (state: ServiceState) => void

  constructor(onState: (state: ServiceState) => void) {
    this.onState = onState
  }

  state(): ServiceState {
    return { ...this.current }
  }

  async start(endpoint: string): Promise<void> {
    if (this.child) return
    const program = await this.resolveProgram()
    this.stderrTail = ''
    this.startupFailureValue = undefined
    this.update({ owned: true, status: 'starting', program: program.path })
    const child = spawn(program.path, buildSpawnArgs(program.args, endpoint), {
      stdio: ['pipe', 'ignore', 'pipe'],
      windowsHide: true,
      detached: false
    })
    this.child = child
    child.stderr?.setEncoding('utf8')
    child.stderr?.on('data', (chunk: string) => {
      this.stderrTail = `${this.stderrTail}${chunk}`.slice(-16 * 1024)
    })
    child.once('exit', (code, signal) => {
      if (this.child !== child) return
      this.child = undefined
      const status = code === null ? `signal ${signal ?? 'unknown'}` : `exit code ${code}`
      const detail = this.stderrTail.trim()
      this.startupFailureValue = new Error(
        `seriald 启动失败（${status}）${detail ? `：${detail}` : ''}`
      )
      this.update({ owned: false, status: 'exited', program: program.path })
    })
    child.once('error', (error) => {
      if (this.child === child) this.child = undefined
      this.startupFailureValue = new Error(`无法启动 seriald：${error.message}`)
      this.update({ owned: false, status: 'exited', program: `${program.path}: ${error.message}` })
    })
  }

  async discoverEndpoint(): Promise<DiscoveredSeriald | undefined> {
    const program = await this.resolveProgram()
    const output = await new Promise<string>((resolve, reject) => {
      execFile(
        program.path,
        buildDiscoverArgs(),
        { encoding: 'utf8', timeout: 3_000, windowsHide: true },
        (error, stdout, stderr) => {
          if (!error) {
            resolve(stdout)
            return
          }
          const detail = stderr.trim() || error.message
          reject(new Error(`无法发现当前 seriald：${detail}`))
        }
      )
    })
    return parseDiscoveredEndpoint(output)
  }

  startupFailure(): Error | undefined {
    return this.startupFailureValue
  }

  startingPid(): number | undefined {
    return this.child?.pid
  }

  markReady(): void {
    if (!this.child) return
    this.update({
      owned: true,
      status: 'running',
      pid: this.child.pid,
      program: this.current.program
    })
  }

  async stop(): Promise<void> {
    const child = this.child
    if (!child) {
      this.update({ owned: false, status: 'stopped' })
      return
    }
    this.child = undefined
    await stopManagedChild(child)
    this.update({ owned: false, status: 'stopped' })
  }

  private update(state: ServiceState): void {
    this.current = state
    this.onState({ ...state })
  }

  private async resolveProgram(): Promise<Program> {
    const executable = process.platform === 'win32' ? '.exe' : ''
    const candidates: Program[] = [
      { path: join(process.resourcesPath, 'bin', `seriald${executable}`), args: ['serve', '--managed'] },
      { path: join(process.resourcesPath, 'bin', `serial${executable}`), args: ['serve', '--managed'] },
      { path: join(dirname(app.getPath('exe')), `seriald${executable}`), args: ['serve', '--managed'] },
      { path: join(dirname(app.getPath('exe')), `serial${executable}`), args: ['serve', '--managed'] },
      { path: join(process.cwd(), 'target', 'debug', `seriald${executable}`), args: ['serve', '--managed'] },
      { path: join(process.cwd(), 'target', 'debug', `serial${executable}`), args: ['serve', '--managed'] }
    ]
    for (const candidate of candidates) {
      try {
        await access(candidate.path, constants.X_OK)
        return candidate
      } catch {
        // Try the next packaged or development location.
      }
    }
    throw new Error('未找到本地 seriald；请先构建 serial 或 seriald')
  }
}

export function parseDiscoveredEndpoint(output: string): DiscoveredSeriald | undefined {
  const value = output.trim()
  if (!value) return undefined
  const record = JSON.parse(value) as Record<string, unknown>
  const endpoint = validHttpOrigin(record.endpoint)
  if (
    record.schema_version !== 1
    || record.protocol_version !== SERIAL_PROTOCOL_VERSION
    || !endpoint
    || typeof record.address !== 'string'
    || !validUuid(record.server_id)
    || !validUuid(record.daemon_epoch)
    || !Number.isInteger(record.pid)
    || Number(record.pid) <= 0
  ) {
    throw new Error('seriald 返回了无效或不兼容的 endpoint marker')
  }
  return {
    endpoint,
    serverId: record.server_id,
    daemonEpoch: record.daemon_epoch,
    protocolVersion: record.protocol_version,
    pid: Number(record.pid)
  }
}

function validHttpOrigin(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  try {
    const url = new URL(value)
    if (
      url.protocol !== 'http:'
      || url.username
      || url.password
      || url.pathname !== '/'
      || url.search
      || url.hash
    ) return undefined
    return value.replace(/\/$/, '')
  } catch {
    return undefined
  }
}

function validUuid(value: unknown): value is string {
  return typeof value === 'string'
    && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
}
