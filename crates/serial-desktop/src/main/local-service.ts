import { access } from 'node:fs/promises'
import { constants } from 'node:fs'
import { dirname, join } from 'node:path'
import { spawn, type ChildProcess } from 'node:child_process'
import { app } from 'electron'
import type { ServiceState } from '../shared/contracts'
import { buildSpawnArgs } from './service-command'
import { stopManagedChild } from './managed-child'

interface Program {
  path: string
  args: string[]
}

export class LocalService {
  private child?: ChildProcess
  private current: ServiceState = { owned: false, status: 'stopped' }
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
    this.update({ owned: true, status: 'starting', program: program.path })
    const child = spawn(program.path, buildSpawnArgs(program.args, endpoint), {
      stdio: ['pipe', 'ignore', 'ignore'],
      windowsHide: true,
      detached: false
    })
    this.child = child
    child.once('exit', () => {
      if (this.child !== child) return
      this.child = undefined
      this.update({ owned: false, status: 'exited', program: program.path })
    })
    child.once('error', (error) => {
      if (this.child === child) this.child = undefined
      this.update({ owned: false, status: 'exited', program: `${program.path}: ${error.message}` })
    })
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
