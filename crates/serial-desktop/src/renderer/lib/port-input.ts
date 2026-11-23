import { HUMAN_COMMAND_UNCERTAIN_MESSAGE, type HumanCommandSubmission } from '../../shared/contracts'
import { commandBuffer, type CommandBuffer } from './command-editor'

export interface CommandSubmissionResolution {
  draft: string
  commitToHistory: boolean
  uncertainty?: string
}

export function resolveCommandSubmission(
  command: string,
  submission: HumanCommandSubmission
): CommandSubmissionResolution {
  if (submission.status === 'accepted') return { draft: '', commitToHistory: true }
  if (submission.status === 'rejected') return { draft: command, commitToHistory: false }
  return { draft: '', commitToHistory: false, uncertainty: submission.message }
}

export interface CommandReceipt {
  id: number
  command: string
  status: 'sending' | HumanCommandSubmission['status']
  message?: string
}

/** One editor per endpoint and port, including while its pane is closed. */
export class PortInput {
  private state = { buffer: commandBuffer(), receipts: [] as CommandReceipt[], pending: false }
  private listeners = new Set<() => void>()
  private nextId = 0
  private edits = 0
  snapshot = () => this.state
  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }
  edit(buffer: CommandBuffer): void {
    const previous = this.state.buffer
    if (buffer.value !== previous.value) this.edits++
    if (buffer.value !== previous.value || buffer.start !== previous.start || buffer.end !== previous.end) {
      this.update({ ...this.state, buffer })
    }
  }
  private update(next: typeof this.state): void {
    this.state = next
    this.listeners.forEach((listener) => listener())
  }
  async submit(command: string, action: () => Promise<HumanCommandSubmission>, clearDraft: boolean): Promise<void> {
    if (this.state.pending) return
    const original = this.state.buffer
    const revision = this.edits
    const receipt: CommandReceipt = { id: ++this.nextId, command, status: 'sending' }
    this.update({ ...this.state, buffer: clearDraft ? commandBuffer() : original, pending: true, receipts: [receipt, ...this.state.receipts].slice(0, 50) })
    let result: HumanCommandSubmission
    try { result = await action() }
    catch { result = { status: 'uncertain', message: HUMAN_COMMAND_UNCERTAIN_MESSAGE } }
    this.update({
      buffer: clearDraft && result.status === 'rejected' && revision === this.edits ? original : this.state.buffer,
      pending: false,
      receipts: [{ ...receipt, ...result }, ...this.state.receipts.slice(1)]
    })
  }
}
