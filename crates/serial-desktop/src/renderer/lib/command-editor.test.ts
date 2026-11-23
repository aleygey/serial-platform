import { PortInput } from './port-input'
import type { HumanCommandSubmission } from '../../shared/contracts'
import { describe, expect, it } from 'vitest'
import { acceptsSuggestion, commandBuffer, humanHistoryCandidates, stepHistory, suggestedSuffix } from './command-editor'

describe('Human command editor', () => {
  it('offers a recent literal prefix without duplicating history or interpreting shell patterns', () => {
    const history = ['cat /new', 'cat /new', 'cat /old', 'echo [a]', '\u0004', 'a\nb', '']
    expect(humanHistoryCandidates(history, 'cat')).toEqual(['cat /new', 'cat /old'])
    expect(suggestedSuffix(commandBuffer('cat'), history)).toBe(' /new')
    expect(suggestedSuffix(commandBuffer('echo ['), history)).toBe('a]')
    expect(humanHistoryCandidates(history, '')).toEqual(['cat /new', 'cat /old', 'echo [a]'])
    expect(suggestedSuffix(commandBuffer(), history)).toBe('')
  })

  it('accepts ghost text only at an unselected line end, never on Enter or IME confirmation', () => {
    const buffer = commandBuffer('echo 中🙂')
    expect(acceptsSuggestion('ArrowRight', buffer, '文', false, false)).toBe(true)
    expect(acceptsSuggestion('End', buffer, '文', false, false)).toBe(true)
    for (const key of ['Enter', 'ArrowLeft']) expect(acceptsSuggestion(key, buffer, '文', false, false)).toBe(false)
    expect(acceptsSuggestion('ArrowRight', { ...buffer, start: 2, end: 2 }, '文', false, false)).toBe(false)
    expect(acceptsSuggestion('ArrowRight', { ...buffer, start: 0 }, '文', false, false)).toBe(false)
    expect(acceptsSuggestion('ArrowRight', buffer, '文', true, false)).toBe(false)
    expect(acceptsSuggestion('ArrowRight', buffer, '文', false, true)).toBe(false)
  })

  it('restores the exact draft and selection after history browsing despite concurrent additions', () => {
    const draft = { value: 'unfinished command', start: 2, end: 4 }
    const first = stepHistory(draft, undefined, ['latest', 'older'], 'older')
    const second = stepHistory(first.buffer, first.cursor, ['new arrival', 'latest', 'older'], 'older')
    expect(second.buffer.value).toBe('older')
    const back = stepHistory(second.buffer, second.cursor, [], 'newer')
    expect(back.buffer.value).toBe('latest')
    expect(stepHistory(back.buffer, back.cursor, [], 'newer')).toEqual({ buffer: draft })
  })
})

it('keeps new typing while an earlier write is awaiting acknowledgement, restoring only an untouched rejected draft', async () => {
  const input = new PortInput()
  input.edit(commandBuffer('first'))
  let reject!: () => void
  const pending = input.submit('first', () => new Promise(resolve => { reject = () => resolve({ status: 'rejected', message: 'busy' }) }), true)
  expect(input.snapshot().buffer.value).toBe('')
  input.edit(commandBuffer('second'))
  reject()
  await pending
  expect(input.snapshot().buffer.value).toBe('second')
  await input.submit('second', async () => ({ status:'rejected', message:'busy' }), true)
  expect(input.snapshot().buffer.value).toBe('second')
})

it('dispatches successive commands, signals and empty Returns in order before any acknowledgement', async () => {
  const input = new PortInput()
  const sent: string[] = []
  const finish: Array<(result: HumanCommandSubmission) => void> = []
  function send(value: string, clear = true) {
    return input.submit(value, () => {
      sent.push(value)
      return new Promise(resolve => finish.push(resolve))
    }, clear)
  }
  input.edit(commandBuffer('version'))
  const first = send('version')
  const enter = send('')
  const signal = send('Ctrl+C', false)
  input.edit(commandBuffer('status'))
  const last = send('status')
  expect(sent).toEqual(['version', '', 'Ctrl+C', 'status'])
  finish[3]({ status: 'accepted' })
  finish[2]({ status: 'uncertain', message: 'disconnected' })
  await Promise.all([last, signal])
  expect(input.snapshot().pending).toBe(true)
  finish[0]({ status: 'rejected', message: 'busy' })
  finish[1]({ status: 'accepted' })
  await Promise.all([first, enter])
  expect(input.snapshot().pending).toBe(false)
  expect(input.snapshot().buffer.value).toBe('')
  expect(input.snapshot().receipts.map(r => [r.command, r.status])).toEqual([
    ['status', 'accepted'], ['Ctrl+C', 'uncertain'], ['', 'accepted'], ['version', 'rejected']
  ])
  expect(sent).toHaveLength(4)
})
