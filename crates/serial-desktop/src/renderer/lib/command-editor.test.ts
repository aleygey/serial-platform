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
