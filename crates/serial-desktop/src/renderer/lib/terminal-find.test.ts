import { describe, expect, it } from 'vitest'
import type { TimelineEvent } from '../../shared/contracts'
import {
  MAX_HIGHLIGHTED_FIND_MATCHES,
  MAX_HIGHLIGHTED_FIND_SPANS,
  sanitizeTerminalText,
  TerminalDocumentIndex
} from './terminal-find'

describe('terminal find document', () => {
  it('searches the exact displayed text across RX event boundaries', () => {
    const index = new TerminalDocumentIndex()
    index.sync([
      rx(1, '\u001b[32mREADY\u001b[0m net'),
      rx(2, 'work\r\nroot# ')
    ])

    index.search('network')

    expect(index.matchCount).toBe(1)
    expect(index.hitsFor(index.chunks[0])).toMatchObject([{ start: 6, end: 9, current: true }])
    expect(index.hitsFor(index.chunks[1])).toMatchObject([{ start: 0, end: 4, current: true }])
    expect(sanitizeTerminalText('\u001b[31mfail\u001b[0m\r\n')).toBe('fail\n')
  })

  it('cycles in both directions, follows appended matches from latest, and retains that anchor on rebuild', () => {
    const index = new TerminalDocumentIndex()
    const initial = [rx(1, 'ready\n'), rx(2, 'READY\n')]
    index.sync(initial)
    index.search('ready')
    const oldLatest = index.activeId

    expect(index.activeIndex).toBe(1)
    expect(index.move(1)?.id).toBe(index.matchAt(0)?.id)
    expect(index.move(-1)?.id).toBe(oldLatest)

    index.sync([...initial, rx(3, 'ready\n')])
    expect(index.matchCount).toBe(3)
    expect(index.activeIndex).toBe(2)
    expect(index.activeId).toBe(index.matchAt(2)?.id)
    expect(index.activeId).not.toBe(oldLatest)
    const appendedLatest = index.activeId

    index.sync(initial.map((event) => ({ ...event })).concat(rx(3, 'ready\n')))
    expect(index.activeId).toBe(appendedLatest)
    expect(index.matchCount).toBe(3)
  })

  it('keeps a browsed old match selected when new matches append', () => {
    const index = new TerminalDocumentIndex()
    const initial = [rx(1, 'ready\n'), rx(2, 'ready\n'), rx(3, 'ready\n')]
    index.sync(initial)
    index.search('ready')
    const browsed = index.move(1)?.id

    expect(index.activeIndex).toBe(0)
    expect(index.sync([...initial, rx(4, 'ready\n')])).toBe('append')
    expect(index.matchCount).toBe(4)
    expect(index.activeIndex).toBe(0)
    expect(index.activeId).toBe(browsed)
  })

  it('drops truncated matches without shifting retained anchors', () => {
    const index = new TerminalDocumentIndex()
    const first = rx(1, 'hit\n')
    const second = rx(2, 'hit\n')
    index.sync([first, second])
    index.search('hit')
    const retained = index.activeId

    expect(index.sync([second, rx(3, 'tail\n')])).toBe('truncate-append')
    expect(index.matchCount).toBe(1)
    expect(index.activeId).toBe(retained)
  })

  it('keeps full-document non-overlapping occurrence semantics after append', () => {
    const index = new TerminalDocumentIndex()
    const initial = [rx(1, 'aa')]
    index.sync(initial)
    index.search('aa')
    expect(matchPositions(index)).toEqual([[0, 2]])

    index.sync([...initial, rx(2, 'a')])
    expect(matchPositions(index)).toEqual([[0, 2]])
  })

  it('realigns non-overlapping occurrences when a matched prefix is truncated', () => {
    const index = new TerminalDocumentIndex()
    const first = rx(1, 'a')
    const second = rx(2, 'aa')
    index.sync([first, second])
    index.search('aa')
    expect(matchPositions(index)).toEqual([[0, 2]])

    expect(index.sync([second, rx(3, '')])).toBe('truncate-append')
    expect(matchPositions(index)).toEqual([[1, 3]])
  })

  it('realigns after truncating a non-active match while preserving the active anchor', () => {
    const index = new TerminalDocumentIndex()
    const first = rx(1, 'a')
    const second = rx(2, 'aaXaa')
    index.sync([first, second])
    index.search('aa')
    const retained = index.activeId
    expect(matchPositions(index)).toEqual([[0, 2], [4, 6]])

    expect(index.sync([second, rx(3, '')])).toBe('truncate-append')
    expect(matchPositions(index)).toEqual([[1, 3], [4, 6]])
    expect(index.activeId).toBe(retained)
  })

  it('uses a bounded tail scan for the streaming performance fixture', () => {
    const index = new TerminalDocumentIndex()
    const fixture = Array.from({ length: 4_000 }, (_, offset) => rx(offset + 1, `line-${offset}\n`))
    index.sync(fixture)
    index.search('needle-across')
    const before = index.stats()
    const lastChunk = index.chunks.at(-1)

    expect(index.sync([...fixture, rx(4_001, 'needle-'), rx(4_002, 'across\n')])).toBe('append')
    const after = index.stats()
    expect(index.matchCount).toBe(1)
    expect(after.rebuilds).toBe(before.rebuilds)
    expect(after.sanitizedChunks - before.sanitizedChunks).toBe(2)
    expect(after.incrementalSearchCharacters - before.incrementalSearchCharacters).toBeLessThanOrEqual(32)
    expect(index.chunks.at(-3)).toBe(lastChunk)

    const anchor = index.activeId
    const beforeSlide = index.stats()
    const slid = [...fixture.slice(1), rx(4_001, 'needle-'), rx(4_002, 'across\n'), rx(4_003, 'tail\n')]
    expect(index.sync(slid)).toBe('truncate-append')
    const afterSlide = index.stats()
    expect(index.activeId).toBe(anchor)
    expect(afterSlide.rebuilds).toBe(beforeSlide.rebuilds)
    expect(afterSlide.fullSearchCharacters).toBe(beforeSlide.fullSearchCharacters)
    expect(afterSlide.incrementalSearchCharacters - beforeSlide.incrementalSearchCharacters).toBeLessThanOrEqual(24)
  })

  it('bounds React highlight spans for one match crossing many RX events', () => {
    const index = new TerminalDocumentIndex()
    const fixture = Array.from({ length: 600 }, (_, offset) => rx(offset + 1, 'a'))
    index.sync(fixture)
    index.search('a'.repeat(600))

    const renderedHits = index.chunks.reduce(
      (count, chunk) => count + index.hitsFor(chunk).length,
      0
    )
    expect(index.matchCount).toBe(1)
    expect(index.activeIndex).toBe(0)
    expect(index.hitsFor(index.chunks[0])).toMatchObject([{ current: true }])
    expect(renderedHits).toBe(MAX_HIGHLIGHTED_FIND_SPANS)
    expect(index.stats().highlightedSpans).toBe(MAX_HIGHLIGHTED_FIND_SPANS)
  })

  it('indexes every high-hit occurrence while materializing only a bounded highlight window', () => {
    const index = new TerminalDocumentIndex()
    const fixture = Array.from(
      { length: 12_000 },
      (_, offset) => rx(offset + 1, `${'a'.repeat(80)}\n`)
    )
    index.sync(fixture)

    const started = performance.now()
    index.search('a')
    const elapsedMs = performance.now() - started
    const renderedHits = index.chunks.reduce(
      (count, chunk) => count + index.hitsFor(chunk).length,
      0
    )
    const stats = index.stats()
    const exactTotal = 960_000

    expect(index.matchCount).toBe(exactTotal)
    expect(index.activeIndex).toBe(exactTotal - 1)
    expect(index.activeId).toBe(index.matchAt(exactTotal - 1)?.id)
    expect(index.highlightedMatchCount).toBe(MAX_HIGHLIGHTED_FIND_MATCHES)
    expect(renderedHits).toBe(MAX_HIGHLIGHTED_FIND_MATCHES)
    expect(stats.highlightedMatches).toBe(MAX_HIGHLIGHTED_FIND_MATCHES)
    expect(stats.highlightedSpans).toBe(MAX_HIGHLIGHTED_FIND_MATCHES)
    expect(stats.positionIndexBytes).toBeLessThanOrEqual(8 * 1024 * 1024)
    expect(index.hitsFor(index.chunks[0])).toHaveLength(0)
    expect(index.hitsFor(index.chunks.at(-1)!)).toHaveLength(80)

    const latest = index.matchAt(exactTotal - 1)
    const earliest = index.matchAt(0)
    expect(index.move(1)).toEqual(earliest)
    expect(index.activeIndex).toBe(0)
    expect(index.hitsFor(index.chunks[0])).toHaveLength(80)
    expect(index.move(-1)).toEqual(latest)
    expect(index.activeIndex).toBe(exactTotal - 1)

    const acrossWindowIndex = 10_017
    const acrossWindow = index.matchAt(acrossWindowIndex)
    index.select(acrossWindow?.id)
    expect(index.activeIndex).toBe(acrossWindowIndex)
    expect(index.move(-1)).toEqual(index.matchAt(acrossWindowIndex - 1))
    expect(index.move(1)).toEqual(acrossWindow)

    // This fixture previously needed one object plus rendered highlights for
    // every result. Keep a generous CI guard while the structural assertions
    // above enforce the compact-index and render bounds deterministically.
    expect(elapsedMs).toBeLessThan(1_500)
  })

  it('keeps an event/local active anchor and exact total after a dense rolling truncate', () => {
    const index = new TerminalDocumentIndex()
    const exact = Array.from({ length: 100 }, (_, offset) => rx(offset + 1, 'hit\n'))
    index.sync(exact)
    index.search('hit')
    expect(index.matchCount).toBe(100)

    const dense = Array.from(
      { length: 12_000 },
      (_, offset) => rx(offset + 1_001, `${'x'.repeat(80)}\n`)
    )
    index.sync(dense)
    index.search('x')
    const retained = index.matchAt(80)?.id
    index.select(retained)
    const before = index.stats()

    const slid = [...dense.slice(1), rx(13_001, `${'x'.repeat(80)}\n`)]
    expect(index.sync(slid)).toBe('truncate-append')
    const after = index.stats()
    expect(index.matchCount).toBe(960_000)
    expect(index.activeId).toBe(retained)
    expect(index.activeIndex).toBe(0)
    expect(index.highlightedMatchCount).toBe(MAX_HIGHLIGHTED_FIND_MATCHES)
    expect(after.fullSearchCharacters).toBe(before.fullSearchCharacters)
    expect(after.incrementalSearchCharacters - before.incrementalSearchCharacters).toBeLessThanOrEqual(81)
  })
})

function matchPositions(index: TerminalDocumentIndex): number[][] {
  return Array.from({ length: index.matchCount }, (_, position) => {
    const match = index.matchAt(position)
    if (!match) throw new Error(`missing match ${position}`)
    return [match.start, match.end]
  })
}

function rx(seq: number, text: string): TimelineEvent {
  return {
    port: 'COM6', daemon_epoch: 'epoch', seq, generation: 1, wall_time_ns: 0,
    kind: 'rx', direction: 'rx', text, metadata: {}, durable: true
  }
}
