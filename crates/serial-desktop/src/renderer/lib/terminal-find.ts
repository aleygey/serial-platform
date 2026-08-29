import type { TimelineEvent } from '../../shared/contracts'

export interface TerminalDecoration {
  start: number
  end: number
  className: 'term-error' | 'term-success' | 'term-warning' | 'term-address'
}

export interface TerminalDocumentChunk {
  key: string
  daemonEpoch: string
  seq: number
  generation: number
  streamOffsetStart?: number
  streamOffsetEnd?: number
  text: string
  absoluteStart: number
  absoluteEnd: number
  decorations: TerminalDecoration[]
}

export interface TerminalFindMatch {
  id: string
  start: number
  end: number
}

export interface TerminalFindHit {
  id: string
  start: number
  end: number
  current: boolean
}

export interface TerminalFindStats {
  rebuilds: number
  sanitizedChunks: number
  fullSearchCharacters: number
  incrementalSearchCharacters: number
  positionIndexBytes: number
  highlightedMatches: number
  highlightedSpans: number
}

interface MatchAnchor {
  firstKey: string
  firstOffset: number
  lastKey: string
  lastEndOffset: number
}

const EMPTY_HITS: readonly TerminalFindHit[] = Object.freeze([])
/**
 * Every occurrence remains navigable in two compact Uint32 position arrays,
 * while only this many nearby matches are expanded into highlight objects.
 * A separate span cap covers unusually long matches crossing many events.
 * Together they keep a dense search exact without building one JS object (and
 * several DOM nodes) per occurrence.
 */
export const MAX_HIGHLIGHTED_FIND_MATCHES = 256
export const MAX_HIGHLIGHTED_FIND_SPANS = 512
const INITIAL_MATCH_CAPACITY = 256
const MAX_UINT32 = 0xffff_ffff
const SEMANTIC_PATTERN = /(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}|\b(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}\b|(?<![A-Za-z0-9_])(?:error|failed|fatal)(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])(?:success|passed|ready)(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])(?:warning|warn)(?![A-Za-z0-9_])/gi

/**
 * An append-aware index over exactly the text rendered by the terminal.
 * Absolute character positions never move during a rolling-window truncate;
 * user-visible match identity is additionally anchored to event keys and local
 * offsets, so a snapshot rebuild can retain the same current match.
 */
export class TerminalDocumentIndex {
  private source?: TimelineEvent[]
  private chunksValue: TerminalDocumentChunk[] = []
  private readonly chunksByKey = new Map<string, TerminalDocumentChunk>()
  private nextAbsolute = 0
  private queryValue = ''
  /** Absolute position represented by offset zero in both typed arrays. */
  private matchOrigin = 0
  private matchStarts = new Uint32Array(0)
  private matchEnds = new Uint32Array(0)
  /** Logical matches occupy `[matchHead, matchHead + matchLength)`. */
  private matchHead = 0
  private matchLength = 0
  private activeIndexValue = -1
  private readonly hitsByChunk = new Map<string, TerminalFindHit[]>()
  private highlightedMatchCountValue = 0
  private highlightedSpanCountValue = 0
  private readonly statsValue: TerminalFindStats = {
    rebuilds: 0,
    sanitizedChunks: 0,
    fullSearchCharacters: 0,
    incrementalSearchCharacters: 0,
    positionIndexBytes: 0,
    highlightedMatches: 0,
    highlightedSpans: 0
  }

  sync(events: TimelineEvent[]): 'unchanged' | 'append' | 'truncate-append' | 'rebuild' {
    const previous = this.source
    if (previous === events) return 'unchanged'
    if (previous && previous.length > 0) {
      if (
        events.length > previous.length
        && eventKey(events[0]) === eventKey(previous[0])
        && eventKey(events[previous.length - 1]) === eventKey(previous.at(-1)!)
      ) {
        const wasFollowingLatest = this.activeIndexValue === this.matchLength - 1
        const preferred = this.activeAnchor()
        const fallback = this.activeIndexValue >= 0 ? this.activeIndexValue : undefined
        this.appendEvents(events.slice(previous.length))
        this.source = events
        this.restoreActive(wasFollowingLatest ? undefined : preferred, wasFollowingLatest ? undefined : fallback)
        return 'append'
      }
      if (
        events.length === previous.length
        && previous.length > 1
        && eventKey(events[0]) === eventKey(previous[1])
        && eventKey(events.at(-2)!) === eventKey(previous.at(-1)!)
      ) {
        const wasFollowingLatest = this.activeIndexValue === this.matchLength - 1
        const preferred = this.activeAnchor()
        const oldIndex = this.activeIndexValue
        const removedMatches = this.removeEvent(previous[0])
        // A one-code-unit literal cannot overlap itself. Longer literals can:
        // removing the first selected occurrence may change `/g` alignment for
        // the retained prefix (`aaa` / `aa`), so rescan that bounded document.
        const requiresRealignment = removedMatches > 0 && this.queryValue.length > 1
        this.appendEvents([events.at(-1)!], !requiresRealignment)
        this.source = events
        const fallback = oldIndex >= 0 ? Math.max(0, oldIndex - removedMatches) : undefined
        const retained = wasFollowingLatest ? undefined : preferred
        const retainedFallback = wasFollowingLatest ? undefined : fallback
        if (requiresRealignment) this.fullSearch(retained, retainedFallback)
        else this.restoreActive(retained, retainedFallback)
        return 'truncate-append'
      }
    }
    this.rebuild(events)
    return 'rebuild'
  }

  search(query: string): void {
    if (query === this.queryValue) return
    this.queryValue = query
    this.fullSearch(undefined)
  }

  move(direction: 1 | -1): TerminalFindMatch | undefined {
    if (this.matchLength === 0) return undefined
    const current = this.activeIndexValue
    const next = current < 0
      ? direction > 0 ? 0 : this.matchLength - 1
      : (current + direction + this.matchLength) % this.matchLength
    this.setActiveIndex(next)
    return this.matchAt(next)
  }

  select(id: string | undefined): void {
    if (!id) {
      this.setActiveIndex(-1)
      return
    }
    if (this.activeId === id) return
    const anchor = matchAnchorFromId(id)
    this.setActiveIndex(anchor ? this.findMatchByAnchor(anchor) : -1)
  }

  get chunks(): readonly TerminalDocumentChunk[] {
    return this.chunksValue
  }

  matchAt(index: number): TerminalFindMatch | undefined {
    const position = this.positionAt(index)
    if (!position) return undefined
    const [start, end] = position
    const id = this.matchId(start, end)
    return id ? { id, start, end } : undefined
  }

  get matchCount(): number {
    return this.matchLength
  }

  get highlightedMatchCount(): number {
    return this.highlightedMatchCountValue
  }

  get activeId(): string | undefined {
    return this.matchAt(this.activeIndexValue)?.id
  }

  get activeIndex(): number {
    return this.activeIndexValue
  }

  hitsFor(chunk: TerminalDocumentChunk): readonly TerminalFindHit[] {
    return this.hitsByChunk.get(chunk.key) ?? EMPTY_HITS
  }

  stats(): TerminalFindStats {
    return {
      ...this.statsValue,
      positionIndexBytes: this.matchStarts.byteLength + this.matchEnds.byteLength,
      highlightedMatches: this.highlightedMatchCountValue,
      highlightedSpans: this.highlightedSpanCountValue
    }
  }

  private rebuild(events: TimelineEvent[]): void {
    const preferred = this.activeAnchor()
    this.chunksValue = []
    this.chunksByKey.clear()
    this.nextAbsolute = 0
    this.source = events
    this.statsValue.rebuilds += 1
    for (const event of events) this.appendChunk(event)
    this.fullSearch(preferred)
  }

  private appendEvents(events: TimelineEvent[], updateMatches = true): void {
    if (events.length === 0) return
    const oldEnd = this.nextAbsolute
    for (const event of events) this.appendChunk(event)
    if (!updateMatches || !this.queryValue || this.nextAbsolute === oldEnd) return
    if (this.nextAbsolute - this.matchOrigin > MAX_UINT32) {
      this.fullSearch(this.activeAnchor())
      return
    }
    const overlap = Math.max(0, this.queryValue.length - 1)
    const base = this.chunksValue[0]?.absoluteStart ?? this.nextAbsolute
    // Preserve the full-document `/g` non-overlap rule. A tail-only scan may
    // otherwise invent an overlapping match after append ("aa" + "a").
    const scanStart = Math.max(base, oldEnd - overlap, this.lastMatchEnd() ?? base)
    const text = this.textBetween(scanStart, this.nextAbsolute)
    this.statsValue.incrementalSearchCharacters += text.length
    this.scanText(text, scanStart, oldEnd)
  }

  private removeEvent(event: TimelineEvent): number {
    if (event.direction !== 'rx') return 0
    const key = eventKey(event)
    const index = this.chunksValue.findIndex((chunk) => chunk.key === key)
    if (index < 0) return 0
    this.chunksValue.splice(index, 1)
    this.chunksByKey.delete(key)
    const base = this.chunksValue[0]?.absoluteStart ?? this.nextAbsolute
    const removed = this.lowerBoundStart(base)
    this.matchHead += removed
    this.matchLength -= removed
    if (this.matchLength === 0) this.matchHead = 0
    else if (this.matchHead > this.matchStarts.length / 2) this.compactMatches()
    return removed
  }

  private appendChunk(event: TimelineEvent): void {
    if (event.direction !== 'rx') return
    const text = sanitizeTerminalText(event.text)
    const absoluteStart = this.nextAbsolute
    this.nextAbsolute += text.length
    const chunk = {
      key: eventKey(event),
      daemonEpoch: event.daemon_epoch,
      seq: event.seq,
      generation: event.generation,
      streamOffsetStart: event.stream_offset_start ?? undefined,
      streamOffsetEnd: event.stream_offset_end ?? undefined,
      text,
      absoluteStart,
      absoluteEnd: this.nextAbsolute,
      decorations: terminalDecorations(text)
    }
    this.chunksValue.push(chunk)
    this.chunksByKey.set(chunk.key, chunk)
    this.statsValue.sanitizedChunks += 1
  }

  private fullSearch(preferred: MatchAnchor | undefined, fallbackIndex?: number): void {
    const text = this.chunksValue.map((chunk) => chunk.text).join('')
    const base = this.chunksValue[0]?.absoluteStart ?? this.nextAbsolute
    if (text.length > MAX_UINT32) {
      throw new Error('串口查找文档超过 4 GiB，无法建立紧凑位置索引')
    }
    this.statsValue.fullSearchCharacters += text.length
    this.matchOrigin = base
    this.matchHead = 0
    this.matchLength = 0
    if (this.queryValue) this.scanText(text, base)
    if (!this.queryValue && this.matchStarts.length > INITIAL_MATCH_CAPACITY) {
      this.matchStarts = new Uint32Array(0)
      this.matchEnds = new Uint32Array(0)
    }
    this.restoreActive(preferred, fallbackIndex)
  }

  private scanText(text: string, base: number, requireEndAfter = Number.NEGATIVE_INFINITY): void {
    if (!this.queryValue || !text) return
    const pattern = new RegExp(escapeRegex(this.queryValue), 'giu')
    for (const found of text.matchAll(pattern)) {
      const start = base + found.index
      const end = start + found[0].length
      if (end > start && end > requireEndAfter) this.appendIndexedMatch(start, end)
    }
  }

  private appendIndexedMatch(start: number, end: number): void {
    const relativeStart = start - this.matchOrigin
    const relativeEnd = end - this.matchOrigin
    if (
      relativeStart < 0 || relativeEnd <= relativeStart
      || relativeStart > MAX_UINT32 || relativeEnd > MAX_UINT32
    ) {
      throw new Error('串口查找位置超出紧凑索引范围')
    }
    const slot = this.reserveMatchSlot()
    this.matchStarts[slot] = relativeStart
    this.matchEnds[slot] = relativeEnd
    this.matchLength += 1
  }

  private reserveMatchSlot(): number {
    const required = this.matchHead + this.matchLength + 1
    if (required <= this.matchStarts.length) return required - 1
    if (this.matchLength + 1 <= this.matchStarts.length) {
      this.compactMatches()
      return this.matchLength
    }
    let capacity = Math.max(INITIAL_MATCH_CAPACITY, this.matchStarts.length)
    while (capacity < this.matchLength + 1) capacity *= 2
    const starts = new Uint32Array(capacity)
    const ends = new Uint32Array(capacity)
    starts.set(this.matchStarts.subarray(this.matchHead, this.matchHead + this.matchLength))
    ends.set(this.matchEnds.subarray(this.matchHead, this.matchHead + this.matchLength))
    this.matchStarts = starts
    this.matchEnds = ends
    this.matchHead = 0
    return this.matchLength
  }

  private compactMatches(): void {
    if (this.matchHead === 0) return
    this.matchStarts.copyWithin(0, this.matchHead, this.matchHead + this.matchLength)
    this.matchEnds.copyWithin(0, this.matchHead, this.matchHead + this.matchLength)
    this.matchHead = 0
  }

  private positionAt(index: number): readonly [number, number] | undefined {
    if (!Number.isInteger(index) || index < 0 || index >= this.matchLength) return undefined
    const slot = this.matchHead + index
    return [
      this.matchOrigin + this.matchStarts[slot],
      this.matchOrigin + this.matchEnds[slot]
    ]
  }

  private lastMatchEnd(): number | undefined {
    return this.positionAt(this.matchLength - 1)?.[1]
  }

  private lowerBoundStart(target: number): number {
    let low = 0
    let high = this.matchLength
    while (low < high) {
      const middle = (low + high) >>> 1
      const start = this.positionAt(middle)?.[0] ?? Number.POSITIVE_INFINITY
      if (start < target) low = middle + 1
      else high = middle
    }
    return low
  }

  private textBetween(start: number, end: number): string {
    const values: string[] = []
    for (const chunk of this.chunksValue) {
      if (chunk.absoluteEnd <= start) continue
      if (chunk.absoluteStart >= end) break
      const localStart = Math.max(0, start - chunk.absoluteStart)
      const localEnd = Math.min(chunk.text.length, end - chunk.absoluteStart)
      values.push(chunk.text.slice(localStart, localEnd))
    }
    return values.join('')
  }

  private rebuildHits(): void {
    this.hitsByChunk.clear()
    this.highlightedMatchCountValue = 0
    this.highlightedSpanCountValue = 0
    if (this.activeIndexValue < 0 || this.matchLength === 0) return
    const count = Math.min(this.matchLength, MAX_HIGHLIGHTED_FIND_MATCHES)
    const half = Math.floor(count / 2)
    const start = Math.max(0, Math.min(this.activeIndexValue - half, this.matchLength - count))
    const end = start + count
    const nearbyIndexes = [this.activeIndexValue]
    for (let distance = 1; nearbyIndexes.length < count; distance += 1) {
      if (this.activeIndexValue - distance >= start) nearbyIndexes.push(this.activeIndexValue - distance)
      if (nearbyIndexes.length < count && this.activeIndexValue + distance < end) {
        nearbyIndexes.push(this.activeIndexValue + distance)
      }
    }
    for (const index of nearbyIndexes) {
      const remainingSpans = MAX_HIGHLIGHTED_FIND_SPANS - this.highlightedSpanCountValue
      if (remainingSpans <= 0) break
      const match = this.matchAt(index)
      if (!match) continue
      const added = this.addMatchHits(match, index === this.activeIndexValue, remainingSpans)
      if (added > 0) this.highlightedMatchCountValue += 1
    }
    for (const hits of this.hitsByChunk.values()) {
      hits.sort((left, right) => left.start - right.start || left.end - right.end)
    }
  }

  private addMatchHits(match: TerminalFindMatch, current: boolean, spanLimit: number): number {
    const firstIndex = this.chunkIndexAt(match.start)
    if (firstIndex < 0) return 0
    let added = 0
    for (let index = firstIndex; index < this.chunksValue.length; index += 1) {
      if (added >= spanLimit) break
      const chunk = this.chunksValue[index]
      if (chunk.absoluteStart >= match.end) break
      const start = Math.max(0, match.start - chunk.absoluteStart)
      const end = Math.min(chunk.text.length, match.end - chunk.absoluteStart)
      if (end <= start) continue
      const hit: TerminalFindHit = {
        id: match.id,
        start,
        end,
        current
      }
      const hits = this.hitsByChunk.get(chunk.key)
      if (hits) hits.push(hit)
      else this.hitsByChunk.set(chunk.key, [hit])
      this.highlightedSpanCountValue += 1
      added += 1
    }
    return added
  }

  private chunkIndexAt(position: number): number {
    let low = 0
    let high = this.chunksValue.length - 1
    while (low <= high) {
      const middle = (low + high) >>> 1
      const chunk = this.chunksValue[middle]
      if (position < chunk.absoluteStart) high = middle - 1
      else if (position >= chunk.absoluteEnd) low = middle + 1
      else return middle
    }
    return -1
  }

  private setActiveIndex(index: number): void {
    const next = Number.isInteger(index) && index >= 0 && index < this.matchLength ? index : -1
    if (this.activeIndexValue === next && this.highlightedMatchCountValue > 0) return
    this.activeIndexValue = next
    this.rebuildHits()
  }

  private restoreActive(preferred: MatchAnchor | undefined, fallbackIndex?: number): void {
    const preferredIndex = preferred ? this.findMatchByAnchor(preferred) : -1
    const fallback = fallbackIndex === undefined ? this.matchLength - 1 : fallbackIndex
    this.activeIndexValue = preferredIndex >= 0
      ? preferredIndex
      : this.matchLength > 0
        ? Math.max(0, Math.min(fallback, this.matchLength - 1))
        : -1
    this.rebuildHits()
  }

  private activeAnchor(): MatchAnchor | undefined {
    const position = this.positionAt(this.activeIndexValue)
    return position ? this.anchorAt(position[0], position[1]) : undefined
  }

  private anchorAt(start: number, end: number): MatchAnchor | undefined {
    const first = this.chunkAt(start)
    const last = this.chunkAt(end - 1)
    if (!first || !last) return undefined
    return {
      firstKey: first.key,
      firstOffset: start - first.absoluteStart,
      lastKey: last.key,
      lastEndOffset: end - last.absoluteStart
    }
  }

  private findMatchByAnchor(anchor: MatchAnchor): number {
    const first = this.chunksByKey.get(anchor.firstKey)
    const last = this.chunksByKey.get(anchor.lastKey)
    if (
      !first || !last
      || anchor.firstOffset < 0 || anchor.firstOffset >= first.text.length
      || anchor.lastEndOffset <= 0 || anchor.lastEndOffset > last.text.length
    ) return -1
    const start = first.absoluteStart + anchor.firstOffset
    const end = last.absoluteStart + anchor.lastEndOffset
    const index = this.lowerBoundStart(start)
    const position = this.positionAt(index)
    return position?.[0] === start && position[1] === end ? index : -1
  }

  private matchId(start: number, end: number): string | undefined {
    const anchor = this.anchorAt(start, end)
    if (!anchor) return undefined
    return [
      encodeURIComponent(anchor.firstKey),
      anchor.firstOffset,
      encodeURIComponent(anchor.lastKey),
      anchor.lastEndOffset
    ].join('|')
  }

  private chunkAt(position: number): TerminalDocumentChunk | undefined {
    let low = 0
    let high = this.chunksValue.length - 1
    while (low <= high) {
      const middle = (low + high) >>> 1
      const chunk = this.chunksValue[middle]
      if (position < chunk.absoluteStart) high = middle - 1
      else if (position >= chunk.absoluteEnd) low = middle + 1
      else return chunk
    }
    return undefined
  }
}

export function sanitizeTerminalText(text: string): string {
  return text
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, '')
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '�')
    .replace(/\r/g, '')
}

function terminalDecorations(text: string): TerminalDecoration[] {
  const decorations: TerminalDecoration[] = []
  for (const match of text.matchAll(SEMANTIC_PATTERN)) {
    const value = match[0]
    const className = /^(error|failed|fatal)$/i.test(value)
      ? 'term-error'
      : /^(success|passed|ready)$/i.test(value)
        ? 'term-success'
        : /^(warning|warn)$/i.test(value)
          ? 'term-warning'
          : 'term-address'
    decorations.push({ start: match.index, end: match.index + value.length, className })
  }
  return decorations
}

function eventKey(event: TimelineEvent): string {
  return `${event.daemon_epoch}:${event.seq}`
}

function matchAnchorFromId(id: string): MatchAnchor | undefined {
  const [encodedFirstKey, firstOffsetText, encodedLastKey, lastEndOffsetText, ...extra] = id.split('|')
  if (!encodedFirstKey || !encodedLastKey || extra.length > 0) return undefined
  const firstOffset = Number(firstOffsetText)
  const lastEndOffset = Number(lastEndOffsetText)
  if (
    !Number.isSafeInteger(firstOffset) || firstOffset < 0
    || !Number.isSafeInteger(lastEndOffset) || lastEndOffset <= 0
  ) return undefined
  try {
    return {
      firstKey: decodeURIComponent(encodedFirstKey),
      firstOffset,
      lastKey: decodeURIComponent(encodedLastKey),
      lastEndOffset
    }
  } catch {
    return undefined
  }
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}
