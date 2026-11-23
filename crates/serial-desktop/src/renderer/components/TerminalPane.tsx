import { ArrowDown, ChevronDown, ChevronUp, Search, X } from 'lucide-react'
import { Fragment, memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { PortSnapshot, TimelineEvent } from '../../shared/contracts'
import type { AgentCommand, MatchRange } from '../lib/history'
import { commandEchoStyles, type EchoStyle } from '../lib/command-echo'
import { InlineTextEffect } from './ui/text-effect'
import { useReducedMotion } from '../lib/use-reduced-motion'
import { displayCommand } from '../lib/history'
import {
  TerminalDocumentIndex,
  type TerminalDocumentChunk,
  type TerminalFindHit
} from '../lib/terminal-find'

interface Props {
  commands?: AgentCommand[]
  currentCommand?: AgentCommand
  heading?: React.ReactNode
  actions?: React.ReactNode
  paused?: boolean
  active?: boolean
  configuredPort?: PortSnapshot
  events: TimelineEvent[]
  selectedCommand?: AgentCommand
  match?: MatchRange
  onClearCommand: () => void
}

const WINDOW_CHUNKS = 320
const TEXT_BLOCK = 128
function chunkIndex(chunks: readonly TerminalDocumentChunk[], seq: number): number {
  let lo = 0, hi = chunks.length
  while (lo < hi) {
    const mid = Math.floor((lo + hi) / 2)
    if (chunks[mid].seq < seq) lo = mid + 1
    else hi = mid
  }
  return lo
}

const NO_COMMANDS: AgentCommand[] = []
const NO_FIND_HITS: readonly TerminalFindHit[] = Object.freeze([])

export const TerminalPane = memo(function TerminalPane({ commands = NO_COMMANDS, currentCommand, heading, actions, paused, active = true, configuredPort, events, selectedCommand, match, onClearCommand }: Props): React.JSX.Element {
  const reduced = useReducedMotion()
  const scrollRef = useRef<HTMLDivElement>(null)
  const findInputRef = useRef<HTMLInputElement>(null)
  const previousFocusRef = useRef<HTMLElement | null>(null)
  const documentRef = useRef<TerminalDocumentIndex | null>(null)
  if (!documentRef.current) documentRef.current = new TerminalDocumentIndex()
  const terminalDocument = documentRef.current
  terminalDocument.sync(events)

  const [follow, setFollow] = useState(true)
  const manualScroll = useRef(false)
  const [findOpen, setFindOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [, setNavigationRevision] = useState(0)
  const initialStream = useRef({ epoch: configuredPort?.daemon_epoch, seq: events.at(-1)?.seq ?? 0 })
  if (initialStream.current.epoch !== configuredPort?.daemon_epoch) initialStream.current = { epoch: configuredPort?.daemon_epoch, seq: events.at(-1)?.seq ?? 0 }
  const chunks = terminalDocument.chunks
  const echoes = useMemo(() => commandEchoStyles(events, chunks, commands, currentCommand), [events, chunks, commands, currentCommand])
  const total = terminalDocument.matchCount
  const highlighted = terminalDocument.highlightedMatchCount
  const current = terminalDocument.activeIndex < 0 ? 0 : terminalDocument.activeIndex + 1
  const activeId = terminalDocument.activeId

  const target = findOpen && activeId ? activeId : selectedCommand ? selectedCommand.daemonEpoch + ':' + selectedCommand.firstSeq : ''
  const [anchor, setAnchor] = useState<{ seq?: number; target: string }>()
  const scrollAnchor = useRef<{ seq: number; offset: number; top: number } | null>(null)
  const textCache = useRef(new WeakMap<TerminalDocumentChunk, { last: TerminalDocumentChunk; text: string }>())
  const pinned = anchor?.target === target
  const range = pinned ? undefined : findOpen && activeId ? terminalDocument.activeChunkRange : selectedCommand
    ? { first: match?.fromSeq ?? selectedCommand.firstSeq, last: match?.fromSeq ?? selectedCommand.firstSeq } : undefined
  const tailStart = Math.max(0, chunks.length - WINDOW_CHUNKS)
  const start = pinned && anchor.seq !== undefined ? Math.max(0, Math.min(tailStart, chunkIndex(chunks, anchor.seq) - Math.floor(WINDOW_CHUNKS / 2))) :
    range ? Math.max(0, Math.min(tailStart, chunkIndex(chunks, range.first) - Math.floor(WINDOW_CHUNKS / 2))) : tailStart
  const end = Math.min(chunks.length, Math.max(start + WINDOW_CHUNKS, range ? chunkIndex(chunks, range.last) + 1 : 0))
  const visible = chunks.slice(start, end)

  // Preserve the browser's exact wrapping, selection and native scrollbar over
  // the whole retained document. Only the viewport needs per-event markup.
  function compactText(from: number, until: number) {
    const blocks: React.ReactNode[] = []
    for (let i = from; i < until;) {
      const first = chunks[i]
      const nextBucket = (Math.floor(first.seq / TEXT_BLOCK) + 1) * TEXT_BLOCK
      const stop = Math.min(until, chunkIndex(chunks, nextBucket))
      const last = chunks[stop - 1]
      let block = textCache.current.get(first)
      if (block?.last !== last) {
        block = { last, text: chunks.slice(i, stop).map(chunk => chunk.text).join('') }
        textCache.current.set(first, block)
      }
      blocks.push(<span key={'text:' + first.key} data-first-seq={first.seq}>{block.text}</span>)
      i = stop
    }
    return blocks
  }

  function rememberViewport(view: HTMLElement) {
    const box = view.getBoundingClientRect()
    // Use a real text position: one compact span can wrap across many screens.
    const range = document.caretRangeFromPoint(box.left + view.clientWidth / 2, box.top + 4)
    const node = range?.startContainer
    const span = node?.parentElement?.closest<HTMLElement>('[data-seq], [data-first-seq]')
    if (!range || !span || !view.contains(span)) return
    const first = Number(span.dataset.seq ?? span.dataset.firstSeq)
    let index = chunkIndex(chunks, first)
    const prefix = range.cloneRange()
    prefix.selectNodeContents(span)
    prefix.setEnd(range.startContainer, range.startOffset)
    let offset = prefix.toString().length
    while (index + 1 < chunks.length && offset >= chunks[index].text.length) offset -= chunks[index++].text.length
    const seq = chunks[index]?.seq
    if (seq === undefined) return
    // The detailed window is grown around the new position after a thumb drag.
    if (!pinned || index < start + TEXT_BLOCK / 2 || index >= end - TEXT_BLOCK / 2) setAnchor({ seq, target })
    scrollAnchor.current = { seq, offset, top: range.getBoundingClientRect().top - box.top }
  }
  const firstKey = chunks[0]?.key
  useLayoutEffect(() => {
    const saved = scrollAnchor.current
    const view = scrollRef.current
    if (!view || !saved || follow || !manualScroll.current) return
    const row = view.querySelector<HTMLElement>(`[data-seq="${saved.seq}"]`)
    if (!row) {
      if (chunks[0] && saved.seq < chunks[0].seq) view.scrollTop = 0
      return
    }
    const walker = document.createTreeWalker(row, NodeFilter.SHOW_TEXT)
    let remaining = saved.offset
    for (let node = walker.nextNode(); node; node = walker.nextNode()) {
      const length = node.textContent?.length ?? 0
      if (remaining <= length) {
        const range = document.createRange()
        range.setStart(node, remaining)
        range.collapse(true)
        view.scrollTop += range.getBoundingClientRect().top - view.getBoundingClientRect().top - saved.top
        break
      }
      remaining -= length
    }
  }, [firstKey])

  const openFind = useCallback((): void => {
    if (!findOpen) {
      const active = document.activeElement
      previousFocusRef.current = active instanceof HTMLElement ? active : null
      setFindOpen(true)
    }
    if (terminalDocument.activeId) setFollow(false)
    requestAnimationFrame(() => {
      findInputRef.current?.focus()
      findInputRef.current?.select()
    })
  }, [findOpen, terminalDocument])

  const closeFind = useCallback((): void => {
    setFindOpen(false)
    const previous = previousFocusRef.current
    requestAnimationFrame(() => {
      if (previous?.isConnected && !('disabled' in previous && Boolean(previous.disabled))) previous.focus()
      else scrollRef.current?.focus()
    })
  }, [])

  const navigate = useCallback((direction: 1 | -1): void => {
    if (!terminalDocument.move(direction)) return
    setFollow(false)
    setNavigationRevision((value) => value + 1)
  }, [terminalDocument])

  const updateSearch = (value: string): void => {
    terminalDocument.search(value)
    setSearch(value)
    if (terminalDocument.activeId) setFollow(false)
    setNavigationRevision((revision) => revision + 1)
  }

  const commandAnchor = selectedCommand ? `${selectedCommand.daemonEpoch}:${selectedCommand.firstSeq}:${match?.fromSeq}:${match?.hasVisibleOutput}:${events[0]?.daemon_epoch}:${events[0]?.seq}` : ''
  useEffect(() => {
    if (match?.hasVisibleOutput) {
      scrollRef.current?.querySelector(`[data-event-key="${match.daemonEpoch}:${match.fromSeq}"]`)?.scrollIntoView({ block: 'center' })
      setFollow(false)
    } else if (selectedCommand) {
      scrollRef.current?.querySelector(`[data-event-key="${selectedCommand.daemonEpoch}:${selectedCommand.firstSeq}"]`)?.scrollIntoView({ block: 'center' })
      setFollow(false)
    }
  }, [commandAnchor])

  useLayoutEffect(() => {
    if (follow && !manualScroll.current) scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'instant' })
  }, [chunks.at(-1)?.key, follow])

  const hasOutput = chunks.length > 0
  useEffect(() => {
    const view = scrollRef.current
    const output = view?.querySelector('pre')
    if (!follow || !view || !output) return
    const observer = new ResizeObserver(() => {
      if (!manualScroll.current) view.scrollTop = view.scrollHeight
    })
    observer.observe(view)
    observer.observe(output)
    return () => observer.disconnect()
  }, [follow, hasOutput])

  useEffect(() => {
    if (findOpen && activeId) {
      scrollRef.current?.querySelector('.term-search-hit.is-current')?.scrollIntoView({ block: 'center' })
    }
  }, [activeId, findOpen, search])

  useEffect(() => {
    if (!active) return
    const handler = (event: KeyboardEvent): void => {
      if (event.defaultPrevented) return
      if (document.querySelector('[aria-modal="true"]')) {
        if (((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'f') || event.key === 'F3') {
          event.preventDefault()
        }
        return
      }
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'f') {
        event.preventDefault()
        openFind()
        return
      }
      if (event.key === 'F3') {
        event.preventDefault()
        openFind()
        if (search) navigate(event.shiftKey ? -1 : 1)
        return
      }
      if (event.key === 'Escape' && findOpen) {
        event.preventDefault()
        closeFind()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [active, closeFind, findOpen, navigate, openFind, search])

  return (
    <section className="terminal-pane">
      <header className="channel-heading terminal-header">
        {heading ?? <strong>{configuredPort?.config.model_name || '未配置机型'}</strong>}
        <div className="channel-tools">
          <button className="terminal-find-trigger" type="button" aria-label="在串口输出中查找" aria-keyshortcuts="Control+f Meta+f" title="查找 (⌘/Ctrl+F)" onClick={openFind}><Search size={15} /></button>
          {actions}
        </div>
      </header>
      {findOpen && (
        <TerminalFindWidget
          current={current}
          inputRef={findInputRef}
          onClose={closeFind}
          onNext={() => navigate(1)}
          onPrevious={() => navigate(-1)}
          onQuery={updateSearch}
          query={search}
          total={total}
          highlighted={highlighted}
        />
      )}
      {selectedCommand && !match && (
        <div className="command-overlay">
          <span>尚未找到这条命令的完整回显</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" aria-label="关闭命令定位" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      {selectedCommand && match && !match.hasVisibleOutput && (
        <div className="command-overlay">
          <span>这段记录中没有设备输出</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" aria-label="关闭命令定位" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      {selectedCommand && match?.inferred && (
        <div className="command-overlay is-inferred">
          <span>旧记录的输出范围为推断值</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" aria-label="关闭命令定位" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      <div
        className="terminal-scroll"
        ref={scrollRef}
        tabIndex={0}
        onPointerDown={event => { if (event.nativeEvent.offsetX >= event.currentTarget.clientWidth) manualScroll.current = true }}
        onTouchMove={() => { manualScroll.current = true }}
        onKeyDown={event => {
          if (['ArrowUp','ArrowDown','PageUp','PageDown','Home','End',' '].includes(event.key)) manualScroll.current = true
        }}
        onWheel={event => {
          if (event.deltaY !== 0) manualScroll.current = true
        }}
        onScroll={(event) => {
          const element = event.currentTarget
          const atEnd = element.scrollHeight - element.scrollTop - element.clientHeight <= 3
          if (atEnd) {
            manualScroll.current = false
            scrollAnchor.current = null
            if (anchor?.seq !== undefined) setAnchor(undefined)
          } else {
            manualScroll.current = true
            rememberViewport(element)
          }
          setFollow(atEnd)
        }}
      >
        {chunks.length === 0 ? (
          <div className="empty-state terminal-empty">
            <strong>{configuredPort?.session_state === 'online' ? '等待设备输出' : configuredPort?.session_state === 'opening' ? '正在打开串口' : configuredPort?.session_state === 'waiting_for_port' ? '等待设备接入' : '串口未打开'}</strong>
          </div>
        ) : (
          <pre className="terminal-output" aria-label="串口输出" onDoubleClick={selectTerminalWord}>
            {compactText(0, start)}
            {visible.map((chunk) => (
              <TerminalChunkText
                reveal={echoes.has(chunk.key) && !paused && !reduced && follow && !selectedCommand && !findOpen && chunk.daemonEpoch === initialStream.current.epoch && chunk.seq > initialStream.current.seq}
                commandMatched={commandRangeIncludes(match, chunk)}
                echoLength={echoes.get(chunk.key)?.length ?? 0} echoTone={echoes.get(chunk.key)?.tone}
                chunk={chunk}
                hits={findOpen ? terminalDocument.hitsFor(chunk) : NO_FIND_HITS}
                key={chunk.key}
              />
            ))}
            {compactText(end, chunks.length)}
          </pre>
        )}
      </div>
      {(!follow || selectedCommand) && (
        <button className="return-live" type="button" onClick={() => { manualScroll.current = false; scrollAnchor.current = null; setAnchor({ target: findOpen && activeId ? activeId : '' }); setFollow(true); onClearCommand() }}>
          <ArrowDown size={14} /> 返回最新
        </button>
      )}
    </section>
  )
})

interface FindWidgetProps {
  query: string
  current: number
  total: number
  highlighted?: number
  inputRef?: React.RefObject<HTMLInputElement | null>
  onQuery: (value: string) => void
  onPrevious: () => void
  onNext: () => void
  onClose: () => void
}

export function TerminalFindWidget({
  query,
  current,
  total,
  highlighted = total,
  inputRef,
  onQuery,
  onPrevious,
  onNext,
  onClose
}: FindWidgetProps): React.JSX.Element {
  const boundedHighlightMessage = total > highlighted
    ? `共 ${total} 项；所有匹配均可导航，当前仅高亮附近 ${highlighted} 项以保持流畅`
    : undefined
  const navigate = (event: React.KeyboardEvent<HTMLInputElement>, direction: 1 | -1): void => {
    event.preventDefault()
    event.stopPropagation()
    if (direction > 0) onNext()
    else onPrevious()
  }
  return (
    <aside aria-label="查找串口输出" className="terminal-find-widget" role="search">
      <Search aria-hidden="true" size={14} />
      <input
        aria-label="查找"
        autoComplete="off"
        onChange={(event) => onQuery(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === 'F3') navigate(event, event.shiftKey ? -1 : 1)
          else if (event.key === 'Escape') {
            event.preventDefault()
            event.stopPropagation()
            onClose()
          }
        }}
        placeholder="查找"
        ref={inputRef}
        spellCheck={false}
        value={query}
      />
      <output
        aria-label={boundedHighlightMessage}
        aria-live="polite"
        className="terminal-find-count"
        title={boundedHighlightMessage}
      >{current}/{total}</output>
      <button aria-label="上一个匹配项 (Shift+Enter)" disabled={!total} onClick={onPrevious} type="button"><ChevronUp size={14} /></button>
      <button aria-label="下一个匹配项 (Enter)" disabled={!total} onClick={onNext} type="button"><ChevronDown size={14} /></button>
      <button aria-label="关闭查找 (Escape)" onClick={onClose} type="button"><X size={14} /></button>
    </aside>
  )
}

const TerminalChunkText = memo(function TerminalChunkText({
  chunk,
  hits,
  commandMatched,
  echoLength, echoTone,
  reveal
}: {
  reveal: boolean
  echoLength: number
  echoTone?: EchoStyle['tone']
  chunk: TerminalDocumentChunk
  hits: readonly TerminalFindHit[]
  commandMatched: boolean
}): React.JSX.Element {
  const [animateArrival] = useState(reveal)
  const boundaries = new Set([0, echoLength, chunk.text.length])
  for (const hit of hits) {
    boundaries.add(hit.start)
    boundaries.add(hit.end)
  }
  for (const decoration of chunk.decorations) {
    boundaries.add(decoration.start)
    boundaries.add(decoration.end)
  }
  const points = [...boundaries].sort((a, b) => a - b)
  const parts: React.ReactNode[] = []
  let hitIndex = 0
  let decorationIndex = 0
  for (let index = 0; index < points.length - 1; index += 1) {
    const start = points[index]
    const end = points[index + 1]
    if (end <= start) continue
    const value = chunk.text.slice(start, end)
    while (hits[hitIndex] && hits[hitIndex].end <= start) hitIndex += 1
    const candidateHit = hits[hitIndex]
    const hit = candidateHit?.start <= start && candidateHit.end >= end ? candidateHit : undefined
    if (hit) {
      parts.push(
        <mark
          aria-current={hit.current ? 'true' : undefined}
          className={`term-search-hit ${hit.current ? 'is-current' : 'is-other'}`}
          data-find-id={hit.id}
          key={`${start}:${end}:find`}
        >{value}</mark>
      )
      continue
    }
    if (echoTone && end <= echoLength) {
      parts.push(<span className="command-ink echo-command" data-tone={echoTone} key={start + ':echo'}>{value}</span>)
      continue
    }
    while (chunk.decorations[decorationIndex] && chunk.decorations[decorationIndex].end <= start) decorationIndex += 1
    const candidateDecoration = chunk.decorations[decorationIndex]
    const decoration = candidateDecoration?.start <= start && candidateDecoration.end >= end
      ? candidateDecoration
      : undefined
    parts.push(decoration
      ? <mark className={decoration.className} key={`${start}:${end}:semantic`}>{value}</mark>
      : <Fragment key={`${start}:${end}:text`}>{value}</Fragment>)
  }
  const attributes = { className: commandMatched ? 'matched-output' : undefined, 'data-event-key': chunk.key, 'data-seq': chunk.seq }
  return animateArrival ? <InlineTextEffect {...attributes}>{parts}</InlineTextEffect> : <span {...attributes}>{parts}</span>
})

function commandRangeIncludes(match: MatchRange | undefined, chunk: TerminalDocumentChunk): boolean {
  if (
    !match
    || chunk.daemonEpoch !== match.daemonEpoch
    || chunk.generation !== match.generation
    || chunk.seq < match.fromSeq
    || chunk.seq > match.throughSeq
  ) return false
  if (
    match.fromStreamOffset !== undefined
    && chunk.streamOffsetEnd !== undefined
    && chunk.streamOffsetEnd <= match.fromStreamOffset
  ) return false
  if (
    match.throughStreamOffset !== undefined
    && chunk.streamOffsetStart !== undefined
    && chunk.streamOffsetStart >= match.throughStreamOffset
  ) return false
  return true
}


function selectTerminalWord(event: React.MouseEvent<HTMLElement>): void {
  const selection = window.getSelection()
  const token = (event.target as HTMLElement).closest('mark')
  if (token && selection) {
    const range = document.createRange()
    range.selectNodeContents(token)
    selection.removeAllRanges()
    selection.addRange(range)
    return
  }
  if (selection?.toString()) return
  const range = document.caretRangeFromPoint?.(event.clientX, event.clientY)
  const node = range?.startContainer
  if (!range || !node || node.nodeType !== Node.TEXT_NODE || !selection) return
  const text = node.textContent ?? ''
  const tokenCharacter = /[\p{L}\p{N}_./:@-]/u
  let start = range.startOffset
  let end = range.startOffset
  while (start > 0 && tokenCharacter.test(text[start - 1])) start -= 1
  while (end < text.length && tokenCharacter.test(text[end])) end += 1
  range.setStart(node, start)
  range.setEnd(node, end)
  selection.removeAllRanges()
  selection.addRange(range)
}
