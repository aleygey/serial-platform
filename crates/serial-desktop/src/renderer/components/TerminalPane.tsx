import { ArrowDown, ChevronDown, ChevronUp, Search, TerminalSquare, X } from 'lucide-react'
import { Fragment, memo, useCallback, useEffect, useRef, useState } from 'react'
import type { PortSnapshot, TimelineEvent } from '../../shared/contracts'
import type { AgentCommand, MatchRange } from '../lib/history'
import { displayCommand } from '../lib/history'
import {
  TerminalDocumentIndex,
  type TerminalDocumentChunk,
  type TerminalFindHit
} from '../lib/terminal-find'

interface Props {
  configuredPort?: PortSnapshot
  events: TimelineEvent[]
  selectedCommand?: AgentCommand
  match?: MatchRange
  onClearCommand: () => void
}

const NO_FIND_HITS: readonly TerminalFindHit[] = Object.freeze([])

export function TerminalPane({ configuredPort, events, selectedCommand, match, onClearCommand }: Props): React.JSX.Element {
  const scrollRef = useRef<HTMLDivElement>(null)
  const findInputRef = useRef<HTMLInputElement>(null)
  const previousFocusRef = useRef<HTMLElement | null>(null)
  const documentRef = useRef<TerminalDocumentIndex | null>(null)
  if (!documentRef.current) documentRef.current = new TerminalDocumentIndex()
  const terminalDocument = documentRef.current
  terminalDocument.sync(events)

  const [follow, setFollow] = useState(true)
  const [findOpen, setFindOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [, setNavigationRevision] = useState(0)
  const chunks = terminalDocument.chunks
  const total = terminalDocument.matchCount
  const highlighted = terminalDocument.highlightedMatchCount
  const current = terminalDocument.activeIndex < 0 ? 0 : terminalDocument.activeIndex + 1
  const activeId = terminalDocument.activeId
  const findShortcut = isMacPlatform() ? '⌘F' : 'Ctrl F'

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

  useEffect(() => {
    if (match?.hasVisibleOutput) {
      scrollRef.current?.querySelector(`[data-event-key="${match.daemonEpoch}:${match.fromSeq}"]`)?.scrollIntoView({ block: 'center' })
      setFollow(false)
    }
  }, [match])

  useEffect(() => {
    if (follow) scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight })
  }, [chunks.at(-1)?.key, follow])

  useEffect(() => {
    if (findOpen && activeId) {
      scrollRef.current?.querySelector('.term-search-hit.is-current')?.scrollIntoView({ block: 'center' })
    }
  }, [activeId, findOpen, search])

  useEffect(() => {
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
  }, [closeFind, findOpen, navigate, openFind, search])

  const modelName = configuredPort?.config.model_name || '未配置机型'
  return (
    <section className="terminal-pane">
      <header className="terminal-header">
        <div className="terminal-title">
          <span className="heading-icon"><TerminalSquare size={15} /></span>
          <div>
            <strong className={configuredPort?.config.model_name ? 'model-identity' : undefined}>{modelName}</strong>
            <small>{sessionLabel(configuredPort?.session_state)}</small>
          </div>
        </div>
        <button className="terminal-find-trigger" type="button" aria-label="在串口输出中查找" title="查找 (⌘/Ctrl+F)" onClick={openFind}>
          <Search size={14} />
          <span>查找</span>
          <kbd>{findShortcut}</kbd>
        </button>
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
          <span>设备回显中未匹配，已定位到命令</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      {selectedCommand && match && !match.hasVisibleOutput && (
        <div className="command-overlay">
          <span>该命令的权威记录中没有可显示的设备输出</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      {selectedCommand && match?.inferred && (
        <div className="command-overlay is-inferred">
          <span>旧记录：输出范围由匹配规则推断</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      <div
        className="terminal-scroll"
        ref={scrollRef}
        tabIndex={-1}
        onScroll={(event) => {
          const element = event.currentTarget
          setFollow(element.scrollHeight - element.scrollTop - element.clientHeight < 36)
        }}
      >
        {chunks.length === 0 ? (
          <div className="empty-state terminal-empty">
            <TerminalSquare size={26} />
            <strong>等待设备输出</strong>
            <span>打开串口后，持久记录和实时数据会出现在这里</span>
          </div>
        ) : (
          <pre className="terminal-output" aria-label="串口输出" onDoubleClick={selectTerminalWord}>
            {chunks.map((chunk) => (
              <TerminalChunkText
                commandMatched={commandRangeIncludes(match, chunk)}
                chunk={chunk}
                hits={findOpen ? terminalDocument.hitsFor(chunk) : NO_FIND_HITS}
                key={chunk.key}
              />
            ))}
          </pre>
        )}
      </div>
      {!follow && (
        <button className="return-live" type="button" onClick={() => { setFollow(true); onClearCommand() }}>
          <ArrowDown size={14} /> 返回最新
        </button>
      )}
    </section>
  )
}

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
  commandMatched
}: {
  chunk: TerminalDocumentChunk
  hits: readonly TerminalFindHit[]
  commandMatched: boolean
}): React.JSX.Element {
  const boundaries = new Set([0, chunk.text.length])
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
    while (chunk.decorations[decorationIndex] && chunk.decorations[decorationIndex].end <= start) decorationIndex += 1
    const candidateDecoration = chunk.decorations[decorationIndex]
    const decoration = candidateDecoration?.start <= start && candidateDecoration.end >= end
      ? candidateDecoration
      : undefined
    parts.push(decoration
      ? <mark className={decoration.className} key={`${start}:${end}:semantic`}>{value}</mark>
      : <Fragment key={`${start}:${end}:text`}>{value}</Fragment>)
  }
  return (
    <span className={commandMatched ? 'matched-output' : undefined} data-event-key={chunk.key} data-seq={chunk.seq}>
      {parts}
    </span>
  )
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

function isMacPlatform(): boolean {
  return typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform)
}

function sessionLabel(state: PortSnapshot['session_state'] | undefined): string {
  if (state === 'online') return '设备在线'
  if (state === 'opening') return '正在打开串口'
  if (state === 'waiting_for_port') return '等待设备接入'
  if (state === 'backoff') return '等待重新连接'
  if (state === 'stopping') return '正在关闭串口'
  return '串口未打开'
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
