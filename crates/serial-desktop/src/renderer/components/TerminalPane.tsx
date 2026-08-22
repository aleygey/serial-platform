import { ArrowDown, Search, TerminalSquare, X } from 'lucide-react'
import { Fragment, useEffect, useMemo, useRef, useState } from 'react'
import type { PortSnapshot, TimelineEvent } from '../../shared/contracts'
import type { AgentCommand, MatchRange } from '../lib/history'
import { displayCommand } from '../lib/history'

interface Props {
  configuredPort?: PortSnapshot
  events: TimelineEvent[]
  selectedCommand?: AgentCommand
  match?: MatchRange
  onClearCommand: () => void
}

export function TerminalPane({ configuredPort, events, selectedCommand, match, onClearCommand }: Props): React.JSX.Element {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [follow, setFollow] = useState(true)
  const [search, setSearch] = useState('')
  const rxEvents = useMemo(() => events.filter((event) => event.direction === 'rx'), [events])

  useEffect(() => {
    if (match) {
      scrollRef.current?.querySelector(`[data-seq="${match.fromSeq}"]`)?.scrollIntoView({ block: 'center' })
      setFollow(false)
    }
  }, [match])

  useEffect(() => {
    if (follow) scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight })
  }, [rxEvents.length, follow])

  useEffect(() => {
    const handler = (event: KeyboardEvent): void => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'f') {
        event.preventDefault()
        document.querySelector<HTMLInputElement>('#terminal-search')?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const modelName = configuredPort?.config.model_name || configuredPort?.config.model_profile || '未配置机型'
  return (
    <section className="terminal-pane">
      <header className="terminal-header">
        <div className="terminal-title">
          <span className="heading-icon"><TerminalSquare size={15} /></span>
          <div>
            <strong>{modelName}</strong>
            <small>{sessionLabel(configuredPort?.session_state)}</small>
          </div>
        </div>
        <label className="terminal-search">
          <Search size={14} />
          <input id="terminal-search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索串口历史" />
          {search && <button type="button" onClick={() => setSearch('')}><X size={13} /></button>}
          <kbd>⌘F</kbd>
        </label>
      </header>
      {selectedCommand && !match && (
        <div className="command-overlay">
          <span>设备回显中未匹配，已定位到命令</span>
          <code>{displayCommand(selectedCommand.text)}</code>
          <button type="button" onClick={onClearCommand}><X size={14} /></button>
        </div>
      )}
      <div
        className="terminal-scroll"
        ref={scrollRef}
        onScroll={(event) => {
          const element = event.currentTarget
          setFollow(element.scrollHeight - element.scrollTop - element.clientHeight < 36)
        }}
      >
        {rxEvents.length === 0 ? (
          <div className="empty-state terminal-empty">
            <TerminalSquare size={26} />
            <strong>等待设备输出</strong>
            <span>打开串口后，持久记录和实时数据会出现在这里</span>
          </div>
        ) : (
          <pre className="terminal-output" aria-label="串口输出" onDoubleClick={selectTerminalWord}>
            {rxEvents.map((event) => (
              <span
                className={match && event.seq >= match.fromSeq && event.seq <= match.throughSeq ? 'matched-output' : undefined}
                data-seq={event.seq}
                key={`${event.daemon_epoch}:${event.seq}`}
              >
                <TerminalText text={safeText(event.text)} search={search} />
              </span>
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

function TerminalText({ text, search }: { text: string; search: string }): React.JSX.Element {
  const pattern = useMemo(() => {
    const searchPattern = search ? escapeRegex(search) : '(?!)'
    return new RegExp(`(${searchPattern}|(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}|\\b(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)(?:\\.(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)){3}\\b|(?<![A-Za-z0-9_])(?:error|failed|fatal)(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])(?:success|passed|ready)(?![A-Za-z0-9_])|(?<![A-Za-z0-9_])(?:warning|warn)(?![A-Za-z0-9_]))`, 'gi')
  }, [search])
  const parts: React.ReactNode[] = []
  let cursor = 0
  for (const match of text.matchAll(pattern)) {
    const index = match.index
    if (index > cursor) parts.push(text.slice(cursor, index))
    const value = match[0]
    const lowered = value.toLowerCase()
    const className = search && lowered.includes(search.toLowerCase())
      ? 'term-search-hit'
      : /^(error|failed|fatal)$/i.test(value)
        ? 'term-error'
        : /^(success|passed|ready)$/i.test(value)
          ? 'term-success'
          : /^(warning|warn)$/i.test(value)
            ? 'term-warning'
            : 'term-address'
    parts.push(<mark className={className} key={`${index}:${value}`}>{value}</mark>)
    cursor = index + value.length
  }
  if (cursor < text.length) parts.push(text.slice(cursor))
  return <Fragment>{parts}</Fragment>
}

function safeText(text: string): string {
  return text
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, '')
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '�')
    .replace(/\r/g, '')
}

function sessionLabel(state: PortSnapshot['session_state'] | undefined): string {
  if (state === 'online') return '设备在线'
  if (state === 'opening') return '正在打开串口'
  if (state === 'waiting_for_port') return '等待设备接入'
  if (state === 'backoff') return '等待重新连接'
  if (state === 'stopping') return '正在关闭串口'
  return '串口未打开'
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
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
