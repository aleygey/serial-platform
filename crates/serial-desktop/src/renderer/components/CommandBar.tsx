import { AlertTriangle, CornerDownLeft, SendHorizontal } from 'lucide-react'
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import {
  HUMAN_COMMAND_UNCERTAIN_MESSAGE,
  type HumanCommandSubmission
} from '../../shared/contracts'
import {
  acceptsSuggestion, commandBuffer, humanHistoryCandidates, stepHistory, suggestedSuffix,
  type CommandBuffer, type HistoryCursor
} from '../lib/command-editor'

interface Props {
  port?: string
  disabled: boolean
  onSend: (command: string) => Promise<HumanCommandSubmission>
  onSignal?: (signal: 'ctrl_c' | 'ctrl_d') => Promise<HumanCommandSubmission>
  humanHistory?: readonly string[]
  drafts?: Map<string, CommandBuffer>
  onQueryHistory?: (query: string, contains: boolean) => Promise<readonly string[]>
}

interface CandidateMenu {
  kind: 'completion' | 'search'
  saved: CommandBuffer
  query: string
  entries: string[]
  index: number
}

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

export function CommandStatusHint({
  sending,
  uncertainty
}: {
  sending: boolean
  uncertainty?: string
}): React.JSX.Element {
  if (uncertainty) {
    return (
      <span aria-label={uncertainty} className="command-hint is-uncertain" role="alert" title={uncertainty}>
        <AlertTriangle size={13} />
        <span>结果不确定：先查看串口时间线，勿直接重发</span>
      </span>
    )
  }
  return <span className="command-hint"><CornerDownLeft size={13} /> {sending ? '发送中…' : 'Enter'}</span>
}

export function CommandBar({ port, disabled, onSend, onSignal, humanHistory = [], drafts, onQueryHistory }: Props): React.JSX.Element {
  const localDrafts = useRef(new Map<string, CommandBuffer>())
  const draftStore = drafts ?? localDrafts.current
  const portKey = port ?? ''
  const [buffer, setBuffer] = useState<CommandBuffer>(() => draftStore.get(portKey) ?? commandBuffer())
  const bufferRef = useRef(buffer)
  const activePort = useRef(portKey)
  const [historyCursor, setHistoryCursor] = useState<HistoryCursor>()
  const [menu, setMenu] = useState<CandidateMenu>()
  const [focused, setFocused] = useState(false)
  const [composing, setComposing] = useState(false)
  const [scrollLeft, setScrollLeft] = useState(0)
  const [remoteSuggestion, setRemoteSuggestion] = useState<{ query: string; entries: readonly string[] }>()
  const [historyWarning, setHistoryWarning] = useState('')
  const queryHistoryRef = useRef(onQueryHistory)
  queryHistoryRef.current = onQueryHistory
  const [sending, setSending] = useState(false)
  const [uncertainties, setUncertainties] = useState<Record<string, string>>({})
  const inputRef = useRef<HTMLInputElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)
  const sendingPorts = useRef(new Set<string>())
  const signalPending = useRef(false)
  const selectionPending = useRef(false)
  const suffix = useMemo(() => focused && !composing && !menu && !sending && !disabled
    ? suggestedSuffix(buffer, remoteSuggestion?.query === buffer.value ? remoteSuggestion.entries : humanHistory) : '', [buffer, humanHistory, remoteSuggestion, focused, composing, menu, sending, disabled])

  const historyQuery = menu?.query ?? buffer.value
  const historyContains = menu?.kind === 'search'
  const queryEnabled = !composing && !disabled && (!!menu || (focused && !!buffer.value))
  useEffect(() => {
    if (!queryEnabled || !queryHistoryRef.current) return
    let active = true
    const timer = setTimeout(() => {
      void queryHistoryRef.current!(historyQuery, historyContains).then((entries) => {
        if (!active) return
        setHistoryWarning('')
        if (!historyContains) setRemoteSuggestion({ query: historyQuery, entries })
        setMenu((current) => current && current.query === historyQuery && (current.kind === 'search') === historyContains
          ? { ...current, entries: [...entries].slice(0, 50), index: Math.min(current.index, Math.max(0, entries.length - 1)) } : current)
      }).catch(() => {
        if (active) setHistoryWarning('完整历史暂不可用，当前显示已加载的历史')
      })
    }, 80)
    return () => { active = false; clearTimeout(timer) }
  }, [historyQuery, historyContains, queryEnabled, portKey])

  useEffect(() => {
    if (menu?.entries.length) document.getElementById(`human-command-option-${menu.index}`)?.scrollIntoView({ block: 'nearest' })
  }, [menu?.index])

  const edit = (next: CommandBuffer, keepNavigation = false): void => {
    bufferRef.current = next
    draftStore.set(activePort.current, next)
    selectionPending.current = true
    setBuffer(next)
    if (!keepNavigation) setHistoryCursor(undefined)
  }

  useLayoutEffect(() => {
    activePort.current = portKey
    const next = draftStore.get(portKey) ?? commandBuffer()
    bufferRef.current = next
    setBuffer(next)
    setHistoryCursor(undefined)
    setMenu(undefined)
    setSending(sendingPorts.current.has(portKey))
    selectionPending.current = true
  }, [portKey, draftStore])

  useLayoutEffect(() => {
    if (!selectionPending.current || !inputRef.current) return
    selectionPending.current = false
    inputRef.current.setSelectionRange(buffer.start, buffer.end)
    setScrollLeft(inputRef.current.scrollLeft)
  }, [buffer])

  useEffect(() => {
    if (menu?.kind === 'search') searchRef.current?.focus()
  }, [menu?.kind])

  useEffect(() => {
    const handler = (event: KeyboardEvent): void => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        if (document.querySelector('[aria-modal="true"]')) return
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const submit = async (): Promise<void> => {
    const target = activePort.current
    const submitted = bufferRef.current
    const command = submitted.value.replace(/[\r\n]+$/, '')
    if (disabled || sendingPorts.current.has(target)) return
    sendingPorts.current.add(target)
    setSending(true)
    let submission: HumanCommandSubmission
    try {
      submission = await onSend(command)
    } catch {
      submission = { status: 'uncertain', message: HUMAN_COMMAND_UNCERTAIN_MESSAGE }
    } finally {
      sendingPorts.current.delete(target)
      if (activePort.current === target) setSending(false)
    }
    const resolution = resolveCommandSubmission(command, submission)
    if (resolution.uncertainty) {
      setUncertainties((current) => ({ ...current, [target]: resolution.uncertainty! }))
    }
    if (submission.status === 'rejected') return
    // A late result from another port must not erase its newer draft or the current editor.
    if (draftStore.get(target) === submitted || (target === activePort.current && bufferRef.current === submitted)) {
      const next = commandBuffer(resolution.draft)
      draftStore.set(target, next)
      if (activePort.current === target) {
        edit(next)
        setMenu(undefined)
      }
    }
  }

  const sendSignal = async (signal: 'ctrl_c' | 'ctrl_d'): Promise<void> => {
    if (disabled || !onSignal || signalPending.current) return
    const target = activePort.current
    signalPending.current = true
    try {
      const result = await onSignal(signal)
      if (result.status === 'uncertain') setUncertainties((current) => ({ ...current, [target]: result.message }))
    } finally {
      signalPending.current = false
    }
  }

  const closeMenu = (accept: boolean): void => {
    if (!menu) return
    edit(accept && menu.entries[menu.index] ? commandBuffer(menu.entries[menu.index]) : menu.saved)
    setMenu(undefined)
    inputRef.current?.focus()
  }

  const menuKey = (event: React.KeyboardEvent): boolean => {
    if (!menu || event.nativeEvent.isComposing) return false
    if (event.key === 'Escape' || ((event.ctrlKey || event.metaKey) && event.key === 'g')) {
      event.preventDefault(); closeMenu(false); return true
    }
    if (event.key === 'Enter') {
      event.preventDefault(); closeMenu(true); return true
    }
    if (['ArrowUp', 'ArrowDown', 'Tab'].includes(event.key) || (event.ctrlKey && event.key === 'r')) {
      event.preventDefault()
      const delta = event.key === 'ArrowUp' || (event.key === 'Tab' && event.shiftKey) ? -1 : 1
      setMenu({ ...menu, index: menu.entries.length ? (menu.index + delta + menu.entries.length) % menu.entries.length : 0 })
      return true
    }
    return false
  }

  return (
    <footer className={`command-bar ${focused ? 'is-focused' : ''}`}>
      <div className="command-target">
        <span className="target-dot" />
        <span>{port || '未选择串口'}</span>
      </div>
      <div className="command-editor">
      <div aria-hidden="true" className="command-ghost"><span style={{ transform: `translateX(${-scrollLeft}px)` }}><span className="command-ghost-prefix">{buffer.value}</span><span>{suffix}</span></span></div>
      <input
        aria-label="输入串口命令"
        aria-autocomplete="both"
        aria-controls={menu ? 'human-command-candidates' : undefined}
        aria-expanded={!!menu}
        aria-activedescendant={menu?.entries.length ? `human-command-option-${menu.index}` : undefined}
        aria-busy={sending}
        autoComplete="off"
        disabled={disabled}
        onChange={(event) => {
          const target = event.target
          edit({ value: target.value, start: target.selectionStart ?? target.value.length, end: target.selectionEnd ?? target.value.length })
          setMenu(undefined)
        }}
        onSelect={(event) => {
          const target = event.currentTarget
          const next = { value: target.value, start: target.selectionStart ?? target.value.length, end: target.selectionEnd ?? target.value.length }
          bufferRef.current = next
          draftStore.set(activePort.current, next)
          setBuffer(next)
        }}
        onScroll={(event) => setScrollLeft(event.currentTarget.scrollLeft)}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        onCompositionStart={() => setComposing(true)}
        onCompositionEnd={() => setComposing(false)}
        onKeyDown={(event) => {
          if (event.nativeEvent.isComposing || composing || event.nativeEvent.keyCode === 229) return
          if (event.ctrlKey && !event.altKey && !event.metaKey && ['c', 'd'].includes(event.key.toLowerCase())) {
            if (event.key.toLowerCase() === 'c' && buffer.start !== buffer.end) return
            event.preventDefault()
            void sendSignal(event.key.toLowerCase() === 'd' ? 'ctrl_d' : 'ctrl_c')
            return
          }
          if (menuKey(event)) return
          if (menu) setMenu(undefined)
          if (sendingPorts.current.has(activePort.current)) {
            if (['Enter', 'ArrowUp', 'ArrowDown'].includes(event.key)) event.preventDefault()
            return
          }
          const current = { value: event.currentTarget.value, start: event.currentTarget.selectionStart ?? buffer.start, end: event.currentTarget.selectionEnd ?? buffer.end }
          if (acceptsSuggestion(event.key, current, suffix, false, event.ctrlKey || event.altKey || event.metaKey || event.shiftKey)) {
            event.preventDefault()
            edit(commandBuffer(current.value + suffix))
          } else if (event.key === 'Enter') {
            event.preventDefault()
            void submit()
          } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'r') {
            event.preventDefault()
            setMenu({ kind: 'search', saved: current, query: '', entries: humanHistoryCandidates(humanHistory, '', true), index: 0 })
          } else if (event.key === 'Tab' && !event.ctrlKey && !event.metaKey) {
            event.preventDefault()
            setMenu({ kind: 'completion', saved: current, query: current.value, entries: humanHistoryCandidates(humanHistory, current.value), index: 0 })
          } else if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
            event.preventDefault()
            const next = stepHistory(current, historyCursor, humanHistory, event.key === 'ArrowUp' ? 'older' : 'newer')
            edit(next.buffer, true)
            setHistoryCursor(next.cursor)
          }
        }}
        placeholder={disabled ? '打开串口后可发送命令' : '输入命令…'}
        readOnly={sending}
        ref={inputRef}
        value={buffer.value}
      />
      {menu && <div className="command-candidates">
        <div className="command-candidates-heading">人工命令历史 <span>↑↓ 选择 · Enter 回填 · Esc 返回</span></div>
        {menu.kind === 'search' && <input aria-label="搜索人工命令历史" ref={searchRef} value={menu.query} onKeyDown={(event) => { menuKey(event) }} onChange={(event) => setMenu({ ...menu, query: event.target.value, entries: humanHistoryCandidates(humanHistory, event.target.value, true), index: 0 })} />}
        <div id="human-command-candidates" role="listbox" aria-label="人工历史候选">
          {menu.entries.map((entry, index) => <button id={`human-command-option-${index}`} aria-selected={index === menu.index} role="option" tabIndex={-1} type="button" key={entry} className={index === menu.index ? 'is-selected' : ''} onMouseDown={(event) => event.preventDefault()} onClick={() => { edit(commandBuffer(entry)); setMenu(undefined); inputRef.current?.focus() }}>{entry}</button>)}
          {!menu.entries.length && <p>没有匹配的人工命令</p>}
        </div>
        {historyWarning && <p role="status">{historyWarning}</p>}
      </div>}
      </div>
      <CommandStatusHint sending={sending} uncertainty={uncertainties[portKey]} />
      <button className="send-button" disabled={disabled || sending} onClick={() => void submit()} type="button" title="发送命令或 Enter">
        <SendHorizontal size={17} />
      </button>
    </footer>
  )
}
