import { History, LoaderCircle, Play, X } from 'lucide-react'
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import type { HumanCommandSubmission } from '../../shared/contracts'
import { PromptBox, DIRECT_SEND } from './ui/prompt-box'
import { ScrollProgress } from './ui/scroll-progress'
import { PortInput, type CommandReceipt } from '../lib/port-input'
export { resolveCommandSubmission } from '../lib/port-input'
import {
  acceptsSuggestion, commandBuffer, humanHistoryCandidates, stepHistory, suggestedSuffix,
  type CommandBuffer, type HistoryCursor
} from '../lib/command-editor'

interface Props {
  port?: string
  disabled: boolean
  onSend: (command: string, keyTime?: number) => Promise<HumanCommandSubmission>
  onSignal?: (signal: 'ctrl_c' | 'ctrl_d') => Promise<HumanCommandSubmission>
  humanHistory?: readonly string[]
  session: PortInput
  active: boolean
  paused?: boolean
  placeholder: string
  onQueryHistory?: (query: string, contains: boolean) => Promise<readonly string[]>
}

interface CandidateMenu {
  kind: 'completion' | 'search'
  saved: CommandBuffer
  query: string
  entries: string[]
  index: number
}

export function CommandStatusHint({
  sending,
  uncertainty,
  receipt
}: {
  sending: boolean
  uncertainty?: string
  receipt?: CommandReceipt
}): React.JSX.Element {
  if (uncertainty) return <span className="sr-only" role="alert">{uncertainty}</span>
  const label = sending ? '发送中' : receipt?.status === 'accepted' ? '已发送' : receipt?.status === 'rejected' ? '被拒绝' : ''
  return <span className="sr-only" role="status">{label}</span>
}

export function CommandBar({ port, disabled, onSend, onSignal, humanHistory = [], session, active, paused, placeholder, onQueryHistory }: Props): React.JSX.Element {
  const menuId = useId()
  const { buffer, pending: sending, receipts } = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot)
  const [historyCursor, setHistoryCursor] = useState<HistoryCursor>()
  const [menu, setMenu] = useState<CandidateMenu>()
  const [focused, setFocused] = useState(false)
  const [composing, setComposing] = useState(false)
  const [scrollLeft, setScrollLeft] = useState(0)
  const [remoteSuggestion, setRemoteSuggestion] = useState<{ query: string; entries: readonly string[] }>()
  const [historyWarning, setHistoryWarning] = useState('')
  const queryHistoryRef = useRef(onQueryHistory)
  queryHistoryRef.current = onQueryHistory
  const inputRef = useRef<HTMLInputElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const formRef = useRef<HTMLFormElement>(null)
  const menuOpen = !!menu
  useEffect(() => {
    if (!menuOpen) return
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Node && !formRef.current?.contains(event.target)) setMenu(undefined)
    }
    document.addEventListener('pointerdown', outside)
    return () => document.removeEventListener('pointerdown', outside)
  }, [menuOpen])
  useEffect(() => {
    if (disabled || !active) setMenu(undefined)
  }, [disabled, active])
  const selectionPending = useRef(true)
  const suffix = useMemo(() => focused && !composing && !menu && !disabled
    ? suggestedSuffix(buffer, remoteSuggestion?.query === buffer.value ? remoteSuggestion.entries : humanHistory) : '', [buffer, humanHistory, remoteSuggestion, focused, composing, menu, disabled])

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
  }, [historyQuery, historyContains, queryEnabled, port])

  useEffect(() => {
    if (menu?.entries.length) document.getElementById(`${menuId}-${menu.index}`)?.scrollIntoView({ block: 'nearest' })
  }, [menu?.index, menuId])

  const edit = (next: CommandBuffer, keepNavigation = false): void => {
    selectionPending.current = true
    session.edit(next)
    if (!keepNavigation) setHistoryCursor(undefined)
  }

  useLayoutEffect(() => {
    if (!selectionPending.current || !inputRef.current) return
    selectionPending.current = false
    if (inputRef.current.selectionStart !== buffer.start || inputRef.current.selectionEnd !== buffer.end) {
      inputRef.current.setSelectionRange(buffer.start, buffer.end)
    }
    setScrollLeft(inputRef.current.scrollLeft)
  }, [buffer])

  useEffect(() => {
    if (menu?.kind === 'search') searchRef.current?.focus()
  }, [menu?.kind])

  useEffect(() => {
    if (!active) return
    const handler = (event: KeyboardEvent): void => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        if (document.querySelector('[aria-modal="true"]')) return
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [active])

  const receipt = !buffer.value || receipts[0]?.command === buffer.value ? receipts[0] : undefined
  const sendTone = sending ? 'working' : receipt?.status === 'rejected' ? 'failed' : receipt?.status === 'uncertain' ? 'waiting' : receipt?.status === 'accepted' ? 'done' : undefined

  const submit = async (keyTime?: number): Promise<void> => {
    if (disabled) return
    const command = session.snapshot().buffer.value.replace(/[\r\n]+$/, '')
    setMenu(undefined)
    await session.submit(command, () => onSend(command, keyTime), true)
  }

  const sendSignal = async (signal: 'ctrl_c' | 'ctrl_d'): Promise<void> => {
    if (disabled || !onSignal) return
    await session.submit(signal === 'ctrl_c' ? 'Ctrl+C' : 'Ctrl+D', () => onSignal(signal), false)
  }

  const closeMenu = (accept: boolean): void => {
    if (!menu) return
    edit(accept && menu.entries[menu.index] ? commandBuffer(menu.entries[menu.index]) : menu.saved)
    setMenu(undefined)
    inputRef.current?.focus()
  }

  const openHistory = (current: CommandBuffer): void => {
    setMenu({ kind: 'search', saved: current, query: '', entries: humanHistoryCandidates(humanHistory, '', true), index: 0 })
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

  const editor = (
    <div className="command-field" data-slot="prompt-box-field"><span aria-hidden className="serial-prompt-sign">›</span><div className="command-editor">
      <div aria-hidden="true" className="command-ghost"><span style={{ transform: `translateX(${-scrollLeft}px)` }}><span className="command-ghost-prefix">{buffer.value}</span><span>{suffix}</span></span></div>
      <input
        data-slot="serial-command-input"
        aria-label="输入串口命令"
        role="combobox"
        aria-autocomplete="both"
        aria-controls={menu ? menuId : undefined}
        aria-expanded={!!menu}
        aria-activedescendant={menu?.entries.length ? `${menuId}-${menu.index}` : undefined}
        aria-busy={sending}
        autoComplete="off"
        disabled={disabled}
        onChange={(event) => {
          const target = event.target
          session.edit({ value: target.value, start: target.selectionStart ?? target.value.length, end: target.selectionEnd ?? target.value.length })
          setHistoryCursor(undefined)
          setMenu(undefined)
        }}
        onSelect={(event) => {
          const target = event.currentTarget
          const next = { value: target.value, start: target.selectionStart ?? target.value.length, end: target.selectionEnd ?? target.value.length }
          session.edit(next)
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
          const current = { value: event.currentTarget.value, start: event.currentTarget.selectionStart ?? buffer.start, end: event.currentTarget.selectionEnd ?? buffer.end }
          if (acceptsSuggestion(event.key, current, suffix, false, event.ctrlKey || event.altKey || event.metaKey || event.shiftKey)) {
            event.preventDefault()
            edit(commandBuffer(current.value + suffix))
          } else if (event.key === 'Enter') {
            event.preventDefault()
            void submit(event.timeStamp)
          } else if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'r') {
            event.preventDefault()
            openHistory(current)
          } else if (event.key === 'Tab' && !event.ctrlKey && !event.metaKey && !event.shiftKey && !!current.value) {
            event.preventDefault()
            setMenu({ kind: 'completion', saved: current, query: current.value, entries: humanHistoryCandidates(humanHistory, current.value), index: 0 })
          } else if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
            event.preventDefault()
            const next = stepHistory(current, historyCursor, humanHistory, event.key === 'ArrowUp' ? 'older' : 'newer')
            edit(next.buffer, true)
            setHistoryCursor(next.cursor)
          }
        }}
        placeholder={placeholder}
        ref={inputRef}
        value={buffer.value}
      />
      {menu && <div className="command-candidates">
        <div className="command-candidates-heading">人工命令历史<button type="button" aria-label="收起人工命令候选" title="收起 (Esc)" onClick={() => closeMenu(false)}><X size={12} /></button></div>
        {menu.kind === 'search' && <input aria-label="搜索人工命令历史" ref={searchRef} value={menu.query} onKeyDown={(event) => { menuKey(event) }} onChange={(event) => setMenu({ ...menu, query: event.target.value, entries: humanHistoryCandidates(humanHistory, event.target.value, true), index: 0 })} />}
        <ScrollProgress containerRef={listRef} className="candidate-progress" />
        <div id={menuId} ref={listRef} role="listbox" aria-label="人工历史候选">
          {menu.entries.map((entry, index) => <button id={`${menuId}-${index}`} aria-selected={index === menu.index} role="option" tabIndex={-1} type="button" key={entry} className={index === menu.index ? 'is-selected' : ''} onMouseDown={(event) => event.preventDefault()} onClick={() => { edit(commandBuffer(entry)); setMenu(undefined); inputRef.current?.focus() }}>{entry}</button>)}
          {!menu.entries.length && <p>没有匹配的人工命令</p>}
        </div>
        {historyWarning && <p role="status">{historyWarning}</p>}
      </div>}
      </div></div>
  )

  return (
    <form ref={formRef} onBlur={event => { if (event.relatedTarget && !event.currentTarget.contains(event.relatedTarget)) setMenu(undefined) }} className={`command-bar ${focused ? 'is-focused' : ''}`} aria-label={`${port ?? ''} 串口命令栏`}
      onSubmit={event => { event.preventDefault(); void submit() }}>
      <PromptBox className="serial-prompt" data-tone={sendTone} value={buffer.value} editor={editor}
        actions={<button className="input-history" type="button" aria-label="查找人工命令历史" title="查找人工命令历史 (Ctrl+R)" disabled={disabled}
          aria-expanded={menu?.kind === 'search'} onClick={() => menu?.kind === 'search' ? closeMenu(false) : openHistory(buffer)}><History size={15} aria-hidden /></button>}
        efforts={DIRECT_SEND} status={sending ? 'sending' : 'idle'} disabled={disabled} paused={paused}
        hint={null} placeholder={placeholder} clearOnSubmit={false} sendLabel="Run" buttonClassName="send-button"
        buttonTitle={receipt?.message || '向当前串口发送命令 (Enter)'}
        submitIcon={sending ? <LoaderCircle className="is-spinning" size={12} /> : <Play size={12} />}
        onSubmit={() => void submit()} feedback={<CommandStatusHint sending={sending} receipt={receipt}
          uncertainty={receipts[0]?.status === 'uncertain' ? receipts[0].message : undefined} />} />
    </form>
  )
}
