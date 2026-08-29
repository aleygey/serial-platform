import { AlertTriangle, CornerDownLeft, SendHorizontal } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import {
  HUMAN_COMMAND_UNCERTAIN_MESSAGE,
  type HumanCommandSubmission
} from '../../shared/contracts'

interface Props {
  port?: string
  disabled: boolean
  onSend: (command: string) => Promise<HumanCommandSubmission>
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

export function CommandBar({ port, disabled, onSend }: Props): React.JSX.Element {
  const [value, setValue] = useState('')
  const [history, setHistory] = useState<string[]>([])
  const [historyIndex, setHistoryIndex] = useState<number>()
  const [sending, setSending] = useState(false)
  const [uncertainties, setUncertainties] = useState<Record<string, string>>({})
  const inputRef = useRef<HTMLInputElement>(null)
  const sendingRef = useRef(false)
  const portKey = port ?? ''

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
    const command = value.replace(/[\r\n]+$/, '')
    if (!command || disabled || sendingRef.current) return
    sendingRef.current = true
    setSending(true)
    let submission: HumanCommandSubmission
    try {
      submission = await onSend(command)
    } catch {
      submission = { status: 'uncertain', message: HUMAN_COMMAND_UNCERTAIN_MESSAGE }
    } finally {
      sendingRef.current = false
      setSending(false)
    }
    const resolution = resolveCommandSubmission(command, submission)
    if (resolution.uncertainty) {
      setUncertainties((current) => ({ ...current, [portKey]: resolution.uncertainty! }))
    }
    if (resolution.commitToHistory) {
      setHistory((current) => current.at(-1) === command ? current : [...current.slice(-99), command])
    }
    if (submission.status === 'rejected') return
    setHistoryIndex(undefined)
    setValue(resolution.draft)
  }

  return (
    <footer className="command-bar">
      <div className="command-target">
        <span className="target-dot" />
        <span>{port || '未选择串口'}</span>
      </div>
      <input
        aria-label="输入串口命令"
        aria-busy={sending}
        autoComplete="off"
        disabled={disabled}
        onChange={(event) => setValue(event.target.value)}
        onKeyDown={(event) => {
          if (sendingRef.current) {
            if (['Enter', 'ArrowUp', 'ArrowDown'].includes(event.key)) event.preventDefault()
            return
          }
          if (event.key === 'Enter') {
            if (event.nativeEvent.isComposing) return
            event.preventDefault()
            void submit()
          } else if (event.key === 'ArrowUp' && history.length) {
            event.preventDefault()
            const next = historyIndex === undefined ? history.length - 1 : Math.max(0, historyIndex - 1)
            setHistoryIndex(next)
            setValue(history[next])
          } else if (event.key === 'ArrowDown' && historyIndex !== undefined) {
            event.preventDefault()
            const next = historyIndex + 1
            if (next >= history.length) {
              setHistoryIndex(undefined)
              setValue('')
            } else {
              setHistoryIndex(next)
              setValue(history[next])
            }
          }
        }}
        placeholder={disabled ? '打开串口后可发送命令' : '输入命令…'}
        readOnly={sending}
        ref={inputRef}
        value={value}
      />
      <CommandStatusHint sending={sending} uncertainty={uncertainties[portKey]} />
      <button className="send-button" disabled={disabled || sending || !value} onClick={() => void submit()} type="button" title="发送命令">
        <SendHorizontal size={17} />
      </button>
    </footer>
  )
}
