import { CornerDownLeft, SendHorizontal } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

interface Props {
  port?: string
  disabled: boolean
  onSend: (command: string) => Promise<void>
}

export function CommandBar({ port, disabled, onSend }: Props): React.JSX.Element {
  const [value, setValue] = useState('')
  const [history, setHistory] = useState<string[]>([])
  const [historyIndex, setHistoryIndex] = useState<number>()
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    const handler = (event: KeyboardEvent): void => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const submit = async (): Promise<void> => {
    const command = value.replace(/[\r\n]+$/, '')
    if (!command || disabled) return
    await onSend(command)
    setHistory((current) => current.at(-1) === command ? current : [...current.slice(-99), command])
    setHistoryIndex(undefined)
    setValue('')
  }

  return (
    <footer className="command-bar">
      <div className="command-target">
        <span className="target-dot" />
        <span>{port || '未选择串口'}</span>
      </div>
      <input
        aria-label="输入串口命令"
        autoComplete="off"
        disabled={disabled}
        onChange={(event) => setValue(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') {
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
        ref={inputRef}
        value={value}
      />
      <span className="command-hint"><CornerDownLeft size={13} /> Enter</span>
      <button className="send-button" disabled={disabled || !value} onClick={() => void submit()} type="button" title="发送命令">
        <SendHorizontal size={17} />
      </button>
    </footer>
  )
}
