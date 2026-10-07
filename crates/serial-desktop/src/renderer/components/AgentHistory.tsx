import { ChevronDown, ChevronRight, Circle, ListTree } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import type { AgentCommand, AgentHistoryItem } from '../lib/history'
import { displayCommand } from '../lib/history'

interface Props {
  items: AgentHistoryItem[]
  selectedCommand?: AgentCommand
  onSelect: (command: AgentCommand) => void
  onExpandRun: (runId: string) => Promise<void>
  onClear: (runIds?: string[]) => Promise<void>
}

export function AgentHistory({ items, selectedCommand, onSelect, onExpandRun, onClear }: Props): React.JSX.Element {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const followRef = useRef(true)
  const [selectedRun, setSelectedRun] = useState<string>()
  const [pendingDelete, setPendingDelete] = useState<{ id: string; at: number }>()
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState('')

  const clear = async (id: string): Promise<void> => {
    if (busy) return
    if (items.some((item) => item.kind === 'run' && item.id === `run:${id}` && item.status === 'running')) {
      setNotice('正在执行的 Run 不能清理。'); return
    }
    if (pendingDelete?.id !== id || Date.now() - pendingDelete.at >= 10000) {
      setPendingDelete({ id, at: Date.now() })
      const run = items.find((item) => item.kind === 'run' && item.id === `run:${id}`)
      setNotice(`再次确认清理${id === '*' ? '当前串口全部已结束 Run' : `整轮「${run?.kind === 'run' ? run.label : id}」`}；原始日志保留。`)
      return
    }
    setPendingDelete(undefined)
    setBusy(true)
    try { await onClear(id === '*' ? undefined : [id]); setNotice('已清理；原始日志保留。') }
    catch (error) { setNotice(error instanceof Error ? error.message : String(error)) }
    finally { setBusy(false) }
  }

  useEffect(() => {
    if (!pendingDelete) return
    const timer = setTimeout(() => { setPendingDelete(undefined); setNotice('') }, 10000)
    return () => clearTimeout(timer)
  }, [pendingDelete])

  useEffect(() => {
    if (followRef.current) scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight })
  }, [items.length])

  return (
    <aside className="agent-pane" onBlur={(event) => {
      if (!event.currentTarget.contains(event.relatedTarget)) { setPendingDelete(undefined); setNotice('') }
    }} onKeyDown={(event) => {
      if (event.key === 'Delete' && (selectedRun || event.ctrlKey)) {
        event.preventDefault(); void clear(event.ctrlKey ? '*' : selectedRun!)
      } else if (event.key !== 'Delete') setPendingDelete(undefined)
    }}>
      <header className="pane-heading">
        <span className="heading-icon"><ListTree size={15} /></span>
        <div>
          <strong>Agent 任务与命令</strong>
          <small>从旧到新 · {items.length} 条记录</small>
        </div>
        <button type="button" disabled={busy} onClick={() => void clear('*')} title="清空当前串口全部已结束的 Agent Run；原始串口日志保留">清空已结束</button>
      </header>
      {notice && <p role="status" className="history-notice">{notice}</p>}
      <div
        className="agent-scroll"
        ref={scrollRef}
        onScroll={(event) => {
          const element = event.currentTarget
          followRef.current = element.scrollHeight - element.scrollTop - element.clientHeight < 48
        }}
      >
        {items.length === 0 && (
          <div className="empty-state compact">
            <Circle size={18} />
            <span>Agent 命令会显示在这里</span>
          </div>
        )}
        {items.filter((item) => item.kind === 'run' || !item.runId || expanded.has(`run:${item.runId}`)).map((item, index) => item.kind === 'run' ? (
          <div className="run-row" key={item.id} title={runStatus(item.status)} aria-label={`${item.label} · ${runStatus(item.status)}`}>
            <span className={`run-mark is-${item.status}`} />
            <button type="button" className="history-summary" aria-expanded={expanded.has(item.id)} onClick={() => {
              setPendingDelete(undefined)
              setSelectedRun(item.id.slice(4))
              setExpanded((current) => toggle(current, item.id))
              if (!expanded.has(item.id)) void onExpandRun(item.id.slice(4))
            }}>
              {expanded.has(item.id) ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
              <strong>{item.label}</strong>
            </button>
            <button type="button" disabled={busy || item.status === 'running'} title="删除整轮 Run 的面板历史；再次确认后生效" onClick={() => { setSelectedRun(item.id.slice(4)); void clear(item.id.slice(4)) }}>删除</button>
          </div>
        ) : (
          <div className={`history-card ${expanded.has(item.id) ? 'is-expanded' : ''}`} key={item.id}>
            <button
              className="history-summary"
              type="button"
              onClick={() => {
                setPendingDelete(undefined)
                setSelectedRun(item.runId)
                setExpanded((current) => toggle(current, item.id))
                if (item.commands[0]) onSelect(item.commands[0])
              }}
            >
              <span className="history-index">{index + 1}</span>
              <span className="history-title">
                <strong>{item.description}</strong>
                <small>{item.commands.length > 1 ? `${item.commands.length} 条连续命令` : displayCommand(item.commands[0]?.text ?? '')}</small>
              </span>
              {expanded.has(item.id) ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
            </button>
            {expanded.has(item.id) && (
              <div className="command-steps">
                {item.commands.map((command, commandIndex) => (
                  <button
                    className={`command-step ${
                      selectedCommand?.id === command.id
                      && selectedCommand.firstSeq === command.firstSeq
                      && selectedCommand.daemonEpoch === command.daemonEpoch
                      && selectedCommand.generation === command.generation
                        ? 'is-selected'
                        : ''
                    }`}
                    key={`${command.id}:${command.firstSeq}`}
                    onClick={() => { setPendingDelete(undefined); setSelectedRun(item.runId); onSelect(command) }}
                    type="button"
                  >
                    <span>{commandIndex + 1}.</span>
                    <code>{displayCommand(command.text)}</code>
                  </button>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </aside>
  )
}

function toggle(current: Set<string>, id: string): Set<string> {
  const next = new Set(current)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  return next
}

function runStatus(status: Extract<AgentHistoryItem, { kind: 'run' }>['status']): string {
  return status === 'running' ? '执行中' : status === 'completed' ? '已完成' : '已中止'
}
