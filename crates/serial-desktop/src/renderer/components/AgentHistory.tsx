import { ChevronDown, ChevronRight, Circle, ListTree } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import type { AgentCommand, AgentHistoryItem } from '../lib/history'
import { displayCommand } from '../lib/history'

interface Props {
  items: AgentHistoryItem[]
  selectedCommand?: AgentCommand
  onSelect: (command: AgentCommand) => void
}

export function AgentHistory({ items, selectedCommand, onSelect }: Props): React.JSX.Element {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const followRef = useRef(true)

  useEffect(() => {
    if (followRef.current) scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight })
  }, [items.length])

  return (
    <aside className="agent-pane">
      <header className="pane-heading">
        <span className="heading-icon"><ListTree size={15} /></span>
        <div>
          <strong>Agent 任务与命令</strong>
          <small>从旧到新 · {items.length} 条记录</small>
        </div>
      </header>
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
        {items.map((item, index) => item.kind === 'run' ? (
          <div className="run-row" key={item.id}>
            <span className={`run-mark is-${item.status}`} />
            <div>
              <strong>{item.label}</strong>
              <small>{runStatus(item.status)}</small>
            </div>
          </div>
        ) : (
          <div className={`history-card ${expanded.has(item.id) ? 'is-expanded' : ''}`} key={item.id}>
            <button
              className="history-summary"
              type="button"
              onClick={() => {
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
                    className={`command-step ${selectedCommand?.id === command.id && selectedCommand.firstSeq === command.firstSeq ? 'is-selected' : ''}`}
                    key={`${command.id}:${command.firstSeq}`}
                    onClick={() => onSelect(command)}
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
