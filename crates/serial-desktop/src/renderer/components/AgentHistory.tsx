import { Trash2 } from 'lucide-react'
import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { useReducedMotion } from 'motion/react'
import { Disclosure, DisclosureContent } from './ui/disclosure'
import { RailToc } from './ui/rail-toc'
import { StepPlayer } from './ui/step-player'
import { TextEffect } from './ui/text-effect'
import type { AgentCommand, AgentHistoryItem } from '../lib/history'
import { commandState, type ExecutionState } from '../lib/console-workspace'
import { displayCommand } from '../lib/history'

export interface AgentHistoryView {
  expanded?: boolean
  rows: Record<string, boolean>
}

interface Props {
  view: AgentHistoryView
  visible: boolean
  items: AgentHistoryItem[]
  paused?: boolean
  currentActionId?: string
  activeRunId?: string
  currentCommand?: AgentCommand
  selectedCommand?: AgentCommand
  onSelect: (command: AgentCommand) => void
  onExpandRun: (runId: string) => Promise<void>
  onClear: (runIds?: string[]) => Promise<void>
}

export function AgentHistory({ view, visible, items, paused, activeRunId, currentActionId, currentCommand, selectedCommand, onSelect, onExpandRun, onClear }: Props): React.JSX.Element {
  const scope = useId()
  const scroll = useRef<HTMLDivElement>(null)
  const follow = useRef(true)
  const [openRows, setOpenRows] = useState(view.rows)
  const setRow = useCallback((id: string, open: boolean) => {
    view.rows = { ...view.rows, [id]: open }
    setOpenRows(view.rows)
  }, [view])
  const [focusedRow, setFocusedRow] = useState<string>()
  const runs = items.filter(item => item.kind === 'run')
  const latestRun = activeRunId ? 'run:' + activeRunId : runs.at(-1)?.id
  useEffect(() => {
    if (latestRun && !(latestRun in view.rows)) setRow(latestRun, true)
  }, [latestRun, setRow, view])
  useEffect(() => {
    if (!visible) return
    for (const [id, open] of Object.entries(openRows)) {
      if (open && id.startsWith('run:')) void onExpandRun(id.slice(4))
    }
  }, [openRows, visible, onExpandRun])
  const runKeys = new Set(runs.map(run => run.id))
  const actions = items.filter(item => item.kind === 'command')
  const currentGroup = actions.find(action => action.id === currentActionId)
  const liveNode = currentGroup && currentCommand && (openRows[currentGroup.id] ?? true)
    ? `${currentGroup.id}/${currentCommand.firstSeq}` : currentGroup?.id ?? (activeRunId ? 'start:' + activeRunId : undefined)
  const reduced = useReducedMotion() || paused
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
    if (visible) follow.current = true
  }, [visible])

  const revealCursor = useCallback((list: HTMLElement, y: number, belowCursor: number) => {
    const viewport = scroll.current
    if (!viewport || !follow.current) return
    const cursor = list.getBoundingClientRect().top + y
    const box = viewport.getBoundingClientRect()
    const bottom = box.bottom - Math.min(Math.max(28, belowCursor + 6), Math.max(0, box.height - 12))
    if (cursor > bottom) viewport.scrollTop += Math.ceil(cursor - bottom)
    else if (cursor < box.top + 12) viewport.scrollTop -= Math.ceil(box.top + 12 - cursor)
  }, [])

  const toggle = (id: string, open: boolean, runId?: string) => {
    follow.current = false
    setPendingDelete(undefined)
    setSelectedRun(runId)
    setFocusedRow(id)
    setRow(id, !open)
  }

  function actionRow(action: Extract<AgentHistoryItem, { kind: 'command' }>) {
    const open = openRows[action.id] ?? true
    const isMacro = action.actionKind === 'macro'
    const multi = isMacro || action.actionKind === 'trigger' || action.actionKind === 'sequence' || action.commands.length > 1
    const marks = action.commands.map((command, index) => ({
      index: command.stepIndex ?? index, ...commandState(command, command === currentCommand),
    }))
    const last = marks.at(-1)
    const stepLabel = isMacro || action.actionKind === 'trigger' ? `已记录 ${action.commands.length} 条命令` : action.stepCount
      ? `已记录 ${action.commands.length} / ${action.stepCount} 步` : `已记录 ${action.commands.length} 步`
    const macroStatus = action.macro && macroExecutionLabel(action.macro)
    let state: ExecutionState | undefined = macroStatus ?? last
    if (action.triggerStatus) state = triggerState(action.triggerStatus)
    if (action.actionKind === 'sequence') {
      const issue = marks.find(mark => mark.tone === 'failed' || mark.tone === 'waiting')
      if (issue) state = issue
      else if (action.stepCount && action.commands.length < action.stepCount) {
        state = action.runId && action.runId === activeRunId ? { tone: 'working', label: '序列进行中' } : { tone: 'idle', label: '部分步骤已记录' }
      } else if (action.stepCount && marks.every(mark => mark.tone === 'done')) {
        state = { tone: 'done', label: '全部已回执' }
      }
    }
    const name = action.description.replace(/^Macro · /, '')
    const detail = [name, state?.label, multi && stepLabel].filter(Boolean).join(' · ')
    return <div className="outline-action" key={action.id}>
      <button className="outline-trigger action-trigger" type="button" data-rail-node={action.id} data-depth="0"
        title={detail} aria-label={detail} aria-expanded={open} aria-controls={scope + action.id} onClick={() => toggle(action.id, open, action.runId)}>
        <span className="outline-label"><strong className="command-ink" data-tone={state?.tone}>{name}</strong></span>
        {multi && <StepPlayer steps={marks} total={action.stepCount} label={stepLabel} paused={paused} />}
      </button>
      <OutlineBranch id={scope + action.id} open={open} reduced={!!reduced}>
        {action.commands.map(command => {
          const id = `${action.id}/${command.firstSeq}`
          const active = selectedCommand?.daemonEpoch === command.daemonEpoch && selectedCommand?.generation === command.generation && selectedCommand.firstSeq === command.firstSeq
          const value = displayCommand(command.text) || 'Enter'
          const state = commandState(command, command === currentCommand)
          const detail = value + ' · ' + state.label
          return <button key={id} data-current-command={command === currentCommand} aria-label={detail} title={detail} data-rail-node={id} data-depth="1" type="button" className={`command-step ${active ? 'is-selected' : ''}`}
            aria-current={active ? 'location' : undefined} onClick={() => { follow.current = false; setSelectedRun(action.runId); setFocusedRow(id); onSelect(command) }}>
            <code className="command-ink" data-tone={state.tone}>{command === currentCommand && !reduced ? <TextEffect as="span" per="line" preset="fade">{value}</TextEffect> : value}</code>
          </button>
        })}
        {!action.commands.length && <small className="outline-empty">暂无命令</small>}
      </OutlineBranch>
    </div>
  }

  return (
    <aside className="agent-pane" data-has-run={runs.length > 0} onBlur={(event) => {
      if (!event.currentTarget.contains(event.relatedTarget)) { setPendingDelete(undefined); setNotice('') }
    }} onKeyDown={(event) => {
      if (event.key === 'Delete' && (selectedRun || event.ctrlKey)) {
        event.preventDefault(); void clear(event.ctrlKey ? '*' : selectedRun!)
      } else if (event.key !== 'Delete') setPendingDelete(undefined)
    }}>
      <div className="history-heading"><strong>Agent 执行历史</strong>
        <button className="history-clear" type="button" disabled={busy || !runs.some(run => run.status !== 'running')}
          onClick={() => void clear('*')} aria-label="清空已结束的 Agent Run" title="清理已结束的 Run"><Trash2 size={13} /></button>
      </div>
      {notice && <p role="status" className="history-notice">{notice}</p>}
      <div className="agent-scroll" ref={scroll} onWheel={() => { follow.current = false }} onTouchStart={() => { follow.current = false }}
        onKeyDown={event => { if (['ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', 'Home', 'End'].includes(event.key)) follow.current = false }} tabIndex={0} role="region" aria-label="命令历史">
        {!items.length && <div className="empty-state compact">暂无 Agent 执行记录
        </div>}
        {runs.map(run => {
          const id = run.id.slice(4)
          const open = openRows[run.id] ?? run.id === latestRun
          const groups = actions.filter(action => action.runId === id)
          const focusGroup = groups.find(action => focusedRow === action.id || focusedRow?.startsWith(action.id + '/'))
          const lastGroup = groups.at(-1)
          const lastCommand = lastGroup?.commands.at(-1)
          const historicalNode = focusGroup ? (openRows[focusGroup.id] !== false ? focusedRow : focusGroup.id)
            : lastGroup && lastCommand && openRows[lastGroup.id] !== false ? `${lastGroup.id}/${lastCommand.firstSeq}` : lastGroup?.id
          return <section className="run-block" key={run.id} aria-label={run.label}>
            <div className="run-row">
              <button type="button" className="outline-trigger run-trigger" title={run.label + ' · ' + runStatus(run.status)} aria-label={run.label + ' · ' + runStatus(run.status)} aria-expanded={open} aria-controls={scope + run.id} onClick={() => toggle(run.id, open, id)}>
                <span className="outline-label"><strong className="command-ink" data-tone={run.status === 'running' ? 'working' : run.status === 'completed' ? 'done' : 'failed'}>{run.label}</strong></span>
              </button>
              {run.status !== 'running' && <button type="button" disabled={busy} aria-label={`清理整轮 ${run.label}`} onClick={() => { setSelectedRun(id); void clear(id) }}><Trash2 size={13} /></button>}
            </div>
            <OutlineBranch id={scope + run.id} open={open} reduced={!!reduced}>
              <RailToc visible={visible && open} aria-label={run.label + '的动作与命令'} selectedId={activeRunId === id ? liveNode : run.status !== 'running' ? 'end:' + id : historicalNode ?? 'start:' + id} paused={paused}
                onCursorMove={run.id === latestRun ? revealCursor : undefined}>
                {run.startSeq !== undefined && <div className="run-lifecycle" data-rail-node={'start:' + id} data-depth="0">
                  <span>Run 已启动</span>{!!run.startedAt && <time>{new Date(run.startedAt / 1e6).toLocaleTimeString('zh-CN', {hour12:false})}</time>}
                </div>}
                {groups.map(actionRow)}
                {run.status !== 'running' && <div className="run-lifecycle command-ink" data-rail-node={'end:' + id} data-depth="0" data-tone={run.status==='completed' ? 'done' : 'failed'}>{run.label}{run.status==='completed' ? '已完成' : '已中止'}</div>}
              </RailToc>
            </OutlineBranch>
          </section>
        })}
        {actions.some(action => !runKeys.has('run:' + action.runId)) && <RailToc visible={visible} aria-label="独立动作" selectedId={focusedRow} paused={paused}>
          {actions.filter(action => !runKeys.has('run:' + action.runId)).map(actionRow)}
        </RailToc>}
      </div>
    </aside>
  )
}

function OutlineBranch({ id, open, reduced, children }: { id: string; open: boolean; reduced: boolean; children: ReactNode }) {
  return <div id={id} inert={!open} className="outline-disclosure">
    <Disclosure open={open} transition={{ duration: reduced ? 0 : 0.2, ease: 'easeOut' }}>
      <DisclosureContent>{children}</DisclosureContent>
    </Disclosure>
  </div>
}

export function macroExecutionLabel(execution: { status: string; outcome_uncertain: boolean }): ExecutionState {
  if (execution.outcome_uncertain) return { label: '结果待核对', tone: 'waiting' }
  switch (execution.status) {
    case 'running': return { label: '执行中', tone: 'working' }
    case 'stopping': return { label: '正在停止', tone: 'waiting' }
    case 'succeeded': return { label: '已完成', tone: 'done' }
    case 'timed_out': return { label: '已超时', tone: 'failed' }
    case 'cancelled': return { label: '已停止', tone: 'waiting' }
    case 'interrupted_by_user': return { label: '人工介入', tone: 'waiting' }
    default: return { label: '执行失败', tone: 'failed' }
  }
}

function triggerState(status: string): ExecutionState {
  switch (status) {
    case 'matched': return { label: '已匹配', tone: 'done' }
    case 'max_fires_reached': return { label: '已达触发次数', tone: 'done' }
    case 'timed_out': return { label: '已超时', tone: 'failed' }
    case 'cancelled': return { label: '已取消', tone: 'waiting' }
    case 'armed': case 'waiting_for_start': return { label: '等待触发', tone: 'waiting' }
    case 'running': return { label: '触发中', tone: 'working' }
    case 'stopping': return { label: '正在停止', tone: 'waiting' }
    default: return { label: '触发中断', tone: 'failed' }
  }
}

function runStatus(status: Extract<AgentHistoryItem, { kind: 'run' }>['status']): string {
  return status === 'running' ? '执行中' : status === 'completed' ? '已完成' : '已中止'
}
