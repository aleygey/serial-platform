import { commandState, type ExecutionState, type portActivity } from '../lib/console-workspace'
import { displayCommand, type AgentCommand } from '../lib/history'
import { useReducedMotion } from '../lib/use-reduced-motion'
import MatrixOrb from './ui/matrix-orb'
import { TextEffect } from './ui/text-effect'
import { RotateCcw } from 'lucide-react'

interface Props {
  port: string
  orbColor: string
  activity: ReturnType<typeof portActivity>
  command?: AgentCommand
  action?: string
  outcome?: ExecutionState
  expanded: boolean
  historyId: string
  onToggleHistory: () => void
  paused: boolean
  onToggleMotion: () => void
  onReplay?: () => void
}

export function AgentDock({ port, orbColor, activity, command, action, outcome, expanded, historyId, onToggleHistory, paused, onToggleMotion, onReplay }: Props) {
  const reduced = useReducedMotion()
  const current = command && (displayCommand(command.text) || 'Enter')
  const state = command && commandState(command, true)
  const ownership = [activity.label, activity.detail].filter(Boolean).join(' · ')
  const toggleLabel = (expanded ? '收起 ' : '展开 ') + port + ' 的执行历史'
  const task = command?.description || action?.replace(/^Macro · /, '') || activity.run?.label
  const status = outcome ?? (activity.run && task
    ? { label: 'Agent 正在' + task, tone: state?.tone ?? activity.tone }
    : { label: activity.phase === 'idle' ? '执行历史' : activity.label, tone: activity.tone })
  const detail = [status.label, current, state?.label, activity.detail].filter(Boolean).join(' · ')
  return <div className="history-strip" data-phase={activity.phase}>
    <button className="orb-control" type="button" aria-label={ownership + '，暂停此栏动效'} aria-pressed={paused}
      title={ownership + (paused ? ' · 恢复动效' : ' · 暂停动效')} onClick={onToggleMotion}>
      <MatrixOrb aria-hidden size={30} dots={7} state={activity.agentOwned ? 'thinking' : 'idle'}
        paused={paused || reduced || !activity.agentOwned} color={orbColor} />
    </button>
    <button className="history-toggle" type="button" aria-expanded={expanded} aria-controls={historyId}
      aria-label={toggleLabel} title={toggleLabel} onClick={onToggleHistory}>
      <span className={command && !outcome ? 'live-command command-ink' : 'live-phase command-ink'} data-tone={status.tone} title={detail}>
        {reduced || paused ? status.label : <TextEffect key={status.label} as="span" per="line" preset="fade" speedSegment={1.5}>{status.label}</TextEffect>}
      </span>
    </button>
    {onReplay && <button type="button" className="agent-replay" onClick={onReplay} title="回放模拟设备的外部 Agent 执行过程"><RotateCcw size={13} aria-hidden />回放示例</button>}
    <span className="sr-only" role="status">{status.label}</span>
  </div>
}
