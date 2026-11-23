import { X } from 'lucide-react'
import { motion, useReducedMotion } from 'motion/react'
import { PortDragHandle, usePortPointerDrag, type PortDragController } from './PortDrag'
import { useCallback, useEffect, useId, useMemo, useRef, useState, type SyntheticEvent } from 'react'
import type { AgentHistoryResponse, DesktopSnapshot, MacroExecution, TimelineEvent } from '../../shared/contracts'
import { createHistorySelector, locateCommandOutput, mergeAgentHistory, type AgentCommand } from '../lib/history'
import { lastRunOutcome, portActivity } from '../lib/console-workspace'
import { PortInput } from '../lib/port-input'
import { ModelIdentityPicker } from './SerialFields'
import { CommandBar } from './CommandBar'
import { AgentDock } from './AgentDock'
import { AgentHistory, macroExecutionLabel, type AgentHistoryView } from './AgentHistory'
import { TerminalPane } from './TerminalPane'
import { message, withToast, submitHumanCommand, type SetToast } from '../lib/feedback'

const EMPTY_EVENTS: TimelineEvent[] = []

interface Props {
  snapshot: DesktopSnapshot
  colors: { accent: string; output: string }
  selectedPort: string
  active: boolean
  split: boolean
  session: PortInput
  historyView: AgentHistoryView
  humanHistory: string[]
  onFocus: () => void
  dragController: PortDragController
  swapTarget: boolean
  animateEntry: boolean
  onReorder: (target: string) => void
  onClose: () => void
  setToast: SetToast
}

export function PortConsole({ snapshot, colors, selectedPort, active, split, session, historyView, humanHistory, onFocus, onClose, onReorder, dragController, swapTarget, animateEntry, setToast }: Props): React.JSX.Element {
  const headingDrag = usePortPointerDrag(selectedPort, split ? dragController : undefined)
  const [animateDrawer, setAnimateDrawer] = useState(false)
  const [drawerPreference, setDrawerPreference] = useState(historyView.expanded)
  const setHistoryOpen = useCallback((open: boolean) => {
    historyView.expanded = open
    setDrawerPreference(open)
  }, [historyView])
  const [paused, setPaused] = useState(false)
  const frame = useRef<HTMLElement>(null)
  const reduced = useReducedMotion()
  const [savingModel, setSavingModel] = useState(false)
  const panelElements = () => Array.from(frame.current?.closest('.workspace-panels')?.querySelectorAll<HTMLElement>('[data-panel]') ?? [])
  const moveTo = (panel?: HTMLElement) => {
    const target = panel?.querySelector<HTMLElement>('[data-port]')?.dataset.port
    if (target && target !== selectedPort) onReorder(target)
  }
  const historyId = useId()
  const [selectedCommand, setSelectedCommand] = useState<AgentCommand>()
  const [agentArchive, setAgentArchive] = useState<AgentHistoryResponse>()
  const [agentArchiveEvents, setAgentArchiveEvents] = useState<Record<string, TimelineEvent[]>>({})
  const [commandEvidence, setCommandEvidence] = useState<{ key: string; events: TimelineEvent[] }>()
  const evidenceLoad = useRef(0)
  const historyContext = useRef('')
  const historyLoad = useRef(new Map<string, { epoch: string; through: number }>())
  const clearCommand = useCallback(() => {
    evidenceLoad.current++
    setCommandEvidence(undefined)
    setSelectedCommand(undefined)
  }, [])
  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const context = `${snapshot?.serverId ?? ''}:${selectedPort ?? ''}`
    historyContext.current = context
    setAgentArchive(undefined)
    setAgentArchiveEvents({})
    setCommandEvidence(undefined)
    evidenceLoad.current++
    const refresh = async (): Promise<void> => {
      if (!selectedPort) return
      try {
        const history = await window.serial.agentHistory(selectedPort)
        if (!cancelled && history.server_id === snapshot?.serverId) setAgentArchive((current) =>
          current && current.visibility.revision > history.visibility.revision ? current : history)
      } catch { /* Live console remains usable when an older daemon has no panel index. */ }
      if (!cancelled) timer = setTimeout(() => void refresh(), 5000)
    }
    void refresh()
    return () => { cancelled = true; historyContext.current = ''; evidenceLoad.current++; historyLoad.current.clear(); if (timer) clearTimeout(timer) }
  }, [selectedPort, snapshot?.serverId])

  const configuredPort = snapshot.configuredPorts.find((item) => item.config.port === selectedPort)
  const events = snapshot.events[selectedPort] ?? EMPTY_EVENTS
  const liveEvents = useRef(events)
  liveEvents.current = events
  const selectHistory = useMemo(createHistorySelector, [selectedPort, snapshot.serverId])
  const liveHistory = useMemo(() => selectHistory(events), [selectHistory, events])
  const activity = portActivity(configuredPort, snapshot.connection === 'connected', snapshot.actor)
  const historyEvents = useMemo(() => {
    const archived = Object.values(agentArchiveEvents)
    if (!archived.length) return liveHistory
    return [...archived.flat(), ...liveHistory].sort((a, b) => a.seq - b.seq)
  }, [agentArchiveEvents, liveHistory])
  const history = useMemo(() => mergeAgentHistory(historyEvents, agentArchive, activity.run), [historyEvents, agentArchive, activity.run])
  const loadRun = useCallback(async (id: string) => {
    const record = agentArchive?.runs.find(item => item.run.id === id)
    if (!record || agentArchive?.visibility.hidden.includes(id)) return
    const through = record.run.end_seq ?? record.through_seq
    const loaded = historyLoad.current.get(id)
    const first = loaded?.epoch === record.epoch ? loaded.through + 1 : record.run.start_seq
    if (through < first) return
    const events = liveEvents.current
    const startIndex = events.findIndex(e => e.daemon_epoch === record.epoch && e.seq === first)
    const endIndex = startIndex + through - first
    if (startIndex >= 0 && events[endIndex]?.seq === through && events[endIndex].daemon_epoch === record.epoch
        && !events.slice(startIndex, endIndex + 1).some(e => e.kind === 'gap')) return
    const request = { epoch: record.epoch, through }
    historyLoad.current.set(id, request)
    try {
      const result = await window.serial.agentRunEvents(record)
      if (historyLoad.current.get(id) !== request) return
      setAgentArchiveEvents(current => ({ ...current, [id]: result.events }))
      if (result.limited) setToast({ kind: 'notice', message: '已加载可用命令，历史日志存在缺口。' })
    } catch (error) {
      if (historyLoad.current.get(id) === request) {
        historyLoad.current.delete(id)
        setToast({ kind: 'error', message: message(error) })
      }
    }
  }, [agentArchive, events[0]?.daemon_epoch, events[0]?.seq, setToast])
  const commands = useMemo(() => history.flatMap(item => item.kind === 'command' ? item.commands : []), [history])
  const currentSelectedCommand = selectedCommand
    ? commands.find((command) => (
        command.id === selectedCommand.id
        && command.firstSeq === selectedCommand.firstSeq
        && command.daemonEpoch === selectedCommand.daemonEpoch
        && command.generation === selectedCommand.generation
      ))
    : undefined
  const displayedEvents = currentSelectedCommand && commandEvidence?.key === commandIdentity(currentSelectedCommand)
    ? commandEvidence.events : events
  const match = currentSelectedCommand
    ? locateCommandOutput(displayedEvents, currentSelectedCommand)
    : undefined
  const selectCommand = async (command: AgentCommand): Promise<void> => {
    setSelectedCommand(command)
    setCommandEvidence(undefined)
    const load = ++evidenceLoad.current
    const context = historyContext.current
    if (!selectedPort) return
    const parent = history.find((item) => item.kind === 'command' && item.commands.includes(command))
    const record = parent?.kind === 'command' ? agentArchive?.runs.find((record) => record.run.id === parent.runId) : undefined
    const next = commands
      .filter((candidate) => candidate.daemonEpoch === command.daemonEpoch && candidate.firstSeq > command.firstSeq)
      .sort((a, b) => a.firstSeq - b.firstSeq)[0]
    const firstSeq = command.capture?.evidenceFromSeq ?? command.firstSeq
    const throughSeq = command.capture?.evidenceThroughSeq ?? (next ? next.firstSeq - 1
      : record?.run.end_seq ?? (configuredPort?.daemon_epoch === command.daemonEpoch ? configuredPort.head_seq : record?.through_seq))
    if (throughSeq === undefined || throughSeq < firstSeq) {
      setToast({ kind: 'notice', message: '缺少该历史命令的准确日志边界，无法执行跳转。' }); return
    }
    const local = events.filter((event) => event.daemon_epoch === command.daemonEpoch && event.seq >= firstSeq && event.seq <= throughSeq)
    if (local.length === throughSeq - firstSeq + 1 && local.every((event, index) => event.seq === firstSeq + index && event.kind !== 'gap')) return
    setToast({ kind: 'notice', message: '正在读取该命令的原始串口上下文…' })
    try {
      const evidence = await window.serial.commandEvidence({ port: selectedPort, epoch: command.daemonEpoch, firstSeq, throughSeq })
      if (context !== historyContext.current || load !== evidenceLoad.current) return
      setCommandEvidence({ key: commandIdentity(command), events: evidence })
      setToast(undefined)
    } catch (error) {
      if (context === historyContext.current && load === evidenceLoad.current) setToast({ kind: 'error', message: message(error) })
    }
  }
  const connected = snapshot.connection === 'connected'
  const online = connected && configuredPort?.session_state === 'online'
  const lastOutcome = useMemo(() => lastRunOutcome(historyEvents, configuredPort, history), [historyEvents, history, selectedPort, configuredPort?.daemon_epoch, configuredPort?.generation])
  const endedCurrentRun = !!activity.run && lastOutcome?.runId === activity.run.id
  const runId = endedCurrentRun ? undefined : activity.run?.id
  const outcome = !runId && (endedCurrentRun || ['idle', 'local', 'agent'].includes(activity.phase)) ? lastOutcome : undefined
  const showHistory = !!(runId || history.some(item => item.kind === 'run'))
  const historyOpen = drawerPreference ?? showHistory
  useEffect(() => {
    if (drawerPreference === undefined && showHistory) setHistoryOpen(true)
  }, [drawerPreference, showHistory, setHistoryOpen])
  const currentStep = useMemo(() => {
    if (!runId || !configuredPort) return
    let latest: { actionId: string; description: string; command?: AgentCommand; macro?: MacroExecution; seq: number } | undefined
    for (const action of history) {
      if (action.kind !== 'command' || action.runId !== runId) continue
      if (!action.commands.length && (!latest || action.firstSeq > latest.seq)) latest = { actionId: action.id, description: action.description, macro: action.macro, seq: action.firstSeq }
      for (const command of action.commands) {
        if (command.daemonEpoch === configuredPort.daemon_epoch && command.generation === configuredPort.generation
          && (!latest || command.firstSeq > latest.seq)) latest = { actionId: action.id, description: action.description, command, seq: command.firstSeq }
      }
    }
    return latest
  }, [history, runId, configuredPort?.daemon_epoch, configuredPort?.generation])
  const currentCommand = currentStep?.command
  const macroState = currentStep?.macro && currentStep.macro.status !== 'running' ? macroExecutionLabel(currentStep.macro) : undefined
  const focusPort = (event: SyntheticEvent) => {
    if (!(event.target instanceof Element) || !event.target.closest('.channel-close')) onFocus()
  }

  return <motion.article className={`port-console ${active ? 'is-active' : ''}`} aria-label={`${selectedPort} 控制台`} data-port={selectedPort}
    data-motion={paused ? 'paused' : undefined} data-swap-target={swapTarget || undefined} ref={frame}
    onPointerDownCapture={focusPort} onFocusCapture={focusPort} layout={reduced ? false : 'position'}
    initial={!reduced && animateEntry ? { scale:0.96, opacity:0.76 } : false} animate={{ scale:1, opacity:1 }}
    transition={reduced ? {duration:0} : {type:'spring', stiffness:420, damping:32, layout: { type: 'spring', stiffness: 490, damping: 39 } }}>
    <div className="console-stack channel-body">
      <TerminalPane paused={paused} active={active} configuredPort={configuredPort} events={displayedEvents} commands={commands} currentCommand={currentCommand}
        heading={<div className="channel-name" {...headingDrag}>
          <ModelIdentityPicker label={`切换 ${selectedPort} 机型`} families={snapshot.modelFamilies} family={configuredPort?.config.model_family}
            name={configuredPort?.config.model_name} disabled={!connected || !configuredPort || savingModel}
            onChange={async (family, name) => {
              setSavingModel(true)
              try { await withToast(() => window.serial.setPortModel(selectedPort, family, name, snapshot.configRevision), setToast, false) }
              finally { setSavingModel(false) }
            }} />
          <h2>{split ? <PortDragHandle keyboardOnly className="pane-grip" port={selectedPort} controller={dragController} aria-label={`移动 ${selectedPort} 分栏`} title="拖动交换分栏，或按左右方向键"
            onKeyDown={event => {
              const direction = event.key === 'ArrowLeft' ? -1 : event.key === 'ArrowRight' ? 1 : 0
              if (!direction) return
              event.preventDefault()
              const panels = panelElements()
              const index = panels.findIndex(panel => panel.contains(frame.current))
              moveTo(panels[index + direction])
            }}>{selectedPort}</PortDragHandle> : selectedPort}</h2>
        </div>}
        actions={<button className="channel-close" type="button" aria-label={`关闭 ${selectedPort} 分栏`} title="关闭分栏" onClick={onClose}><X size={15} /></button>}
        selectedCommand={currentSelectedCommand} match={match} onClearCommand={clearCommand} />
    </div>
    <section className="command-history-dock" aria-label={selectedPort + ' 执行历史'}>
      <div id={historyId} className="history-drawer" data-animate={animateDrawer || undefined} data-open={!!historyOpen} inert={!historyOpen} aria-hidden={!historyOpen}>
        <div className="history-drawer-clip">
          {(historyOpen || drawerPreference !== undefined) && <AgentHistory view={historyView} visible={historyOpen} items={history} paused={paused} activeRunId={runId} currentActionId={currentStep?.actionId} currentCommand={currentCommand}
          selectedCommand={currentSelectedCommand} onSelect={(command) => void selectCommand(command)}
          onExpandRun={loadRun}
          onClear={async (ids) => {
            if (!selectedPort) return
            const context = historyContext.current
            const visibility = await window.serial.clearAgentHistory(selectedPort, ids)
            if (context !== historyContext.current) return
            for (const id of visibility.hidden) historyLoad.current.delete(id)
            setAgentArchive((current) => current ? { ...current, visibility } : {
              server_id: snapshot.serverId ?? '', runs: [], visibility, truncated: false, indexing: false
            })
            setAgentArchiveEvents(current => Object.fromEntries(Object.entries(current).filter(([id]) => !visibility.hidden.includes(id))))
            setSelectedCommand(undefined)
            evidenceLoad.current++
            setCommandEvidence(undefined)
            setToast({ kind: 'notice', message: 'Agent 历史已清理；原始串口日志和人工历史保留。' })
          }} />}
        </div>
      </div>
      <AgentDock port={selectedPort} activity={activity} orbColor={activity.agentOwned ? colors.accent : colors.output} command={currentCommand}
        outcome={outcome ?? (macroState && { ...macroState, label: currentStep!.description + macroState.label })}

        action={currentStep?.description}
        expanded={historyOpen} historyId={historyId} onToggleHistory={() => { setAnimateDrawer(true); setHistoryOpen(!historyOpen) }}
        paused={paused} onToggleMotion={() => setPaused(!paused)} />
    </section>
    <CommandBar port={selectedPort} paused={paused} session={session} active={active} humanHistory={humanHistory}
      disabled={!activity.canWrite}
      placeholder={!online ? '打开串口后可发送命令' : !activity.canWrite ? '等待串口控制权释放' : '输入命令…'}
      onQueryHistory={async (query, contains) => (await window.serial.queryHumanHistory(query, contains)).entries.map((entry) => entry.command)}
      onSend={(command) => submitHumanCommand(() => window.serial.sendCommand(selectedPort, command), setToast)}
      onSignal={(signal) => submitHumanCommand(() => window.serial.sendSignal(selectedPort, signal), setToast)} />
  </motion.article>
}

function commandIdentity(command: AgentCommand): string {
  return `${command.daemonEpoch}:${command.generation}:${command.firstSeq}:${command.id}`
}
