import { Command, LoaderCircle, Moon, Power, RefreshCw, Server, Settings2, Square, Sun, Wifi, WifiOff, X } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { HUMAN_COMMAND_UNCERTAIN_MESSAGE } from '../shared/contracts'
import type {
  DesktopEvent,
  DesktopSnapshot,
  HumanCommandSubmission,
  RunStartDecision,
  ThemePreference
} from '../shared/contracts'
import { isApprovalActionable } from '../shared/run-start'
import { AgentHistory } from './components/AgentHistory'
import { CommandBar } from './components/CommandBar'
import { PortRail } from './components/PortRail'
import { SettingsPage } from './components/SettingsPage'
import { TerminalPane } from './components/TerminalPane'
import { RunStartApprovalModal } from './components/RunStartApprovalModal'
import { buildAgentHistory, locateCommandOutput, type AgentCommand } from './lib/history'
import { resolveBackendControl } from './lib/backend-control'
import iconUrl from './assets/icon.png'

type Page = 'console' | 'settings'

export function App(): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<DesktopSnapshot>()
  const [page, setPage] = useState<Page>('console')
  const [selectedPort, setSelectedPort] = useState<string>()
  const [selectedCommand, setSelectedCommand] = useState<AgentCommand>()
  const [toast, setToast] = useState<{ kind: 'notice' | 'error'; message: string }>()
  const [expiredApprovals, setExpiredApprovals] = useState<Set<string>>(() => new Set())
  const [loadingMessage, setLoadingMessage] = useState('正在启动本地工作台…')

  const applyEvent = useCallback((event: DesktopEvent): void => {
    if (event.type === 'snapshot' || event.type === 'timeline' || event.type === 'connection' || event.type === 'service') {
      setSnapshot((current) => mergeDesktopSnapshotEvent(current, event))
    }
    if (event.type === 'connection') {
      setLoadingMessage(event.message)
    } else if (event.type === 'notice' || event.type === 'error') {
      setToast({ kind: event.type, message: event.message })
    }
  }, [])

  useEffect(() => {
    // Main subscribes to the live socket while the bootstrap IPC request is
    // still in flight. Buffer those events so a timeline/capture delivered in
    // that window cannot be discarded because React has no initial snapshot,
    // or overwritten by the slightly older bootstrap reply.
    let buffering = true
    let cancelled = false
    const buffered: DesktopEvent[] = []
    const unsubscribe = window.serial.onEvent((event) => {
      if (buffering) buffered.push(event)
      else applyEvent(event)
    })
    window.serial.bootstrap().then((value) => {
      if (cancelled) return
      const merged = buffered.reduce<DesktopSnapshot>(
        (current, event) => mergeDesktopSnapshotEvent(current, event) ?? current,
        value
      )
      buffering = false
      setSnapshot(merged)
      setSelectedPort(merged.preferences.selectedPort ?? merged.configuredPorts[0]?.config.port ?? merged.availablePorts[0]?.name)
      for (const event of buffered) {
        if (event.type === 'connection') setLoadingMessage(event.message)
        else if (event.type === 'notice' || event.type === 'error') {
          setToast({ kind: event.type, message: event.message })
        }
      }
      buffered.length = 0
    }).catch((error) => {
      if (cancelled) return
      buffering = false
      buffered.length = 0
      setToast({ kind: 'error', message: message(error) })
    })
    return () => {
      cancelled = true
      unsubscribe()
    }
  }, [applyEvent])

  useEffect(() => {
    if (snapshot && !selectedPort) {
      setSelectedPort(snapshot.preferences.selectedPort ?? snapshot.configuredPorts[0]?.config.port ?? snapshot.availablePorts[0]?.name)
    }
  }, [snapshot, selectedPort])

  useEffect(() => {
    if (!snapshot) return
    return applyTheme(snapshot.preferences.theme)
  }, [snapshot?.preferences.theme])

  useEffect(() => {
    if (!toast) return
    const timer = setTimeout(() => setToast(undefined), toast.kind === 'error' ? 6_000 : 3_000)
    return () => clearTimeout(timer)
  }, [toast])

  useEffect(() => {
    if (!snapshot) return
    const pending = new Set(snapshot.configuredPorts.flatMap((port) => port.pending_run_start?.id ?? []))
    setExpiredApprovals((current) => {
      const retained = new Set([...current].filter((id) => pending.has(id)))
      return retained.size === current.size ? current : retained
    })
  }, [snapshot])

  useEffect(() => {
    const handler = (event: KeyboardEvent): void => {
      if ((event.metaKey || event.ctrlKey) && event.key === ',') {
        event.preventDefault()
        setPage('settings')
      }
      if ((event.metaKey || event.ctrlKey) && event.key === '1') {
        event.preventDefault()
        setPage('console')
      }
      if (event.key === 'Escape' && page === 'settings') setPage('console')
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [page])

  const selectPort = (port: string): void => {
    setSelectedPort(port)
    setSelectedCommand(undefined)
    if (snapshot) {
      const preferences = { ...snapshot.preferences, selectedPort: port }
      setSnapshot({ ...snapshot, preferences })
      void window.serial.savePreferences(preferences)
    }
  }

  const startBackend = async (): Promise<void> => { await withToast(() => window.serial.startLocalService(), setToast, false) }
  const stopBackend = async (): Promise<void> => { await withToast(() => window.serial.stopLocalService(), setToast, false) }
  const savePreferences = async (preferences: DesktopSnapshot['preferences']): Promise<void> => {
    setSnapshot((current) => current ? { ...current, preferences } : current)
    await withToast(() => window.serial.savePreferences(preferences), setToast)
  }

  const pendingApproval = snapshot?.connection === 'connected'
    ? snapshot.configuredPorts
        .flatMap((port) => port.pending_run_start && !expiredApprovals.has(port.pending_run_start.id)
          && isApprovalActionable(port, port.pending_run_start, snapshot.actor)
          ? [port.pending_run_start]
          : [])
        .sort((left, right) => left.requested_wall_time_ns - right.requested_wall_time_ns)[0]
    : undefined

  const decideRunStart = async (decision: RunStartDecision): Promise<boolean> => {
    if (!pendingApproval) return false
    return withToast(
      () => window.serial.decideRunStart(pendingApproval.port, pendingApproval.id, decision),
      setToast,
      false
    )
  }

  if (!snapshot) {
    return (
      <div className="startup-screen">
        <BrandMark />
        <div className="startup-pulse"><span /><span /><span /></div>
        <strong>{loadingMessage}</strong>
        <small>后端、持久记录与实时通道正在就绪</small>
        {toast?.kind === 'error' && <button type="button" onClick={() => window.location.reload()}>重试连接</button>}
      </div>
    )
  }

  if (page === 'settings') {
    return (
      <div className="app-frame">
        <WindowBar snapshot={snapshot} page={page} onPage={setPage} onStartBackend={startBackend} onStopBackend={stopBackend} onSavePreferences={savePreferences} />
        <SettingsPage
          configuredPorts={snapshot.configuredPorts}
          availablePorts={snapshot.availablePorts}
          transportProfiles={snapshot.transportProfiles}
          modelProfiles={snapshot.modelProfiles}
          modelFamilies={snapshot.modelFamilies}
          preferences={snapshot.preferences}
          initialPort={selectedPort}
          onBack={() => setPage('console')}
          onSaveSerial={async (draft) => withToast(
            () => window.serial.saveSerialConfiguration(draft, snapshot.configRevision),
            setToast
          )}
          onSaveModels={async (profiles) => withToast(() => window.serial.saveModelProfiles(profiles, snapshot.configRevision), setToast)}
          onSaveModelFamilies={async (families) => withToast(() => window.serial.saveModelFamilies(families, snapshot.configRevision), setToast)}
          onSavePreferences={savePreferences}
        />
        <Toast value={toast} onClose={() => setToast(undefined)} />
        {pendingApproval && (
          <RunStartApprovalModal
            approval={pendingApproval}
            key={pendingApproval.id}
            onDecide={decideRunStart}
            onExpired={() => setExpiredApprovals((current) => new Set(current).add(pendingApproval.id))}
          />
        )}
      </div>
    )
  }

  const configuredPort = snapshot.configuredPorts.find((item) => item.config.port === selectedPort)
  const events = selectedPort ? snapshot.events[selectedPort] ?? [] : []
  const history = buildAgentHistory(events)
  const currentSelectedCommand = selectedCommand
    ? history.flatMap((item) => item.kind === 'command' ? item.commands : []).find((command) => (
        command.id === selectedCommand.id
        && command.firstSeq === selectedCommand.firstSeq
        && command.daemonEpoch === selectedCommand.daemonEpoch
        && command.generation === selectedCommand.generation
      ))
    : undefined
  const match = currentSelectedCommand
    ? locateCommandOutput(events, currentSelectedCommand)
    : undefined
  return (
    <div className="app-frame">
      <WindowBar snapshot={snapshot} page={page} onPage={setPage} onStartBackend={startBackend} onStopBackend={stopBackend} onSavePreferences={savePreferences} />
      <div className="workspace-grid">
        <PortRail
          configuredPorts={snapshot.configuredPorts}
          availablePorts={snapshot.availablePorts}
          selectedPort={selectedPort}
          onSelect={selectPort}
          onToggle={(port, open) => void withToast(() => window.serial.setPortOpen(port, open), setToast)}
          onSettings={() => setPage('settings')}
        />
        <div className="console-stack">
          <TerminalPane
            configuredPort={configuredPort}
            events={events}
            selectedCommand={currentSelectedCommand}
            match={match}
            onClearCommand={() => setSelectedCommand(undefined)}
          />
          <CommandBar
            port={selectedPort}
            disabled={!configuredPort || configuredPort.session_state !== 'online' || snapshot.connection !== 'connected'}
            onSend={(command) => submitHumanCommand(
              () => window.serial.sendCommand(selectedPort!, command),
              setToast
            )}
          />
        </div>
        <AgentHistory items={history} selectedCommand={currentSelectedCommand} onSelect={setSelectedCommand} />
      </div>
      <Toast value={toast} onClose={() => setToast(undefined)} />
      {pendingApproval && (
        <RunStartApprovalModal
          approval={pendingApproval}
          key={pendingApproval.id}
          onDecide={decideRunStart}
          onExpired={() => setExpiredApprovals((current) => new Set(current).add(pendingApproval.id))}
        />
      )}
    </div>
  )
}

export function mergeDesktopSnapshotEvent(
  current: DesktopSnapshot | undefined,
  event: DesktopEvent
): DesktopSnapshot | undefined {
  if (event.type === 'snapshot') return event.snapshot
  if (!current) return current
  if (event.type === 'connection') {
    return { ...current, connection: event.state, connectionMessage: event.message }
  }
  if (event.type === 'service') return { ...current, service: event.service }
  if (event.type !== 'timeline') return current

  const configured = current.configuredPorts.find((port) => port.config.port === event.event.port)
  if (!configured || configured.daemon_epoch !== event.event.daemon_epoch) return current
  const prior = current.events[event.event.port] ?? []
  const events = prior.some((item) => item.daemon_epoch !== event.event.daemon_epoch) ? [] : prior
  const last = events.at(-1)
  if (last && last.seq >= event.event.seq) return current
  return {
    ...current,
    events: {
      ...current.events,
      [event.event.port]: [...events, event.event].slice(-12_000)
    }
  }
}

interface WindowBarProps {
  snapshot: DesktopSnapshot
  page: Page
  onPage: (page: Page) => void
  onStartBackend: () => Promise<void>
  onStopBackend: () => Promise<void>
  onSavePreferences: (preferences: DesktopSnapshot['preferences']) => Promise<void>
}

export function WindowBar({ snapshot, page, onPage, onStartBackend, onStopBackend, onSavePreferences }: WindowBarProps): React.JSX.Element {
  const connected = snapshot.connection === 'connected'
  const nextTheme: ThemePreference = snapshot.preferences.theme === 'dark' ? 'light' : 'dark'
  const backend = resolveBackendControl(snapshot.service, snapshot.connection)
  const BackendIcon = backend.kind === 'start'
    ? Power
    : backend.kind === 'stop'
      ? Square
      : backend.kind === 'external'
        ? Server
        : LoaderCircle
  return (
    <header className="window-bar">
      <div className="brand-lockup"><BrandMark /><div><strong>Serial Platform</strong><small>Human × Agent Workspace</small></div></div>
      <nav className="view-tabs">
        <button className={page === 'console' ? 'is-active' : ''} onClick={() => onPage('console')} type="button"><Command size={14} /> 控制台</button>
        <button className={page === 'settings' ? 'is-active' : ''} onClick={() => onPage('settings')} type="button"><Settings2 size={14} /> 配置</button>
      </nav>
      <div className="window-actions">
        <button className="connection-pill" type="button" title={snapshot.connectionMessage} onClick={() => void window.serial.refresh()}>
          {connected ? <Wifi size={14} /> : <WifiOff size={14} />}
          <span>{connected ? '已连接' : snapshot.connection === 'reconnecting' ? '重连中' : '离线'}</span>
          {snapshot.service.owned && <small>PID {snapshot.service.pid}</small>}
        </button>
        <button
          aria-label={backend.label}
          className={`backend-control is-${backend.kind}`}
          disabled={backend.disabled}
          onClick={() => void (backend.kind === 'stop' ? onStopBackend() : onStartBackend())}
          title={backend.title}
          type="button"
        >
          <BackendIcon className={backend.kind === 'busy' ? 'is-spinning' : undefined} size={14} />
          <span>{backend.label}</span>
        </button>
        <button className="icon-button" type="button" title="刷新" onClick={() => void window.serial.refresh()}><RefreshCw size={16} /></button>
        <button className="icon-button" type="button" title="切换主题" onClick={() => {
          const preferences = { ...snapshot.preferences, theme: nextTheme }
          void onSavePreferences(preferences)
        }}>{snapshot.preferences.theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}</button>
      </div>
    </header>
  )
}

function BrandMark(): React.JSX.Element {
  return <span className="brand-mark"><img src={iconUrl} alt="" /></span>
}

function Toast({ value, onClose }: { value?: { kind: 'notice' | 'error'; message: string }; onClose: () => void }): React.JSX.Element | null {
  if (!value) return null
  return <div className={`toast is-${value.kind}`}><span>{value.message}</span><button type="button" onClick={onClose}><X size={14} /></button></div>
}

export async function submitHumanCommand(
  action: () => Promise<HumanCommandSubmission>,
  setToast: (toast: { kind: 'notice' | 'error'; message: string } | undefined) => void
): Promise<HumanCommandSubmission> {
  try {
    const submission = await action()
    if (
      !submission
      || !['accepted', 'rejected', 'uncertain'].includes(submission.status)
      || (submission.status !== 'accepted' && typeof submission.message !== 'string')
    ) {
      throw new Error('App 未收到可识别的人工命令结果')
    }
    if (submission.status !== 'accepted') {
      setToast({ kind: 'error', message: submission.message })
    }
    return submission
  } catch {
    const submission: HumanCommandSubmission = {
      status: 'uncertain',
      message: HUMAN_COMMAND_UNCERTAIN_MESSAGE
    }
    setToast({ kind: 'error', message: submission.message })
    return submission
  }
}

async function withToast(
  action: () => Promise<void>,
  setToast: (toast: { kind: 'notice' | 'error'; message: string } | undefined) => void,
  announce = true
): Promise<boolean> {
  try {
    await action()
    if (announce) setToast({ kind: 'notice', message: '配置已保存并生效' })
    return true
  } catch (error) {
    setToast({ kind: 'error', message: message(error) })
    return false
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function applyTheme(theme: ThemePreference): () => void {
  const media = window.matchMedia('(prefers-color-scheme: dark)')
  const update = (): void => {
    document.documentElement.dataset.theme = theme === 'system' ? (media.matches ? 'dark' : 'light') : theme
  }
  update()
  media.addEventListener('change', update)
  return () => media.removeEventListener('change', update)
}
