import { Braces, SlidersHorizontal, Cable, LoaderCircle, Power, RefreshCw, Server, Settings2, Square, SquareTerminal, Wifi, WifiOff, X } from 'lucide-react'
import { Fragment, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { message, withToast, type ToastValue } from './lib/feedback'
export { submitHumanCommand } from './lib/feedback'
import type {
  DesktopEvent,
  DesktopSnapshot,
  MacroExecution,
  RunStartDecision
} from '../shared/contracts'
import { isApprovalActionable } from '../shared/run-start'
import { paletteId, palettes } from '../shared/appearance'
import { Group, Panel, Separator, type GroupImperativeHandle, type Layout } from 'react-resizable-panels'
import { motion } from 'motion/react'
import { DockPreview, InsertionGap, usePortDrag } from './components/PortDrag'
import { PortConsole } from './components/PortConsole'
import type { AgentHistoryView } from './components/AgentHistory'
import { PortInput } from './lib/port-input'
import { closePane, dockedWidths, insertPane, reorderPane, selectPane, type PaneLayout } from './lib/console-workspace'
import { MacroPage, type MacroDraftStore } from './components/MacroPage'
import { PortRail } from './components/PortRail'
import { GooeyNav } from './components/ui/gooey-nav'
import { SettingsPage } from './components/SettingsPage'
import { FirstUseSetup } from './components/FirstUseSetup'
import { RunStartApprovalModal } from './components/RunStartApprovalModal'
import { resolveBackendControl } from './lib/backend-control'
import { applyAppearance, useAppearanceMode } from './lib/appearance'

type Page = 'console' | 'settings' | 'macros'

export function App(): React.JSX.Element {
  const [snapshot, setSnapshot] = useState<DesktopSnapshot>()
  const mode = useAppearanceMode(snapshot?.preferences.theme ?? 'system')
  const colors = palettes[paletteId(snapshot?.preferences.palette)][mode]
  const [page, setPage] = useState<Page>('console')
  const [setupOpen, setSetupOpen] = useState<boolean>()
  const [layout, setLayout] = useState<PaneLayout>({ ports: [] })
  const deckRef = useRef<HTMLDivElement>(null)
  const groupRef = useRef<GroupImperativeHandle>(null)
  const pendingWidths = useRef<Layout>(undefined)
  const keepEmpty = useRef(false)
  const selectedPort = layout.active
  const [toast, setToast] = useState<ToastValue>()
  const [expiredApprovals, setExpiredApprovals] = useState<Set<string>>(() => new Set())
  const [loadingMessage, setLoadingMessage] = useState('正在启动本地工作台…')
  const [macroExecution, setMacroExecution] = useState<MacroExecution>()
  const macroDrafts = useRef<MacroDraftStore>({ drafts: new Map() })
  const endpoint = snapshot?.preferences.endpoint
  const lastServer = useRef(new Map<string, string>())
  if (endpoint && snapshot?.serverId) lastServer.current.set(endpoint, snapshot.serverId)
  const scope = JSON.stringify([endpoint, endpoint ? lastServer.current.get(endpoint) : undefined])
  const layoutScope = useRef(scope)
  const sessions = useMemo(() => new Map<string, { input: PortInput; history: AgentHistoryView }>(), [scope])
  const knownPorts = useMemo(() => [...new Set([...(snapshot?.configuredPorts ?? []).map((port) => port.config.port), ...(snapshot?.availablePorts ?? []).map((port) => port.name)])], [snapshot?.configuredPorts, snapshot?.availablePorts])
  const humanHistory = useMemo(() => snapshot?.humanHistory?.entries.map((item) => item.command) ?? [], [snapshot?.humanHistory])
  const drag = usePortDrag(deckRef, (port, slot) => {
    if (!knownPorts.includes(port)) return
    keepEmpty.current = false
    const next = insertPane(layout, port, slot)
    pendingWidths.current = dockedWidths(next.ports, groupRef.current?.getLayout() ?? {})
    setLayout(next)
  })
  useEffect(() => { drag.cancel() }, [page, scope, drag.cancel])

  useLayoutEffect(() => {
    const widths = pendingWidths.current
    if (!widths) return
    // A previously opened panel combination can have its own cached sizes.
    // Apply the drop geometry after the group has registered the new children.
    const frame = requestAnimationFrame(() => {
      const group = groupRef.current
      const actual = group?.getLayout()
      if (group && actual && Object.keys(actual).length === Object.keys(widths).length &&
          Object.keys(widths).every(port => Object.hasOwn(actual, port))) {
        group.setLayout(widths)
      }
      pendingWidths.current = undefined
    })
    return () => cancelAnimationFrame(frame)
  }, [layout.ports, page])

  useLayoutEffect(() => {
    const deck = deckRef.current
    if (!deck || !selectedPort || deck.clientWidth >= deck.scrollWidth) return
    const panel = document.getElementById(selectedPort)
    if (!panel || !deck.contains(panel)) return
    const bounds = deck.getBoundingClientRect()
    const view = panel.getBoundingClientRect()
    if (view.left < bounds.left) deck.scrollLeft += view.left - bounds.left
    else if (view.right > bounds.right) deck.scrollLeft += view.right - bounds.right
  }, [layout.ports, selectedPort, page])

  const applyEvent = useCallback((event: DesktopEvent): void => {
    if (event.type === 'macro') setMacroExecution(event.execution)
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
    if (!snapshot) return
    const changedServer = layoutScope.current !== scope
    layoutScope.current = scope
    setLayout((current) => {
      if (!changedServer && snapshot.connection !== 'connected') return current
      const ports = changedServer ? [] : current.ports.filter((port) => knownPorts.includes(port))
      if (ports.length) {
        if (ports.length === current.ports.length) return current
        return { ports, active: current.active && ports.includes(current.active) ? current.active : ports[0] }
      }
      if (!knownPorts.length) return current.ports.length ? { ports: [] } : current
      const preferred = snapshot.preferences.selectedPort
      const active = preferred && knownPorts.includes(preferred) ? preferred : knownPorts[0]
      if (keepEmpty.current && !changedServer) return current.ports.length ? { ports: [] } : current
      keepEmpty.current = false
      return { ports: [active], active }
    })
  }, [scope, knownPorts, snapshot?.connection])

  useEffect(() => {
    const preferences = snapshot?.preferences
    if (!preferences || (setupOpen ?? preferences.firstUse === 'new') || !selectedPort || preferences.selectedPort === selectedPort || !knownPorts.includes(selectedPort)) return
    const timer = setTimeout(() => {
      void withToast(() => window.serial.savePreferences({ selectedPort }), setToast, false)
    }, 180)
    return () => clearTimeout(timer)
  }, [scope, selectedPort, setupOpen, snapshot?.preferences.selectedPort, snapshot?.preferences.firstUse])

  useLayoutEffect(() => {
    applyAppearance(mode, snapshot?.preferences.palette)
  }, [mode, snapshot?.preferences.palette])

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
  }, [snapshot?.configuredPorts])

  useEffect(() => {
    const handler = (event: KeyboardEvent): void => {
      if (event.isComposing || event.defaultPrevented || document.querySelector('[aria-modal="true"]')) return
      if ((event.metaKey || event.ctrlKey) && event.key === ',') {
        event.preventDefault()
        setPage('settings')
      }
      if ((event.metaKey || event.ctrlKey) && event.key === '1') {
        event.preventDefault()
        setPage('console')
      }
      if (event.key === 'Escape' && page === 'settings' && !(event.target as Element)?.closest('[data-slot="model-picker-panel"]') && !event.defaultPrevented) setPage('console')
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [page])

  const selectPort = (port: string): void => {
    if (selectedPort === port) return
    keepEmpty.current = false
    setLayout((current) => selectPane(current, port))
  }

  const startBackend = async (): Promise<void> => { await withToast(() => window.serial.startLocalService(), setToast, false) }
  const stopBackend = async (): Promise<void> => { await withToast(() => window.serial.stopLocalService(), setToast, false) }
  const savePreferences = async (preferences: DesktopSnapshot['preferences']): Promise<void> => {
    const previous = snapshot?.preferences
    setSnapshot((current) => current ? { ...current, preferences } : current)
    const saved = await withToast(() => window.serial.savePreferences(preferences), setToast, false)
    if (!saved && previous) {
      setSnapshot((current) => current?.preferences === preferences ? { ...current, preferences: previous } : current)
    }
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
        <div className="startup-pulse"><span /><span /><span /></div>
        <strong>{loadingMessage}</strong>
        <small>后端、持久记录与实时通道正在就绪</small>
        {toast?.kind === 'error' && <button type="button" onClick={() => window.location.reload()}>重试连接</button>}
      </div>
    )
  }

  const slot = drag.drag?.index
  const from = drag.drag ? layout.ports.indexOf(drag.drag.port) : -1
  const inserting = slot != null && (from < 0 || (slot !== from && slot !== from + 1))
  const draggedPort = drag.drag?.port
  const dockPreview = draggedPort ? {
    port: draggedPort,
    model: snapshot.configuredPorts.find(p => p.config.port === draggedPort)?.config.model_name
      ?? snapshot.availablePorts.find(p => p.name === draggedPort)?.product ?? undefined,
    lines: (snapshot.events[draggedPort] ?? []).slice(-12).filter(e => e.direction === 'rx' || e.direction === 'tx')
      .flatMap(e => e.text.slice(-240).split(/[\r\n]+/)).filter(Boolean).slice(-3),
  } : undefined
  const gap = (index: number) => <InsertionGap active={inserting && slot === index} preview={dockPreview} weight={drag.drag?.weight ?? 1} />
  const closePortView = (port: string) => {
    if (layout.ports.length === 1) keepEmpty.current = true
    setLayout(current => closePane(current, port))
  }
  const split = layout.ports.length > 1
  const connected = snapshot.connection === 'connected'
  const preferences = snapshot.preferences
  const showSetup = setupOpen ?? preferences.firstUse === 'new'
  async function leaveSetup(complete: boolean, nextPage: Page = 'console') {
    await window.serial.savePreferences({ firstUse:complete || preferences.firstUse==='complete' ? 'complete' : 'dismissed',
      ...(complete && { setupTask:undefined }) })
    setSetupOpen(false); setPage(nextPage)
    if (complete && preferences.setupTask?.port && knownPorts.includes(preferences.setupTask.port)) {
      selectPort(preferences.setupTask.port)
    } else if (nextPage === 'console' && window.serial.replaySampleRun && !complete) {
      selectPort('COM7')
      window.serial.replaySampleRun()
    }
  }
  return (
    <div className="app-frame">
      <WindowBar page={page} onPage={next=>{ if(showSetup) void withToast(()=>leaveSetup(false,next),setToast,false); else setPage(next) }} />
      {showSetup ? <FirstUseSetup key={scope} snapshot={snapshot} execution={macroExecution} onExecution={setMacroExecution} onLeave={leaveSetup} /> : page === 'settings' ? (
        <SettingsPage
          onOpenSetup={()=>setSetupOpen(true)}
          onOpenRunExample={window.serial.openRunExample && (()=>void withToast(()=>window.serial.openRunExample!(),setToast,false))}
          serviceControls={<BackendControls snapshot={snapshot} onStartBackend={startBackend} onStopBackend={stopBackend} />}
          configuredPorts={snapshot.configuredPorts}
          availablePorts={snapshot.availablePorts}
          transportProfiles={snapshot.transportProfiles}
          modelProfiles={snapshot.modelProfiles}
          modelFamilies={snapshot.modelFamilies}
          preferences={snapshot.preferences}
          initialPort={selectedPort}
          onSaveSerial={async (draft) => withToast(
            () => window.serial.saveSerialConfiguration(draft, snapshot.configRevision),
            setToast, false
          )}
          onSaveModels={async (profiles) => withToast(() => window.serial.saveModelProfiles(profiles, snapshot.configRevision), setToast, false)}
          onSaveModelFamilies={async (families) => withToast(() => window.serial.saveModelFamilies(families, snapshot.configRevision), setToast, false)}
          onSavePreferences={savePreferences}
        />
      ) : page === 'macros' ? (
        <MacroPage events={snapshot.events} ports={snapshot.configuredPorts} modelFamilies={snapshot.modelFamilies} connected={snapshot.connection === 'connected'} selectedPort={selectedPort} drafts={macroDrafts.current} execution={macroExecution} onExecution={setMacroExecution} />
      ) : (
        <div className="workspace-shell workspace-grid">
          <PortRail configuredPorts={snapshot.configuredPorts} availablePorts={snapshot.availablePorts} connected={connected}
            selectedPort={selectedPort} onSelect={selectPort} drag={drag}
            onToggle={(port, open) => void withToast(() => window.serial.setPortOpen(port, open), setToast, false)}
            onRefresh={() => void withToast(async () => { await window.serial.refresh() }, setToast, false)}
            endpoint={snapshot.preferences.endpoint} />
          <main className="console-workspace">
            <div ref={deckRef} className={`pane-deck ${split ? 'is-split' : ''}`}
              data-dragging={!!drag.drag || undefined} data-settling={drag.settling || undefined}
              onScroll={({ currentTarget: deck }) => {
                // On a phone, a swipe selects the terminal that snaps into view.
                const width = deck.clientWidth
                if (drag.drag || !split || !width || Math.abs((deck.querySelector('[data-panel]')?.getBoundingClientRect().width ?? 0) - width) > 1) return
                const index = Math.round(deck.scrollLeft / width)
                const port = layout.ports[index]
                if (port && Math.abs(deck.scrollLeft - index * width) < 1) selectPort(port)
              }}>
              {layout.ports.length > 0 ? <Group className="workspace-panels" orientation="horizontal" aria-label="串口分栏"
                groupRef={groupRef} defaultLayout={pendingWidths.current} style={{ '--pane-count': layout.ports.length } as React.CSSProperties}>
              {layout.ports.map((port, index) => {
                let session = sessions.get(port)
                if (!session) { session = { input: new PortInput(), history: { rows: {} } }; sessions.set(port, session) }
                return <Fragment key={JSON.stringify([scope, port])}>
                  {gap(index)}
                  {index > 0 && <Separator className="pane-divider" aria-label={`调整 ${layout.ports[index - 1]} 与 ${port} 的宽度`}><span /></Separator>}
                  <Panel id={port} minSize="250px"><PortConsole snapshot={snapshot} selectedPort={port} colors={colors}
                  active={selectedPort === port} split={split} dragController={drag} lifted={inserting && drag.drag?.port === port}
                  animateEntry={!!pendingWidths.current && selectedPort === port} session={session.input} historyView={session.history} humanHistory={humanHistory}
                  onReorder={target => setLayout(current => reorderPane(current, port, target))}
                  setToast={setToast} onFocus={() => selectPort(port)} onClose={() => closePortView(port)} /></Panel>
                </Fragment>
              })}
              {gap(layout.ports.length)}
              </Group> : <div className="workspace-empty" data-drop-preview={inserting || undefined}>
                {inserting && dockPreview ? <DockPreview preview={dockPreview} /> : knownPorts.length ? <p>选择串口，或将串口拖到此处</p>
                  : <><Cable size={28} /><h2>{connected ? '等待串口设备' : '等待后端连接'}</h2>
                    <button type="button" onClick={() => setPage('settings')}><Settings2 size={15} /> 打开配置</button></>}
              </div>}
            </div>
            {drag.drag && !drag.drag.keyboard && !inserting && dockPreview && <motion.div className="drag-port-label" aria-hidden style={{x:drag.x, y:drag.y}}><DockPreview preview={dockPreview} floating /></motion.div>}
            {drag.drag?.keyboard && <span className="sr-only" role="status">{`${drag.drag.port} · 插入位置 ${(slot ?? 0) + 1}，左右键调整，回车放置，Esc 取消`}</span>}
          </main>
        </div>
      )}
      <div className="picker-layer" />
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

const pages = ['console', 'macros', 'settings'] as const
const pageItems = [
  { label: '控制台', icon: <SquareTerminal size={14} aria-hidden /> },
  { label: '宏', icon: <Braces size={14} aria-hidden /> },
  { label: '配置', icon: <SlidersHorizontal size={14} aria-hidden /> },
]

export function WindowBar({ page, onPage }: { page: Page; onPage: (page: Page) => void }): React.JSX.Element {
  return <header className="window-bar">
    <GooeyNav aria-label="工作区" items={pageItems} value={pages.indexOf(page)} onChange={index => onPage(pages[index])}
      size="sm" activeColor="var(--run-bg)" activeLabelColor="var(--run-text)" separation={7} radius={8} />
  </header>
}

interface BackendControlsProps {
  snapshot: DesktopSnapshot
  onStartBackend: () => Promise<void>
  onStopBackend: () => Promise<void>
}

export function BackendControls({ snapshot, onStartBackend, onStopBackend }: BackendControlsProps): React.JSX.Element {
  const connected = snapshot.connection === 'connected'
  const backend = resolveBackendControl(snapshot.service, snapshot.connection)
  const BackendIcon = backend.kind === 'start'
    ? Power
    : backend.kind === 'stop'
      ? Square
      : backend.kind === 'external'
        ? Server
        : LoaderCircle
  return (
      <div className="backend-settings" aria-label="后端服务">
        <button className={`connection-pill ${connected ? 'is-connected' : ''}`} type="button" title={snapshot.connectionMessage} onClick={() => void window.serial.refresh()}>
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
      </div>
  )
}

function Toast({ value, onClose }: { value?: { kind: 'notice' | 'error'; message: string }; onClose: () => void }): React.JSX.Element | null {
  if (!value) return null
  return <div className={`toast is-${value.kind}`}><span>{value.message}</span><button type="button" aria-label="关闭提示" onClick={onClose}><X size={14} /></button></div>
}
