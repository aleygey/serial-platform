import { CircleStop, Power, RefreshCw } from 'lucide-react'
import { PortDragHandle, type PortDragController } from './PortDrag'
import { WorkspaceSidebar } from './WorkspaceSidebar'
import { BounceSidebar } from './ui/bounce-sidebar'
import type { PortDescriptor, PortSnapshot } from '../../shared/contracts'

interface Props {
  configuredPorts: PortSnapshot[]
  availablePorts: PortDescriptor[]
  selectedPort?: string
  connected?: boolean
  endpoint?: string
  onSelect: (port: string) => void
  drag?: PortDragController
  onRefresh?: () => void
  onToggle: (port: string, open: boolean) => void
}

export function PortRail({ configuredPorts, availablePorts, selectedPort, connected = true, endpoint,
  onSelect, drag, onRefresh, onToggle }: Props): React.JSX.Element {
  const ports = [...new Set([...configuredPorts.map((item) => item.config.port), ...availablePorts.map((port) => port.name)])]
  return <WorkspaceSidebar className="device-rail" title="串口设备" count={ports.length} actions={
      <button type="button" title="刷新设备列表" aria-label="刷新设备列表" onClick={onRefresh}><RefreshCw size={15} /></button>}>
    <div className="devices-list">
      <BounceSidebar items={ports} value={ports.indexOf(selectedPort ?? '')} onChange={index => onSelect(ports[index])}
        aria-label="选择串口" renderItem={(index, button) => {
        const port = ports[index]
        const configured = configuredPorts.find((item) => item.config.port === port)
        const descriptor = availablePorts.find((item) => item.name === port)
        const online = connected && configured?.session_state === 'online'
        const present = connected && (configured?.endpoint_present ?? !!descriptor)
        const selected = selectedPort === port
        const label = online ? '在线' : !present ? '离线' : configured?.session_state === 'opening' ? '连接中' : '待连接'
        return <div className={`device-row ${selected ? 'is-selected' : ''}`} key={port}>
          <PortDragHandle {...button} className={button.className + ' device-choice'} port={port} controller={drag} aria-pressed={selected} title={port + ' · ' + label + ' · 拖入分栏（键盘用空格拾取）'}>
            <span className="device-copy"><strong>{port}</strong><small>{configured?.config.model_name || descriptor?.product || '未配置机型'}</small>
              </span>
          </PortDragHandle>
          <div className="device-tools">
            <span className={`device-state ${online ? 'online' : present ? 'available' : ''}`} title={label} role="img" aria-label={label}><i /></span>
            {configured && <button disabled={!connected} aria-label={`${configured.config.enabled ? '关闭' : '打开'} ${port}`}
              title={configured.config.enabled ? '关闭串口连接' : '打开串口连接'} type="button" onClick={() => onToggle(port, !configured.config.enabled)}>
              {configured.config.enabled ? <CircleStop size={14} /> : <Power size={14} />}</button>}
          </div>
        </div>
      }} />
      {!ports.length && <p className="rail-empty">尚未发现串口</p>}
    </div>
    <div className="device-footer">
      {endpoint && <div className="endpoint-label"><div><span>后端地址</span><code title={endpoint}>{endpoint.replace(/^https?:\/\//, '')}</code></div></div>}
    </div>
  </WorkspaceSidebar>
}
