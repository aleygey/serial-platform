import { Cable, CircleStop, Power, Radio, Settings2 } from 'lucide-react'
import type { PortDescriptor, PortSnapshot } from '../../shared/contracts'

interface Props {
  configuredPorts: PortSnapshot[]
  availablePorts: PortDescriptor[]
  selectedPort?: string
  onSelect: (port: string) => void
  onToggle: (port: string, open: boolean) => void
  onSettings: () => void
}

export function PortRail({ configuredPorts, availablePorts, selectedPort, onSelect, onToggle, onSettings }: Props): React.JSX.Element {
  const allPorts = [...new Set([...configuredPorts.map((item) => item.config.port), ...availablePorts.map((port) => port.name)])]
  return (
    <aside className="port-rail">
      <div className="rail-heading">
        <span>串口</span>
        <span className="rail-count">{allPorts.length}</span>
      </div>
      <div className="port-list">
        {allPorts.map((port) => {
          const configured = configuredPorts.find((item) => item.config.port === port)
          const descriptor = availablePorts.find((item) => item.name === port)
          const open = configured?.session_state === 'online'
          const available = configured?.endpoint_present ?? Boolean(descriptor)
          return (
            <div
              className={`port-card ${selectedPort === port ? 'is-selected' : ''}`}
              key={port}
              onClick={() => onSelect(port)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault()
                  onSelect(port)
                }
              }}
              role="button"
              tabIndex={0}
              title={port}
            >
              <span className={`port-icon ${open ? 'is-open' : ''}`}>
                {open ? <Radio size={16} /> : <Cable size={16} />}
              </span>
              <span className="port-copy">
                <strong>{port}</strong>
                <small>{configured?.config.model_profile || descriptor?.product || '未配置机型'}</small>
              </span>
              <span className={`status-dot ${open ? 'is-open' : available ? 'is-idle' : 'is-offline'}`} />
              {configured && (
                <button
                  aria-label={configured.config.enabled ? `关闭 ${port}` : `打开 ${port}`}
                  className="port-power"
                  onClick={(event) => {
                    event.stopPropagation()
                    onToggle(port, !configured.config.enabled)
                  }}
                  title={configured.config.enabled ? '关闭串口' : '打开串口'}
                  type="button"
                >
                  {configured.config.enabled ? <CircleStop size={14} /> : <Power size={14} />}
                </button>
              )}
            </div>
          )
        })}
      </div>
      <button className="rail-settings" type="button" onClick={onSettings}>
        <Settings2 size={16} />
        配置设备
        <kbd>⌘,</kbd>
      </button>
    </aside>
  )
}
