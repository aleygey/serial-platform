import type { ConnectionState, ServiceState } from '../../shared/contracts'

export interface BackendControl {
  kind: 'start' | 'stop' | 'external' | 'busy'
  label: string
  disabled: boolean
  title: string
}

export function resolveBackendControl(service: ServiceState, connection: ConnectionState): BackendControl {
  if (service.owned && service.status === 'starting') {
    return { kind: 'busy', label: '启动中', disabled: true, title: '正在启动本地后端' }
  }
  if (service.owned) {
    return { kind: 'stop', label: '停止后端', disabled: false, title: '停止由 App 启动的本地后端' }
  }
  if (connection === 'connected') {
    return { kind: 'external', label: '外部后端', disabled: true, title: '该后端由其他进程管理，App 不会停止它' }
  }
  if (connection === 'starting') {
    return { kind: 'busy', label: '连接中', disabled: true, title: '正在连接后端' }
  }
  return { kind: 'start', label: '启动后端', disabled: false, title: '启动并连接本地 Serial Platform 后端' }
}
