import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { createOfflineSnapshot } from '../../shared/offline-snapshot'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { WindowBar } from '../App'
import { resolveBackendControl } from '../lib/backend-control'
import { ModelEditor, resolveModelProfilePolicy, SettingsPage } from './SettingsPage'

const action = async (): Promise<void> => undefined

describe('desktop backend controls', () => {
  it('exposes a reachable stop action only for an App-owned backend', () => {
    const owned = createQaSnapshot()
    const ownedMarkup = renderToStaticMarkup(
      <WindowBar snapshot={owned} page="console" onPage={vi.fn()} onStartBackend={action} onStopBackend={action} onSavePreferences={action} />
    )
    expect(ownedMarkup).toContain('aria-label="停止后端"')
    expect(ownedMarkup).not.toContain('aria-label="停止后端" disabled=""')

    const external = { ...owned, service: { owned: false, status: 'stopped' as const } }
    const externalMarkup = renderToStaticMarkup(
      <WindowBar snapshot={external} page="console" onPage={vi.fn()} onStartBackend={action} onStopBackend={action} onSavePreferences={action} />
    )
    expect(externalMarkup).toContain('aria-label="外部后端"')
    expect(externalMarkup).toContain('disabled=""')
  })

  it('renders the persisted auto-start preference as an accessible switch', () => {
    const snapshot = createQaSnapshot()
    const markup = renderToStaticMarkup(
      <SettingsPage
        configuredPorts={snapshot.configuredPorts}
        availablePorts={snapshot.availablePorts}
        transportProfiles={snapshot.transportProfiles}
        modelProfiles={snapshot.modelProfiles}
        preferences={snapshot.preferences}
        initialPort="COM6"
        onBack={vi.fn()}
        onSaveSerial={action}
        onSaveModels={action}
        onSavePreferences={action}
      />
    )
    expect(markup).toContain('aria-label="自动启动本地后端"')
    expect(markup).toMatch(/aria-label="自动启动本地后端"[^>]*checked=""/)
  })

  it('keeps configuration and manual backend start reachable after an invalid endpoint with auto-start disabled', () => {
    const snapshot = createOfflineSnapshot(
      { endpoint: 'not a valid endpoint', autoStartLocal: false, theme: 'dark' },
      { owned: false, status: 'stopped' },
      'Invalid URL'
    )
    expect(resolveBackendControl(snapshot.service, snapshot.connection).kind).toBe('start')

    const windowMarkup = renderToStaticMarkup(
      <WindowBar snapshot={snapshot} page="settings" onPage={vi.fn()} onStartBackend={action} onStopBackend={action} onSavePreferences={action} />
    )
    const settingsMarkup = renderToStaticMarkup(
      <SettingsPage
        configuredPorts={snapshot.configuredPorts}
        availablePorts={snapshot.availablePorts}
        transportProfiles={snapshot.transportProfiles}
        modelProfiles={snapshot.modelProfiles}
        preferences={snapshot.preferences}
        onBack={vi.fn()}
        onSaveSerial={action}
        onSaveModels={action}
        onSavePreferences={action}
      />
    )
    expect(windowMarkup).toContain('aria-label="启动后端"')
    expect(settingsMarkup).toContain('value="not a valid endpoint"')
    expect(settingsMarkup).toMatch(/aria-label="自动启动本地后端"(?![^>]*checked="")/)
  })

  it('treats existing model names as stable IDs and protects bound profiles from deletion', () => {
    expect(resolveModelProfilePolicy(false, [])).toEqual({ nameReadOnly: false, deleteDisabled: false })
    expect(resolveModelProfilePolicy(true, [])).toEqual({ nameReadOnly: true, deleteDisabled: false })
    expect(resolveModelProfilePolicy(true, ['COM6'])).toEqual({ nameReadOnly: true, deleteDisabled: true })

    const snapshot = createQaSnapshot()
    const markup = renderToStaticMarkup(
      <ModelEditor profiles={snapshot.modelProfiles} configuredPorts={snapshot.configuredPorts} onSave={action} />
    )
    expect(markup).toContain('title="已有 Profile 名称是稳定标识"')
    expect(markup).toContain('readOnly=""')
    expect(markup).toMatch(/aria-label="删除机型 Profile TL-AS7230 1.0"[^>]*disabled=""/)
    expect(markup).toContain('如需删除，请先在“串口配置”改绑或解绑。')
  })
})
