import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { createOfflineSnapshot } from '../../shared/offline-snapshot'
import { createQaSnapshot } from '../../shared/qa-fixture'
import { WindowBar } from '../App'
import { resolveBackendControl } from '../lib/backend-control'
import {
  ModelEditor,
  ModelFamilyEditor,
  resolveModelProfilePolicy,
  selectModelFamily,
  selectModelProfile,
  SettingsPage,
  validateModelFamilyCatalog,
  validateModelProfileCatalog
} from './SettingsPage'
import { PortRail } from './PortRail'
import { TerminalPane } from './TerminalPane'

const action = async (): Promise<void> => undefined
const saveAction = async (): Promise<boolean> => true

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
        modelFamilies={snapshot.modelFamilies}
        preferences={snapshot.preferences}
        initialPort="COM6"
        onBack={vi.fn()}
        onSaveSerial={saveAction}
        onSaveModels={saveAction}
        onSaveModelFamilies={saveAction}
        onSavePreferences={action}
      />
    )
    expect(markup).toContain('aria-label="自动启动本地后端"')
    expect(markup).toMatch(/aria-label="自动启动本地后端"[^>]*checked=""/)
    expect(markup).toContain('TL-AS7230-W 1.0')
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
        modelFamilies={snapshot.modelFamilies}
        preferences={snapshot.preferences}
        onBack={vi.fn()}
        onSaveSerial={saveAction}
        onSaveModels={saveAction}
        onSaveModelFamilies={saveAction}
        onSavePreferences={action}
      />
    )
    expect(windowMarkup).toContain('aria-label="启动后端"')
    expect(settingsMarkup).toContain('value="not a valid endpoint"')
    expect(settingsMarkup).toMatch(/aria-label="自动启动本地后端"(?![^>]*checked="")/)
  })

  it('treats existing profile and family names as stable IDs and protects bound entries', () => {
    expect(resolveModelProfilePolicy(false, [])).toEqual({ nameReadOnly: false, deleteDisabled: false })
    expect(resolveModelProfilePolicy(true, [])).toEqual({ nameReadOnly: true, deleteDisabled: false })
    expect(resolveModelProfilePolicy(true, ['COM6'])).toEqual({ nameReadOnly: true, deleteDisabled: true })

    const snapshot = createQaSnapshot()
    const profileMarkup = renderToStaticMarkup(
      <ModelEditor profiles={snapshot.modelProfiles} configuredPorts={snapshot.configuredPorts} onSave={action} />
    )
    expect(profileMarkup).toContain('title="已有 Profile 名称是稳定标识"')
    expect(profileMarkup).toContain('readOnly=""')
    expect(profileMarkup).toMatch(/aria-label="删除机型 Profile TL-AS7230 Shell"[^>]*disabled=""/)
    expect(profileMarkup).toContain('如需删除，请先在“串口配置”改绑或解绑。')

    const familyMarkup = renderToStaticMarkup(
      <ModelFamilyEditor families={snapshot.modelFamilies} configuredPorts={snapshot.configuredPorts} onSave={action} />
    )
    expect(familyMarkup).toMatch(/aria-label="删除一级机型 TL-AS7230"[^>]*disabled=""/)
    expect(familyMarkup).toMatch(/aria-label="二级具体型号 1"[^>]*readOnly=""/)
    expect(familyMarkup).toMatch(/aria-label="删除具体型号 TL-AS7230-W 1.0"[^>]*disabled=""/)
    expect(familyMarkup).toContain('已绑定 COM6')
  })

  it('rejects silently removing a concrete model that is still bound to a port', () => {
    const snapshot = createQaSnapshot()
    const removed = snapshot.modelFamilies.map((family) => ({ ...family, model_names: ['TL-AS7230-F4GE 2.0'] }))
    expect(validateModelFamilyCatalog(removed, snapshot.configuredPorts)).toContain('端口 COM6 正在使用机型“TL-AS7230-W 1.0”')
    expect(validateModelFamilyCatalog(snapshot.modelFamilies, snapshot.configuredPorts)).toBeUndefined()
    expect(validateModelProfileCatalog(snapshot.modelProfiles, snapshot.configuredPorts)).toBeUndefined()
  })

  it('keeps behavior profile and two-level model identity independent', () => {
    const snapshot = createQaSnapshot()
    const draft = {
      port: 'COM6', enabled: true, transportProfile: snapshot.transportProfiles[0],
      modelProfile: 'TL-AS7230 Shell', modelFamily: 'TL-AS7230', modelName: 'TL-AS7230-W 1.0'
    }
    expect(selectModelProfile(draft, null, snapshot.modelProfiles)).toMatchObject({
      modelProfile: null, modelFamily: 'TL-AS7230', modelName: 'TL-AS7230-W 1.0'
    })
    expect(selectModelFamily(draft, 'TL-AS7230', snapshot.modelFamilies).modelName).toBe('TL-AS7230-W 1.0')
    expect(selectModelFamily(draft, null, snapshot.modelFamilies)).toMatchObject({ modelFamily: null, modelName: null })
  })

  it('uses the exact concrete model name in the port rail and terminal title', () => {
    const snapshot = createQaSnapshot()
    const rail = renderToStaticMarkup(
      <PortRail
        configuredPorts={snapshot.configuredPorts}
        availablePorts={snapshot.availablePorts}
        selectedPort="COM6"
        onSelect={vi.fn()}
        onToggle={vi.fn()}
        onSettings={vi.fn()}
      />
    )
    const terminal = renderToStaticMarkup(
      <TerminalPane
        configuredPort={snapshot.configuredPorts[0]}
        events={[]}
        onClearCommand={vi.fn()}
      />
    )
    expect(rail).toContain('TL-AS7230-W 1.0')
    expect(terminal).toContain('TL-AS7230-W 1.0')
    expect(terminal).not.toContain('TL-AS7230 Shell</strong>')

    const withoutIdentity = {
      ...snapshot.configuredPorts[0],
      config: {
        ...snapshot.configuredPorts[0].config,
        model_family: null,
        model_name: null
      }
    }
    const unboundTerminal = renderToStaticMarkup(
      <TerminalPane configuredPort={withoutIdentity} events={[]} onClearCommand={vi.fn()} />
    )
    expect(unboundTerminal).toContain('未配置机型')
    expect(unboundTerminal).not.toContain('TL-AS7230 Shell</strong>')
  })
})
